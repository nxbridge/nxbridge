import { execFile } from 'node:child_process';
import { realpathSync } from 'node:fs';
import path from 'node:path';
import { promisify } from 'node:util';
import { BridgeError } from '../shared/errors.js';
import { readJson, sameDirectory } from '../shared/files.js';
import { remainingTime } from '../shared/time-budget.js';
import { cacheBelongsTo, installedCacheDirectories, managedMarker, ownsMarketplace } from '../install/installation.js';
import type { OwnedProcessRecord } from './owned-processes.js';
import { ownedProcessState } from './process-stop.js';
import { codexArguments, processAncestors, processDescendants, processIdentity, type ProcessIdentity } from './process.js';
import { psEnvironment } from './posix/posix-process.js';

const execute = promisify(execFile);
const generation = (identity: Pick<ProcessIdentity, 'pid' | 'startedAt'>) => identity.pid + ':' + identity.startedAt;
const ended = (identity: ProcessIdentity) => ['Z', 'X'].includes(identity.state ?? '');
const unavailable = () => new BridgeError('runtime_workload_unverified', '无法核验原运行时内所有可能被回收的进程，未停止原会话。');

async function posixChildren(pids: number[], deadline: number): Promise<ProcessIdentity[]> {
  let stdout: string;
  try {
    ({ stdout } = await execute(process.platform === 'darwin' ? '/usr/bin/pgrep' : 'pgrep', ['-P', pids.join(',')], {
      encoding: 'utf8', env: psEnvironment(), timeout: remainingTime(deadline, 2000), maxBuffer: 65536,
    }));
  } catch (error) {
    const result = error as { code?: number; killed?: boolean; stdout?: string };
    if (result.code === 1 && !result.killed && !result.stdout?.trim()) return [];
    throw error;
  }
  const children = stdout.trim().split(/\s+/).map(Number);
  if (children.some(pid => !Number.isSafeInteger(pid) || pid <= 0) || children.length > 128) throw unavailable();
  const identities = await Promise.all(children.map(async pid => {
    try { return await processIdentity(pid); }
    catch (error) { if ((error as BridgeError).code === 'process_missing') return undefined; throw error; }
  }));
  return identities.flatMap(child => child && !ended(child) && pids.includes(child.parentPid) ? [child] : []);
}

export interface RuntimeWorkload {
  native: ProcessIdentity;
  owners: OwnedProcessRecord[];
  infrastructure?: (candidate: ProcessIdentity) => boolean | Promise<boolean>;
}

                                   
export async function assertRuntimeWorkloadIdle({ native, owners, infrastructure }: RuntimeWorkload): Promise<void> {
  const containers = new Map<string, OwnedProcessRecord>();
  for (const owner of owners) if (owner.windowsJob || owner.posixGroup) {
    const key = JSON.stringify([generation(owner.identity), owner.windowsJob, !!owner.posixGroup]), previous = containers.get(key);
    if (previous?.native && owner.native && generation(previous.native) !== generation(owner.native)) throw unavailable();
    if (!previous || !previous.native) containers.set(key, owner);
  }
  if (!owners.length || containers.size > 1 || owners.some(owner => owner.releasedToUser)) throw unavailable();
  const container = containers.values().next().value, structural = new Set([generation(native)]), roots = [native];
  if (!container && !owners.some(owner => generation(owner.identity) === generation(native))) throw unavailable();
  if (container) {
    const chain = await processAncestors(native.pid, 32);
    const leader = chain.findIndex(process => generation(process) === generation(container.identity));
    if (chain[0] && generation(chain[0]) === generation(native) && leader >= 0) {
      for (const process of chain.slice(0, leader + 1)) {
        if (ended(process)) throw unavailable();
        structural.add(generation(process));
        if (process.pid !== native.pid) roots.push(process);
      }
    } else if (container.posixGroup && container.native && generation(container.native) === generation(native)) {
                                            
      const current = await processIdentity(container.identity.pid);
      if (generation(current) !== generation(container.identity) || ended(current)) throw unavailable();
      structural.add(generation(current));roots.push(current);
    } else if (container.windowsJob) {
                                            
      if (await ownedProcessState(container.identity) !== 'dead') throw unavailable();
    } else throw unavailable();
  }
  const scope = new Map(roots.map(process => [generation(process), process]));
  const collect = (processes: ProcessIdentity[]) => {
    for (const process of processes) if (!ended(process)) scope.set(generation(process), process);
  };
  if (container) {
    try {
      const members = await processDescendants(container.identity, container);
      if (generation(native) !== generation(container.identity) && !members.some(member => generation(member) === generation(native))) throw unavailable();
      collect(members);
    } catch {
      throw unavailable();
    }
  }
  for (const owner of owners) {
    if (structural.has(generation(owner.identity))) continue;
    const state = await ownedProcessState(owner.identity);
    if (state === 'unknown') throw unavailable();
    if (state === 'alive') {
      const current = await processIdentity(owner.identity.pid);
      if (generation(current) !== generation(owner.identity)) throw unavailable();
      collect([current]);
    }
  }
  const inspect = async (processes: ProcessIdentity[]) => {
    for (const process of processes) {
      if (!structural.has(generation(process)) && !await infrastructure?.(process))
        throw new BridgeError('runtime_workload_active', `原运行时还有无法证明可安全回收的进程（PID ${process.pid}），等待它结束后再试。`);
    }
  };
  await inspect([...scope.values()]);
  if (process.platform !== 'win32') {
    const deadline = performance.now() + 5000, seen = new Set<number>();
    let frontier = [...scope.values()].map(process => process.pid);
    while (frontier.length) {
      for (const pid of frontier) seen.add(pid);
      if (seen.size > 128) throw unavailable();
      const children = await posixChildren(frontier, deadline);
      await inspect(children.filter(child => !scope.has(generation(child))));
      frontier = children.flatMap(child => seen.has(child.pid) ? [] : [child.pid]);
    }
  }
  if (await ownedProcessState(native) !== 'alive') throw unavailable();
}

export interface McpCommand { command: string; args?: readonly string[] }

export function mcpCommands(servers: unknown): McpCommand[] {
  return Object.values(servers && typeof servers === 'object' ? servers : {}).flatMap(server =>
    server?.enabled !== false && typeof server?.command === 'string'
    && (server.args === undefined || Array.isArray(server.args) && server.args.every((arg: unknown) => typeof arg === 'string'))
      ? [{ command: server.command, args: server.args as string[] | undefined }] : []);
}

export function nxbridgeMcpCommands(home: string, host: 'claude' | 'codex', configHome: string): McpCommand[] {
  const marketplace = path.join(home, 'marketplace'), roots = installedCacheDirectories(configHome, host).filter(directory => cacheBelongsTo(directory, host, home));
  if (ownsMarketplace(marketplace, home)) roots.push(path.join(marketplace, 'plugins', host));
  return roots.flatMap(root => {
    try {
      const configured = readJson<{mcpServers?: Record<string,unknown>}>(path.join(root, '.mcp.json'));
      return mcpCommands(configured.mcpServers).flatMap(command => {
        const args = command.args?.map(arg => arg.replace(/\$\{(?:CLAUDE|CODEX)_PLUGIN_ROOT\}/g, root));
        return args?.[1] === 'mcp' && args[2] === '--host' && args[3] === host ? [{ ...command, args }] : [];
      });
    } catch { return []; }
  });
}

export async function nxbridgeRuntimeInfrastructure(home: string, candidate: ProcessIdentity): Promise<boolean> {
  const filename = path.join(home, 'run', `runtime-${candidate.pid}-${candidate.startedAt.replace(/[^\w-]/g, '_')}.json`);
  let record: {owner?:string;pid?:number;generation?:string;entry?:string};
  try { record = readJson<typeof record>(filename); }
  catch { return false; }
  if (record.owner !== managedMarker(home) || record.pid !== candidate.pid || record.generation !== candidate.startedAt || !record.entry) return false;
  const argv = await codexArguments(candidate);
  const index = argv.findIndex(arg => path.isAbsolute(arg) && (sameDirectory(arg, record.entry!)
    || path.basename(arg) === 'mcp-entry.mjs' && sameDirectory(path.dirname(arg), path.dirname(record.entry!))));
  return index > 0 && argv[index + 1] === 'mcp' && argv[index + 2] === '--host' && ['codex','claude'].includes(argv[index + 3] ?? '');
}

                                         
export async function configuredMcpInfrastructure(native: ProcessIdentity, commands: readonly McpCommand[], candidate: ProcessIdentity): Promise<boolean> {
  if (candidate.parentPid !== native.pid || !candidate.image || !commands.length) return false;
  const argv = await codexArguments(candidate);
  if (!argv.length) return false;
  const matchesImage = (command: string) => {
    if (!path.isAbsolute(command)) {
      if (path.basename(command) !== command) return false;
      const basename = (value: string) => process.platform === 'win32' ? path.basename(value).toLowerCase().replace(/\.exe$/, '') : path.basename(value);
      return basename(command) === basename(argv[0]!) && basename(candidate.image!) === basename(command);
    }
    try { return sameDirectory(realpathSync(command), realpathSync(candidate.image!)); }
    catch { return false; }
  };
  return commands.some(({ command, args = [] }) => matchesImage(command) && args.length === argv.length - 1 && args.every((arg, index) => arg === argv[index + 1]));
}
