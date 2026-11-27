import { createHash } from 'node:crypto';
import { realpathSync } from 'node:fs';
import path from 'node:path';
import { isCodexRecycleTransientObservation, recycleEvidenceReadFailure, type CodexRecycleEvidence, type CodexRecycleGeneration, type CodexRecycleLaunch, type CodexRecycleTrigger, type CodexRecycleWork } from '../../broker/recovery/codex-recycle.js';
import { removeOwnedSocket } from '../../ipc/home.js';
import type { RpcClient } from '../../ipc/rpc.js';
import { currentLinuxBootId, ownedProcesses, ownedRecordState, retireDeadOwnedProcess } from '../../process/owned-processes.js';
import { ownedProcessState, stopOwnedProcess } from '../../process/process-stop.js';
import { liveProcess, nativeConfigHome, processIdentity } from '../../process/process.js';
import { assertRuntimeWorkloadIdle, configuredMcpInfrastructure, mcpCommands, nxbridgeMcpCommands, nxbridgeRuntimeInfrastructure, ownedWindowsConsoleInfrastructure } from '../../process/runtime-workload.js';
import { BridgeError } from '../../shared/errors.js';
import { sameDirectory } from '../../shared/files.js';
import type { Endpoint, Peer } from '../../types.js';
import { managedRuntimeNativeState, managedRuntimeState, removeManagedRuntime, runtimeFor, type ManagedRuntime } from '../managed-runtimes.js';
import type { CodexThread } from './adapter.js';
import { catalogCodexThreads } from './codex-evidence.js';
import { codexServerVersion } from './codex-client-identity.js';
import { verifyCodexNativeGeneration, verifyDedicatedCodex } from './codex-isolation.js';
import { codexParent, codexSubagent } from './codex-lineage.js';
import { loadedCodexThreads } from './codex-restart.js';
import { prepareRestartProfile } from './codex-restart-profile.js';
import { timeCodexRestart } from './codex-restart-timing.js';
import { builtinCodexPermission, normalizedPermission, planCodexResume, readSessionProfile, verifyRestartProfileCurrent, withPersistedCodexPermission, type SessionProfile } from './codex-session-profile.js';

export type CodexRecycleObservation =
  | { evidence:Extract<CodexRecycleEvidence,{kind:'available'}>; runtime:ManagedRuntime; profile:SessionProfile }
  | { evidence:Exclude<CodexRecycleEvidence,{kind:'available'}>; runtime?:undefined; profile?:undefined };
interface CollectOptions {
  home:string; peer:Peer; endpoint:Endpoint; trigger:CodexRecycleTrigger;
  connect:(endpoint:string)=>Promise<RpcClient>;
  preflight:(runtime:ManagedRuntime,profile:SessionProfile,args:Record<string,unknown>,rpc:RpcClient)=>Promise<void>;
  probeWork?:(rpc:RpcClient,id:string)=>Promise<'idle'|'pending'|'unknown'>;
  hasHeldInput?:()=>boolean;
}
const authorityError=()=>new BridgeError('codex_recycle_owner_unverified','没有确认该专用后台仍由 nxbridge 独占，已保留原会话。');
const lineageError=()=>new BridgeError('codex_recycle_lineage_unverified','没能从原生全局目录核对完整的会话父链，已保留原会话。');
const profileError=()=>new BridgeError('owned_profile_unverified','未能核对原后台 Codex 的完整设置，已保留原会话和消息。');
const terminal=new Set(['completed','failed','interrupted']);

export function codexRecycleRuntimeGeneration(runtime:ManagedRuntime):CodexRecycleGeneration {
  if(!runtime.pid||!runtime.procStart)throw authorityError();
  return {runtimeId:runtime.id,endpoint:runtime.endpoint,pid:runtime.pid,procStart:runtime.procStart,bootId:runtime.bootId};
}

function owners(home:string,old:CodexRecycleGeneration) {
  const {records,notes}=ownedProcesses(home);
  const related=records.filter(record=>record.proof.launchId===old.runtimeId
    ||record.identity.pid===old.pid&&record.identity.startedAt===old.procStart
    ||record.native?.pid===old.pid&&record.native.startedAt===old.procStart);
  if(notes.length||related.some(record=>record.releasedToUser||record.proof.launchId!==old.runtimeId))throw authorityError();
  return related;
}

function exactOld(home:string,old:CodexRecycleGeneration) {
  const record=runtimeFor(home,{id:old.runtimeId});
  if(record){
    const runtime=record.value;
    if(!runtime.dedicated||runtime.kind!=='owned'||runtime.endpoint!==old.endpoint||runtime.pid!==old.pid
      ||runtime.procStart!==old.procStart||runtime.bootId!==old.bootId)throw authorityError();
  }
  return record;
}

                                                
export async function readCodexRecycleScope(rpc:RpcClient,rootId:string,options:{
  registry?:Set<string>;ownedRootRollout?:SessionProfile;
}={}):Promise<{loaded:Set<string>;threads:Map<string,CodexThread>;originalRootOmitted:boolean}> {
  const loaded=options.registry??await loadedCodexThreads(rpc);
  let complete=false;
  const threads=await catalogCodexThreads(rpc,new Set([...loaded,rootId]),{independent:true,ancestorThreadId:rootId,onComplete:()=>{complete=true;}});
  let root=threads.get(rootId),originalRootOmitted=false;
  const rollout=options.ownedRootRollout,meta=rollout?.meta;
                                                                 
  if(!root&&loaded.has(rootId)&&complete&&rollout?.verified&&rollout.complete&&rollout.file&&rollout.sessionId===rootId
    &&meta?.id===rootId&&meta.session_id===rootId&&!meta.parent_thread_id&&!meta.attached_thread
    &&typeof meta.cwd==='string'&&(!meta.thread_source||meta.thread_source==='user')&&['cli','vscode','exec','mcp','appServer','app_server'].includes(meta.source)){
    root={id:rootId,cwd:meta.cwd,path:rollout.file};threads.set(rootId,root);originalRootOmitted=true;
  }
  if(!root||codexSubagent(root)||[...loaded].some(id=>!threads.has(id)))throw lineageError();
  for(const id of loaded){
    if(id===rootId)continue;
    let current=threads.get(id)!;
    const seen=new Set<string>();
    while(current.id!==rootId){
      if(seen.size>=64||seen.has(current.id))throw lineageError();
      seen.add(current.id);
      if(!codexSubagent(current))throw new BridgeError('shared_runtime','同一后台还有其他根会话，已保留原会话。');
      const parent=codexParent(current);
      if(!parent)throw lineageError();
      let row=threads.get(parent);
      if(!row){
        row=(await catalogCodexThreads(rpc,new Set([parent]),{independent:true,ancestorThreadId:rootId})).get(parent);
        if(!row)throw lineageError();
        threads.set(parent,row);
      }
      current=row;
    }
  }
  return {loaded,threads,originalRootOmitted};
}

interface ThreadWork { active?:string; recent?:number; unknown?:string }
interface TurnSummary {id?:string;status?:string}
async function latestTurn(rpc:RpcClient,id:string):Promise<TurnSummary|null|undefined>{
  try{
    const {data}=await timeCodexRestart('native_turn_summary',()=>rpc.request('thread/turns/list',{threadId:id,limit:1,sortDirection:'desc',itemsView:'summary'},{timeout:3000}));
    return Array.isArray(data)?data[0]??null:undefined;
  }catch{return undefined;}
}

export async function readCodexRecycleThreadWork(rpc:RpcClient,thread:CodexThread,profile:SessionProfile,loaded:boolean,originalRootOmitted=false,
  observed?:{latest:TurnSummary|null|undefined}):Promise<ThreadWork> {
  if(thread.status?.activeFlags?.some(flag=>/approval|input|question/i.test(flag)))
    throw new BridgeError('codex_recycle_native_wait','原线程正在等待原生审批或输入，已保留原会话。');
  const latest=observed?observed.latest:await latestTurn(rpc,thread.id),history=profile.turn;
  if(latest===undefined)return {unknown:'turns_unavailable'};
  if(latest?.status==='inProgress'||history?.state==='running'){
    const active=latest?.status==='inProgress'?latest.id:history?.id;
    const recent=history?.state==='running'&&history.id===active?
      Math.max(history.at,profile.rolloutWork?.lastWorkAt??0):undefined;
    return typeof active==='string'&&active?{active,recent}:{unknown:'turn_identity_unavailable'};
  }
  if(!latest?.status||!terminal.has(latest.status)||!profile.complete||!profile.verified||history?.id!==latest.id){
    return {unknown:'terminal_history_unavailable'};
  }
  if(profile.rolloutWork&&profile.rolloutWork.lastWorkOffset>profile.rolloutWork.turnOffset){
    return {unknown:'rollout_work_after_turn',recent:profile.rolloutWork.lastWorkAt};
  }
  if(loaded&&!originalRootOmitted&&thread.status?.type!=='idle')return {unknown:'thread_state_unavailable'};
  return {};
}

function settingsKey(args:Record<string,unknown>,profile:SessionProfile,runtime:ManagedRuntime):string {
                                         
  return createHash('sha256').update(JSON.stringify([args,normalizedPermission(profile.settings.permissionProfile),
    profile.settings.collaborationMode,profile.settings.disabledPluginIds,
    runtime.configHome,runtime.executable,runtime.version,runtime.profileId,runtime.launchMethod,runtime.launchCommandKey])).digest('hex');
}

function originalRollout(runtime:ManagedRuntime,file:string|undefined):string|undefined {
  if(!file)return;
  const home=realpathSync.native(path.resolve(runtime.configHome!)),named=realpathSync.native(path.resolve(file)),relative=path.relative(home,named);
  if(path.isAbsolute(relative)||relative.startsWith('..'+path.sep)||!['sessions','archived_sessions'].includes(relative.split(path.sep)[0]!))throw profileError();
  return named;
}

export function planExactCodexRecycle(profile:SessionProfile,cwd:string):Record<string,unknown> {
  const {settings,live}=profile;
  if(typeof settings.cwd!=='string'||!sameDirectory(settings.cwd,cwd)||typeof settings.modelProvider!=='string'
    ||!settings.modelProvider||!Array.isArray(settings.runtimeWorkspaceRoots)||!settings.runtimeWorkspaceRoots.every((root:unknown)=>typeof root==='string')
    ||!settings.permissionProfile)throw profileError();
  const {args}=planCodexResume(profile.sessionId,profile,cwd,{exact:true});
                                      
  if(profile.activePermission&&profile.activePermission.id!==args.permissions)throw profileError();
  if(args.permissions&&(!live?.activePermissionProfile?.id||!builtinCodexPermission(String(args.permissions))
    &&!profile.livePermissionConfig?.['permissions.'+String(args.permissions)]))throw profileError();
  if(live?.activePermissionProfile?.id!==undefined&&live.activePermissionProfile.id!==args.permissions)throw profileError();
  return args;
}

export async function collectCodexRecycleEvidence(options:CollectOptions):Promise<CodexRecycleObservation> {
  const {home,peer,endpoint,trigger}=options,id=peer.sessionId;
  let eligible=trigger!=='loaded_unleased',rpc:RpcClient|undefined,loaded: Set<string>|undefined;
  let evidenceSource='catalog';
  let recentTurns:Map<string,{latest:TurnSummary|null|undefined}>|undefined;
  try{
    if(!eligible){
      if(!id)return {evidence:{kind:'natural'}};
      rpc=await options.connect(endpoint.address);
      loaded=await loadedCodexThreads(rpc);
      if(!loaded.has(id))return {evidence:{kind:'natural'}};
      recentTurns=new Map(await Promise.all([...loaded].map(async thread=>[thread,{latest:await latestTurn(rpc!,thread)}] as const)));
      const summaries=[...recentTurns.values()].map(row=>row.latest);
      eligible=summaries.some(turn=>turn?.status==='inProgress'&&typeof turn.id==='string'&&!!turn.id)&&(options.hasHeldInput?.()??true);
      if(!eligible)return {evidence:{kind:'natural',...(summaries.every(turn=>turn?.status&&terminal.has(turn.status))?{idleLoaded:true}:{})}};
    }
    evidenceSource='托管运行时归属记录';
    const runtime=endpoint.runtimeId?runtimeFor(home,{id:endpoint.runtimeId})?.value:undefined,nativeEndpoint=endpoint.native??endpoint;
    if(!id||peer.host!=='codex'||peer.owner!=='nxbridge'||!['root','fork'].includes(peer.kind)||endpoint.peerId!==peer.id||endpoint.mode!=='app-server'||endpoint.daemon
      ||endpoint.processKind==='foreground'||endpoint.nativeSession?.interactive
      ||!runtime||runtime.kind!=='owned'||runtime.state!=='ready'||!runtime.dedicated||runtime.peerId!==peer.id||runtime.sessionId!==id
      ||runtime.endpoint!==endpoint.address||runtime.pid!==nativeEndpoint.pid||runtime.procStart!==nativeEndpoint.procStart
      ||!runtime.configHome||!runtime.executable||!runtime.launchMethod||endpoint.configHome&&!sameDirectory(runtime.configHome,endpoint.configHome))
      throw authorityError();
    const old=codexRecycleRuntimeGeneration(runtime),records=owners(home,old);
    if(!records.some(record=>record.role==='codex-runtime'&&record.peerId===peer.id&&record.configHome&&sameDirectory(record.configHome,runtime.configHome!)
      &&(record.native?.pid===old.pid&&record.native.startedAt===old.procStart||record.identity.pid===old.pid&&record.identity.startedAt===old.procStart)))
      throw authorityError();
    evidenceSource='process';
    const native=await verifyCodexNativeGeneration(runtime);
    await verifyDedicatedCodex(runtime.endpoint,native);
    const observedHome=await nativeConfigHome(native,'codex');
    if(observedHome&&!sameDirectory(observedHome,runtime.configHome))throw new BridgeError('original_home_changed','当前原生后台与记录的原配置目录不一致。');
    evidenceSource='catalog';
    rpc??=await options.connect(runtime.endpoint);
    const connection=rpc;
    const version=codexServerVersion(rpc);
    if(!version||runtime.version&&!runtime.version.includes(version))throw new BridgeError('profile_version_unsupported','原原生代次未能确认原安装版本。');
    runtime.version=version;
    loaded??=await loadedCodexThreads(rpc);
    const registry=loaded;
    if(!loaded.has(id)&&trigger!=='cold_exhausted')return {evidence:{kind:'natural'}};
    evidenceSource='rollout';
    const savedFile=originalRollout(runtime,endpoint.nativeSession?.transcriptPath),savedRollout=savedFile?await readSessionProfile(savedFile,id,{trackWork:true}):undefined;
                                   
    if(savedRollout?.pendingTail)await verifyRestartProfileCurrent(savedRollout,{current:savedRollout});
    evidenceSource='catalog';
    const {threads,originalRootOmitted}=await readCodexRecycleScope(rpc,id,{registry:loaded,ownedRootRollout:savedRollout});
    evidenceSource='rollout';
    const root=threads.get(id)!,file=originalRollout(runtime,root.path)??savedFile;
    if(!file||savedFile&&!sameDirectory(savedFile,file)||!sameDirectory(root.cwd,peer.cwd))throw profileError();
    const persisted=savedRollout??await readSessionProfile(file,id,{trackWork:true});
    evidenceSource='profile';
    const profile=withPersistedCodexPermission(persisted);
    if(!profile.file||!sameDirectory(profile.file,file)||profile.sessionId!==id)throw profileError();
    await verifyRestartProfileCurrent(profile,{exact:true,current:persisted});
    const args=planExactCodexRecycle(profile,peer.cwd);
    await options.preflight(runtime,profile,args,rpc);

    evidenceSource='history';
    const work:CodexRecycleWork={activeTurnIds:[],progress:'idle',roles:[],unresolved:[]};
    let lastProgressAt:number|undefined;
    const roots=[...threads.values()];
    const history=await Promise.all(roots.map(async thread=>readCodexRecycleThreadWork(connection,thread,thread.id===id?persisted:
      await readSessionProfile(thread.path,thread.id,{trackWork:true}),registry.has(thread.id),thread.id===id&&originalRootOmitted,recentTurns?.get(thread.id))));
    for(const item of history){
      if(item.active)work.activeTurnIds.push(item.active);
      if(item.unknown)work.unresolved.push(item.unknown);
      if(item.recent!==undefined)lastProgressAt=Math.max(lastProgressAt??0,item.recent);
    }
    if(options.probeWork){
      const pending=await Promise.all([...loaded].filter(threadId=>trigger!=='native_thread_wedged'||threadId!==id)
        .map(threadId=>options.probeWork!(connection,threadId).catch(()=>'unknown' as const)));
      if(pending.includes('pending'))work.unresolved.push('native_work_pending');
      if(pending.includes('unknown'))work.unresolved.push('native_work_unavailable');
    }
    try{
      const config=await rpc.request('config/read',{cwd:peer.cwd},{timeout:2000}).catch(()=>undefined);
      const commands=[...mcpCommands(config?.config?.mcp_servers),...nxbridgeMcpCommands(home,'codex',runtime.configHome)];
      await assertRuntimeWorkloadIdle({native,owners:records,infrastructure:async (candidate,scope)=>{
        if(ownedWindowsConsoleInfrastructure(candidate,scope)||await nxbridgeRuntimeInfrastructure(home,candidate)||await configuredMcpInfrastructure(native,commands,candidate))return true;
        const image=candidate.image?path.basename(candidate.image):candidate.name;
        const role=/^[\w.+-]{1,64}$/.test(image)?image.toLowerCase():'unknown';
        work.roles.push(role);
        (work.unattested??=[]).push({pid:candidate.pid,procStart:candidate.startedAt,role,parentPid:candidate.parentPid,image:candidate.image});
        return true;
      }});
    }catch{work.unresolved.push('process_scope_unavailable');}
    evidenceSource='catalog';
    const refreshed=await loadedCodexThreads(rpc);
    if(refreshed.size!==registry.size||[...refreshed].some(threadId=>!registry.has(threadId)))throw lineageError();
    work.lastProgressAt=lastProgressAt;
    if(work.activeTurnIds.length||work.roles.length||work.unresolved.length)work.progress='unknown';
    return {runtime,profile,evidence:{kind:'available',old,profile:{sourceId:runtime.id,sessionId:id,file,settingsKey:settingsKey(args,profile,runtime),lineage:originalRootOmitted?'saved_rollout':'native_catalog',
      launch:{executable:runtime.executable,version,configHome:runtime.configHome,method:runtime.launchMethod,profileId:runtime.profileId,commandKey:runtime.launchCommandKey}},work}};
  }catch(error){
    if(!eligible&&!(error instanceof BridgeError&&isCodexRecycleTransientObservation(error.code)))return {evidence:{kind:'natural'}};
    return {evidence:recycleEvidenceReadFailure(evidenceSource,error)};
  }finally{rpc?.close();}
}

export function persistCodexRecycleProfile(home:string,observation:Extract<CodexRecycleObservation,{runtime:ManagedRuntime}>):void {
  prepareRestartProfile(home,observation.runtime,observation.profile,{recycle:true})();
}

export async function inspectCodexRecycleOld(home:string,old:CodexRecycleGeneration):Promise<'alive'|'dead'|'unknown'> {
  const record=exactOld(home,old);
  const boot=currentLinuxBootId();
  if(old.bootId&&boot&&old.bootId!==boot||!liveProcess(old.pid))return 'dead';
  if(record&&await managedRuntimeNativeState(home,record.value)==='alive')return 'alive';
                                           
  try{
    const current=await processIdentity(old.pid);
    if(current.startedAt!==old.procStart)return 'dead';
  }catch(error){if(!(error instanceof BridgeError)||error.code!=='process_missing')return 'unknown';}
  return ownedProcessState({pid:old.pid,startedAt:old.procStart});
}

export async function stopCodexRecycleOld(home:string,old:CodexRecycleGeneration,deadline:number,markSignal:()=>void,force=false):Promise<void> {
  const record=exactOld(home,old);
  const boot=currentLinuxBootId();
  if(old.bootId&&boot&&old.bootId!==boot)throw authorityError();
  if(!record||!owners(home,old).length)throw authorityError();
  const native=await verifyCodexNativeGeneration(record.value);
  await verifyDedicatedCodex(record.value.endpoint,native);
  await stopOwnedProcess(native,{deadline,force,beforeSignal:()=>{
    if(Date.now()>=deadline||!exactOld(home,old)||!owners(home,old).length)throw authorityError();
    markSignal();
  }});
}

                                               
export async function retireCodexRecycleOld(home:string,old:CodexRecycleGeneration,guard:()=>void,work?:CodexRecycleWork):Promise<void> {
  if(await inspectCodexRecycleOld(home,old)!=='dead')throw authorityError();
  const record=exactOld(home,old);
  const scope=owners(home,old);
  const [ownersState,unattested]=await Promise.all([Promise.all(scope.map(ownedRecordState)),
    Promise.all((work?.unattested??[]).map(holder=>ownedProcessState({pid:holder.pid,startedAt:holder.procStart})))]);
  const leftover=unattested.filter(state=>state==='alive').length;
  if(unattested.some(state=>state!=='dead')||ownersState.some(state=>state!=='dead')||record&&await managedRuntimeState(home,record.value)!=='dead')
    throw Object.assign(new BridgeError('codex_recycle_old_holder','原后台还有未核验结束的监督进程或后代，暂停恢复同一会话。'),{leftover});
  guard();
  for(const owner of scope){
    if(!owners(home,old).some(current=>current.id===owner.id))continue;
    guard();
    await retireDeadOwnedProcess(owner,'codex-recycle',()=>{
      guard();
      if(!owners(home,old).some(current=>current.id===owner.id&&current.windowsHolder?.pid===owner.windowsHolder?.pid
        &&current.windowsHolder?.startedAt===owner.windowsHolder?.startedAt))throw authorityError();
    });
  }
  if(record){guard();await removeOwnedSocket(home,record.value.endpoint);guard();removeManagedRuntime(record);}
}

export async function inspectCodexRecycleLaunch(home:string,peer:Peer,intent:CodexRecycleLaunch):Promise<
  {kind:'matched';fresh:CodexRecycleGeneration}|{kind:'unstarted'|'absent'|'unknown'}
> {
  const record=runtimeFor(home,{id:intent.runtimeId});
  if(!record){
    const {records,notes}=ownedProcesses(home);
    return {kind:notes.length||records.some(owner=>owner.proof.launchId===intent.runtimeId)?'unknown':'unstarted'};
  }
  const runtime=record.value;
  if(runtime.kind!=='owned'||runtime.peerId!==peer.id||runtime.sessionId!==peer.sessionId)return {kind:'unknown'};
  if(runtime.launchSettled&&await managedRuntimeState(home,runtime)==='dead')return {kind:'absent'};
  if(!runtime.dedicated||!runtime.pid||!runtime.procStart||await managedRuntimeNativeState(home,runtime)!=='alive')return {kind:'unknown'};
  const fresh=codexRecycleRuntimeGeneration(runtime),scope=owners(home,fresh);
  if(!scope.some(owner=>owner.peerId===peer.id&&(owner.native?.pid===fresh.pid&&owner.native.startedAt===fresh.procStart
    ||owner.identity.pid===fresh.pid&&owner.identity.startedAt===fresh.procStart)))return {kind:'unknown'};
  await verifyDedicatedCodex(runtime.endpoint,await verifyCodexNativeGeneration(runtime));
  return {kind:'matched',fresh};
}
