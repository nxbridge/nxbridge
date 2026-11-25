import test from 'node:test';
import assert from 'node:assert/strict';
import { writeFileSync } from 'node:fs';
import path from 'node:path';
import { probeClaudeInboxCapability } from '../hosts/claude/claude-inbox-capability.js';
import { selectHostLaunch } from '../hosts/host-launch.js';
import { DEFAULT_CONFIG } from '../config/defaults.js';
import { temporary } from './support/temporary.js';

test('持久收件参数探针只接受明确的解析结果，使用选定的启动入口和参数',async t=>{
  const home=temporary(t),script=path.join(home,'claude.cjs'),target=path.join(home,'probe inbox.sock');
  writeFileSync(script,`
    const [mode, flag, target, sentinel] = process.argv.slice(2);
    if (flag !== '--messaging-socket-path' || target !== ${JSON.stringify(target)} || sentinel !== '--nxbridge-capability-probe') process.exit(3);
    if (mode === 'ignored') process.exit(0);
    if (mode === 'supported' || mode === 'ambiguous') console.error("error: unknown option '" + sentinel + "'");
    if (mode === 'unsupported' || mode === 'ambiguous') console.error("error: unknown option '" + flag + "'");
    if (mode === 'other') console.error("error: unknown option '--wrapper-option'");
    process.exitCode = 1;
  `,'utf8');
  const launch=selectHostLaunch('claude',{...DEFAULT_CONFIG,claudeHome:home,claudeBinary:script},{home,cwd:home});
  const probe=(mode:string)=>probeClaudeInboxCapability({...launch,executable:{...launch.executable,prefix:[...launch.executable.prefix,mode]}},target);
  assert.equal(await probe('supported'),true);
  assert.equal(await probe('unsupported'),false);
  for(const mode of ['ignored','ambiguous','other'])await assert.rejects(probe(mode),{code:'claude_inbox_capability_unknown'});
});
