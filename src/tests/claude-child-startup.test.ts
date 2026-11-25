import test,{type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {mkdirSync,writeFileSync,existsSync} from 'node:fs';
import {Store} from '../storage/store.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {atomicJson} from '../shared/files.js';
import {claudeChild,claudeChildFile,ensureClaudeChild,type ClaudeChildRecord} from '../hosts/claude/claude-child.js';
import {ensureHome} from '../ipc/home.js';
import {listenBroker} from '../ipc/ipc.js';
import {processIdentity} from '../process/process.js';
import {recordOwnedProcess,forgetOwnedProcess,type OwnedProcessRecord} from '../process/owned-processes.js';
import {temporaryDirectory} from './support/temporary.js';
import {registration} from './support/registration.js';

const linux={skip:process.platform!=='linux'};
async function setup(t:TestContext){
  const temp=temporaryDirectory(),home=temp.directory,store=new Store(':memory:'),sessionId='00000000-0000-4000-8000-000000000001';
  let server:Awaited<ReturnType<typeof listenBroker>>|undefined,owned:OwnedProcessRecord|undefined;
  t.after(async()=>{await server?.close();if(owned)forgetOwnedProcess(owned);store.close();await temp.cleanup();});
  await ensureHome(home);const peer={...store.placeholder('claude',home),sessionId},marker=path.join(home,'spawned'),program=path.join(home,'claude.cjs');
  writeFileSync(program,`if(process.argv.includes('--version'))console.log('2.1.328 (Claude Code)');else{require('node:fs').writeFileSync(${JSON.stringify(marker)},'started');process.exitCode=71;}`,'utf8');
  const projects=path.join(home,'projects','fixture');mkdirSync(projects,{recursive:true});writeFileSync(path.join(projects,sessionId+'.jsonl'),JSON.stringify({type:'user',version:'2.1.328',sessionId})+'\n','utf8');
  const config={...DEFAULT_CONFIG,claudeHome:home,claudeCommand:[process.execPath,program]},record:ClaudeChildRecord={version:1,peerId:peer.id,sessionId,cwd:home,address:path.join(home,'run','cc-startup.sock'),phase:'starting',hasSession:true,configHome:home,updatedAt:Date.now()};
  return{home,peer,config,record,marker,serve:async(current:ClaudeChildRecord)=>{
    const identity=await processIdentity(process.pid);owned=await recordOwnedProcess(home,{role:'claude-worker',identity,peerId:peer.id,sessionId,proof:{kind:'spawn'}});
    server=await listenBroker(home,async()=>current,record.address);
  }};
}

test('旧启动记录尚无 worker 身份时保留未知，不能把未填 PID 当成没有启动',linux,async t=>{
  const f=await setup(t);atomicJson(claudeChildFile(f.home,f.peer.id),f.record);
  await assert.rejects(ensureClaudeChild(f.home,f.config,f.peer),{code:'claude_child_pending'});
  assert.deepEqual(claudeChild(f.home,f.peer.id),f.record);assert.equal(existsSync(f.marker),false);
});

test('旧 worker 已认领控制通道时恢复它的身份，不创建第二份或覆盖活动会话',linux,async t=>{
  const f=await setup(t),native=await processIdentity(process.pid),active:ClaudeChildRecord={...f.record,phase:'active',worker:native,native,registration:registration('claude',f.peer.sessionId)};
  atomicJson(claudeChildFile(f.home,f.peer.id),f.record);await f.serve(active);
  const result=await ensureClaudeChild(f.home,f.config,f.peer);
  assert.equal(result.worker?.pid,native.pid);assert.equal(claudeChild(f.home,f.peer.id)?.phase,'active');assert.equal(existsSync(f.marker),false);
});

test('启动预检让出执行权后再核对记录，晚到的 worker 身份不能被新意图覆盖',linux,async t=>{
  const f=await setup(t),native=await processIdentity(process.pid),before={...f.record,phase:'failed' as const},active:ClaudeChildRecord={...f.record,phase:'active',worker:native,native,registration:registration('claude',f.peer.sessionId)};
  atomicJson(claudeChildFile(f.home,f.peer.id),before);
  await assert.rejects(ensureClaudeChild(f.home,f.config,f.peer,async()=>{atomicJson(claudeChildFile(f.home,f.peer.id),active);}),{code:'claude_child_pending'});
  assert.deepEqual(claudeChild(f.home,f.peer.id),active);assert.equal(existsSync(f.marker),false);
});

test('明确没有提交的 worker 启动失败可重试，失败回执只更新自己的启动意图',linux,async t=>{
  const f=await setup(t);atomicJson(claudeChildFile(f.home,f.peer.id),{...f.record,phase:'failed'});
  let previous:string|undefined;
  for(let attempt=0;attempt<2;attempt++){
    await assert.rejects(ensureClaudeChild(f.home,f.config,f.peer,undefined,undefined,async()=>{throw Error('创建前拒绝');}),/创建前拒绝/);
    const record=claudeChild(f.home,f.peer.id)!;assert.equal(record.phase,'failed');assert.ok(record.launchId);assert.notEqual(record.launchId,previous);previous=record.launchId;
  }
  assert.equal(existsSync(f.marker),false);
});
