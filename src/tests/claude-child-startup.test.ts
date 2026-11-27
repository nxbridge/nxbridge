import test,{type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {mkdirSync,writeFileSync,existsSync} from 'node:fs';
import {Store} from '../storage/store.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {atomicJson} from '../shared/files.js';
import {childCall,claudeChild,claudeChildFile,ensureClaudeChild,findClaudeChild,stopClaudeChild,type ClaudeChildRecord} from '../hosts/claude/claude-child.js';
import {ensureHome} from '../ipc/home.js';
import {listenBroker} from '../ipc/ipc.js';
import {liveProcess,processIdentity} from '../process/process.js';
import {currentLinuxBootId,processRegistryDirectory,recordOwnedProcess,forgetOwnedProcess,rememberOwnedChild,releaseOwnedProcess,type OwnedProcessRecord} from '../process/owned-processes.js';
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

test('Linux 上次启动的托管记录只保留恢复资料，复用的 PID 不被探测或停止',linux,async t=>{
  const f=await setup(t),neighbor=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore'}),closed=once(neighbor,'exit');
  await once(neighbor,'spawn');
  const owned=await rememberOwnedChild(f.home,neighbor,'host-control');assert.ok(owned);
  t.after(async()=>{await releaseOwnedProcess(owned);await closed;});
  const file=claudeChildFile(f.home,f.peer.id),previous:ClaudeChildRecord={...f.record,bootId:'previous-boot',phase:'active',native:owned.identity,
    registration:registration('claude',f.peer.sessionId)};
  const bootId=currentLinuxBootId();assert.ok(bootId);
  atomicJson(file,previous);
  const recovered=findClaudeChild(f.home,f.peer.sessionId)!;
  assert.equal(recovered.phase,'stopped');assert.equal(recovered.native,undefined);assert.equal(recovered.registration,undefined);
  await assert.rejects(childCall(f.home,previous,'status'),{code:'claude_child_offline'});
  assert.equal((await stopClaudeChild(f.home,previous)).phase,'released');
  assert.equal(liveProcess(owned.identity.pid),true);
  atomicJson(file,{...previous,phase:'user',foreground:owned.identity});
  await assert.rejects(ensureClaudeChild(f.home,f.config,f.peer),{code:'claude_handoff'});
  atomicJson(file,previous);
  const abort=Error('测试在实际派生 worker 前结束');
  await assert.rejects(ensureClaudeChild(f.home,f.config,f.peer,undefined,undefined,()=>{
    const intent=claudeChild(f.home,f.peer.id)!;
    assert.equal(intent.phase,'starting');assert.equal(intent.bootId,bootId);assert.equal(intent.sessionId,f.peer.sessionId);assert.equal(intent.hasSession,true);
    throw abort;
  }),error=>error===abort);
  assert.equal(liveProcess(owned.identity.pid),true);assert.equal(existsSync(f.marker),false);
});

test('Linux 旧托管文件没有启动代号时，保留无法核验的活进程且不另起副本',linux,async t=>{
  const f=await setup(t),neighbor=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore'}),closed=once(neighbor,'exit');
  await once(neighbor,'spawn');const owned=await rememberOwnedChild(f.home,neighbor,'host-control');assert.ok(owned);
  t.after(async()=>{await releaseOwnedProcess(owned);await closed;});
  atomicJson(claudeChildFile(f.home,f.peer.id),{...f.record,phase:'active',native:owned.identity});
  await assert.rejects(ensureClaudeChild(f.home,f.config,f.peer),{code:'claude_child_orphan'});
  await assert.rejects(stopClaudeChild(f.home,claudeChild(f.home,f.peer.id)!),{code:'claude_child_orphan'});
  assert.equal(liveProcess(owned.identity.pid),true);assert.equal(existsSync(f.marker),false);
  const legacy=await recordOwnedProcess(f.home,{role:'host-control',identity:owned.identity,parent:await processIdentity(process.pid),peerId:f.peer.id,proof:{kind:'spawn'}});
  t.after(()=>forgetOwnedProcess(legacy));delete legacy.bootId;atomicJson(path.join(processRegistryDirectory(f.home),legacy.id+'.json'),legacy);
  atomicJson(claudeChildFile(f.home,f.peer.id),{...f.record,phase:'active',native:owned.identity});
  await assert.rejects(ensureClaudeChild(f.home,f.config,f.peer),{code:'claude_child_orphan'});
  await assert.rejects(stopClaudeChild(f.home,claudeChild(f.home,f.peer.id)!),{code:'claude_child_orphan'});
  assert.equal(liveProcess(owned.identity.pid),true,'旧归属记录同样缺少启动代号时不能绕过原生停止边界');
  assert.equal(claudeChild(f.home,f.peer.id)?.phase,'active');assert.equal(existsSync(f.marker),false);
});

test('Linux 升级后仍通过存活旧 worker 的原控制通道停止未带启动代号的进程',linux,async t=>{
  const f=await setup(t),worker=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore'}),closed=once(worker,'exit');
  await once(worker,'spawn');const owned=await rememberOwnedChild(f.home,worker,'claude-worker',{peerId:f.peer.id});assert.ok(owned);
  t.after(async()=>{if(worker.exitCode===null&&worker.signalCode===null)worker.kill();await closed;forgetOwnedProcess(owned);});
  delete owned.bootId;atomicJson(path.join(processRegistryDirectory(f.home),owned.id+'.json'),owned);
  const legacy:ClaudeChildRecord={...f.record,phase:'active',worker:owned.identity,native:owned.identity};atomicJson(claudeChildFile(f.home,f.peer.id),legacy);
  let requests=0;
  const server=await listenBroker(f.home,async method=>{
    assert.equal(method,'stop');requests++;worker.kill();await closed;
    return{...legacy,phase:'stopped'};
  },legacy.address);t.after(()=>server.close());
  const result=await stopClaudeChild(f.home,legacy);assert.equal(result.phase,'stopped');assert.equal(requests,1);assert.equal(liveProcess(owned.identity.pid),false);
});
