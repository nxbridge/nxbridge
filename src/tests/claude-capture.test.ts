import test, { type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import fs, { copyFileSync, existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { ChildProcess, spawn } from 'node:child_process';
import { once } from 'node:events';
import { Server } from 'node:net';
import { randomUUID } from 'node:crypto';
import { ClaudeAdapter } from '../hosts/claude/adapter.js';
import { acquireHostControl } from '../hosts/host-control.js';
import { bindNativeLaunch, captureNativeProcess, forgetNativeOwnership, nativeOwnership, saveNativeOwnership, type NativeOwnership } from '../process/owned-native.js';
import { currentLinuxBootId, forgetOwnedProcess, ownedProcesses, processRegistryDirectory, rememberOwnedChild, releaseOwnedProcess, recordOwnedProcess, type OwnedProcessRecord } from '../process/owned-processes.js';
import { saveClaudeBackground } from '../hosts/claude/claude-capability.js';
import { atomicJson } from '../shared/files.js';
import { liveProcess } from '../process/process.js';
import { ownedProcessState } from '../process/process-stop.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import { Store } from '../storage/store.js';
import { temporaryDirectory } from './support/temporary.js';

async function fixture(t:TestContext){
  const temp=temporaryDirectory(),home=temp.directory,store=new Store(':memory:'),peer=store.placeholder('claude',home),sessionId=randomUUID(),executable=path.join(home,process.platform==='win32'?'claude.exe':'claude');
  let child:ChildProcess|undefined,processProof:OwnedProcessRecord|undefined,adapter:ClaudeAdapter|undefined;
  t.after(async()=>{
    adapter?.close();
    try{const current=nativeOwnership(home,peer.id,{includePreviousBoot:true});if(current)forgetNativeOwnership(current);}
    finally{
      try{if(processProof)await releaseOwnedProcess(processProof);else if(child?.pid&&child.exitCode===null&&child.signalCode===null){const closed=once(child,'exit');child.kill();await closed;}}
      finally{store.close();await temp.cleanup();}
    }
  });
  copyFileSync(process.execPath,executable);
  const spawned=child=spawn(executable,['-e',"process.title='claude';process.send('ready');process.disconnect();setInterval(()=>{},1000)"],{stdio:['ignore','ignore','ignore','ipc'],windowsHide:true});
                                      
  const exited=new Promise<void>(resolve=>spawned.once('exit',()=>resolve()));
  await once(spawned,'message');
  processProof=await rememberOwnedChild(home,child,'claude-native',{peerId:peer.id,sessionId,configHome:home});assert.ok(processProof);
  const identity=processProof.identity,record={sessionId,cwd:home,procStart:identity.procStartFt??identity.procStart??identity.startedAt,messagingSocketPath:'native-inbox'};
  mkdirSync(path.join(home,'sessions'));const file=path.join(home,'sessions',identity.pid+'.json');writeFileSync(file,JSON.stringify(record),'utf8');
  const proof:NativeOwnership={version:1,id:randomUUID(),home,peerId:peer.id,configHome:home,phase:'bound',createdAt:Date.now(),sessionId,handle:'abc00001',native:{pid:identity.pid,startedAt:identity.startedAt}};
  saveNativeOwnership(proof);
  adapter=new ClaudeAdapter({...DEFAULT_CONFIG,claudeBinary:executable,claudeHome:home},home);const agent={id:proof.handle,sessionId,pid:identity.pid,kind:'background',status:'idle'};
  adapter.list=async()=>[agent];
  return{home,peer,record,file,proof,identity,agent,adapter,processProof,exited};
}

async function assertExited(f:Awaited<ReturnType<typeof fixture>>){
  assert.equal(await ownedProcessState(f.identity),'dead');
  await f.exited;
  assert.equal(liveProcess(f.identity.pid),false);
}

function identityQueries(t:TestContext,pid:number){
  const prototype=ChildProcess.prototype as unknown as {spawn:(options:any)=>unknown},original=prototype.spawn,count={value:0};
  t.mock.method(prototype,'spawn',function(this:ChildProcess,options:any){
                                                 
    if(options.args?.[1]==='read-result'&&options.args[2]==='ancestors'&&options.args[3]===String(pid)&&options.args[4]==='1')count.value++;
    return Reflect.apply(original,this,[options]);
  });
  return count;
}

test('Claude 暂未发布 PID 但原控制器仍存活时不重复恢复、不替换启动归属',async t=>{
  const f=await fixture(t),proof={...f.proof,native:{pid:2147483647,startedAt:'已退出的原生子进程'}};
  saveNativeOwnership(proof);saveClaudeBackground(f.home,(f.adapter as any).config,{available:true,checkedAt:Date.now()});
  await recordOwnedProcess(f.home,{role:'claude-native',identity:f.identity,peerId:f.peer.id,sessionId:f.record.sessionId,configHome:f.home,proof:{kind:'native-session',launchId:proof.id}});
  f.adapter.list=async()=>[{...f.agent,pid:undefined}];let submissions=0;
  await assert.rejects(f.adapter.wake({...f.peer,owner:'nxbridge',sessionId:f.record.sessionId},undefined,undefined,()=>{submissions++;}),{code:'claude_start_pending'});
  assert.equal(submissions,0);assert.deepEqual(nativeOwnership(f.home,f.peer.id),proof);
  const waiting=(f.adapter as any).nativeRestartWaits.get(f.peer.id);waiting.at-=60000;
  await assert.rejects(f.adapter.wake({...f.peer,owner:'nxbridge',sessionId:f.record.sessionId},undefined,undefined,()=>{submissions++;}),{code:'claude_native_recovery_waiting'});
  assert.equal(submissions,0);assert.deepEqual(nativeOwnership(f.home,f.peer.id),proof);
});

test('旧启动回执没有 supervisor 字段时也等待同一 handle，不重新恢复出副本',async t=>{
  const f=await fixture(t),proof={...f.proof,native:{pid:2147483647,startedAt:'已退出'}};saveNativeOwnership(proof);
  saveClaudeBackground(f.home,(f.adapter as any).config,{available:true,checkedAt:Date.now()});f.adapter.list=async()=>[{...f.agent,pid:undefined,state:'working'}];
  await assert.rejects(f.adapter.wake({...f.peer,owner:'nxbridge',sessionId:f.record.sessionId}),{code:'claude_start_pending'});
  (f.adapter as any).nativeRestartWaits.get(f.peer.id).at-=60000;
  await assert.rejects(f.adapter.wake({...f.peer,owner:'nxbridge',sessionId:f.record.sessionId}),{code:'claude_native_recovery_waiting'});
  assert.deepEqual(nativeOwnership(f.home,f.peer.id),proof);
});

test('原生 PID 先于后台列表到达时，不缓存瞬时缺失阻塞后续身份核验',async t=>{
  const f=await fixture(t),input={host:'claude' as const,sessionId:f.record.sessionId,cwd:f.home,kind:'root' as const,endpoint:{mode:'claude-inbox' as const,address:'native-inbox',epoch:'test',pid:f.identity.pid,procStart:f.identity.startedAt}};
  let queries=0;f.adapter.list=async()=>++queries===1?[]:[f.agent];
  assert.equal(await f.adapter.corroborateRegistration(input,f.proof),undefined);
  assert.equal((await f.adapter.corroborateRegistration(input,f.proof))?.native?.pid,f.identity.pid);assert.equal(queries,2);
});

for(const [epoch,label] of [['current','当前启动'],['previous','上次启动'],['legacy','未记录启动代号']] as const)
  test(`同 PID 和代次的已登记记录按平台核对启动身份：${label}`,async t=>{
    const f=await fixture(t),bootId=currentLinuxBootId(),file=path.join(processRegistryDirectory(f.home),f.processProof.id+'.json');
    if(process.platform==='linux')assert.ok(bootId);
    const existing:OwnedProcessRecord={...f.processProof,proof:{kind:'native-session',launchId:f.proof.id},
      bootId:epoch==='current'?bootId:epoch==='previous'?'previous-boot':undefined,
      bootTime:epoch==='previous'?0:f.processProof.bootTime};
    atomicJson(file,existing);
    try{
      await captureNativeProcess(f.proof,f.agent);
      const records=ownedProcesses(f.home).records;
      const newGeneration=process.platform==='linux'&&epoch==='previous';
      assert.equal(records.length,newGeneration?2:1);
      assert.equal(records.find(record=>record.id===existing.id)?.bootId,existing.bootId);
      const fresh=records.find(record=>record.id!==existing.id);
      if(newGeneration){
        assert.ok(fresh);assert.equal(fresh.bootId,bootId);
        assert.deepEqual(fresh.proof,{kind:'native-session',launchId:f.proof.id});
        assert.deepEqual([fresh.identity.pid,fresh.identity.startedAt],[f.identity.pid,f.identity.startedAt]);
      }
      await captureNativeProcess(f.proof,f.agent);
      assert.deepEqual(ownedProcesses(f.home).records.map(record=>record.id).sort(),records.map(record=>record.id).sort());
    }finally{
                                          
      atomicJson(file,f.processProof);
      for(const record of ownedProcesses(f.home).records)if(record.id!==f.processProof.id)forgetOwnedProcess(record);
    }
  });

for(const transferred of [false,true])test(`没有启动绑定时，Claude 唤醒和停止只接受仍有效的原生进程归属（${transferred?'已转交':'启动代号变化'}）`,async t=>{
  const f=await fixture(t),peer={...f.peer,owner:'nxbridge' as const,sessionId:f.record.sessionId};
  const file=path.join(processRegistryDirectory(f.home),f.processProof.id+'.json');
  forgetNativeOwnership(f.proof);
  saveClaudeBackground(f.home,(f.adapter as any).config,{available:true,checkedAt:Date.now()});
  atomicJson(path.join(f.home,'daemon.status.json'),{supervisorPid:f.identity.pid,supervisorProcStart:f.record.procStart});
  atomicJson(file,transferred?{...f.processProof,releasedToUser:true}:{...f.processProof,bootId:'previous-boot',bootTime:0});
  try{
    let stopChecks=0;
    const blocked=Error('隔离夹具不提交停止请求'),beforeStop=()=>{stopChecks++;throw blocked;};
    if(transferred||process.platform==='linux'){
      await assert.rejects(f.adapter.wake(peer),{code:'claude_launch_unverified'});
      await assert.rejects(f.adapter.release(peer,beforeStop),{code:'claude_launch_unverified'});
      assert.equal(stopChecks,0);
    }else{
      await f.adapter.wake(peer);
      await assert.rejects(f.adapter.release(peer,beforeStop),error=>error===blocked);
      assert.equal(stopChecks,1,'Windows 的原生进程代次不依赖启动时钟推断');
    }
  }finally{atomicJson(file,f.processProof);}
});

async function stopFixture(t:TestContext,f:Awaited<ReturnType<typeof fixture>>,updates:ReadonlyArray<readonly [string,unknown]>=[]){
  const program=path.join(f.home,'claude-stop.cjs'),sentinel=path.join(f.home,'stop-submitted');
  writeFileSync(program,`
    const fs=require('node:fs');
    if(process.argv.includes('stop')){
      fs.writeFileSync(${JSON.stringify(sentinel)},'stop');
      for(const [filename,value] of ${JSON.stringify(updates)})fs.writeFileSync(filename,JSON.stringify(value));
    }
  `,'utf8');
  await recordOwnedProcess(f.home,{role:'test-daemon',identity:f.identity,configHome:f.home,parent:f.processProof.parent,proof:{kind:'spawn'}});
  atomicJson(path.join(f.home,'daemon.status.json'),{supervisorPid:f.identity.pid,supervisorProcStart:f.record.procStart});
  const adapter=new ClaudeAdapter({...DEFAULT_CONFIG,claudeBinary:program,claudeHome:f.home},f.home);
  adapter.list=async()=>[f.agent];t.after(()=>adapter.close());
  return{adapter,sentinel,peer:{...f.peer,owner:'nxbridge' as const,sessionId:f.record.sessionId}};
}

test('Claude 在控制队列中等待时归属发生变化，不会提交停止命令',async t=>{
  const f=await fixture(t),{adapter,sentinel,peer}=await stopFixture(t,f);
  const unlock=await acquireHostControl(process.execPath);
  let checks=0,callbackReached!:()=>void,callbackFailed!:(error:unknown)=>void;
  const callback=new Promise<void>((resolve,reject)=>{callbackReached=resolve;callbackFailed=reject;});
  const pending=adapter.release(peer,()=>{checks++;callbackReached();});void pending.catch(callbackFailed);
  try{
    await callback;
    await new Promise<void>(resolve=>setImmediate(resolve));
    saveNativeOwnership({...f.proof,releasedToUser:true});
  }finally{unlock();}
  await assert.rejects(pending,{code:'process_record_changed'});
  assert.equal(checks,1);assert.equal(existsSync(sentinel),false);assert.equal(liveProcess(f.identity.pid),true);
});

test('Windows 控制容器准备期间 Claude 被转交，不会提交停止命令',{skip:process.platform!=='win32'},async t=>{
  const f=await fixture(t),{adapter,sentinel,peer}=await stopFixture(t,f),listen=Server.prototype.listen;
  let changes=0;
  t.mock.method(Server.prototype,'listen',function(this:Server,...args:Parameters<typeof listen>){
    if(typeof args[0]==='string'&&args[0].includes('nxbridge-command-')){
      changes++;saveNativeOwnership({...f.proof,releasedToUser:true});
    }
    return Reflect.apply(listen,this,args);
  });
  await assert.rejects(adapter.release(peer),{code:'process_record_changed'});
  assert.equal(changes,1);assert.equal(existsSync(sentinel),false);assert.equal(liveProcess(f.identity.pid),true);
});

for(const [source,change] of [['bound','handoff'],['bound','replacement'],['record','handoff'],['record','replacement']] as const)
  test(`等待停止前回调时 Claude 归属改变，阻止原生停止：${source==='bound'?'绑定':'进程记录'}/${change==='handoff'?'转交':'替换'}`,async t=>{
    const f=await fixture(t),{adapter,sentinel,peer}=await stopFixture(t,f);
    const registry=processRegistryDirectory(f.home),file=path.join(registry,f.processProof.id+'.json');
    const replacement={...f.processProof,id:randomUUID()};
    if(source==='record')forgetNativeOwnership(f.proof);
    let checks=0;
    try{
      await assert.rejects(adapter.release(peer,async()=>{
        checks++;await Promise.resolve();
        if(source==='bound')saveNativeOwnership(change==='handoff'?{...f.proof,releasedToUser:true}:{...f.proof,id:randomUUID()});
        else if(change==='handoff')atomicJson(file,{...f.processProof,releasedToUser:true});
        else{forgetOwnedProcess(f.processProof);atomicJson(path.join(registry,replacement.id+'.json'),replacement);}
      }),{code:'process_record_changed'});
      assert.equal(checks,1);assert.equal(existsSync(sentinel),false);assert.equal(liveProcess(f.identity.pid),true);
    }finally{
      if(source==='record'){atomicJson(file,f.processProof);if(change==='replacement')forgetOwnedProcess(replacement);}
    }
  });

test('原生停止返回时遇到后继绑定，不删除后继证据或按过期范围终止进程',async t=>{
  const f=await fixture(t),registry=processRegistryDirectory(f.home);
  const successor={...f.proof,id:randomUUID()},newRecord={...f.processProof,id:randomUUID(),proof:{kind:'native-session' as const,launchId:successor.id}};
  const {adapter,sentinel,peer}=await stopFixture(t,f,[
    [path.join(registry,'native-'+f.peer.id+'.json'),successor],
    [path.join(registry,newRecord.id+'.json'),newRecord],
  ]);
  try{
    await assert.rejects(adapter.release(peer),{code:'process_record_changed'});
    assert.equal(existsSync(sentinel),true);
    assert.equal(nativeOwnership(f.home,f.peer.id)?.id,successor.id);
    assert.ok(ownedProcesses(f.home).records.some(record=>record.id===newRecord.id));
    assert.equal(liveProcess(f.identity.pid),true);
  }finally{forgetOwnedProcess(newRecord);}
});

test('原生停止返回时回收同次启动新登记的进程，保留其他启动的记录',async t=>{
  const f=await fixture(t),late=await fixture(t),other=await fixture(t),registry=processRegistryDirectory(f.home);
  const added=(source:typeof late,launchId:string):OwnedProcessRecord=>({...source.processProof,id:randomUUID(),
    home:f.home,peerId:f.peer.id,sessionId:f.record.sessionId,configHome:f.home,proof:{kind:'native-session',launchId}});
  const sameLaunch=added(late,f.proof.id),differentLaunch=added(other,randomUUID());
  const {adapter,sentinel,peer}=await stopFixture(t,f,[sameLaunch,differentLaunch].map(record=>[path.join(registry,record.id+'.json'),record]));
  try{
    await adapter.release(peer);
    assert.equal(existsSync(sentinel),true);
    const remaining=ownedProcesses(f.home).records;
    assert.ok(!remaining.some(record=>record.id===sameLaunch.id));
    await assertExited(late);
    assert.ok(remaining.some(record=>record.id===differentLaunch.id));assert.equal(liveProcess(other.identity.pid),true);
    assert.equal(nativeOwnership(f.home,f.peer.id),undefined);
  }finally{forgetOwnedProcess(sameLaunch);forgetOwnedProcess(differentLaunch);}
});

for(const [label,bound,hasSupervisor] of [['相同启动绑定',true,true],['仅有进程记录',false,true],['无在线原生代理',true,false]] as const)
  test(`归属未变化时正常清理 Claude 后台：${label}`,async t=>{
    const f=await fixture(t),{adapter,sentinel,peer}=await stopFixture(t,f);
    if(!bound)forgetNativeOwnership(f.proof);
    if(!hasSupervisor)atomicJson(path.join(f.home,'daemon.status.json'),{});
    await adapter.release(peer);
    assert.equal(existsSync(sentinel),hasSupervisor);assert.equal(nativeOwnership(f.home,f.peer.id),undefined);
    assert.ok(!ownedProcesses(f.home).records.some(record=>record.id===f.processProof.id));
    await assertExited(f);
  });

test('上次 Linux 启动的回执不能认领或停止复用同一 PID 与代次的 Claude',{skip:process.platform!=='linux'},async t=>{
  const f=await fixture(t),{adapter,sentinel,peer}=await stopFixture(t,f),previous={...f.proof,bootId:'previous-boot'};
  assert.ok(currentLinuxBootId());
  saveClaudeBackground(f.home,(adapter as any).config,{available:true,checkedAt:Date.now()});
  saveNativeOwnership(previous);
  const file=path.join(processRegistryDirectory(f.home),f.processProof.id+'.json');
  atomicJson(file,{...f.processProof,bootId:previous.bootId});
  try{
    assert.equal(nativeOwnership(f.home,f.peer.id),undefined);
    await assert.rejects(bindNativeLaunch({...previous,phase:'intent'},f.agent),{code:'process_record_changed'});
    await assert.rejects(captureNativeProcess(previous,f.agent),{code:'claude_launch_unverified'});
    await assert.rejects(adapter.wake(peer),{code:'claude_launch_unverified'});
    let stopChecks=0;
    await adapter.release(peer,()=>{stopChecks++;});
    assert.equal(stopChecks,0);assert.equal(existsSync(sentinel),false);assert.equal(liveProcess(f.identity.pid),true);
    assert.equal(nativeOwnership(f.home,f.peer.id,{includePreviousBoot:true}),undefined);
  }finally{atomicJson(file,f.processProof);}
});

test('Linux 重启后保留用户转交；允许原会话恢复一次并核对新的启动代号',{skip:process.platform!=='linux'},async t=>{
  const f=await fixture(t),peer={...f.peer,owner:'nxbridge' as const,sessionId:f.record.sessionId},bootId=currentLinuxBootId(),previous={...f.proof,bootId:'previous-boot'};
  assert.ok(bootId);
  saveClaudeBackground(f.home,(f.adapter as any).config,{available:true,checkedAt:Date.now()});
  saveNativeOwnership({...previous,releasedToUser:true});
  await assert.rejects(f.adapter.wake(peer),{code:'process_owned_by_user'});
  await assert.rejects(f.adapter.release(peer),{code:'process_owned_by_user'});
  saveNativeOwnership(previous);
  const projects=path.join(f.home,'projects','fixture');mkdirSync(projects,{recursive:true});
  writeFileSync(path.join(projects,peer.sessionId+'.jsonl'),'','utf8');
  let submitted=0;
  f.adapter.list=async()=>submitted?[f.agent]:[];
  t.mock.method(f.adapter as any,'submitLaunch',async(_peer:unknown,intent:NativeOwnership,_launch:unknown,args:string[])=>{
    submitted++;assert.deepEqual(args,['--resume',peer.sessionId,'--bg']);assert.equal(intent.bootId,bootId);assert.notEqual(intent.id,previous.id);
    const accepted={...intent,handle:f.agent.id};saveNativeOwnership(accepted);return accepted;
  });
  await f.adapter.wake(peer);await f.adapter.wake(peer);
  assert.equal(submitted,1);assert.equal(nativeOwnership(f.home,f.peer.id)?.bootId,bootId);
});

test('托管 Claude 发现与人工副本登记复用同次原生核验，端点来自实际会话',async t=>{
  const f=await fixture(t),queries=identityQueries(t,f.identity.pid);
  for(const registration of [await f.adapter.discover(f.record.sessionId),await f.adapter.manualCopyRegistration({...f.peer,sessionId:f.record.sessionId})]){
    assert.equal(registration?.sessionId,f.record.sessionId);assert.equal(registration?.endpoint.pid,f.identity.pid);assert.equal(registration?.endpoint.procStart,f.identity.startedAt);assert.equal(registration?.endpoint.address,f.record.messagingSocketPath);
  }
  if(process.platform==='win32')assert.equal(queries.value,2,'两个入口各自只查询一次实际原生身份');
});

test('被动登记只接受同一原生 PID 和代次，不借用旧 hook 的身份',async t=>{
  const f=await fixture(t),queries=identityQueries(t,f.identity.pid),input={host:'claude' as const,sessionId:f.record.sessionId,cwd:f.home,name:'后台',kind:'root' as const,endpoint:{mode:'claude-inbox' as const,address:'native-inbox',epoch:'test',pid:f.identity.pid,procStart:f.identity.startedAt}};
  assert.equal(await f.adapter.corroborateRegistration({...input,endpoint:{...input.endpoint,pid:f.identity.pid+1}},f.proof),undefined);
  assert.equal(await f.adapter.corroborateRegistration({...input,endpoint:{...input.endpoint,procStart:'old-generation'}},f.proof),undefined);
  assert.equal((await f.adapter.corroborateRegistration(input,f.proof))?.native?.startedAt,f.identity.startedAt);
  if(process.platform==='win32')assert.equal(queries.value,2,'PID 不符不查询；每次同 PID 登记仅查询一次代次');
});

for(const operation of ['bind','capture'] as const)test('原生列表等待期间已交给用户的同次启动，旧参数不能重新认领：'+operation,async t=>{
  const f=await fixture(t),stale={...f.proof,...(operation==='bind'?{phase:'intent' as const}:{})},released={...stale,releasedToUser:true};
  saveNativeOwnership(released);
  await assert.rejects((operation==='bind'?bindNativeLaunch:captureNativeProcess)(stale,f.agent),{code:operation==='bind'?'process_record_changed':'claude_launch_unverified'});
  assert.deepEqual(nativeOwnership(f.home,f.peer.id),released);
});

for(const changed of ['session','generation','launch','handoff'] as const)test('PTY 核验等待期间事实改变时不返回旧证据或覆盖归属：'+changed,async t=>{
  const f=await fixture(t),read=fs.readFileSync,successor=randomUUID();let observed=false,reads=0;
  t.mock.method(fs,'readFileSync',((file:Parameters<typeof fs.readFileSync>[0],...args:any[])=>{
    if(file===f.file&&++reads===2){
      if(changed==='session'||changed==='generation')writeFileSync(f.file,JSON.stringify({...f.record,...(changed==='session'?{sessionId:randomUUID()}:{procStart:'new-generation'})}),'utf8');
      else saveNativeOwnership({...f.proof,...(changed==='launch'?{id:successor}:{releasedToUser:true})});
    }
    return Reflect.apply(read,fs,[file,...args]);
  }) as typeof fs.readFileSync);syncBuiltinESMExports();
  try{
    await assert.rejects(captureNativeProcess(f.proof,f.agent,()=>{observed=true;}),{code:changed==='session'||changed==='generation'?'claude_launch_unverified':'process_record_changed'});
    assert.equal(observed,false);
    if(changed==='launch')assert.equal(nativeOwnership(f.home,f.peer.id)?.id,successor);
    if(changed==='handoff')assert.equal(nativeOwnership(f.home,f.peer.id)?.releasedToUser,true);
  }finally{t.mock.restoreAll();syncBuiltinESMExports();}
});
