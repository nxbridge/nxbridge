import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { processIdentity } from '../process/process.js';
import { ownedProcessState,stopOwnedProcess } from '../process/process-stop.js';
import { setupPublish,uninstallPublish } from './publish-fixture.js';
import { multiRuntimeFixture } from './multi-runtime-fixture.js';
import { ownedProcesses } from '../process/owned-processes.js';
import { managedRuntimes } from '../hosts/managed-runtimes.js';
import { updateBroker } from '../install/setup-broker.js';
import { activeRuntime,runtimeManifest } from '../install/runtime-install.js';
import { brokerCall } from '../ipc/ipc.js';
import { sessionFile } from '../hosts/codex/codex-session-profile.js';
import { readCodexTurnEvidence } from '../hosts/codex/codex-turn-evidence.js';
import { delay } from '../shared/time.js';
import { MOD_REVISION } from '../mod-revision.js';
import { deliveryHealthKey } from '../broker/health/contracts.js';

test('两组忙碌原生运行时更新保留进程，卸载回收全部自有资源并保留用户进程',{timeout:170000},async t=>{
  const f=await multiRuntimeFixture(t,true),identities=await Promise.all(f.endpoints.map(e=>processIdentity(e.pid)));
  const user=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore',windowsHide:true});await once(user,'spawn');const native=await processIdentity(user.pid!);t.after(()=>stopOwnedProcess(native));
  await Promise.all([f.busy(0),f.busy(1)]);const questions=await Promise.all([f.question(0),f.question(1)]),before=await f.call('ping');
  const updated=await setupPublish() as any;assert.equal(updated.installed,true,JSON.stringify(updated));assert.equal(updated.brokerRestarted,true,JSON.stringify(updated));assert.notEqual((await f.call('ping')).pid,before.pid);
  assert.deepEqual(updated.handoff,{supported:true,used:true,transferred:2,held:0});
  await f.wait(async()=>await ownedProcessState({pid:before.pid,startedAt:before.runtime.generation})==='dead','已移交两个真实订阅的旧协调器未退出',7000);
  const s=await f.status();assert.deepEqual(s.pairs.map((p:any)=>p.id).sort(),f.pairs.map(p=>p.id).sort());
  for(let i=0;i<2;i++){assert.equal((await processIdentity(identities[i]!.pid)).startedAt,identities[i]!.startedAt);assert.equal(s.peers.find((p:any)=>p.id===f.targets[i].id).sessionId,f.targets[i].sessionId);const q=s.questions.find((v:any)=>v.id===questions[i].request_id);assert.equal(q.status,'pending');assert.equal(new Date(q.expiresAt).toISOString(),questions[i].expires_at);}

                                                
  const target=f.targets[0],source=f.sources[0],sourceEndpoint=s.endpoints.find((e:any)=>e.peerId===source.id&&e.active);
  const file=sessionFile(f.endpoints[0].configHome,target.sessionId);assert.ok(file);
  const previous=(await readCodexTurnEvidence(file,target.sessionId)).openTurnId;assert.ok(previous);
                                              
  f.release(0);
  await f.call('question.cancel',{actor:f.actor(target,'gap-cancel'),request_id:questions[0].request_id});
  await f.wait(async()=>{
    const [status,turns]=await Promise.all([f.status(),readCodexTurnEvidence(file,target.sessionId)]);
    return !turns.openTurnId&&!!turns.latestTurnId&&turns.latestTurnId!==previous&&status.peers.some((row:any)=>row.id===target.id&&row.state==='ready')&&
      status.messages.some((row:any)=>row.callId==='question-result:'+questions[0].request_id&&row.state==='contextObserved');
  },'测试预先提交的取消问题结果尚未结算',30000);
  await f.busy(0,'_GAP');
  const now=Date.now();
  await f.call('register',{host:'claude',sessionId:source.sessionId,cwd:source.cwd,name:source.name,kind:'root',state:'ready',endpoint:{mode:sourceEndpoint.mode,address:sourceEndpoint.address,epoch:sourceEndpoint.epoch,pid:sourceEndpoint.pid,procStart:sourceEndpoint.procStart,modRevision:MOD_REVISION,nativeHealth:{turnId:'gap-frontend',state:'ready',lastEventAt:now,lastProgressAt:now,completion:'completed',tools:[]}}});
  const activeTurn=(await readCodexTurnEvidence(file,target.sessionId)).openTurnId;
  assert.ok(file&&activeTurn,'必须从实际持久历史确认原轮次仍活动');
  const original=(await f.status()).messages.find((m:any)=>m.body==='MULTI_BUSY_0_GAP');assert.ok(original);
  const pointer=activeRuntime(f.home)!,manifest=runtimeManifest(pointer.directory)!,requests=f.requests.length;
  const readEvents=async(after=0)=>{
    const all:any[]=[];
    for(;;){const page=await f.call('events',{after}) as any[];all.push(...page);if(page.length<100)return all;after=page.at(-1).seq;}
  };
  const prior=(await readEvents()).at(-1)?.seq??0;
  let gapCompleted=false;
  const gated:typeof brokerCall=async(method,params,options)=>{
    if(method==='ping'&&options?.reserved&&options.custody){
      await assert.rejects(brokerCall('ping',{}, {home:f.home,start:false,timeout:300}));
      f.release(0);
      const stop=performance.now()+9000;
      for(;;){
        const value=await readCodexTurnEvidence(file,target.sessionId);
        if(value.caughtUp&&value.turns.some(turn=>turn.turnId===activeTurn)){gapCompleted=true;break;}
        if(performance.now()>=stop)assert.fail('原生轮次未在接班启动前写入最终结果：'+JSON.stringify({activeTurn,value,tail:readFileSync(file,'utf8').slice(-2500)}));await delay(50);
      }
    }
    return brokerCall(method,params,options);
  };
  const gap=await updateBroker(f.home,manifest,gated);
  assert.equal(gap.restarted,true,JSON.stringify({...gap,error:String(gap.error)}));assert.equal(gapCompleted,true);
  assert.deepEqual(gap.handoff,{supported:true,used:true,transferred:2,held:0});
  await f.wait(async()=>{
    const current=await f.status();return current.messages.some((m:any)=>m.id===original.id&&m.state==='contextObserved');
  },'接班协调器未结算原消息的持久上下文回执',10000);
  const end=(await readEvents(prior)).filter(row=>row.type==='turn.end.completed'&&row.data?.turnId===activeTurn);
  assert.equal(end.length,1,'空档里的原生完成事实只能折叠一次');
  const {DatabaseSync}=createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  const ledger=new DatabaseSync(path.join(f.home,'nxbridge.db'),{readOnly:true});
  try{
    const setting=(key:string)=>JSON.parse(String(ledger.prepare('SELECT value FROM settings WHERE key=?').get(key)?.value??'null'));
    const currentNotices=()=>((setting(deliveryHealthKey(f.pairs[0].id,source.id))?.notices??[]) as any[]).filter(row=>row.source==='background_completed'&&row.factKey===activeTurn);
    const signal=async(id:string)=>((await f.call('health.monitor',{actor:f.actor(source,'gap-notice'),admit:false,notice_id:id,mod_revision:MOD_REVISION})).notices as any[]).filter(row=>row.source==='background_completed');
    let id:string|undefined,observedFact:unknown;
    await f.wait(async()=>{
      const fact=setting('turnEndFact:'+target.id);
      if(fact?.turnId!==activeTurn||!fact.noticeId)return false;
      const candidate=fact.noticeId as string;
      const notices=await signal(candidate);
      if(notices.length===1&&notices[0].id===candidate){id=candidate;observedFact=fact;return true;}
      return false;
    },'接班协调器未提供原轮次的唯一完成提醒',10000);
                                             
    const repeated=await signal(id!),latest=setting('turnEndFact:'+target.id),rows=currentNotices();
    const sample={observedFact,latest,rows:rows.map(({id,factKey,state,admitted,error})=>({id,factKey,state,admitted,error})),repeated,requests:f.requests.length};
    assert.ok(repeated.every(row=>row.id===id),JSON.stringify(sample));
    assert.equal(latest?.noticeId,id,JSON.stringify(sample));
    assert.deepEqual(currentNotices().map(row=>row.id),[id],'同一轮次只保留一个完成提醒编号');
  }finally{ledger.close();}
  assert.equal(f.requests.length,requests,'接班不得提交额外模型输入');
  const ownership=ownedProcesses(f.home),runtimes=managedRuntimes(f.home),removed=await uninstallPublish() as any;assert.equal(removed.removed,true,JSON.stringify(removed));
  for(const id of identities){
    const record=runtimes.find(record=>record.value.pid===id.pid);assert.ok(record);
    if(record.value.dedicated===true)assert.equal(await ownedProcessState(id),'dead');
    else{assert.equal(await ownedProcessState(id),'alive');assert.match(removed.notes.join('；'),/独占|共享/);assert.ok(ownership.records.some(record=>record.identity.pid===id.pid));}
  }
  assert.equal(await ownedProcessState(native),'alive');
  f.release(0);f.release(1);assert.equal(f.errors.length,0);t.diagnostic(JSON.stringify({updated:true,uninstalled:true,owned:identities.map(v=>v.pid),userPreserved:native.pid,realModelCalls:0}));
});
