import test, { type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import {fixture, lifecycle} from './support/broker-fixture.js';
import {BridgeError} from '../shared/errors.js';
import {unknownContainerFixture} from './unknown-container-fixture.js';
import {forgetOwnedProcess,ownedProcesses,processRegistryDirectory} from '../process/owned-processes.js';
import {saveManagedRuntime,runtimeFor,type ManagedRuntime} from '../hosts/managed-runtimes.js';
import {managedMarker} from '../install/installation.js';
import {existsSync} from 'node:fs';
import {randomUUID} from 'node:crypto';
import path from 'node:path';
import {atomicJson} from '../shared/files.js';
import {managedRecoveryKey, type ManagedIncident} from '../broker/recovery/managed-recovery.js';
import {CodexAdapter} from '../hosts/codex/adapter.js';
import {codexRecycleRuntimeGeneration} from '../hosts/codex/codex-recycle.js';

function unpairedReplacement(t:TestContext){
  const f=fixture(t),home=f.broker.home,pair=f.store.pair(f.c.id,f.x.id),route=f.store.endpoint(f.x.id)!;
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});
  const old={runtimeId:randomUUID(),endpoint:route.address,pid:2147483646,procStart:'old-generation'};
  f.store.saveEndpoint({...route,runtimeId:old.runtimeId,pid:old.pid,procStart:old.procStart});
  const replacement:ManagedRuntime={id:randomUUID(),endpoint:'ws://127.0.0.1:33221',pid:2147483647,procStart:'replacement-generation',owner:managedMarker(home),
    kind:'owned',state:'ready',peerId:f.x.id,sessionId:f.x.sessionId!,dedicated:true,createdAt:Date.now()};
  saveManagedRuntime(home,replacement);
  const fresh=codexRecycleRuntimeGeneration(replacement);
  const attempt={id:randomUUID(),at:0,stopDeadline:15000,old,profile:{sourceId:old.runtimeId,sessionId:f.x.sessionId!,file:'rollout.jsonl',settingsKey:'verified'},
    decision:{revision:randomUUID(),noticeId:randomUUID(),since:0,autoEligible:true,facts:'idle',work:{activeTurnIds:[],progress:'idle' as const,roles:[],unresolved:[]}},
    ledger:{held:[],uncertain:[]}};
  const incident:ManagedIncident={version:1,id:randomUUID(),peerId:f.x.id,pairId:pair.id,host:'codex',sessionId:f.x.sessionId!,configHome:home,
    openedAt:0,updatedAt:0,state:'recovering',proof:{},attempts:[],nextAt:0,noticeId:randomUUID(),
    recycle:{phase:'verifying',step:randomUUID(),trigger:'native_thread_wedged',fresh,launch:{runtimeId:fresh.runtimeId},attempt,resumeSubmitted:true}};
  f.store.setSetting(managedRecoveryKey(f.x.id),incident);
  f.store.unpair(pair.id);
  return {...f,old,fresh};
}

test('解除配对回收订阅核验中尚未登记路由的替代代次',async t=>{
  const f=unpairedReplacement(t),home=f.broker.home,adapter=new CodexAdapter(f.broker.config,home);
  const unrelated=randomUUID(),retired:{address?:string;runtimeId?:string}[]=[];
  saveManagedRuntime(home,{id:unrelated,endpoint:f.fresh.endpoint,pid:f.fresh.pid,procStart:'another-generation',owner:managedMarker(home),
    kind:'owned',state:'failed',peerId:randomUUID(),dedicated:true,createdAt:0});
  t.after(()=>adapter.close());
  const release:CodexAdapter['releaseRuntime']=async(refs,address,peers,options)=>{
    retired.push({address,runtimeId:options?.runtimeId});return adapter.releaseRuntime(refs,address,peers,options);
  };
  t.mock.method(f.codex,'releaseRuntime',release);
  assert.deepEqual(await lifecycle(f.broker).releaseOwned([f.x.id]),[]);
  assert.deepEqual(retired,[f.old,f.fresh].map(runtime=>({address:runtime.endpoint,runtimeId:runtime.runtimeId})));
  assert.equal(runtimeFor(home,{id:f.fresh.runtimeId}),undefined);
  assert.ok(runtimeFor(home,{id:unrelated}));
});

test('退订期间重新配对，旧回收保留当前路由与后台',async t=>{
  const f=unpairedReplacement(t);
  let entered!:()=>void,continueRelease!:()=>void;
  const started=new Promise<void>(resolve=>{entered=resolve;}),resume=new Promise<void>(resolve=>{continueRelease=resolve;});
  t.mock.method(f.codex,'release',async()=>{entered();await resume;});
  const releasing=lifecycle(f.broker).releaseOwned([f.x.id]);
  try{await Promise.race([started,releasing.then(()=>assert.fail('退订尚未开始'))]);f.store.pair(f.c.id,f.x.id);}finally{continueRelease();await releasing;}
  assert.equal(f.store.endpoint(f.x.id)?.runtimeId,f.old.runtimeId);
  assert.equal(f.store.peer(f.x.id).state,'ready');
  assert.equal(f.codex.runtimeReleased,false);
});

test('替代后台发信号前发生用户接管，停止权限即时失效',async t=>{
  const f=unpairedReplacement(t);let signalled=false;
  const release:CodexAdapter['releaseRuntime']=async(_refs,address,_peers,options)=>{
    if(address!==f.fresh.endpoint)return undefined;
    assert.ok(options?.guard);await Promise.resolve();f.store.updatePeer(f.x.id,{owner:'user',state:'ready'});
    options.guard();signalled=true;return undefined;
  };
  t.mock.method(f.codex,'releaseRuntime',release);
  assert.deepEqual(await lifecycle(f.broker).releaseOwned([f.x.id]),[]);
  assert.equal(signalled,false);
  assert.equal(f.store.peer(f.x.id).state,'ready');
  assert.ok(runtimeFor(f.broker.home,{id:f.fresh.runtimeId}));
});

test('回收途中关闭成员保留预约，重新开启后才继续回收',async t=>{
  const f=unpairedReplacement(t),manager=lifecycle(f.broker);
  const release:CodexAdapter['releaseRuntime']=async(_refs,address,_peers,options)=>{
    if(address===f.fresh.endpoint)f.store.updatePeer(f.x.id,{enabled:false});
    options?.guard?.();return undefined;
  };
  const blocked=t.mock.method(f.codex,'releaseRuntime',release);
  const retirement=()=>f.store.setting<Record<string,unknown>>('processRetirements')?.[f.x.id];
  const ids=manager.queueRetirements([f.x.id]),queued=retirement();assert.ok(queued);
  await manager.releaseOwned(ids);
  assert.equal(blocked.mock.callCount(),2);assert.deepEqual(retirement(),queued);
  await f.broker.pump();await manager.waitForRelease(f.x.id);
  assert.equal(blocked.mock.callCount(),2);assert.deepEqual(retirement(),queued);
  blocked.mock.restore();f.store.updatePeer(f.x.id,{enabled:true});
  await f.broker.pump();await manager.waitForRelease(f.x.id);
  assert.equal(f.codex.runtimeReleased,true);assert.equal(retirement(),undefined);
});

test('无可推进身份的原生启动记录立即停止自动重试，显式回收仍可重新核验',async t=>{
  const f=fixture(t);f.store.updatePeer(f.c.id,{owner:'nxbridge'});
  (f.claude as any).release=async()=>{throw new BridgeError('claude_release_unresolved','仅剩未结算记录');};
  await lifecycle(f.broker).releaseOwned([f.c.id]);
  const pending=f.store.setting<any>('processRetirements')[f.c.id];assert.equal(pending.attempts,1);assert.equal(pending.nextAt,Number.MAX_SAFE_INTEGER);
  await f.broker.pump();assert.equal(f.store.setting<any>('processRetirements')[f.c.id].attempts,1);
  (f.claude as any).release=async()=>{};assert.equal((await f.broker.dispatch('runtime.retire',{peer_id:f.c.id})as any).released,true);
});

test('残留后台区分历史失败与当前归属诊断，活动配对及用户窗口拒绝回收',async t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);
  await assert.rejects(f.broker.dispatch('runtime.retire',{peer_id:f.x.id}),{code:'runtime_in_use'});
  f.store.updatePeer(f.x.id,{owner:'nxbridge'});await assert.rejects(f.broker.dispatch('runtime.retire',{peer_id:f.x.id}),{code:'runtime_in_use'});
  f.store.unpair(pair.id);(f.codex as any).releaseRuntime=async()=> 'socket 对端独占性无法核验';
  for(let index=0;index<3;index++)await lifecycle(f.broker).releaseOwned([f.x.id]);
  const status:any=await f.broker.dispatch('status',{}),retired=status.capabilities.retirements.find((value:any)=>value.peerId===f.x.id);
  assert.equal(retired.exhausted,true);assert.equal(retired.nextAt,Number.MAX_SAFE_INTEGER);assert.match(retired.lastFailure,/socket 对端独占性无法核验/);assert.equal(retired.action,'nxbridge retire --member '+f.x.id);
  assert.equal(retired.state,'record-only');assert.deepEqual(retired.processes,[]);assert.match(retired.currentReason,/未结算记录/);assert.equal(retired.error,undefined);
  (f.codex as any).releaseRuntime=async()=>undefined;
  assert.equal((await f.broker.dispatch('runtime.retire',{peer_id:f.x.id}) as any).released,true);assert.equal(f.store.setting<any>('processRetirements')[f.x.id],undefined);
});

test('显式 retire 可以撤销未知容器，自动回收继续保留且不报告整组退出',{skip:process.platform!=='win32'},async t=>{
  const f=fixture(t),home=(f.broker as any).home as string;f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  const record=await unknownContainerFixture(home,f.x.id);saveManagedRuntime(home,{id:record.proof.launchId!,peerId:f.x.id,owner:managedMarker(home),kind:'owned',state:'starting',createdAt:Date.now(),endpoint:f.store.endpoint(f.x.id)!.address});
  const sibling={...record,id:randomUUID(),role:'host-control' as const,windowsJob:undefined,completionFile:undefined};atomicJson(path.join(processRegistryDirectory(home),sibling.id+'.json'),sibling);
  try{
    (f.codex as any).releaseRuntime=async()=> '容器归属未知';
    await lifecycle(f.broker).releaseOwned([f.x.id]);assert.equal(ownedProcesses(home).records.length,2);assert.ok(runtimeFor(home,{peerId:f.x.id}));
    const result=await f.broker.dispatch('runtime.retire',{peer_id:f.x.id}) as any;
    assert.equal(result.released,true);assert.equal(result.unconfirmedContainersRetired,true);assert.match(result.notes.join(' '),/未确认.*全部退出/);assert.equal(f.store.setting<any>('processRetirements')[f.x.id],undefined);
    assert.deepEqual(ownedProcesses(home).records,[]);assert.equal(runtimeFor(home,{peerId:f.x.id}),undefined);assert.equal(existsSync(record.completionFile!),false);
  }finally{forgetOwnedProcess(record);forgetOwnedProcess(sibling);}
});

test('显式 retire 不能删除已有但尚无法核验的原生 PID 预约',{skip:process.platform!=='win32'},async t=>{
  const f=fixture(t),home=(f.broker as any).home as string;f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  const record=await unknownContainerFixture(home,f.x.id);saveManagedRuntime(home,{id:record.proof.launchId!,peerId:f.x.id,owner:managedMarker(home),kind:'owned',state:'starting',createdAt:Date.now(),endpoint:f.store.endpoint(f.x.id)!.address,pid:process.pid,procStart:''});
  try{
    (f.codex as any).releaseRuntime=async()=> '原生代次尚无法核验';
    const result=await f.broker.dispatch('runtime.retire',{peer_id:f.x.id}) as any;
    assert.equal(result.released,false);assert.equal(result.unconfirmedContainersRetired,undefined);assert.ok(runtimeFor(home,{peerId:f.x.id}));assert.equal(ownedProcesses(home).records.length,1);assert.ok(f.store.setting<any>('processRetirements')[f.x.id]);
  }finally{forgetOwnedProcess(record);}
});
