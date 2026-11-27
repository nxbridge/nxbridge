import test,{type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {RpcClient} from '../ipc/rpc.js';
import {CodexOwnedRoutes} from '../hosts/codex/codex-owned-routes.js';
import {Takeovers} from '../broker/recovery/takeover.js';
import {CodexForegroundWindows} from '../hosts/codex/codex-foreground.js';
import {saveManagedRuntime} from '../hosts/managed-runtimes.js';
import {managedMarker} from '../install/installation.js';
import {processIdentity} from '../process/process.js';
import {codexDeliveryHoldText} from '../broker/delivery/codex-delivery-hold-text.js';
import {hostingRecord,writeHosting} from '../broker/recovery/session-hosting.js';
import {formatGlobalStatus} from '../integration/mcp/presentation.js';
import {RuntimeRecoveryHold} from '../shared/errors.js';
import {type Peer,type Endpoint} from '../types.js';
import {fixture,lifecycle} from './support/broker-fixture.js';

async function setup(t:TestContext){
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),identity=await processIdentity(process.pid),runtimeId=randomUUID();
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});
  f.store.saveEndpoint({...f.store.endpoint(f.x.id)!,mode:'app-server',address:'ws://127.0.0.1:9123',
    active:true,runtimeId,pid:process.pid,procStart:identity.startedAt,configHome:f.broker.home});
  saveManagedRuntime(f.broker.home,{id:runtimeId,peerId:f.x.id,owner:managedMarker(f.broker.home),kind:'owned',state:'ready',
    endpoint:'ws://127.0.0.1:9123',authenticatedLoopback:true,pid:process.pid,procStart:identity.startedAt,createdAt:Date.now()});
  const routes=new CodexOwnedRoutes(),rpc=new RpcClient(()=>{},()=>{});
  t.after(()=>rpc.close());
  routes.bind(id=>lifecycle(f.broker).ownedCodexRoute(id));
  const peer=()=>f.store.peer(f.x.id),endpoint=()=>f.store.endpoint(f.x.id)!;
  const confirm=()=>routes.subscribed({peer:peer(),endpoint:endpoint()},rpc);
  f.codex.hasSubscription=(p,e)=>routes.permits({peer:p,endpoint:e},rpc);
  Object.assign(f.codex,{attach:async(p:Peer,e:Endpoint,options?:{prepare?:()=>void})=>{
    options?.prepare?.();f.codex.attached.push({peer:p,endpoint:e});
    if(f.codex.attachFailure)throw f.codex.attachFailure;
    f.store.saveEndpoint({...e,active:true});confirm();return rpc;
  }});
  confirm();
  const takeovers=new Takeovers(f.store),lease=()=>takeovers.begin(peer(),endpoint());
  const wake=()=>((f.broker as unknown as {signalCodexWake(p:Peer,e:Endpoint):Promise<unknown>}).signalCodexWake(peer(),endpoint()));
  const hosting=()=>((f.broker.status() as any).peers.find((p:Peer)=>p.id===f.x.id).hosting);
  return{...f,pair,routes,rpc,peer,endpoint,confirm,takeovers,lease,wake,hosting};
}

test('远程窗口打开后保留原后台的物理订阅许可',async t=>{
  const f=await setup(t),route=lifecycle(f.broker).ownedCodexRoute(f.x.id)!;
  assert.ok(route);assert.equal(f.routes.permits(route,f.rpc),true);
  f.lease();
  assert.equal(f.routes.permits(route,f.rpc),true,'接管窗口沿用同一后台时，已核验订阅不能因此失效');
});

for(const ordinary of [false,true])test('租约结束只撤下本次接管的核验失败，不清除后来普通恢复的门禁：'+ordinary,async t=>{
  const f=await setup(t),foreground={pid:91231,procStart:'ended-window'};
  const takeovers=new Takeovers(f.store,async rows=>rows.map(row=>row.pid===foreground.pid?'dead':'alive'),undefined,undefined,()=>false);
  const lease=takeovers.begin(f.peer(),f.endpoint());takeovers.claim(f.x.id,lease.id,foreground);
  const event={memberId:f.x.id,sessionId:f.peer().sessionId,runtimeId:f.endpoint().runtimeId,endpoint:f.endpoint().address};
  f.codex.emit('sessionVerificationFailed',{...event,takeover:true,error:{code:'takeover_profile_unverified',message:'原窗口记录尚未确认'}});
  assert.equal(f.hosting().phase,'blocked');
  if(ordinary){
    f.codex.emit('sessionVerifying',{...event,runtimeId:'ordinary-restart'});
    f.codex.emit('sessionVerificationFailed',{...event,runtimeId:'ordinary-restart',error:{code:'policy_verification_failed',message:'普通恢复仍待核验'}});
  }
  assert.equal(await takeovers.returned(f.x.id,lease.id),true);
  assert.equal(f.hosting().phase,ordinary?'blocked':'background');
  assert.equal(hostingRecord(f.store,f.peer())?.verificationError?.code,ordinary?'policy_verification_failed':undefined);
  assert.equal(f.codex.ensured,0);assert.deepEqual(f.codex.created,[]);assert.deepEqual(f.codex.released,[]);
});

test('接管前台结局未知仍沿用原订阅投递，退出租约后不重复送信或唤醒',async t=>{
  const f=await setup(t);let foreground:'unknown'|'dead'='unknown';
  const takeovers=new Takeovers(f.store,async rows=>rows.map(row=>row.pid===91231?foreground:'alive'),undefined,undefined,()=>false);
  const lease=takeovers.begin(f.peer(),f.endpoint());takeovers.claim(f.x.id,lease.id,{pid:91231,procStart:'foreground'});
  const message=f.store.accept(f.pair,f.c,'原任务继续','held-task');
  assert.equal(await takeovers.returned(f.x.id,lease.id),false);
  await f.wake();assert.equal(f.store.message(message.id).state,'contextObserved');assert.equal(f.store.message(message.id).attempts,1);
  assert.equal(f.codex.injected.length,1);assert.equal(f.codex.woken.length,1);
  foreground='dead';assert.equal(await takeovers.returned(f.x.id,lease.id),true);
  await f.wake();assert.equal(f.codex.injected.length,1);assert.equal(f.codex.woken.length,1);
  assert.equal(f.codex.ensured,0);assert.deepEqual(f.codex.created,[]);assert.deepEqual(f.codex.released,[]);
});

test('接管中的忙碌会话保持既有取件语义，空闲后只接续一次',async t=>{
  const f=await setup(t);f.lease();f.store.updatePeer(f.x.id,{state:'busy'});
  const message=f.store.accept(f.pair,f.c,'忙碌时到达','busy-task');
  await f.wake();assert.equal(f.store.message(message.id).state,'accepted');assert.equal(f.codex.woken.length,0);
  assert.equal(f.hosting().delivery_hold.code,'session_busy');
  f.store.updatePeer(f.x.id,{state:'ready'});await f.wake();await f.wake();
  assert.equal(f.store.message(message.id).state,'contextObserved');assert.equal(f.store.message(message.id).attempts,1);
  assert.equal(f.codex.injected.length,1);assert.equal(f.codex.woken.length,1);
});

test('窗口新输入与来信竞态不另开轮次，原生空闲后按原消息投递',async t=>{
  const f=await setup(t);f.lease();const read=f.codex.injectionState.bind(f.codex);
  const message=f.store.accept(f.pair,f.c,'与人工输入同时到达','input-race');
  f.codex.injectionState=async(...args)=>{
    const value=await read(...args);f.store.updatePeer(f.x.id,{state:'busy'});
    return{...value,thread:{...value.thread,status:{type:'active'},turns:[{id:'user-turn',status:'inProgress'}]}};
  };
  await f.wake();assert.equal(f.store.message(message.id).state,'accepted');assert.equal(f.store.message(message.id).attempts,0);
  assert.equal(f.codex.injected.length,0);assert.equal(f.codex.woken.length,0);
  f.codex.injectionState=read;f.store.updatePeer(f.x.id,{state:'ready'});await f.wake();await f.wake();
  assert.equal(f.store.message(message.id).state,'contextObserved');assert.equal(f.codex.injected.length,1);assert.equal(f.codex.woken.length,1);
});

test('接管期间丢失订阅时仅修复原连接，完整准备及重启和接班认领仍被拦住',async t=>{
  const f=await setup(t);f.lease();f.routes.revoke(f.endpoint().address,f.peer().sessionId!);
  await lifecycle(f.broker).schedulePairedRecovery(f.x.id,true);
  assert.equal(f.codex.hasSubscription(f.peer(),f.endpoint()),true);assert.equal(f.codex.attached.length,1);
  await assert.rejects(lifecycle(f.broker).prepare(f.peer()),{code:'foreground_active'});
  await assert.rejects((f.broker as any).restartCodex({peer_id:f.x.id}),{code:'foreground_active'});
  assert.deepEqual((f.broker as any).handoffRoutes(),[]);
  assert.equal(f.codex.ensured,0);assert.deepEqual(f.codex.created,[]);assert.deepEqual(f.codex.released,[]);
});

test('未取得订阅的真实原因同时显示在托管状态和发送方既有提醒中',async t=>{
  const f=await setup(t);f.lease();f.routes.revoke(f.endpoint().address,f.peer().sessionId!);
  t.mock.timers.enable({apis:['Date'],now:Date.now()});
  f.store.accept(f.pair,f.c,'等待已核验订阅','unsubscribed');t.mock.timers.tick(120001);
  assert.equal(f.hosting().delivery_hold.code,'owned_subscription_waiting','尚未尝试重接，不能先断言窗口归属无法核验');
  f.codex.attachFailure=new RuntimeRecoveryHold('takeover_unverified','持有者尚未确认');
  await lifecycle(f.broker).schedulePairedRecovery(f.x.id,true);
  const status=f.hosting(),direct=await f.broker.dispatch('hosting.status',{peer_id:f.x.id}) as any;
  assert.equal(status.wake_capable,true);assert.equal(status.delivery_hold.code,'takeover_unverified');
  assert.equal(status.delivery_hold.status,codexDeliveryHoldText('takeover_unverified').status);
  assert.equal(status.delivery_hold.model,codexDeliveryHoldText('takeover_unverified').model);
  assert.deepEqual(direct.hosting.delivery_hold,status.delivery_hold);
  f.broker.pairAnomalies.observe(f.pair,f.peer());
  const issue=f.broker.pairAnomalies.issues(f.x.id).find(row=>row.kind==='delivery_accepted')!;
  assert.ok(issue);assert.ok(issue.message.includes(codexDeliveryHoldText('takeover_unverified').model));
  assert.ok(issue.message.includes('takeover_unverified'));
  assert.ok(formatGlobalStatus(f.broker.status()).includes('消息暂缓：'+status.delivery_hold.status));
  await f.wake();assert.equal(f.codex.woken.length,0,'有唤醒通道不代表本次已取得订阅许可');
  f.confirm();assert.equal(f.hosting().wake_capable,true);assert.equal(f.hosting().delivery_hold,undefined);
});

test('过期接管租约不能借用同地址的新原生代次',async t=>{
  const f=await setup(t);f.lease();f.store.saveEndpoint({...f.endpoint(),procStart:'replacement',native:{pid:process.pid,procStart:'replacement'}});
  assert.equal(lifecycle(f.broker).ownedCodexRoute(f.x.id),undefined);
  assert.equal(await lifecycle(f.broker).schedulePairedRecovery(f.x.id,true),false);
  assert.equal(f.codex.attached.length,0);assert.equal(f.hosting().delivery_hold.code,'takeover_unverified');
  assert.equal(f.codex.ensured,0);
});

for(const second of ['alive','unknown'] as const)test('同会话另一窗口仍为 '+second+' 时不结束接管租约',async t=>{
  const f=await setup(t);
  const first={pid:91231,procStart:'first'},other={pid:91232,procStart:'second'};
  const probe=async(rows:{pid:number;procStart:string}[])=>rows.map(row=>row.pid===first.pid?'dead' as const:row.pid===other.pid?second:'alive' as const);
  const takeovers=new Takeovers(f.store,probe,undefined,undefined,()=>false),lease=takeovers.begin(f.peer(),f.endpoint());
  takeovers.claim(f.x.id,lease.id,first);
  const windows=new CodexForegroundWindows(f.store,async()=>assert.fail('另一个窗口尚未确认退出'),probe);
  windows.register({endpoint:f.endpoint().address,sessionId:f.peer().sessionId!,foreground:first});
  windows.register({endpoint:f.endpoint().address,sessionId:f.peer().sessionId!,foreground:other});
  assert.equal(await takeovers.returned(f.x.id,lease.id),false);
  assert.ok(takeovers.get(f.x.id));
  assert.equal(f.routes.permits(lifecycle(f.broker).ownedCodexRoute(f.x.id)!,f.rpc),true);
});

test('核验前台退出期间新增同会话窗口，旧快照不结清租约',async t=>{
  const f=await setup(t),first={pid:91231,procStart:'first'},other={pid:91232,procStart:'second'};
  let enter!:()=>void,resume!:()=>void;
  const entered=new Promise<void>(resolve=>{enter=resolve;}),release=new Promise<void>(resolve=>{resume=resolve;});
  const takeovers=new Takeovers(f.store,async rows=>{
    enter();await release;return rows.map(row=>row.pid===first.pid?'dead' as const:'alive' as const);
  },undefined,undefined,()=>false),lease=takeovers.begin(f.peer(),f.endpoint());
  takeovers.claim(f.x.id,lease.id,first);
  const returning=takeovers.returned(f.x.id,lease.id);await entered;
  const windows=new CodexForegroundWindows(f.store,async()=>{});
  windows.register({endpoint:f.endpoint().address,sessionId:f.peer().sessionId!,foreground:other});
  resume();assert.equal(await returning,false);assert.ok(takeovers.get(f.x.id));
  assert.equal(f.codex.ensured,0);assert.deepEqual(f.codex.released,[]);
});

for(const state of ['alive','unknown','dead'] as const)test('追加订阅仅由同代次存活持有者授权：'+state,async t=>{
  const f=await setup(t),foreground={pid:91231,procStart:'verified-holder'};
  const takeovers=new Takeovers(f.store,async rows=>rows.map(row=>row.pid===foreground.pid?state:'alive'),undefined,undefined,()=>true);
  const lease=takeovers.begin(f.peer(),f.endpoint());takeovers.claim(f.x.id,lease.id,foreground);
  f.routes.revoke(f.endpoint().address,f.peer().sessionId!);
  if(state!=='alive'){
    await assert.rejects(takeovers.codexSubscriptionGuard(f.peer(),f.endpoint()),{code:'takeover_unverified'});
    assert.equal(f.hosting().delivery_hold.code,'owned_subscription_waiting');return;
  }
  const guard=await takeovers.codexSubscriptionGuard(f.peer(),f.endpoint());guard();
  f.store.saveEndpoint({...f.endpoint(),procStart:'new-generation'});
  assert.throws(guard,{code:'takeover_unverified'});
});

test('另一明确同会话窗口可授权追加订阅，未知会话窗口只保护租约不授予许可',async t=>{
  const f=await setup(t),foreground={pid:91231,procStart:'other-holder'};
  const takeovers=new Takeovers(f.store,async rows=>rows.map(()=>'alive'),undefined,undefined,()=>true);
  takeovers.begin(f.peer(),f.endpoint());
  const windows=new CodexForegroundWindows(f.store,async()=>{});
  windows.register({endpoint:f.endpoint().address,foreground});
  await assert.rejects(takeovers.codexSubscriptionGuard(f.peer(),f.endpoint()),{code:'takeover_unverified'});
  windows.register({endpoint:f.endpoint().address,sessionId:f.peer().sessionId!,foreground});
  const guard=await takeovers.codexSubscriptionGuard(f.peer(),f.endpoint());guard();
  f.store.setSetting('codexForegroundWindows',[]);assert.throws(guard,{code:'takeover_unverified'});
});

test('核验存活期间接管租约结束，迟到证明不能授权追加订阅',async t=>{
  const f=await setup(t),foreground={pid:91231,procStart:'old-holder'};
  let enter!:()=>void,resume!:()=>void;
  const entered=new Promise<void>(resolve=>{enter=resolve;}),release=new Promise<void>(resolve=>{resume=resolve;});
  const takeovers=new Takeovers(f.store,async rows=>{enter();await release;return rows.map(()=>'alive');},undefined,undefined,()=>true);
  const lease=takeovers.begin(f.peer(),f.endpoint());takeovers.claim(f.x.id,lease.id,foreground);
  const proof=assert.rejects(takeovers.codexSubscriptionGuard(f.peer(),f.endpoint()),{code:'takeover_unverified'});
  await entered;f.store.setSetting('takeover:'+f.x.id,null);resume();await proof;
});

test('恢复原订阅途中打开接手窗口，交给接手路径而不记录启动失败或中断投递',async t=>{
  const f=await setup(t),attach=f.codex.attach.bind(f.codex);let enter!:()=>void,resume!:()=>void;
  const entered=new Promise<void>(resolve=>{enter=resolve;}),release=new Promise<void>(resolve=>{resume=resolve;});
  Object.assign(f.codex,{attach:async(...args:Parameters<typeof attach>)=>{enter();await release;return attach(...args);}});
  const preparing=lifecycle(f.broker).prepare(f.peer());await entered;f.lease();resume();await preparing;
  assert.equal(f.peer().state,'ready');assert.equal(f.peer().launchFailure,undefined);
  assert.notEqual(f.broker.managedRecovery.get(f.x.id)?.state,'blocked');
  const message=f.store.accept(f.pair,f.c,'接手竞态后的来信','takeover-prepare-race');await f.wake();
  assert.equal(f.store.message(message.id).state,'contextObserved');assert.equal(f.store.message(message.id).attempts,1);
  assert.equal(f.codex.ensured,0);assert.deepEqual(f.codex.created,[]);assert.deepEqual(f.codex.released,[]);
});

test('会话暂停与配对暂停分别说明，原生冲突展示不被暂停原因遮住',async t=>{
  const f=await setup(t);f.lease();f.store.updatePeer(f.x.id,{state:'paused',pauseReason:'restart_incomplete'});
  f.store.setSetting('managedRuntimeConflict:'+f.x.id,{sessionId:'other-root'});
  assert.equal(f.hosting().delivery_hold.code,'restart_incomplete');assert.equal(f.hosting().phase,'blocked');assert.equal(f.hosting().reason,'session_conflict');
  assert.equal(f.hosting().delivery_hold.model,codexDeliveryHoldText('restart_incomplete').model);
  f.store.updatePeer(f.x.id,{pauseReason:'interrupted'});assert.equal(f.hosting().delivery_hold.code,'peer_paused');
  f.store.setPaused(true,f.pair.id);
  assert.equal(f.hosting().delivery_hold.code,'pair_paused');assert.equal(f.hosting().phase,'blocked');assert.equal(f.hosting().reason,'session_conflict');
});

test('接手期间等待生效的模型设置说明窗口关闭条件，仍不把消息交给旧配置',async t=>{
  const f=await setup(t),lease=f.lease();f.broker.profiles.set(f.peer(),{model:'requested-model'});
  const message=f.store.accept(f.pair,f.c,'等设置生效再处理','takeover-profile');await f.wake();
  assert.equal(f.hosting().delivery_hold.code,'profile_pending_takeover');assert.equal(f.store.message(message.id).state,'accepted');assert.equal(f.codex.woken.length,0);
  assert.equal(await f.takeovers.returned(f.x.id,lease.id,true),true);assert.equal(f.hosting().delivery_hold.code,'profile_unverified');
});

for(const state of ['window_closed','awaiting_window','blocked','transition'] as const)test('托管等待原因保留实际下一步：'+state,async t=>{
  const f=await setup(t);
  if(state==='window_closed'){f.store.updatePeer(f.x.id,{owner:'user',state:'offline'});f.store.deactivate(f.x.id);}
  if(state==='awaiting_window'){f.store.updatePeer(f.x.id,{owner:'user',state:'offline'});writeHosting(f.store,f.peer(),{phase:'awaiting_window',released:true});}
  if(state==='blocked')writeHosting(f.store,f.peer(),{reason:'bootstrap_policy_unsupported'});
  if(state==='transition')writeHosting(f.store,f.peer(),{phase:'adopting',reason:undefined});
  const expected={window_closed:'hosting_window_closed',awaiting_window:'hosting_awaiting_window',blocked:'hosting_blocked',transition:'hosting_transition'};
  assert.equal(f.hosting().delivery_hold.code,expected[state]);
});

test('接手订阅暂缺安静退避，不累积故障，接回后沿用原消息一次投递',async t=>{
  const f=await setup(t);f.lease();f.routes.revoke(f.endpoint().address,f.peer().sessionId!);
  t.mock.timers.enable({apis:['Date'],now:Date.now()});
  f.codex.attachFailure=new RuntimeRecoveryHold('takeover_unverified','持有者尚未确认');
  const message=f.store.accept(f.pair,f.c,'等接回原连接','takeover-quiet');
  for(let tick=0;tick<5;tick++){assert.equal(await lifecycle(f.broker).schedulePairedRecovery(f.x.id,true),false);t.mock.timers.tick(1000);}
  assert.equal(f.codex.attached.length,1);assert.deepEqual(f.store.faults.status(),[]);assert.equal(f.peer().launchFailure,undefined);
  assert.equal(f.hosting().delivery_hold.code,'takeover_unverified');assert.equal(f.store.message(message.id).attempts,0);
  assert.match(f.hosting().delivery_hold.reason,/持有者尚未确认/);
  f.codex.attachFailure=new RuntimeRecoveryHold('owned_subscription_waiting','原生订阅回执尚未到达');
  assert.equal(await lifecycle(f.broker).schedulePairedRecovery(f.x.id,true),false);
  assert.equal(f.hosting().delivery_hold.code,'owned_subscription_waiting');assert.match(f.hosting().delivery_hold.reason,/原生订阅回执尚未到达/);
  t.mock.timers.tick(5000);
  f.codex.attachFailure=undefined;assert.equal(await lifecycle(f.broker).schedulePairedRecovery(f.x.id,true),true);await f.wake();
  assert.equal(f.codex.attached.length,3);assert.equal(f.hosting().delivery_hold,undefined);assert.equal(f.store.message(message.id).state,'contextObserved');assert.equal(f.store.message(message.id).attempts,1);
});

test('带恢复事故的准备竞态尚未接回订阅时，不提前公布同会话恢复成功',async t=>{
  const f=await setup(t),attach=f.codex.attach.bind(f.codex);let enter!:()=>void,resume!:()=>void;
  const entered=new Promise<void>(resolve=>{enter=resolve;}),release=new Promise<void>(resolve=>{resume=resolve;});
  f.broker.managedRecovery.observe(f.peer(),f.pair,f.endpoint());
  const settled=t.mock.method(f.broker.managedRecovery,'settled');
  (f.broker as any).runtimeHealth.probeRuntime=async()=>({});
  Object.assign(f.codex,{attach:async(...args:Parameters<typeof attach>)=>{enter();await release;return attach(...args);}});
  const recovering=lifecycle(f.broker).recover(f.peer(),'隔离夹具核验接手竞态');await entered;
  f.lease();f.routes.revoke(f.endpoint().address,f.peer().sessionId!);
  f.codex.attachFailure=new RuntimeRecoveryHold('takeover_unverified','窗口归属仍未核验');resume();
  assert.equal(await recovering,false);assert.equal(settled.mock.callCount(),0);
  assert.equal(f.codex.hasSubscription(f.peer(),f.endpoint()),false);assert.equal(f.hosting().delivery_hold.code,'takeover_unverified');
});

test('接手期间显式轮次恢复不发送中断或继续输入，预览也不提供恢复命令',async t=>{
  const f=await setup(t);f.lease();
  const recover=(f.broker as any).recoverTurn.bind(f.broker);
  await assert.rejects(recover('stalled',false,'lease-recover',{peer_id:f.x.id}),{code:'foreground_active'});
  (f.broker as any).runtimeHealth.probeTurnHealth=async()=>({turnId:'stalled',state:'running',lastProgressAt:Date.now()-60000,tools:[]});
  const preview=await recover(undefined,true,undefined,{peer_id:f.x.id});
  assert.equal(preview.recoveryCommand,null);assert.equal(preview.recoveryBlocked,'foreground_active');
  assert.equal(f.codex.connected.length,0);assert.equal(f.codex.woken.length,0);assert.equal(f.codex.injected.length,0);
});

test('轮次恢复连接期间出现接手窗口，副作用前的复核阻止中断和继续输入',async t=>{
  const f=await setup(t);let requests=0;
  (f.broker as any).runtimeHealth.probeTurnHealth=async()=>({turnId:'stalled',state:'running',lastProgressAt:Date.now()-60000,tools:[]});
  Object.assign(f.codex,{connect:async()=>{f.lease();return{request:async()=>{requests++;assert.fail('接手之后不能提交轮次控制请求');}};}});
  await assert.rejects((f.broker as any).recoverTurn('stalled',false,'race-recover',{peer_id:f.x.id}),{code:'foreground_active'});
  assert.equal(requests,0);assert.equal(f.codex.woken.length,0);assert.equal(f.codex.injected.length,0);
});
