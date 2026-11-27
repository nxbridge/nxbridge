import test from 'node:test';
import assert from 'node:assert/strict';
import {setImmediate as settle} from 'node:timers/promises';
import type {BrokerHealthCoordinator} from '../broker/health/coordinator.js';
import {fixture,lifecycle} from './support/broker-fixture.js';
import {registration} from './support/registration.js';
import {CODEX_WAKE_RETRY_MS} from '../hosts/codex/codex-wake.js';

interface ScheduledBroker {
  armSchedulers():void;
  scheduling:boolean;
  pumping?:Promise<void>;
  runtimeHealth:BrokerHealthCoordinator;
  wakeScheduledDelivery(id:string):void;
}

test('离线待投递只驱动目标恢复，正常目标仍在下一次一秒检查推进',async t=>{
  t.mock.timers.enable({apis:['Date','setTimeout'],now:100000});
  t.mock.method(performance,'now',()=>Date.now());
  const f=fixture(t),broker=f.broker as unknown as ScheduledBroker,members=lifecycle(f.broker),pair=f.store.pair(f.c.id,f.x.id);
  const endpoint=f.store.endpoint(f.c.id)!;
  f.store.updatePeer(f.c.id,{owner:'nxbridge',state:'offline'});
  f.store.deactivate(f.c.id,endpoint.epoch);
  const msg=f.store.accept(pair,f.x,'等待同一会话恢复','paused-inbox');
  const pump=t.mock.method(f.broker,'pump',()=>Promise.resolve()),recovery=t.mock.method(members,'schedulePairedRecovery',()=>Promise.resolve(false));
  t.mock.method(members,'schedulePairedRecoveries',()=>new Map());
  t.mock.method(members,'observeManagedExits',()=>Promise.resolve());
  t.mock.method(f.broker,'reconcile',()=>Promise.resolve(true));
  broker.scheduling=true;broker.armSchedulers();
  for(let i=0;i<4;i++){t.mock.timers.tick(1000);await settle();}
  assert.equal(pump.mock.callCount(),1);
  assert.deepEqual(recovery.mock.calls.map(call=>call.arguments),Array.from({length:4},()=>[f.c.id,true]));
  assert.equal(f.store.message(msg.id).state,'accepted');
  t.mock.timers.tick(1000);await settle();assert.equal(pump.mock.callCount(),2);
  f.store.saveEndpoint(endpoint);
  f.store.updatePeer(f.c.id,{state:'ready'});
  t.mock.timers.tick(1000);await settle();assert.equal(pump.mock.callCount(),3);
});

test('有待发消息的忙碌 Codex 不加速巡检，原生取件无需等定时器',async t=>{
  t.mock.timers.enable({apis:['Date','setTimeout'],now:100000});
  t.mock.method(performance,'now',()=>Date.now());
  const f=fixture(t),broker=f.broker as unknown as ScheduledBroker,pair=f.store.pair(f.c.id,f.x.id);
  f.store.updatePeer(f.x.id,{state:'busy'});
  const message=f.store.accept(pair,f.c,'交给正在工作的会话','busy-target');
  const pump=t.mock.method(f.broker,'pump',()=>Promise.resolve());
                                             
  t.mock.method(f.codex,'observeActive',async()=>{});
  broker.scheduling=true;broker.armSchedulers();
  for(let i=0;i<4;i++){t.mock.timers.tick(1000);await settle();}
  f.store.updatePeer(f.x.id,{state:'ready'});
  f.codex.emit('state',f.x.sessionId,'busy',f.store.endpoint(f.x.id)!.address);
  await settle();
  assert.equal(f.store.peer(f.x.id).state,'busy');
  assert.equal(pump.mock.callCount(),1);
  assert.equal(f.store.message(message.id).attempts,0);
  const before=Date.now();
  const actor={host:'codex' as const,sessionId:f.x.sessionId!,callId:'busy-pickup'};
  const result=f.broker.receive(actor) as {messages:{message_id:string}[]};
  assert.deepEqual(result.messages.map(row=>row.message_id),[message.id]);
  assert.equal(f.store.message(message.id).state,'unknown');
  await f.broker.dispatch('hook',{actor,session_id:f.x.sessionId,hook_event_name:'PostToolUse',tool_name:'claude.receive_messages',tool_response:result});
  assert.equal(Date.now(),before);
  assert.equal(f.store.message(message.id).state,'contextObserved');
});

test('歧义端点进入成员级退避时，其他成员仍按一秒时钟投递和恢复',async t=>{
  t.mock.timers.enable({apis:['Date','setTimeout'],now:100000});
  t.mock.method(performance,'now',()=>Date.now());
  const f=fixture(t),broker=f.broker as unknown as ScheduledBroker,members=lifecycle(f.broker);
  const brokenPair=f.store.pair(f.c.id,f.x.id),broken=f.store.endpoint(f.c.id)!;
  f.store.saveEndpoint({...broken,epoch:'duplicate-endpoint'});
  f.store.accept(brokenPair,f.x,'目标端点存在歧义','ambiguous');
  const offline=f.store.register(registration('claude','offline-scheduling',{state:'offline'})),offlineSender=f.store.register(registration('codex','offline-sender',{state:'ready'}));
  f.store.deactivate(offline.id);f.store.updatePeer(offline.id,{owner:'nxbridge'});
  f.store.accept(f.store.pair(offline.id,offlineSender.id),offlineSender,'独立恢复','offline');
  const healthy=f.store.register(registration('claude','healthy-scheduling',{state:'ready'})),healthySender=f.store.register(registration('codex','healthy-sender',{state:'ready'}));
  const healthyPair=f.store.pair(healthy.id,healthySender.id);
  const pump=t.mock.method(f.broker,'pump',()=>Promise.resolve()),recovery=t.mock.method(members,'schedulePairedRecovery',()=>Promise.resolve(false));
  t.mock.method(members,'schedulePairedRecoveries',()=>new Map());
  t.mock.method(members,'observeManagedExits',()=>Promise.resolve());
  t.mock.method(f.broker,'reconcile',()=>Promise.resolve(true));
  broker.scheduling=true;broker.armSchedulers();
  for(let i=0;i<2;i++){t.mock.timers.tick(1000);await settle();}
  assert.equal(f.store.faults.ready('delivery_ready:'+f.c.id),false);
  assert.equal(f.store.faults.ready('timer_pending'),true);
  assert.equal(recovery.mock.calls.filter(call=>call.arguments[0]===offline.id).length,2);
  const previous=pump.mock.callCount();
  f.store.accept(healthyPair,healthySender,'健康目标','healthy');
  t.mock.timers.tick(1000);await settle();
  assert.equal(pump.mock.callCount(),previous+1);
  assert.equal(f.store.faults.ready('delivery_ready:'+f.c.id),false);
});

test('远程唤醒仅在当前预约到期后推进，耗尽的老预约不遮住新收件锚点',async t=>{
  t.mock.timers.enable({apis:['Date','setTimeout'],now:100000});
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);
  f.store.saveEndpoint({...f.store.endpoint(f.x.id)!,address:'ws://remote-test'});
  const first=f.store.accept(pair,f.c,'原来的收件','first');
  const broker=f.broker as unknown as ScheduledBroker & {deliveryMayAdvance(id:string):boolean};
  const eligible=()=>broker.deliveryMayAdvance(f.x.id);
  assert.equal(eligible(),true);
  for(let i=0;i<3;i++){
    await f.broker.pump();assert.equal(eligible(),false);
    t.mock.timers.tick(CODEX_WAKE_RETRY_MS);assert.equal(eligible(),true);
  }
  await f.broker.pump();assert.equal(f.codex.woken.length,3);
  assert.equal(eligible(),false);
  f.store.transition(first.id,'contextObserved');
  f.store.accept(pair,f.c,'新收件','next');
  assert.equal(eligible(),true);
});

test('嵌入式严格模式与投递或恢复阶段退避不会加速巡检',async t=>{
  t.mock.timers.enable({apis:['Date','setTimeout'],now:100000});
  t.mock.method(performance,'now',()=>Date.now());
  const f=fixture(t,{compatibilityWake:'strict'}),broker=f.broker as unknown as ScheduledBroker;
  const pair=f.store.pair(f.c.id,f.x.id),endpoint=f.store.endpoint(f.x.id)!;
  f.store.saveEndpoint({...endpoint,mode:'embedded'});
  f.store.accept(pair,f.c,'等待原生取件','strict');
  const pump=t.mock.method(f.broker,'pump',()=>Promise.resolve());
  broker.scheduling=true;broker.armSchedulers();
  t.mock.timers.tick(1000);await settle();
  assert.equal(pump.mock.callCount(),1);
  f.store.saveEndpoint(endpoint);
  for(const name of ['delivery','member_recovery']){
    const stage=name+':'+f.x.id,before=pump.mock.callCount();
    for(let i=0;i<3;i++)f.store.faults.failed(stage,Error('隔离目标故障'));
    t.mock.timers.tick(1000);await settle();
    assert.equal(pump.mock.callCount(),before,stage);
    f.store.faults.recovered(stage);
    t.mock.timers.tick(1000);await settle();
    assert.equal(pump.mock.callCount(),before+1,stage);
  }
});

test('配对的 Codex 尚在接班补证时，Claude 待发消息不加速巡检',async t=>{
  t.mock.timers.enable({apis:['Date','setTimeout'],now:100000});
  t.mock.method(performance,'now',()=>Date.now());
  const f=fixture(t),broker=f.broker as unknown as ScheduledBroker & {startupRoutes:{begin(ids:string[]):void;finish(id:string):void}};
  const pair=f.store.pair(f.c.id,f.x.id);
  f.store.accept(pair,f.x,'等同一配对完成接班补证','startup-claude');
  const pump=t.mock.method(f.broker,'pump',()=>Promise.resolve());
  broker.startupRoutes.begin([f.x.id]);
  broker.scheduling=true;broker.armSchedulers();
  t.mock.timers.tick(1000);await settle();
  assert.equal(pump.mock.callCount(),1);
  broker.startupRoutes.finish(f.x.id);
  t.mock.timers.tick(1000);await settle();
  assert.equal(pump.mock.callCount(),2);
});

for(const stopping of [false,true])test('恢复登记与在途投递或关闭并发时只补查有效路线：'+stopping,async t=>{
  const f=fixture(t),broker=f.broker as unknown as ScheduledBroker,pair=f.store.pair(f.c.id,f.x.id);
  const endpoint=f.store.endpoint(f.c.id)!;
  f.store.updatePeer(f.c.id,{state:'ready'});f.store.saveEndpoint(endpoint);
  f.store.accept(pair,f.x,'恢复后重查新端点','restore-inbox');
  const pump=t.mock.method(f.broker,'pump',()=>Promise.resolve());
  let finish!:()=>void;
  broker.pumping=new Promise<void>(resolve=>{finish=resolve;});broker.scheduling=true;
  broker.wakeScheduledDelivery(f.c.id);broker.wakeScheduledDelivery(f.c.id);
  await settle();assert.equal(pump.mock.callCount(),0);
  const stop=stopping?f.broker.stop():undefined;
  broker.pumping=undefined;finish();
  await stop;await settle();assert.equal(pump.mock.callCount(),stopping?0:1);
});

test('存活的专用 Codex 在原生请求失去确认后立即重开传输探测',async t=>{
  const f=fixture(t),broker=f.broker as unknown as ScheduledBroker,members=lifecycle(f.broker);
  f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});
  const peer=f.store.peer(f.x.id),endpoint={...f.store.endpoint(f.x.id)!,mode:'app-server' as const,address:'ws://127.0.0.1:35264'};
  f.store.saveEndpoint(endpoint);
  const probe=t.mock.method(f.codex,'probe',async()=>({state:'reachable' as const,reasons:[],checked_at:Date.now(),threadConfirmed:true as const}));
  const ready=t.mock.method(f.broker.managedRecovery,'ready');
  await members.schedulePairedRecovery(peer.id,false);assert.equal(probe.mock.callCount(),0);assert.equal(ready.mock.callCount(),0);
  await members.schedulePairedRecovery(peer.id,true);assert.equal(probe.mock.callCount(),0,'仅有待发消息不要求对健康后台重复读线程');
  broker.scheduling=true;
  f.codex.emit('threadUnconfirmed',peer.sessionId,endpoint.address);
  await settle();await broker.runtimeHealth.waitForIdle();
  assert.equal(probe.mock.callCount(),1,'原生失败信号立即重开探测');
  await members.schedulePairedRecovery(peer.id,false);assert.equal(probe.mock.callCount(),1);
});

test('同一代次在复查期间再次失败，迟到的成功不撤销后续探测',async t=>{
  const f=fixture(t),health=(f.broker as unknown as ScheduledBroker).runtimeHealth;
  f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});
  const peer=f.store.peer(f.x.id),endpoint=f.store.endpoint(peer.id)!;
  let succeed!:()=>void;
  const pending=new Promise<void>(resolve=>{succeed=resolve;});
  let confirmed=true;
  t.mock.method(f.codex,'probe',async()=>{await pending;return{state:'reachable' as const,reasons:[],checked_at:Date.now(),...(confirmed?{threadConfirmed:true as const}:{})};});
  health.recordRuntimeProbeFailure(peer,endpoint);
  const checking=health.probeRuntime(peer,true);
  health.recordRuntimeProbeFailure(peer,endpoint);
  succeed();await checking;
  assert.equal(health.runtimeProbeFailed(peer,endpoint),true);
  confirmed=false;await health.probeRuntime(peer,true);
  assert.equal(health.runtimeProbeFailed(peer,endpoint),true,'没有线程回执的可达性不能清除原生失败');
  confirmed=true;
  await health.probeRuntime(peer,true);
  assert.equal(health.runtimeProbeFailed(peer,endpoint),false);
});
