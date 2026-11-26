import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { randomUUID } from 'node:crypto';
import type { CodexAdapter } from '../hosts/codex/adapter.js';
import { saveManagedRuntime } from '../hosts/managed-runtimes.js';
import { managedMarker } from '../install/installation.js';
import { Store } from '../storage/store.js';
import { BridgeError, RuntimeRecoveryHold } from '../shared/errors.js';
import { DeliveryHealth } from '../broker/health/delivery.js';
import { CodexRecycleControl } from '../broker/recovery/codex-recycle-control.js';
import { ManagedRecovery } from '../broker/recovery/managed-recovery.js';
import { RECYCLE_CLEANUP_USER_STEP } from '../broker/stuck-session-text.js';
import type { CodexRecycleWork } from '../broker/recovery/codex-recycle.js';
import { registration } from './support/registration.js';
import { temporary } from './support/temporary.js';

function recycleFixture(t:TestContext,initialWork:CodexRecycleWork,loadedUnleased=false){
  const home=temporary(t),store=new Store(':memory:');
  t.after(()=>store.close());
  const target=store.register({...registration('codex','recycle-root'),cwd:home,owner:'nxbridge'} as ReturnType<typeof registration>);
  store.updatePeer(target.id,{owner:'nxbridge',state:'ready'});
  const peer=store.peer(target.id),sender=store.register(registration('claude','originating-root')),pair=store.pair(sender.id,peer.id);
  const old={runtimeId:randomUUID(),endpoint:'ws://127.0.0.1:12345',pid:111,procStart:'old'};
  const fresh={runtimeId:randomUUID(),endpoint:'ws://127.0.0.1:12346',pid:222,procStart:'fresh'};
  const endpoint={...store.endpoint(peer.id)!,address:old.endpoint,runtimeId:old.runtimeId,pid:old.pid,procStart:old.procStart,configHome:home};
  store.saveEndpoint(endpoint);
  saveManagedRuntime(home,{id:old.runtimeId,endpoint:old.endpoint,pid:old.pid,procStart:old.procStart,owner:managedMarker(home),
    kind:'owned',state:'ready',peerId:peer.id,sessionId:peer.sessionId!,configHome:home,dedicated:true,createdAt:Date.now()});
  const recovery=new ManagedRecovery(store),delivery=new DeliveryHealth(store);
  let work=initialWork,oldAlive=true,afterObserve:(()=>void)|undefined,checkingHolders:(()=>void)|undefined,afterSignal:(()=>void)|undefined,
    retiringOld:(()=>void)|undefined,draining=false,lostAck=false,baseSubscription=false,observeFailed=false;
  const effects={stops:0,launches:0,subscriptions:0};
  const input={...registration('codex',peer.sessionId!),cwd:home,endpoint:{
    ...registration('codex',peer.sessionId!).endpoint,address:fresh.endpoint,epoch:fresh.runtimeId,runtimeId:fresh.runtimeId,
    pid:fresh.pid,procStart:fresh.procStart,configHome:home,
  }};
  const codex={hasSubscription:()=>baseSubscription,inspectRecycleOld:async()=>oldAlive?'alive':'dead',recycleHost:()=>({
    observe:async(trigger:string)=>{
      afterObserve?.();
      if(observeFailed)return {kind:'held',code:'codex_recycle_evidence_unverified',reason:'原进程已无法连接'};
      if(trigger==='loaded_unleased'&&work.progress==='idle')return {kind:'natural',idleLoaded:true};
      return {kind:'available',old,profile:{sourceId:old.runtimeId,sessionId:peer.sessionId,file:'rollout',settingsKey:'exact'},work};
    },
    persistProfile(){},inspectOld:async()=>oldAlive?'alive':'dead',stop:async(_old:unknown,_deadline:number,_guard:()=>void,markSignal:()=>void)=>{
      markSignal();effects.stops++;afterSignal?.();oldAlive=false;
    },retireOld:async()=>{retiringOld?.();},
    prepareLaunch:async()=>({runtimeId:fresh.runtimeId}),launch:async()=>{effects.launches++;},inspectLaunch:async()=>effects.launches?{kind:'matched',fresh}:{kind:'unstarted'},
    verify:async(_fresh:unknown,_profile:unknown,{markSubmitted}:{markSubmitted:()=>void})=>{
      markSubmitted();if(lostAck)throw new BridgeError('codex_recycle_ack_missing','无法证明原订阅回执');
    },
    prepareUnsubscribed:async()=>input,
    admission:()=>({input,receipts:[],policy:{source:'persisted'}}),subscribe:()=>{effects.subscriptions++;},
  })} as unknown as CodexAdapter;
  const control=new CodexRecycleControl({home,store,codex,recovery,delivery,stopped:()=>draining,scope:()=>pair.id,
    incident:target=>recovery.observe(target,pair,endpoint),noExternalHolder:async()=>{checkingHolders?.();},fold:()=>{}});
  const hold=loadedUnleased?Object.assign(new RuntimeRecoveryHold('owned_subscription_waiting','等待原生订阅'),{recycleEvidence:'loaded_unleased'}):
    new RuntimeRecoveryHold('native_thread_wedged','原生工作不可用');
  assert.equal(control.noteHold(peer,hold),true);
  return {store,peer,sender,pair,fresh,recovery,delivery,control,effects,
    advanceStable:async(acceptance?:Parameters<typeof control.advance>[1])=>{
      await control.advance(peer,acceptance);
      const stage=recovery.get(peer.id)?.recycle;
      if(stage?.phase==='observing'&&stage.basis){
        (control as unknown as {due:Map<string,number>}).due.delete(peer.id);
        await control.advance(peer);
      }
    },
    setWork:(next:CodexRecycleWork)=>{work=next;},
    observeHook:(hook?:()=>void)=>{afterObserve=hook;},
    holderHook:(hook?:()=>void)=>{checkingHolders=hook;},
    signalHook:(hook?:()=>void)=>{afterSignal=hook;},
    retireHook:(hook?:()=>void)=>{retiringOld=hook;},
    setDraining:(value:boolean)=>{draining=value;},
    loseAck:()=>{lostAck=true;},
    installBaseSubscription:()=>{baseSubscription=true;},
    exitBeforeObserve:()=>{oldAlive=false;observeFailed=true;},
    notices:(part:'decision'|'result')=>delivery.peek(pair.id,sender.id).filter(notice=>notice.kind?.startsWith('codex_recycle_'+part+':'))};
}

test('同一事故的实际消息账本保持，只有当前工作证据可接入并通知原发送方',async t=>{
  const work:CodexRecycleWork={activeTurnIds:['active'],progress:'stalled',lastProgressAt:10,roles:[],unresolved:[]};
  const {store,peer,sender,pair,fresh,recovery,delivery,control,effects,setWork,notices,advanceStable}=recycleFixture(t,work);
  const holding=store.accept(pair,sender,'尚未投递','held'),unknown=store.accept(pair,sender,'无法确认投递','unknown');
  store.claim(unknown.id);store.transition(unknown.id,'unknown');
  const originalUnknown=store.message(unknown.id);
  await advanceStable();
  const first=recovery.get(peer.id)!,initial=first.recycle;
  assert.equal(initial?.phase,'awaiting_partner');
  if(initial?.phase!=='awaiting_partner')return;
  const acceptance={incidentId:first.id,evidenceRevision:initial.basis.decision.revision,reason:'已核对可能中断的工作。'};
  setWork({...work,progress:'progressing',lastProgressAt:11});
  await advanceStable(acceptance);
  const current=recovery.get(peer.id)!.recycle;
  assert.equal(current?.phase,'awaiting_partner');
  assert.equal(effects.stops,0);assert.equal(effects.launches,0);
  assert.equal(notices('decision').length,1);
  if(current?.phase!=='awaiting_partner')return;
  store.updatePeer(sender.id,{state:'busy'});
  await control.advance(peer,{...acceptance,evidenceRevision:current.basis.decision.revision});
  assert.equal(recovery.get(peer.id)?.recycle?.phase,'ready');
  assert.deepEqual(effects,{stops:1,launches:1,subscriptions:1});
  assert.equal(store.endpoint(peer.id)?.runtimeId,fresh.runtimeId);
  assert.deepEqual(store.message(unknown.id),originalUnknown);
  assert.equal(store.message(holding.id).state,'accepted');
  const results=notices('result');
  assert.equal(results.length,1);assert.equal(results[0]?.wake,false);
  assert.deepEqual(results[0]!.affectedMessageIds,[unknown.id]);
  assert.ok(results[0]!.message.includes(unknown.id));
  store.updatePeer(sender.id,{state:'ready'});
  delivery.promoteIdleNotices(pair.id,sender.id);delivery.promoteIdleNotices(pair.id,sender.id);
  const wake=notices('result');
  assert.deepEqual(wake.map(notice=>({id:notice.id,wake:notice.wake})),[{id:results[0]!.id,wake:true}]);
  await control.advance(store.peer(peer.id),acceptance);
  assert.equal(effects.launches,1);
});

test('协调器交接在旧进程退出后暂停，恢复后沿用原尝试启动并重新接入',async t=>{
  let now=Date.now();t.mock.method(Date,'now',()=>now);
  const f=recycleFixture(t,{activeTurnIds:[],progress:'idle',roles:[],unresolved:[]});
  f.retireHook(()=>f.setDraining(true));await f.control.advance(f.peer);
  const paused=f.recovery.get(f.peer.id)?.recycle;
  assert.ok(paused?.phase==='held'&&paused.paused?.phase==='stopped'&&paused.suspend==='broker_restarting');
  assert.deepEqual(f.effects,{stops:1,launches:0,subscriptions:0});
  now+=60000;f.retireHook();f.setDraining(false);await f.control.advance(f.peer);
  const ready=f.recovery.get(f.peer.id)?.recycle;
  assert.ok(ready?.phase==='ready'&&ready.finalized&&ready.attempt.id===paused.paused.attempt.id);
  assert.deepEqual(f.effects,{stops:1,launches:1,subscriptions:1});
});

for(const leftover of [undefined,2])test(`${leftover===undefined?'无法确认':'已经核对'}剩余进程数量时，状态和通知使用同一证据`,async t=>{
  let now=Date.now();t.mock.method(Date,'now',()=>now);
  const f=recycleFixture(t,{activeTurnIds:[],progress:'idle',roles:[],unresolved:[]});
  const reason='原后台还有未核验结束的监督进程或后代，暂停恢复同一会话。';
  f.retireHook(()=>{throw Object.assign(new BridgeError('codex_recycle_old_holder',reason),{leftover});});
  await f.control.advance(f.peer);
  const status=f.control.summary(f.peer,f.recovery.get(f.peer.id)!) as {status_text:string};
  assert.match(status.status_text,leftover===undefined?/程序可能还在跑，还没确认清理干净/:/2 个程序还在跑、认不出归属/);
  assert.ok(status.status_text.endsWith(RECYCLE_CLEANUP_USER_STEP));
  now+=5001;await f.control.advance(f.peer);
  const notices=f.delivery.peek(f.pair.id,f.sender.id).filter(notice=>notice.kind?.startsWith('codex_recycle_held:'));
  assert.equal(notices.length,1);
  assert.match(notices[0]!.message,leftover===undefined?/processes it started may still be running \(not confirmed gone\)/:/2 processes it started are still running and unattested/);
  assert.ok(notices[0]!.message.includes(RECYCLE_CLEANUP_USER_STEP));
  assert.deepEqual(f.effects,{stops:1,launches:0,subscriptions:0});
});

test('移交普通接续登记新代次但不授予订阅，核验后只通知一次原发送方',async t=>{
  let now=Date.now();t.mock.method(Date,'now',()=>now);
  const f=recycleFixture(t,{activeTurnIds:[],progress:'idle',roles:[],unresolved:[]});
  const input=f.store.accept(f.pair,f.sender,'结果不明的原任务','uncertain');
  f.store.claim(input.id);f.store.transition(input.id,'unknown');
  f.loseAck();assert.equal(await f.control.advance(f.peer),false);
  const handed=f.recovery.get(f.peer.id)?.recycle;
  assert.ok(handed?.phase==='handback'&&handed.attempt.ledger.uncertain[0]?.id===input.id);
  assert.equal(f.store.endpoint(f.peer.id)?.runtimeId,f.fresh.runtimeId);
  assert.equal(f.control.fenced(f.peer.id),false);assert.deepEqual(f.notices('result'),[]);
  now+=5001;f.installBaseSubscription();await f.control.advance(f.store.peer(f.peer.id));
  now+=5001;await f.control.advance(f.store.peer(f.peer.id));
  const result=f.notices('result');assert.equal(result.length,1);assert.deepEqual(result[0]?.affectedMessageIds,[input.id]);
  assert.equal(f.store.message(input.id).state,'unknown');
  assert.deepEqual(f.effects,{stops:1,launches:1,subscriptions:0});
});

for(const {reason,by,work} of [
  {reason:'仅观测到未知进程',by:'partner',work:{activeTurnIds:[],progress:'unknown',roles:['conhost.exe'],unresolved:[]}},
  {reason:'原生工作证据缺失',by:'user',work:{activeTurnIds:[],progress:'unknown',roles:[],unresolved:['native_work_unavailable']}},
] satisfies {reason:string;by:'partner'|'user';work:CodexRecycleWork}[])test(`${reason}时，决策和结果都保留工作状态的不确定性`,async t=>{
  const beforeWork=by==='partner'?{...work,activeTurnIds:['prior-active'],roles:[]}:work;
  const {store,peer,sender,pair,recovery,delivery,control,effects,setWork,notices,advanceStable}=recycleFixture(t,beforeWork);
  await advanceStable();
  if(by==='partner'){
    const before=recovery.get(peer.id)!;
    assert.ok(before.recycle?.phase==='awaiting_partner');
    const stale={incidentId:before.id,evidenceRevision:before.recycle.basis.decision.revision,reason:'同意中断先前确认的活动轮次。'};
    setWork(work);
    await advanceStable(stale);
    const explanation=control.acceptanceText(peer,before,stale);
    assert.ok(explanation);
    assert.match(explanation,/not applied:.*no active turn seen/i);
    assert.doesNotMatch(explanation,/a different turn is running/i);
  }
  const incident=recovery.get(peer.id)!,decision=incident.recycle;
  assert.ok(decision?.phase==='awaiting_partner');
  const offer=notices('decision')[0];
  assert.ok(offer);
  assert.match(offer.message,/no active turn seen.*may cut/i);
  assert.doesNotMatch(offer.message,/cuts the running turn|nothing running is lost/i);
  const status=control.summary(peer,incident) as {status_text:string};
  assert.match(status.status_text,/没法确认它已经停下/);
  assert.equal(effects.stops,0);
  const busySender=by==='user';
  store.updatePeer(sender.id,{state:busySender?'busy':'ready'});
  await control.advance(peer,{incidentId:incident.id,evidenceRevision:decision.basis.decision.revision,by,reason:'同意可能损失未核验的工作。'});
  assert.equal(recovery.get(peer.id)?.recycle?.phase,'ready');
  const result=notices('result')[0];
  assert.ok(result);
  assert.doesNotMatch(result.message,/Its running turn was cut|nothing was cut|Cut work still needed/i);
  assert.match(result.message,/No active turn seen, but unconfirmed work may have been cut/);
  const userLine=store.setting<{line:string}>('codexRecycleUserLine:'+sender.id)?.line;
  assert.ok(userLine);
  assert.doesNotMatch(userLine,/那一轮中断了|当时没在干活|程序在跑/);
  assert.match(userLine,/如果当时还有工作在跑，可能被一起中断了/);
  assert.equal(result.wake,!busySender);
  assert.notEqual(result.consumesNoticeCredit,false);
  assert.deepEqual(result.affectedMessageIds,[]);
  if(busySender){
    store.updatePeer(sender.id,{state:'ready'});
    delivery.promoteIdleNotices(pair.id,sender.id);delivery.promoteIdleNotices(pair.id,sender.id);
    assert.deepEqual(notices('result').map(notice=>({id:notice.id,wake:notice.wake})),[{id:result.id,wake:true}]);
  }
  assert.deepEqual(effects,{stops:1,launches:1,subscriptions:1});
});

test('确认空闲时自动恢复只告知；确认轮次中断时即使没有未确认消息也唤醒空闲发送方',async t=>{
  const idle=recycleFixture(t,{activeTurnIds:[],progress:'idle',roles:[],unresolved:[]});
  await idle.control.advance(idle.peer);
  assert.equal(idle.recovery.get(idle.peer.id)?.recycle?.phase,'ready');
  assert.equal(idle.notices('decision').length,0);
  const passive=idle.notices('result')[0];
  assert.ok(passive);
  assert.equal(passive.wake,false);
  assert.equal(passive.consumesNoticeCredit,false);
  assert.match(passive.message,/It was idle; nothing was cut/);

  const active=recycleFixture(t,{activeTurnIds:['observed'],progress:'unknown',roles:[],unresolved:[]});
  await active.advanceStable();
  const incident=active.recovery.get(active.peer.id)!,decision=incident.recycle;
  assert.ok(decision?.phase==='awaiting_partner');
  assert.match(active.notices('decision')[0]!.message,/running turn/);
  await active.control.advance(active.peer,{incidentId:incident.id,evidenceRevision:decision.basis.decision.revision,reason:'同意中断已确认的活动轮次。'});
  const wake=active.notices('result')[0];
  assert.ok(wake);
  assert.equal(wake.wake,true);
  assert.deepEqual(wake.affectedMessageIds,[]);
  assert.match(wake.message,/running turn was cut/);
});

test('普通重连时只有活动轮次和安全待投递消息同时存在才提供回收选项',async t=>{
  const uncertain:CodexRecycleWork={activeTurnIds:[],progress:'unknown',roles:['tool'],unresolved:[]};
  const active:CodexRecycleWork={activeTurnIds:['turn'],progress:'stalled',roles:[],unresolved:[]};
  const unobserved=recycleFixture(t,uncertain,true);
  unobserved.store.accept(unobserved.pair,unobserved.sender,'安全待投递消息','held');
  assert.equal(await unobserved.control.advance(unobserved.peer),false);
  assert.equal(unobserved.notices('decision').length,0);
  assert.equal(unobserved.recovery.get(unobserved.peer.id)?.recycle,undefined);

  const {store,peer,sender,pair,recovery,control,effects,notices,advanceStable}=recycleFixture(t,active,true);
  const unknown=store.accept(pair,sender,'送达结果不明','unknown');
  store.claim(unknown.id);store.transition(unknown.id,'unknown');
  const held=store.accept(pair,sender,'安全待投递消息','held');
  await advanceStable();
  const incident=recovery.get(peer.id)!;
  assert.ok(incident.recycle?.phase==='awaiting_partner');
  assert.equal(notices('decision').length,1);
  store.transition(held.id,'contextObserved');
  const acceptance={incidentId:incident.id,evidenceRevision:incident.recycle.basis.decision.revision,reason:'同意回收。'};
  assert.equal(await control.advance(peer,acceptance),false);
  assert.equal(recovery.get(peer.id)?.recycle,undefined);
  assert.equal(recovery.lastRecycle(peer.id),undefined);
  assert.equal(notices('decision').length,0);
  assert.match(control.acceptanceText(peer,incident,acceptance)??'',/No recycle was performed/);
  assert.deepEqual(effects,{stops:0,launches:0,subscriptions:0});
  assert.equal(store.message(unknown.id).state,'unknown');
});

test('候选原代次在首次取证前已退出时让普通恢复接手',async t=>{
  const f=recycleFixture(t,{activeTurnIds:[],progress:'idle',roles:[],unresolved:[]});
  f.exitBeforeObserve();
  assert.equal(await f.control.advance(f.peer),false);
  const incident=f.recovery.get(f.peer.id)!;
  assert.equal(incident.recycle,undefined);assert.equal(f.recovery.due(incident),true);
  assert.equal(f.recovery.lastRecycle(f.peer.id),undefined);
  assert.deepEqual(f.effects,{stops:0,launches:0,subscriptions:0});
});

test('原生卸载超期后仍使用当前未知工作证据，后续轮询保留有效的回收选项',async t=>{
  let now=Date.now();t.mock.method(Date,'now',()=>now);
  const {peer,recovery,control,effects,setWork,notices}=recycleFixture(t,{activeTurnIds:[],progress:'idle',roles:[],unresolved:[]},true);
  assert.equal(await control.advance(peer),false);
  now+=65001;
  setWork({activeTurnIds:[],progress:'unknown',roles:['tool'],unresolved:[]});
  await control.advance(peer);
  now+=5001;
  await control.advance(peer);
  const first=recovery.get(peer.id)!;
  assert.ok(first.recycle?.phase==='awaiting_partner');
  assert.equal(first.recycle.trigger,'loaded_unleased_expired');
  assert.match(notices('decision')[0]!.message,/didn't unload when expected/);
  now+=5001;
  await control.advance(peer);
  const next=recovery.get(peer.id)!;
  assert.ok(next.recycle?.phase==='awaiting_partner');
  assert.equal(next.recycle.basis.decision.revision,first.recycle.basis.decision.revision);
  assert.equal(notices('decision').length,1);
  await control.advance(peer,{incidentId:next.id,evidenceRevision:next.recycle.basis.decision.revision,reason:'接受可能中断未确认的工作。'});
  assert.equal(recovery.get(peer.id)?.recycle?.phase,'ready');
  assert.deepEqual(effects,{stops:1,launches:1,subscriptions:1});
});

test('预检时原生权限要求人工处理，释放打开窗口所需的生命周期栅栏',async t=>{
  const {store,peer,recovery,control,effects,observeHook,advanceStable}=recycleFixture(t,{activeTurnIds:['work'],progress:'stalled',roles:[],unresolved:[]});
  await advanceStable();
  const incident=recovery.get(peer.id)!;
  assert.ok(incident.recycle?.phase==='awaiting_partner');
  const acceptance={incidentId:incident.id,evidenceRevision:incident.recycle.basis.decision.revision,reason:'同意回收。'};
  observeHook(()=>{
    if(recovery.get(peer.id)?.recycle?.phase==='preflight')store.updatePeer(peer.id,{state:'needsHuman'});
  });
  assert.equal(await control.advance(peer,acceptance),true);
  assert.equal(control.fenced(peer.id),false);
  assert.doesNotThrow(()=>control.requireUnfenced(peer.id));
  assert.equal(recovery.lastRecycle(peer.id),undefined);
  assert.deepEqual(effects,{stops:0,launches:0,subscriptions:0});
});

test('停止意图登记后出现人工审批，不消耗额度并允许打开窗口',async t=>{
  const {store,peer,recovery,control,effects,holderHook}=recycleFixture(t,{activeTurnIds:[],progress:'idle',roles:[],unresolved:[]});
  holderHook(()=>{
    const stage=recovery.get(peer.id)?.recycle;
    assert.ok(stage?.phase==='stop_requested'&&stage.signal==='unsubmitted');
    store.updatePeer(peer.id,{state:'needsHuman'});
  });
  await control.advance(peer);
  const incident=recovery.get(peer.id)!;
  const stage=incident.recycle;
  assert.ok(stage?.phase==='observing');
  assert.equal(control.fenced(peer.id),false);
  assert.doesNotThrow(()=>control.requireUnfenced(peer.id));
  assert.equal(recovery.lastRecycle(peer.id),undefined);
  assert.deepEqual(effects,{stops:0,launches:0,subscriptions:0});
});

test('停止信号可能已经提交时暂停，人工审批结束后只核对旧代次',async t=>{
  let now=Date.now();t.mock.method(Date,'now',()=>now);
  const {store,peer,recovery,control,effects,signalHook}=recycleFixture(t,{activeTurnIds:[],progress:'idle',roles:[],unresolved:[]});
  signalHook(()=>{
    store.updatePeer(peer.id,{state:'needsHuman'});
    throw new Error('原生停止结果不明');
  });
  await control.advance(peer);
  assert.throws(()=>control.requireUnfenced(peer.id),{code:'restart_in_progress'});
  now+=15000;
  await control.advance(store.peer(peer.id));
  assert.throws(()=>control.requireUnfenced(peer.id),{code:'restart_in_progress'});
  now+=5001;store.updatePeer(peer.id,{state:'ready'});await control.advance(store.peer(peer.id));
  assert.doesNotThrow(()=>control.requireUnfenced(peer.id));
  const stage=recovery.get(peer.id)?.recycle;
  assert.ok(stage?.phase==='held'&&stage.code==='codex_recycle_stop_deadline');
  assert.match(stage.reason,/原进程仍在运行/);
  assert.equal(recovery.lastRecycle(peer.id),now-20001);
  assert.deepEqual(effects,{stops:1,launches:0,subscriptions:0});
});
