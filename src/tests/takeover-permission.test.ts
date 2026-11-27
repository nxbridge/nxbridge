import test,{type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import {appendFileSync,writeFileSync} from 'node:fs';
import path from 'node:path';
import {TakeoverPermissions} from '../broker/recovery/takeover-permission.js';
import {recordManagedAdoption,Takeovers} from '../broker/recovery/takeover.js';
import {DeliveryHealth} from '../broker/health/delivery.js';
import {BridgeError,DeliveryError} from '../shared/errors.js';
import {acceptedTakeoverProfile,captureNamedTakeoverDefinition,readTakeoverPolicy,takeoverRestoreArgs,takeoverRestoreUnavailable} from '../hosts/codex/codex-takeover-permission.js';
import {readSessionProfile,verifyRestartProfileCurrent} from '../hosts/codex/codex-session-profile.js';
import {takeoverPermissionFields,type TakeoverPermissionEvidence} from '../hosts/takeover-permission.js';
import type {RpcClient} from '../ipc/rpc.js';
import {fixture,lifecycle} from './support/broker-fixture.js';
import {registration} from './support/registration.js';

const gate=()=>({pid:60001,procStart:'foreground'});
const childState=()=>({pid:60002,procStart:'other'});
const oldSandbox={type:'managed',file_system:{type:'restricted',entries:[{path:{type:'special',value:{kind:'root'}},access:'read'}]},network:'restricted'};
function setup(t:TestContext){
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});
  const peer=()=>f.store.peer(f.x.id),endpoint=()=>f.store.endpoint(f.x.id)!;
  const base:TakeoverPermissionEvidence={sessionId:peer().sessionId!,file:path.join(f.broker.home,'history.jsonl'),offset:10,cwd:f.broker.home,roots:[f.broker.home],
    policy:{approval:'on-request',reviewer:'user',sandbox:oldSandbox,active:{id:'before'}},
    modelSettings:{model:'test-model',modelProvider:'test-provider',reasoningEffort:'high',serviceTier:'fast'}};
  const policy=(approval:unknown,offset:number)=>({...base,offset,policy:{...base.policy,approval}});
  let current=base,ready=true,calls=0,readable=true,preflight:undefined|(()=>void|Promise<void>),writeError:unknown;
  let activeProbe:undefined|(()=>TakeoverPermissionEvidence),probePreflight:(()=>void|Promise<void>)|undefined,probes=0,probeFrames=0;
  let references:TakeoverPermissionEvidence[]|undefined,generation:string|undefined='initial';
  const states=new Map<number,'alive'|'dead'|'unknown'>();
  if(endpoint().pid)states.set(endpoint().pid!,'alive');
  const takeovers=new Takeovers(f.store,async values=>values.map(value=>states.get(value.pid)??'dead'),undefined,undefined,()=>false);
  const delivery=new DeliveryHealth(f.store);
  const make=()=>new TakeoverPermissions(f.store,delivery,{codex:{
    observe:async()=>{if(!readable)throw new BridgeError('policy_unverified','记录尚未写完。');return current;},
    unavailable:takeoverRestoreUnavailable,
    references:()=>{if(!references)throw new Error('档案不可用');return references;},
    probeGeneration:()=>generation,
    probe:async(_peer,_endpoint,_before,guard,submitting)=>{
      guard();probes++;
      await probePreflight?.();guard();
      submitting();probeFrames++;
      if(!activeProbe)throw new DeliveryError('disconnected','后台暂不可达。');
      return activeProbe();
    },
    restore:async(_peer,_endpoint,old,_current,guard,submitting)=>{
      assert.ok(ready);
      guard();
      await preflight?.();
      guard();submitting({at:Date.now(),before:current});calls++;
      if(writeError)throw writeError;
      current={...old,offset:current.offset+1};
      return current;
    },
  }},{
    active:id=>!!takeovers.get(id),available:()=>ready,authorized:()=>true,occupy:()=>()=>{},stopped:()=>false,
  });
  let choices=make();takeovers.permissions=choices;
  let lease:ReturnType<Takeovers['begin']>;
  const open=()=>{lease=takeovers.begin(peer(),endpoint(),undefined,undefined,undefined,current);takeovers.claim(peer().id,lease.id,gate());return lease;};
  const close=async()=>{const result=await takeovers.returned(peer().id,lease.id);await choices.synchronize(peer().id);return result;};
  const notices=()=>delivery.peek(pair.id,f.c.id).flatMap(notice=>notice.message.split('\n').map(message=>({...notice,message})));
  return{...f,pair,peer,endpoint,base,policy,open,close,delivery,notices,states,takeovers,choices:()=>choices,setCurrent:(value:TakeoverPermissionEvidence)=>{current=value;},
    ready:(value:boolean)=>{ready=value;},readable:(value:boolean)=>{readable=value;},preflight:(hook?:()=>void|Promise<void>)=>{preflight=hook;},
    reject:(error:unknown)=>{writeError=error;},ambiguous:()=>{writeError=new DeliveryError('timeout','未收到回执。',true);},
    activeProbe:(hook:()=>TakeoverPermissionEvidence)=>{activeProbe=hook;},probes:()=>probes,references:(value:TakeoverPermissionEvidence[])=>{references=value;},
    probePreflight:(hook?:()=>void|Promise<void>)=>{probePreflight=hook;},probeFrames:()=>probeFrames,
    generation:(value:string|undefined)=>{generation=value;},
    writes:()=>calls,reopenBroker:()=>{choices=make();takeovers.permissions=choices;}};
}

test('最后一个持有者关闭后接受已核验的权限，重建协调器不追加提醒',async t=>{
  const f=setup(t),first=f.open();
  f.store.setSetting('codexForegroundWindows',[{endpoint:f.endpoint().address,sessionId:f.peer().sessionId,foreground:childState()}]);
  f.states.set(gate().pid,'dead');f.states.set(childState().pid,'unknown');
  f.setCurrent(f.policy('never',20));assert.equal(await f.close(),false);
  assert.equal(f.choices().view(f.peer()),undefined);
  f.states.set(childState().pid,'dead');assert.equal(await f.close(),true);
  assert.equal(f.choices().accepted(f.peer().id)?.final.policy.approval,'never');
  assert.equal(f.choices().holdsDelivery(f.peer().id),false);
  const firstOffer=f.delivery.peek(f.pair.id,f.c.id)[0]!;
  assert.equal(firstOffer.wake,false);assert.equal(firstOffer.source,'takeover_permission_change');assert.ok(firstOffer.message.includes(first.id));
  f.delivery.prepare(f.pair.id,f.c.id,firstOffer.id);
  f.reopenBroker();await f.choices().synchronize(f.peer().id);
  assert.equal(f.notices().length,0);assert.equal(f.choices().view(f.peer())?.state,'pending');
});

test('后来接管保留最早未回答的原设置，相同差异不重复提醒，回到原值直接结算',async t=>{
  const f=setup(t),first=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.open();assert.equal(f.choices().view(f.peer())?.state,'superseded');
  assert.throws(()=>f.choices().decide(f.peer(),first.id,'restore'),{code:'permission_choice_changed'});
  f.setCurrent(f.policy('never',25));await f.close();
  assert.equal(f.choices().view(f.peer())?.change_id,first.id);
  assert.equal(f.notices().length,1);
  f.open();f.setCurrent(f.policy('on-request',30));await f.close();
  assert.equal(f.choices().view(f.peer()),undefined);
  assert.equal(f.notices().length,0);
});

test('前次闭窗记录暂不可读时，重新接管仍保存最早的基线',async t=>{
  const f=setup(t);f.open();
  f.setCurrent(f.policy('never',20));f.readable(false);assert.equal(await f.close(),true);
  assert.equal(f.choices().view(f.peer()),undefined);
  const next=f.open();
  f.setCurrent(f.policy('on-failure',30));f.readable(true);f.reopenBroker();await f.close();
  const choice=f.choices().view(f.peer())!;
  assert.equal(choice.change_id,next.id);
  assert.deepEqual(choice.changed_fields,[{field:'approval_policy',before:'on-request',after:'on-failure'}]);
  assert.deepEqual(f.choices().accepted(f.peer().id)?.previous.map(value=>value.policy.approval),['never']);
});

test('恢复选择只在空闲时提交，未知结果跨协调器只读核实并只通知一次',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ready(false);f.ambiguous();
  assert.equal(f.choices().decide(f.peer(),change.id,'restore').state,'restore_pending');
  await f.choices().synchronize(f.peer().id);assert.equal(f.writes(),0);
  assert.equal(f.choices().holdsDelivery(f.peer().id),false);
  f.ready(true);await f.choices().synchronize(f.peer().id);assert.equal(f.writes(),1);
  assert.equal(f.choices().view(f.peer())?.state,'restoring');
  assert.equal(f.choices().holdsDelivery(f.peer().id),true);
  f.reopenBroker();await f.choices().synchronize(f.peer().id);assert.equal(f.writes(),1);
  f.setCurrent(f.policy('on-request',30));await f.choices().synchronize(f.peer().id);await f.choices().synchronize(f.peer().id);
  assert.equal(f.writes(),1);assert.equal(f.choices().holdsDelivery(f.peer().id),false);
  assert.equal(f.choices().view(f.peer())?.state,'restored');
  assert.equal(f.notices().length,1);assert.equal(f.notices()[0]?.source,'takeover_permission_result');assert.match(f.notices()[0]!.message,/original restored/);
});

test('原生明确拒绝解除门禁；丢失回执时主动核实未写入或第三种权限',async t=>{
  const f=setup(t);
  for(const [kind,result] of [
    ['native_error','-32001'],['unchanged','restore_not_applied'],['other','restore_mismatch'],
  ] as const){
    const change=f.open(),offset=20+20*f.writes();
    f.setCurrent(f.policy('never',offset));await f.close();
    f.reject(new DeliveryError('-32001','原生拒绝请求。'));
    if(kind!=='native_error')f.ambiguous();
    f.choices().decide(f.peer(),change.id,'restore');
    await f.choices().synchronize(f.peer().id);
    if(kind!=='native_error'){
      assert.equal(f.choices().holdsDelivery(f.peer().id),true);
      f.activeProbe(()=>f.policy(kind==='other'?'on-failure':'never',offset+5));
      f.reopenBroker();await f.choices().synchronize(f.peer().id);
    }
    const state=f.choices().view(f.peer());
    assert.equal(state?.state,'failed');assert.equal(state?.error_code,result);
    assert.equal(state?.error_phase,kind==='native_error'?'before_write':'after_write');
    if(kind==='other')assert.deepEqual(state?.actual_policy?.[0],{field:'approval_policy',value:'on-failure'});
    assert.equal(f.choices().holdsDelivery(f.peer().id),false);
    f.setCurrent(f.policy('on-request',offset+8));
  }
  assert.equal(f.writes(),3);assert.equal(f.probes(),2);
});

test('重启核验同样检查模型四项，并撤销尚未领取的旧结果',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');await f.choices().synchronize(f.peer().id);
  t.mock.timers.enable({apis:['Date'],now:Date.now()+11000});
  await f.choices().synchronize(f.peer().id);
  const oldNotice=f.delivery.peek(f.pair.id,f.c.id)[0]!;
  assert.equal(f.choices().view(f.peer())?.error_code,'restore_unconfirmed');
  f.reopenBroker();f.setCurrent({...f.policy('on-request',30),modelSettings:{...f.base.modelSettings!,serviceTier:'standard'}});
  await f.choices().synchronize(f.peer().id);
  assert.equal(f.choices().view(f.peer())?.error_code,'restore_verification_mismatch');
  assert.equal(f.choices().view(f.peer())?.state,'failed');
  assert.equal(f.delivery.get(f.pair.id,f.c.id,oldNotice.id),undefined);
  assert.equal(f.choices().holdsDelivery(f.peer().id),false);
  assert.equal(f.writes(),1);
});

test('原生证实未写入后仅在用户重新选择时使用原变更编号提交',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');await f.choices().synchronize(f.peer().id);
  f.activeProbe(()=>f.policy('never',25));
  await f.choices().synchronize(f.peer().id);await f.choices().synchronize(f.peer().id);
  assert.equal(f.choices().view(f.peer())?.error_code,'restore_not_applied');
  assert.equal(f.writes(),1);
  f.reject(undefined);f.setCurrent(f.policy('never',25));
  assert.equal(f.choices().decide(f.peer(),change.id,'restore').state,'restore_pending');
  await f.choices().synchronize(f.peer().id);
  assert.equal(f.choices().view(f.peer())?.state,'restored');assert.equal(f.writes(),2);
});

test('提交前断连保留授权，等待取消时不能执行已排队的恢复',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.preflight(()=>{throw new DeliveryError('disconnected','后台正在重连。');});
  f.choices().decide(f.peer(),change.id,'restore');
  await f.choices().synchronize(f.peer().id);
  assert.equal(f.choices().view(f.peer())?.state,'restore_pending');
  assert.equal(f.choices().holdsDelivery(f.peer().id),false);
  let entered!:()=>void,resume!:()=>void;
  const started=new Promise<void>(resolve=>{entered=resolve;}),paused=new Promise<void>(resolve=>{resume=resolve;});
  f.preflight(()=>{entered();return paused;});
  const waiting=f.choices().synchronize(f.peer().id);await started;
  assert.equal(f.choices().decide(f.peer(),change.id,'keep').state,'kept');
  resume();await waiting;
  assert.equal(f.choices().view(f.peer())?.state,'kept');
  assert.equal(f.writes(),0);
});

test('提交前的临时权限核验失败只能在用户明确重试时用同一编号恢复',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.preflight(()=>{throw new BridgeError('policy_unverified','暂时无法核验原权限。');});f.choices().decide(f.peer(),change.id,'restore');
  await f.choices().synchronize(f.peer().id);
  assert.equal(f.choices().view(f.peer())?.error_code,'policy_unverified');
  assert.equal(f.choices().holdsDelivery(f.peer().id),false);
  f.preflight();await f.choices().synchronize(f.peer().id);assert.equal(f.writes(),0);
  assert.equal(f.choices().decide(f.peer(),change.id,'restore').state,'restore_pending');
  await f.choices().synchronize(f.peer().id);assert.equal(f.writes(),1);
  assert.equal(f.choices().view(f.peer())?.state,'restored');assert.equal(f.notices().length,2);assert.ok(f.notices().every(notice=>notice.source==='takeover_permission_result'));
});

test('重新打开前先主动核对，晚到证据可更正同一配对已领取的结果',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');
  await f.choices().synchronize(f.peer().id);
  assert.throws(()=>f.open(),{code:'restore_unconfirmed'});
  t.mock.timers.enable({apis:['Date'],now:Date.now()+11000});
  await f.choices().synchronize(f.peer().id);
  assert.equal(f.choices().view(f.peer())?.error_code,'restore_unconfirmed');
  const prior=f.delivery.peek(f.pair.id,f.c.id)[0]!;
  assert.ok(f.delivery.prepare(f.pair.id,f.c.id,prior.id));
  assert.equal(f.choices().holdsDelivery(f.peer().id),true);
  f.store.updatePeer(f.c.id,{sessionId:'current-permission-recipient'});
  f.activeProbe(()=>f.policy('on-request',30));f.reopenBroker();
  await f.choices().prepareOpen(f.peer().id);
  assert.equal(f.choices().holdsDelivery(f.peer().id),false);
  assert.equal(f.choices().view(f.peer())?.state,'restored');
  assert.equal(f.choices().view(f.peer())?.correction,true);
  f.setCurrent(f.policy('on-request',30));f.open();await f.close();
  assert.equal(f.choices().view(f.peer())?.change_id,change.id);
  assert.ok(f.notices().every(notice=>notice.source==='takeover_permission_result'));
  assert.equal(f.writes(),1);
});

test('仍不确定时的接手窗口有独立基线，用户改回不能冒充旧请求成功',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');await f.choices().synchronize(f.peer().id);
  t.mock.timers.enable({apis:['Date'],now:Date.now()+11000});
  await f.choices().prepareOpen(f.peer().id);
  const prior=f.delivery.peek(f.pair.id,f.c.id)[0]!;
  assert.ok(f.delivery.prepare(f.pair.id,f.c.id,prior.id));
  const recovery=f.open();f.reopenBroker();
  f.setCurrent(f.policy('on-request',30));await f.close();
  const after=f.choices().view(f.peer());
  assert.equal(after?.state,'pending');assert.equal(after?.change_id,recovery.id);
  assert.deepEqual(after?.changed_fields,[{field:'approval_policy',before:'never',after:'on-request'}]);
  assert.equal(f.choices().holdsDelivery(f.peer().id),false);
  const notices=f.delivery.peek(f.pair.id,f.c.id)[0]!;
  assert.ok(notices.componentIds?.some(id=>id.includes(change.id+':failed:')));
  assert.ok(notices.componentIds?.some(id=>id.includes(recovery.id)));
  assert.doesNotMatch(notices.message,/original restored/);
  assert.equal(f.writes(),1);
});

test('启动失败且从未登记前台的接手预约不影响迟到的恢复证明',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');await f.choices().synchronize(f.peer().id);
  t.mock.timers.enable({apis:['Date'],now:Date.now()+11000});
  await f.choices().prepareOpen(f.peer().id);
  const aborted=f.takeovers.begin(f.peer(),f.endpoint(),undefined,undefined,undefined,f.policy('never',20));
  assert.deepEqual(await f.broker.dispatch('open.return',{peer_id:f.peer().id,session_id:aborted.sessionId,lease_id:aborted.id,
    foreground_ended:true,foreground_outcome:'not_started'}),{returned:true});
  f.reopenBroker();f.setCurrent(f.policy('on-request',30));
  await f.choices().synchronize(f.peer().id);
  const result=f.choices().view(f.peer());
  assert.equal(result?.change_id,change.id);assert.equal(result?.state,'restored');
  assert.equal(f.writes(),1);
});

test('启动过但前台登记失败的租约独立比较关窗权限，不能用窗口改动确认原写入',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');await f.choices().synchronize(f.peer().id);
  t.mock.timers.enable({apis:['Date'],now:Date.now()+11000});
  await f.choices().prepareOpen(f.peer().id);
  const unclaimed=f.takeovers.begin(f.peer(),f.endpoint(),undefined,undefined,undefined,f.policy('never',20));
  f.reopenBroker();f.setCurrent(f.policy('on-request',30));
                                   
  assert.deepEqual(await f.broker.dispatch('open.return',{peer_id:f.peer().id,session_id:unclaimed.sessionId,lease_id:unclaimed.id,
    foreground_ended:true}),{returned:true});
  await f.choices().synchronize(f.peer().id);
  const result=f.choices().view(f.peer()),notices=f.delivery.peek(f.pair.id,f.c.id)[0]!;
  assert.equal(result?.change_id,unclaimed.id);assert.equal(result?.state,'pending');
  assert.deepEqual(result?.changed_fields,[{field:'approval_policy',before:'never',after:'on-request'}]);
  assert.ok(notices.componentIds?.some(id=>id.includes(change.id+':failed:')));
  assert.doesNotMatch(notices.message,/original restored/);
  assert.equal(f.choices().holdsDelivery(f.peer().id),false);
  assert.equal(f.writes(),1);
});

test('等待旧核对的多个打开请求保持串行，后台核对复用正在运行的请求',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');await f.choices().synchronize(f.peer().id);
  const steps=Array.from({length:3},()=>{
    let entered!:()=>void,release!:()=>void;
    const started=new Promise<void>(resolve=>{entered=resolve;}),paused=new Promise<void>(resolve=>{release=resolve;});
    return {entered,release,started,paused};
  });
  let count=0;
  f.probePreflight(()=>{const next=steps[count++]!;next.entered();return next.paused;});
  const background=f.choices().synchronize(f.peer().id);
  await steps[0]!.started;
  const first=f.choices().prepareOpen(f.peer().id),second=f.choices().prepareOpen(f.peer().id);
  steps[0]!.release();await background;await steps[1]!.started;
  const joined=f.choices().synchronize(f.peer().id);
  await new Promise(resolve=>setImmediate(resolve));assert.equal(count,2);
  steps[1]!.release();await first;await joined;await steps[2]!.started;
  assert.equal(count,3);
  steps[2]!.release();await second;
  assert.equal(f.probeFrames(),3);assert.equal(f.writes(),1);
});

test('读线程超时或忙碌后退避并提供重查，仍保留发帧额度',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');await f.choices().synchronize(f.peer().id);
  t.mock.timers.enable({apis:['Date'],now:Date.now()});
  for(const [code,delay] of [['timeout',2000],['session_busy',8000],['owned_subscription_waiting',0]] as const){
    f.probePreflight(()=>{throw new BridgeError(code,'模型重报尚未发送。');});
    await f.choices().synchronize(f.peer().id);await f.choices().synchronize(f.peer().id);
    t.mock.timers.tick(delay);
  }
  assert.equal(f.probes(),3);assert.equal(f.probeFrames(),0);
  assert.equal(f.choices().view(f.peer())?.recheck_via_open,true);
  assert.match(f.delivery.peek(f.pair.id,f.c.id)[0]?.message??'',/nxbridge open codex --member /);
  t.mock.timers.tick(59000);await f.choices().synchronize(f.peer().id);
  assert.equal(f.probes(),3);
  f.reopenBroker();f.probePreflight();
  f.activeProbe(()=>f.policy('on-request',30));await f.choices().synchronize(f.peer().id);
  assert.equal(f.probeFrames(),0);
  await f.choices().prepareOpen(f.peer().id);
  assert.equal(f.probeFrames(),1);assert.equal(f.choices().view(f.peer())?.state,'restored');
});

test('原生路由尚未就绪时可以提示重查，等待期间不发探测帧',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');await f.choices().synchronize(f.peer().id);
  t.mock.timers.enable({apis:['Date'],now:Date.now()});
  f.generation(undefined);f.ready(false);
  for(const delay of [2000,8000,0]){
    await f.choices().synchronize(f.peer().id);t.mock.timers.tick(delay);
  }
  assert.equal(f.probes(),0);
  assert.equal(f.choices().view(f.peer())?.recheck_via_open,true);
  assert.equal(f.choices().holdsDelivery(f.peer().id),true);
});

test('探测尊重配对暂停，耗尽当前连接的预算后只用新连接或被动证据核对',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');await f.choices().synchronize(f.peer().id);
  t.mock.timers.enable({apis:['Date'],now:Date.now()});
  f.store.setPaused(true,f.pair.id);
  await f.choices().synchronize(f.peer().id);assert.equal(f.probes(),0);
  f.store.setPaused(false,f.pair.id);
  for(let attempt=1;attempt<=5;attempt++){
    await f.choices().synchronize(f.peer().id);t.mock.timers.tick(31000);
    assert.equal(f.probes(),Math.min(attempt,3));
  }
  assert.equal(f.choices().holdsDelivery(f.peer().id),true);
  assert.equal(f.choices().view(f.peer())?.recheck_via_open,true);
  f.reopenBroker();await f.choices().synchronize(f.peer().id);assert.equal(f.probes(),3);
  f.generation('reconnected');
  await f.choices().synchronize(f.peer().id);assert.equal(f.probes(),4);
  for(let attempt=0;attempt<3;attempt++){t.mock.timers.tick(31000);await f.choices().synchronize(f.peer().id);}
  assert.equal(f.probes(),6);
  const relocated={...f.policy('never',2),file:path.join(f.base.cwd,'relocated.jsonl'),roots:['changed']};
  let openProbes=0;
  Object.assign(f.codex,{
    takeoverProbeGeneration:()=> 'reconnected',
    observeTakeoverPermission:async()=>f.policy('never',20),
    probeTakeoverPermission:async(_peer:unknown,_endpoint:unknown,_baseline:unknown,guard:()=>void,submitting:()=>void)=>{
      guard();assert.equal(lifecycle(f.broker).isRestarting(f.peer().id),true);submitting();openProbes++;return relocated;
    },
    captureTakeoverPermission:async()=>relocated,
  });
  const opened=await f.broker.dispatch('open',{host:'codex',peer_id:f.peer().id}) as {leaseId?:string};
  assert.ok(opened.leaseId);
  await f.choices().synchronize(f.peer().id);
  assert.equal(openProbes,1);assert.equal(f.probes(),6);assert.equal(f.writes(),1);
  assert.equal(f.choices().holdsDelivery(f.peer().id),false);
  const result=f.choices().view(f.peer());
  assert.equal(result?.error_code,'restore_mismatch');
  assert.deepEqual(result?.actual_policy?.[0],{field:'approval_policy',value:'never'});
});

for(const admitted of [false,true])test('探测耗尽向当前搭档只提醒一次重查入口，迟到证明结算过时提醒：'+(admitted?'已领取':'未领取'),async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');await f.choices().synchronize(f.peer().id);
  t.mock.timers.enable({apis:['Date'],now:Date.now()});
  await f.choices().synchronize(f.peer().id);assert.equal(f.probes(),1);
  t.mock.timers.tick(11000);
  await f.choices().synchronize(f.peer().id);assert.equal(f.probes(),2);
  const prior=f.delivery.peek(f.pair.id,f.c.id)[0]!,command='nxbridge open codex --member '+f.peer().id;
  assert.equal(prior.source,'takeover_permission_result');
  assert.ok(!prior.message.includes(command));
  assert.equal(f.choices().view(f.peer())?.recheck_via_open,undefined);
  if(admitted)assert.ok(f.delivery.prepare(f.pair.id,f.c.id,prior.id));
  f.store.updatePeer(f.c.id,{sessionId:'recipient-after-permission-timeout'});
  t.mock.timers.tick(8000);
  await f.choices().synchronize(f.peer().id);assert.equal(f.probes(),3);
  assert.equal(f.choices().view(f.peer())?.recheck_via_open,true);
  assert.equal(f.choices().decide(f.peer(),change.id,'restore').recheck_via_open,true);
  const cap=f.delivery.peek(f.pair.id,f.c.id)[0]!;
  assert.equal(cap.source,'takeover_permission_result');assert.notEqual(cap.id,prior.id);
  assert.ok(cap.message.includes(command));
  assert.deepEqual(cap.componentIds,[cap.id]);
  assert.equal(f.delivery.get(f.pair.id,f.c.id,prior.id),undefined);
  f.reopenBroker();t.mock.timers.tick(31000);
  await f.choices().synchronize(f.peer().id);await f.choices().synchronize(f.peer().id);
  assert.deepEqual(f.delivery.peek(f.pair.id,f.c.id)[0]?.componentIds,[cap.id]);
  f.generation('new-permission-connection');
  for(let attempt=0;attempt<3;attempt++){
    await f.choices().synchronize(f.peer().id);t.mock.timers.tick(31000);
  }
  assert.deepEqual(f.delivery.peek(f.pair.id,f.c.id)[0]?.componentIds,[cap.id]);
  assert.equal(f.writes(),1);assert.equal(f.probes(),6);
  f.setCurrent(f.policy('on-request',30));await f.choices().synchronize(f.peer().id);
  assert.equal(f.delivery.get(f.pair.id,f.c.id,cap.id),undefined);
  const resolved=f.delivery.peek(f.pair.id,f.c.id)[0]!;
  assert.match(resolved.message,/original restored/);
  assert.equal(f.choices().view(f.peer())?.correction,admitted?true:undefined);
  assert.equal(f.choices().view(f.peer())?.recheck_via_open,undefined);
});

for(const restored of [false,true])test('提交后换搭档由现任接收核对结果：'+(restored?'更正':'重新询问'),async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ambiguous();f.choices().decide(f.peer(),change.id,'restore');await f.choices().synchronize(f.peer().id);
  t.mock.timers.enable({apis:['Date'],now:Date.now()+11000});
  await f.choices().synchronize(f.peer().id);
  const prior=f.delivery.peek(f.pair.id,f.c.id)[0]!;
  if(restored)assert.ok(f.delivery.prepare(f.pair.id,f.c.id,prior.id));
  f.store.unpair(f.pair.id);
  const replacement=f.store.register(registration('claude','new-permission-partner',{cwd:f.base.cwd}));
  const pair=f.store.pair(replacement.id,f.peer().id);
  for(let attempt=1;attempt<=2;attempt++){
    t.mock.timers.tick(31000);await f.choices().synchronize(f.peer().id);
  }
  const cap=f.delivery.peek(pair.id,replacement.id)[0]!;
  assert.ok(cap.message.includes('nxbridge open codex --member '+f.peer().id));
  assert.equal(f.delivery.get(f.pair.id,f.c.id,prior.id),undefined);
  f.setCurrent(f.policy(restored?'on-request':'never',30));await f.choices().synchronize(f.peer().id);
  assert.equal(f.delivery.get(pair.id,replacement.id,cap.id),undefined);
  const result=f.delivery.peek(pair.id,replacement.id)[0]!,choice=f.choices().view(f.peer());
  assert.equal(choice?.change_id,change.id);
  if(restored){
    assert.equal(choice?.state,'restored');assert.equal(choice?.correction,true);
    assert.match(result.message,/original restored/);
  }else{
    assert.equal(choice?.state,'pending');assert.match(result.message,/verified not applied/);
    assert.ok(result.componentIds?.length===2);assert.ok(result.componentIds.some(id=>id.endsWith(':new-permission-partner')));
  }
  assert.equal(f.writes(),1);
});

test('忙碌时可以取消尚未提交的权限恢复，保留现在的设置',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.ready(false);f.choices().decide(f.peer(),change.id,'restore');
  assert.equal(f.choices().decide(f.peer(),change.id,'keep').state,'kept');
  f.ready(true);await f.choices().synchronize(f.peer().id);
  assert.equal(f.writes(),0);assert.equal(f.choices().holdsDelivery(f.peer().id),false);
});

test('失去原配对后保留闭窗选择给下一搭档，旧搭档排队的恢复不会写入',async t=>{
  const f=setup(t),change=f.open();
  f.store.unpair(f.pair.id);f.setCurrent(f.policy('never',20));await f.close();
  assert.equal(f.choices().view(f.peer())?.change_id,change.id);
  const next=f.store.pair(f.c.id,f.x.id);
  await f.choices().synchronize(f.peer().id);
  assert.equal(f.delivery.peek(next.id,f.c.id).length,1);
  let entered!:()=>void,resume!:()=>void;
  const started=new Promise<void>(resolve=>{entered=resolve;}),paused=new Promise<void>(resolve=>{resume=resolve;});
  f.preflight(()=>{entered();return paused;});
  f.choices().decide(f.peer(),change.id,'restore');
  const waiting=f.choices().synchronize(f.peer().id);await started;
  f.store.unpair(next.id);
  const replacement=f.store.pair(f.c.id,f.x.id);
  resume();await waiting;
  assert.equal(f.writes(),0);assert.equal(f.choices().view(f.peer())?.state,'pending');
  assert.equal(f.delivery.peek(replacement.id,f.c.id)[0]?.source,'takeover_permission_change');
});

test('同一配对换接收会话保留排队授权，未领取结果交给现任会话',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  f.choices().decide(f.peer(),change.id,'restore');
  f.store.updatePeer(f.c.id,{sessionId:'next-permission-recipient'});
  f.reopenBroker();await f.choices().synchronize(f.peer().id);
  assert.equal(f.writes(),1);assert.equal(f.choices().view(f.peer())?.state,'restored');
  f.store.updatePeer(f.c.id,{sessionId:'later-permission-recipient'});
  const offer=f.delivery.peek(f.pair.id,f.c.id)[0]!;
  assert.equal(offer.source,'takeover_permission_result');
  assert.ok(f.delivery.prepare(f.pair.id,f.c.id,offer.id));
  assert.equal(f.delivery.peek(f.pair.id,f.c.id).length,0);
});

test('搭档换会话后同一差异仅重新提醒一次，作用范围改变时只可保留',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent({...f.policy('never',20),roots:[...f.base.roots,'added-root']});await f.close();
  const first=f.delivery.peek(f.pair.id,f.c.id)[0]!;
  assert.equal(f.choices().view(f.peer())?.restore_available,false);
  assert.equal(f.choices().view(f.peer())?.restore_unavailable_reason,'policy_changed');
  assert.throws(()=>f.choices().decide(f.peer(),change.id,'restore'),{code:'policy_changed'});
  f.store.updatePeer(f.c.id,{sessionId:'replacement-permission-recipient'});
  assert.equal(f.delivery.peek(f.pair.id,f.c.id).length,0);
  await f.choices().synchronize(f.peer().id);await f.choices().synchronize(f.peer().id);
  const offered=f.delivery.peek(f.pair.id,f.c.id);
  assert.equal(offered.length,1);assert.notEqual(offered[0]?.id,first.id);
  assert.equal(f.choices().view(f.peer())?.change_id,change.id);
});

test('重启只继承连续且仍有档案引用的权限证明，旧或收养租约不会生成新差异',async t=>{
  const f=setup(t);f.references([f.base]);
  f.open();f.setCurrent(f.policy('never',20));await f.close();
  f.open();f.setCurrent(f.policy('on-failure',30));await f.close();
  assert.deepEqual(f.choices().accepted(f.peer().id)?.previous.map(value=>value.policy.approval),['on-request','never']);
  const change=f.choices().view(f.peer())?.change_id;
  const legacy=f.takeovers.begin(f.peer(),f.endpoint());
  f.setCurrent(f.policy('unknown-external',35));await f.takeovers.returned(f.peer().id,legacy.id,'ended');
  recordManagedAdoption(f.store,f.peer(),registration('codex',f.base.sessionId,{cwd:f.base.cwd}));
  const adopted=f.takeovers.get(f.peer().id)!;
  await f.takeovers.returned(f.peer().id,adopted.id,'ended');
  await f.choices().synchronize(f.peer().id);
  assert.equal(f.choices().view(f.peer())?.change_id,change);
  f.open();f.setCurrent(f.policy('never',40));await f.close();
  assert.deepEqual(f.choices().accepted(f.peer().id)?.previous.map(value=>value.policy.approval),['unknown-external']);
});

test('搭档工具只让有效配对的独立窗口按变更编号结算原权限',async t=>{
  const f=setup(t),change=f.open();
  f.setCurrent(f.policy('never',20));await f.close();
  const binding=f.store.bindings.current(f.c.id),actor={host:'claude' as const,memberId:f.c.id,sessionId:f.c.sessionId!,callId:'permission-change',
    ...(binding?{binding:{key:binding.key,epoch:binding.epoch}}:{})};
  const invoke=(raw:object)=>f.broker.dispatch('partner.profile',raw) as Promise<{permission_change?:{change_id:string;state:string}}>;
  assert.equal((await invoke({actor})).permission_change?.change_id,change.id);
  await assert.rejects(invoke({host:'codex',change_id:change.id,action:'keep'}),{code:'invalid_arguments'});
  await assert.rejects(invoke({actor,change_id:change.id,action:'restore',model:'other'}),{code:'invalid_arguments'});
  await assert.rejects(invoke({actor:{...actor,via:'child'},change_id:change.id,action:'keep'}),{code:'subagent_denied'});
  assert.equal((await invoke({actor,change_id:change.id,action:'keep'})).permission_change?.state,'kept');
  assert.equal(f.notices().length,0);
  assert.equal(f.writes(),0);
});

test('命名权限跨原生代次核验实际定义，更新的轮次上下文使旧权限证据失效',async t=>{
  const f=setup(t),file=f.base.file,id=f.base.sessionId;
  const row=(type:string,payload:unknown)=>JSON.stringify({type,payload})+'\n';
  const applied=(model:string,approval:string)=>row('event_msg',{type:'thread_settings_applied',thread_id:id,thread_settings:{
    model,approval_policy:approval,approvals_reviewer:'user',cwd:f.base.cwd,runtime_workspace_roots:f.base.roots,active_permission_profile:{id:'before'},permission_profile:oldSandbox,
  }});
  writeFileSync(file,row('session_meta',{id})+applied('latest','on-request'),'utf8');
  const {evidence,profile}=await readTakeoverPolicy(file,id);
                        
  appendFileSync(file,row('turn_context',{model:'latest',approval_policy:'never'}),'utf8');
  await assert.rejects(readTakeoverPolicy(file,id),{code:'policy_unverified'});
  appendFileSync(file,row('turn_context',{approval_policy:'on-request'}),'utf8');
  await assert.rejects(readTakeoverPolicy(file,id),{code:'policy_unverified'});
  appendFileSync(file,applied('latest','on-request'),'utf8');
  assert.ok((await readTakeoverPolicy(file,id)).evidence.offset>evidence.offset);
  let configured={description:null,extends:null,filesystem:{':root':'read',glob_scan_max_depth:null},network:{enabled:false,mode:null}};
  const rpc={request:async()=>({config:{permissions:{before:configured}}})} as unknown as RpcClient;
  evidence.named=await captureNamedTakeoverDefinition(rpc,evidence,profile);
  assert.ok(evidence.named);
  const changed={...evidence,policy:{...evidence.policy,active:null,sandbox:{type:'disabled'},approval:'never'}};
  assert.deepEqual(await takeoverRestoreArgs(rpc,evidence,changed),{approvalPolicy:'on-request',permissions:'before'});
  configured={...configured,filesystem:{...configured.filesystem,':root':'write'}};
  await assert.rejects(takeoverRestoreArgs(rpc,evidence,changed),{code:'policy_not_representable'});
  const inherited={...evidence,policy:{...evidence.policy,active:{id:':read-only',extends:'base'}}};
  await assert.rejects(takeoverRestoreArgs(rpc,inherited,changed),{code:'policy_not_representable'});
  assert.deepEqual(await takeoverRestoreArgs(rpc,inherited,{...inherited,policy:{...inherited.policy,approval:'never'}}),{approvalPolicy:'on-request'});
  const detail=takeoverPermissionFields(evidence.policy,{...evidence.policy,sandbox:{...oldSandbox,file_system:{type:'restricted',entries:[]}}});
  assert.equal(detail[0]?.detail_only,true);
});

test('闭窗已接受的降级只替换重启权限证明，仍检查不相关的会话设置',async t=>{
  const f=setup(t),file=f.base.file,id=f.base.sessionId;
  const row=(type:string,payload:unknown)=>JSON.stringify({type,payload})+'\n';
  const applied=(model:string,approvals:string)=>row('event_msg',{type:'thread_settings_applied',thread_id:id,thread_settings:{
    model,approval_policy:approvals,approvals_reviewer:'user',cwd:f.base.cwd,runtime_workspace_roots:f.base.roots,active_permission_profile:{id:'before'},permission_profile:oldSandbox,
  }});
  writeFileSync(file,row('session_meta',{id})+applied('saved','on-request'),'utf8');
  const original=await readTakeoverPolicy(file,id);
  appendFileSync(file,applied('saved','never'),'utf8');
  const {evidence:final}=await readTakeoverPolicy(file,id);
  await assert.rejects(verifyRestartProfileCurrent(original.profile,{exact:true}),{code:'restart_profile_changed'});
  const accepted={previous:[original.evidence],final};
  const adopted=await acceptedTakeoverProfile(original.profile,accepted);
  assert.equal(adopted.settings.approvalPolicy,'never');
  await verifyRestartProfileCurrent(adopted,{exact:true});
  appendFileSync(file,row('turn_context',{disabled_plugin_ids:['different']}),'utf8');
  await assert.rejects(verifyRestartProfileCurrent(await acceptedTakeoverProfile(original.profile,accepted),{exact:true,current:await readSessionProfile(file,id)}),{code:'restart_profile_changed'});
});
