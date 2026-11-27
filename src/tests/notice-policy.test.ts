import test from 'node:test';
import assert from 'node:assert/strict';
import {DeliveryHealth} from '../broker/health/delivery.js';
import {deliveryHealthKey,emptyDeliveryCounts,runtimeNoticeText} from '../broker/health/contracts.js';
import { fixture } from './support/broker-fixture.js';

test('不同健康来源合并一次提交，共用持久的接收方一分钟闸门',t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),health=f.broker.deliveryHealth;
  for(const [id,source] of [['hooks','pair_anomaly'],['idle','idle_unanswered'],['turn','turn_health']] as const)health.enqueue(pair.id,f.c.id,{id,source,kind:id,message:id,state:'pending'});
  const batch=health.take(pair.id,f.c.id);assert.equal(batch.length,1);assert.deepEqual(batch[0]!.componentIds,['hooks','idle','turn']);
  const submitted=health.prepare(pair.id,f.c.id,batch[0]!.id)!;assert.equal(submitted.message,'hooks\nidle\nturn');
  health.outcome(pair.id,f.c.id,submitted.id,'submitted');
  assert.ok(f.store.setting<any>(deliveryHealthKey(pair.id,f.c.id)).notices.every((n:any)=>n.state==='submitted'));
  const restarted=new DeliveryHealth(f.store);restarted.enqueue(pair.id,f.c.id,{id:'other',source:'pair_anomaly',kind:'other',message:'新条件',state:'pending'});
  t.mock.timers.tick(59999);assert.deepEqual(restarted.take(pair.id,f.c.id),[]);assert.equal(restarted.prepare(pair.id,f.c.id,'other'),undefined);
  restarted.enqueue(pair.id,f.x.id,{id:'independent',message:'另一接收方',state:'pending'});assert.ok(restarted.prepare(pair.id,f.x.id,'independent'));
  t.mock.timers.tick(1);assert.ok(restarted.prepare(pair.id,f.c.id,'other'));assert.equal(f.store.messages().length,0);
});

test('历史恢复积压在同一上下文中只输出一份相同正文，所有分量仍逐一结清',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),health=f.broker.deliveryHealth,body='同一归属变化的完整通知',ids:string[]=[];
  for(let index=0;index<200;index++){
    const id='legacy-authority-'+index;ids.push(id);
    health.enqueue(pair.id,f.c.id,{id,source:'managed_recovery',kind:'codex_recycle_authority_lost:'+f.x.id,scope:'same-context',
      incidentId:'old-incident-'+index,factKey:'old-incident-'+index,consumesNoticeCredit:false,tier:index===199?4:3,wake:index===199,message:body,state:'pending'});
  }
  ids.push('different');
  health.enqueue(pair.id,f.c.id,{id:'different',kind:'different',scope:'same-context',tier:3,message:body+' ',state:'pending'});
  const preview=health.peek(pair.id,f.c.id);assert.equal(preview.length,1);
  assert.equal(preview[0]!.message,body+'\n'+body+' ');assert.deepEqual(preview[0]!.componentIds,ids);
  const prepared=new DeliveryHealth(f.store).prepare(pair.id,f.c.id,ids[0]!,'same-context')!;
  assert.equal(runtimeNoticeText(prepared).split('\n').slice(1).join('\n'),body+'\n'+body+' ');
  assert.deepEqual(prepared.componentIds,ids);assert.equal(prepared.tier,4);assert.equal(prepared.wake,true);
  health.outcome(pair.id,f.c.id,prepared.id,'submitted');
  const recorded=f.store.setting<{notices:{id:string;admitted?:boolean;state:string}[]}>(deliveryHealthKey(pair.id,f.c.id))!.notices;
  assert.deepEqual(recorded.map(n=>n.id),ids);assert.ok(recorded.every(n=>n.admitted&&n.state==='submitted'));
  assert.deepEqual(new DeliveryHealth(f.store).take(pair.id,f.c.id),[]);
});

test('重启信息按事实和查看者只报告一次，重建协调器和重复发送不重复提示',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),health=f.broker.deliveryHealth;
  const facts=(prefix:string)=>Array.from({length:3},(_,index)=>{
    const message=f.store.accept(pair,f.x,'仍需核对的原消息',prefix+index);f.store.claim(message.id);f.store.transition(message.id,'unknown');
    return{id:message.id,state:'unknown'};
  });
  const first=facts('first-'),counts={...emptyDeliveryCounts(),unknown:3},messages=f.store.messages(),clock=f.store.setting('healthNoticeAdmission:'+f.c.id);
  health.restarted(pair,f.x,counts,first,0);
  const shown=health.notes(pair.id,f.x.id,f.c.id);assert.equal(shown.length,1);
  assert.match(shown[0]!.text,/unknown=3/);
  assert.deepEqual(new DeliveryHealth(f.store).notes(pair.id,f.x.id,f.c.id),[]);
  assert.equal(health.notes(pair.id,f.x.id,f.x.id).length,1,'向搭档报告不代替原查看者消费');
  health.restarted(pair,f.x,counts,first,0);
  assert.deepEqual(health.notes(pair.id,f.x.id,f.c.id),[]);
  assert.equal(health.notes(pair.id,f.x.id).length,1,'显式全局诊断仍能查看历史事实');
  assert.deepEqual(f.store.messages(),messages);assert.deepEqual(f.store.setting('healthNoticeAdmission:'+f.c.id),clock);
  const next=facts('next-');health.restarted(pair,f.x,counts,next,0);
  assert.equal(health.notes(pair.id,f.x.id,f.c.id).length,1,'数量相同的新消息仍是新事实');
  assert.deepEqual(health.notes(pair.id,f.x.id,f.c.id),[]);
});

test('提示输出限额只消费本次实际返回的分量',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),health=f.broker.deliveryHealth;
  for(const id of ['old','new'])health.enqueue(pair.id,f.x.id,{id,kind:'bridge_restart',tier:2,message:id,state:'pending'});
  assert.deepEqual(health.notes(pair.id,f.x.id,f.c.id,1),[{text:'new'}]);
  assert.deepEqual(health.notes(pair.id,f.x.id,f.c.id,1),[{text:'old'}]);
  assert.deepEqual(health.notes(pair.id,f.x.id,f.c.id,1),[]);
});

test('同类反复故障十分钟内保持待定，真实恶化也不能越过一分钟硬闸门',t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),health=f.broker.deliveryHealth;
  const enqueue=(id:string,severity=1)=>health.enqueue(pair.id,f.c.id,{id,kind:'runtime',severity,message:id,state:'pending'});
  enqueue('unknown');assert.ok(health.prepare(pair.id,f.c.id,'unknown'));
  enqueue('worse',2);t.mock.timers.tick(59999);assert.equal(health.prepare(pair.id,f.c.id,'worse'),undefined);
  t.mock.timers.tick(1);assert.ok(health.prepare(pair.id,f.c.id,'worse'));
  enqueue('flap',2);t.mock.timers.tick(599999);assert.equal(health.prepare(pair.id,f.c.id,'flap'),undefined);
  t.mock.timers.tick(1);assert.ok(new DeliveryHealth(f.store).prepare(pair.id,f.c.id,'flap'));
  t.mock.timers.tick(600000);assert.deepEqual(health.take(pair.id,f.c.id),[],'已提交但回执未知的正文仍然不能重放');
});

test('用户明确恢复不受自动通知时钟限制，也不提前夹带仍限频的自动通知',t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),health=f.broker.deliveryHealth;
  health.enqueue(pair.id,f.x.id,{id:'auto-old',kind:'runtime',state:'pending',message:'旧自动通知'});
  assert.ok(health.prepare(pair.id,f.x.id,'auto-old'));
  health.enqueue(pair.id,f.x.id,{id:'auto-new',kind:'runtime',state:'pending',message:'限频自动通知'});
  const recovery=(id:string,factKey:string)=>health.enqueue(pair.id,f.x.id,{id,kind:'explicit_recovery',source:'turn_health',factKey,state:'pending',message:id});
  recovery('requested','failed-1');
  const prepared=health.prepare(pair.id,f.x.id,'requested')!;
  assert.deepEqual(prepared.componentIds,['requested']);assert.equal(prepared.wake,true);
  assert.equal(health.prepare(pair.id,f.x.id,'auto-new'),undefined);
  recovery('duplicate','failed-1');assert.equal(health.prepare(pair.id,f.x.id,'duplicate'),undefined);
  recovery('next-failed-turn','failed-2');assert.ok(health.prepare(pair.id,f.x.id,'next-failed-turn'));
  assert.equal(health.prepare(pair.id,f.x.id,'auto-new'),undefined);
});

test('最终合并排除已恢复分量，并发消费者只有一个能推进提交时钟',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),health=f.broker.deliveryHealth;
  for(const id of ['a','b','c'])health.enqueue(pair.id,f.c.id,{id,kind:id,message:id,state:'pending'});
  assert.equal(health.take(pair.id,f.c.id).length,1);health.cancel(pair.id,f.c.id,'b');
  const first=health.prepare(pair.id,f.c.id,'a')!;assert.equal(first.message,'a\nc');assert.deepEqual(first.componentIds,['a','c']);
  assert.equal(new DeliveryHealth(f.store).prepare(pair.id,f.c.id,'c'),undefined);
  health.outcome(pair.id,f.c.id,'a','unknown');assert.deepEqual(health.take(pair.id,f.c.id),[]);
});

test('L2 决策通知确定未提交后可重新准入，同一前台边界最多两次，未知结果永不重放',t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),health=f.broker.deliveryHealth,id='l2-decision';
  health.enqueue(pair.id,f.c.id,{id,source:'managed_recovery',kind:'codex_recycle_decision:'+f.x.id,factKey:'same-decision',message:'等待当前接收方处理',tier:4,state:'pending'});
  const offer=()=>health.prepare(pair.id,f.c.id,id);
  assert.ok(offer());health.outcome(pair.id,f.c.id,id,'pending');
  t.mock.timers.tick(59999);assert.equal(offer(),undefined,'未提交重试仍遵守一分钟限频');
  t.mock.timers.tick(1);assert.ok(offer());health.outcome(pair.id,f.c.id,id,'pending');
  for(let index=0;index<5;index++){t.mock.timers.tick(60001);assert.equal(offer(),undefined);}
  assert.equal(health.retryHold(pair.id,f.c.id,id),'notice_retry_held');
  f.broker.turnEnds.begin(f.store.peer(f.c.id),'next-foreground');f.broker.turnEnds.stopped(f.store.peer(f.c.id),'next-foreground');
  assert.ok(offer());health.outcome(pair.id,f.c.id,id,'submitted');
  t.mock.timers.tick(600001);assert.equal(offer(),undefined,'已提交后不能随新边界重复提供');
  health.enqueue(pair.id,f.c.id,{id:'unknown',source:'managed_recovery',kind:'codex_recycle_decision:next',factKey:'next-decision',message:'结果不明',tier:4,state:'pending'});
  assert.ok(health.prepare(pair.id,f.c.id,'unknown'));health.outcome(pair.id,f.c.id,'unknown','unknown');
  f.broker.turnEnds.begin(f.store.peer(f.c.id),'another-foreground');f.broker.turnEnds.stopped(f.store.peer(f.c.id),'another-foreground');
  t.mock.timers.tick(600001);assert.equal(health.prepare(pair.id,f.c.id,'unknown'),undefined);
});

test('自有接收方实际忙完只解封被动通知，下一次正常取件只送一次且不新增唤醒',t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),health=f.broker.deliveryHealth,id='owned-passive';
  f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  health.enqueue(pair.id,f.x.id,{id,kind:'runtime',factKey:'same-fault',message:'随正常对话取件',tier:3,state:'pending'});
  for(let index=0;index<2;index++){
    assert.ok(health.prepare(pair.id,f.x.id,id));health.outcome(pair.id,f.x.id,id,'pending');t.mock.timers.tick(60001);
  }
  assert.equal(health.retryHold(pair.id,f.x.id,id),'notice_retry_held');assert.deepEqual(health.take(pair.id,f.x.id),[]);
  f.broker.turnEnds.begin(f.store.peer(f.x.id),'owned-next');f.broker.turnEnds.stopped(f.store.peer(f.x.id),'owned-next');
  assert.equal(health.retryHold(pair.id,f.x.id,id),undefined);assert.deepEqual(f.codex.woken,[]);assert.deepEqual(f.codex.injected,[]);
  const pickup=health.take(pair.id,f.x.id);assert.equal(pickup.length,1);assert.equal(pickup[0]!.wake,false);
  const prepared=health.prepare(pair.id,f.x.id,pickup[0]!.id)!;assert.equal(prepared.message,'随正常对话取件');
  health.outcome(pair.id,f.x.id,prepared.id,'submitted');assert.deepEqual(health.take(pair.id,f.x.id),[]);assert.deepEqual(f.codex.woken,[]);
});

test('短暂握手恢复不结清投递故障，稳定功能恢复不追加模型通知',t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),health=f.broker.deliveryHealth;
  f.store.accept(pair,f.c,'实际待投递消息','dependency');
  const observe=(state:'reachable'|'blocked',functionalSuccessAt?:number)=>health.observe(pair,f.c,f.x,{...emptyDeliveryCounts(),accepted:1},{state,reasons:state==='blocked'?['真实故障']:[],checked_at:Date.now(),functionalSuccessAt},true);
  observe('blocked');t.mock.timers.tick(300000);observe('blocked');const alert=health.take(pair.id,f.c.id)[0]!;assert.ok(health.prepare(pair.id,f.c.id,alert.id));
  t.mock.timers.tick(1000);observe('reachable');assert.deepEqual(health.take(pair.id,f.c.id),[]);
  t.mock.timers.tick(1000);observe('blocked');t.mock.timers.tick(60000);
  assert.deepEqual(health.take(pair.id,f.c.id),[],'旧恢复已撤销，新同类告警处于十分钟限频');
  const functional=Date.now();observe('reachable',functional);t.mock.timers.tick(30000);observe('reachable',functional);assert.deepEqual(health.take(pair.id,f.c.id),[]);assert.equal(health.active(pair.id,f.c.id),false);
});
