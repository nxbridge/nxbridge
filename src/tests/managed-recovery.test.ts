import test from 'node:test';
import assert from 'node:assert/strict';
import { ManagedRecovery, managedRecoveryKey, recoveryUserStep, type ManagedIncident } from '../broker/recovery/managed-recovery.js';
import {BridgeError,RuntimeRecoveryHold} from '../shared/errors.js';
import {fixture, lifecycle} from './support/broker-fixture.js';
import { Takeovers, recordManagedAdoption, foregroundMatchesTakeover } from '../broker/recovery/takeover.js';
import { writeHosting } from '../broker/recovery/session-hosting.js';
import {deliveryHealthKey,type RuntimeNotice} from '../broker/health/contracts.js';
import { RECYCLE_CLEANUP_USER_STEP, RECYCLE_OLD_UNKNOWN_USER_STEP } from '../broker/stuck-session-text.js';

test('普通托管接续取得新订阅后按原回收尝试结算未知消息一次',async t=>{
  let now=Date.now();t.mock.method(Date,'now',()=>now);
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),recovery=f.broker.managedRecovery;
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'offline'});
  const peer=f.store.peer(f.x.id),existing=f.store.endpoint(peer.id)!;
  const fresh={runtimeId:'replacement',endpoint:existing.address,pid:process.pid,procStart:existing.procStart!};
  f.store.saveEndpoint({...existing,runtimeId:fresh.runtimeId});
  const message=f.store.accept(pair,f.c,'无法确认已收到的消息','handback-unknown');f.store.claim(message.id);f.store.transition(message.id,'unknown');
  const incident=recovery.observe(peer,pair,existing),old={...fresh,runtimeId:'old',procStart:'previous-generation'};
  recovery.updateRecycle(incident,undefined,{phase:'handback',step:'handback-step',trigger:'native_thread_wedged',fresh,attempt:{
    id:'attempt',at:now,stopDeadline:now+15000,old,profile:{sourceId:old.runtimeId,sessionId:peer.sessionId!,file:'original',settingsKey:'profile'},
    decision:{revision:'revision',facts:'facts',work:{activeTurnIds:[],progress:'idle',roles:[],unresolved:[]},since:now,noticeId:'notice',autoEligible:true},
    ledger:{held:[],uncertain:[{id:message.id,senderId:f.c.id}]},
  }},()=>recovery.recordRecycle(peer.id,now));
  let attached=false;
  const attach=f.codex.attach.bind(f.codex);
  f.codex.attach=async(...args)=>{const rpc=await attach(...args);attached=true;return rpc;};
  f.codex.hasSubscription=()=>attached;
  Object.assign(f.codex,{recycleHost:()=>({})});
  const members=lifecycle(f.broker);
  assert.equal(recovery.due(recovery.get(peer.id)!),true);
  assert.equal(await members.recover(peer),true);
  assert.equal(attached,true);
  now+=6000;await Promise.all(members.schedulePairedRecoveries().values());
  now+=6000;await Promise.all(members.schedulePairedRecoveries().values());
  assert.equal(recovery.get(peer.id)?.recycle?.phase,'ready');
  const state=f.store.setting<{notices:RuntimeNotice[]}>(deliveryHealthKey(pair.id,f.c.id));
  const results=state?.notices.filter(notice=>notice.kind?.startsWith('codex_recycle_result:'))??[];
  assert.equal(results.length,1);assert.deepEqual(results[0]?.affectedMessageIds,[message.id]);
  assert.equal(f.store.message(message.id).state,'unknown');
});

test('后台订阅等待保留托管身份，原生卸载后自动投递原消息',async t=>{
  let now=Date.now();t.mock.method(Date,'now',()=>now);
  const f=fixture(t),endpoint=f.store.endpoint(f.x.id)!;
  f.store.pair(f.c.id,f.x.id);
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'offline',model:'gpt-6.1-sol',effort:'max'});
  writeHosting(f.store,f.store.peer(f.x.id),{verified:true});
  f.codex.attachFailure=new RuntimeRecoveryHold('owned_subscription_waiting','等待原线程按原设置接回');
  const actor={host:'claude' as const,sessionId:f.c.sessionId!,callId:'held-through-unload'};
  const sent=await f.broker.dispatch('send',{actor,message:'保持原会话和模型处理这条消息'}) as any;
  assert.equal(sent.delivery_state,'accepted');
  assert.equal(sent.read_confirmed,false);
  assert.equal(sent.hosting.phase,'background');
  assert.equal(sent.selection_id,undefined);
  await f.broker.pump();await f.broker.pump();
  assert.equal(f.store.message(sent.message_id).state,'accepted');assert.equal(f.codex.delivered.length,0);
  assert.equal(f.broker.managedRecovery.get(f.x.id)?.preconditionCode,'owned_subscription_waiting');
  f.codex.attachFailure=undefined;
  f.codex.emit('state',f.x.sessionId,'offline',endpoint.address);
  now+=31000;
  await f.broker.pump();await f.broker.pump();
  assert.equal(f.store.message(sent.message_id).state,'contextObserved');
  assert.equal(f.codex.delivered.filter(message=>message.id===sent.message_id).length,1);
  assert.equal(f.store.peer(f.x.id).sessionId,f.x.sessionId);assert.equal(f.store.peer(f.x.id).model,'gpt-6.1-sol');assert.equal(f.store.peer(f.x.id).effort,'max');
  assert.equal(f.codex.created.length,0);assert.equal(f.codex.ensured,0);
  assert.equal(f.broker.managedRecovery.get(f.x.id)?.attempts.length,0,'重连原线程不消耗新进程启动额度');
});

test('后台原权限仍待核验时保留门禁，旧托管偏好不会触发用户关窗选择',async t=>{
  const f=fixture(t);f.store.pair(f.c.id,f.x.id);
  const peer=f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'offline'});
  writeHosting(f.store,peer,{background:false,verifying:true,verified:false});
  const sent=await f.broker.dispatch('send',{actor:{host:'claude',sessionId:f.c.sessionId!,callId:'verify-owned'},message:'核验后再处理'}) as any;
  assert.equal(sent.delivery_state,'accepted');
  assert.equal(sent.read_confirmed,false);
  assert.equal(sent.hosting.verifying,true);
  assert.equal(sent.hosting.phase,'verifying');
  assert.equal(sent.selection_id,undefined);
  assert.equal(f.store.message(sent.message_id).state,'accepted');
  assert.equal(f.codex.delivered.length,0);
});

for(const gate of ['人工审批','会话冲突','解除配对'] as const)test(`${gate}按权限保留或结束已提交的停止尝试`,async t=>{
  let now=Date.now();t.mock.method(Date,'now',()=>now);
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),recovery=f.broker.managedRecovery;
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:gate==='人工审批'?'needsHuman':'ready'});
  const peer=f.store.peer(f.x.id),endpoint=f.store.endpoint(peer.id)!;
  const incident=recovery.observe(peer,pair,endpoint),old={runtimeId:'isolated-old',endpoint:endpoint.address,pid:1729,procStart:'isolated-start'};
  recovery.updateRecycle(incident,undefined,{phase:'stop_requested',signal:'possibly_submitted',step:'original-step',trigger:'native_thread_wedged',attempt:{
    id:'original-attempt',old,at:now,stopDeadline:now+15000,ledger:{held:[],uncertain:[]},
    profile:{sourceId:old.runtimeId,sessionId:peer.sessionId!,file:'original-rollout',settingsKey:'original-settings'},
    decision:{revision:'original-revision',facts:'original-facts',work:{activeTurnIds:[],progress:'idle',roles:[],unresolved:[]},since:now,noticeId:'original-notice',autoEligible:true},
  }},()=>recovery.recordRecycle(peer.id,now));
  let inspected=0;
  Object.assign(f.codex,{inspectRecycleOld:async(proof:typeof old)=>{assert.deepEqual(proof,old);inspected++;return 'alive';}});
  const members=lifecycle(f.broker);
  if(gate==='会话冲突')f.store.setSetting('managedSessionConflict:'+peer.id,true);
  if(gate==='解除配对')f.store.unpair(pair.id);
  else if(gate==='人工审批')await assert.rejects(f.broker.dispatch('open',{host:'codex'}),{code:'restart_in_progress'});
  now+=15000;
  await Promise.all(members.schedulePairedRecoveries().values());
  const current=recovery.get(peer.id);
  assert.equal(current?.state,gate==='人工审批'?'recovering':'closed');
  assert.equal(current?.recycle?.phase,'stop_requested');
  assert.equal(inspected,0);
  assert.equal(recovery.lastRecycle(peer.id),now-15000);
  if(gate==='人工审批'){
    await assert.rejects(f.broker.dispatch('open',{host:'codex'}),{code:'restart_in_progress'});
  }
  assert.equal(f.codex.ensured,0);
});

test('回收阻塞的恢复状态与通知使用同一处理步骤',t=>{
  const {store,broker,c,x}=fixture(t),pair=store.pair(c.id,x.id),recovery=broker.managedRecovery;
  const incident=recovery.observe(x,pair,store.endpoint(x.id));
  for(const [code,step] of [['codex_recycle_old_holder',RECYCLE_CLEANUP_USER_STEP],['codex_recycle_old_unknown',RECYCLE_OLD_UNKNOWN_USER_STEP]] as const){
    const current=recovery.get(x.id)!;
    recovery.updateRecycle(incident,current.recycle,{phase:'held',step:code,trigger:'native_thread_wedged',code,reason:'等待核验证据。'});
    assert.equal(lifecycle(broker).recoveryStatus(x)?.user_step,step);
  }
});

test('恢复步骤直接对用户说当前这一步，只写用到的目录、入口和会话',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),recovery=new ManagedRecovery(f.store);
  const families:[string[],RegExp][]=[
    [['workspace_untrusted','trust_required'],/打开一次.*接受目录信任提示/],
    [['bootstrap_policy_unsupported'],/更新.*nxbridge setup --codex-command/],
    [['launch_proxy_unavailable'],/本机代理/],
    [['launch_profile_missing'],/重新安装或更新.*会自动重试.*config set hosts\./],
    [['launch_command_missing','launch_command_invalid'],/改正.*\.command.*config get/],
    [['launch_probe_unavailable'],/暂时没能核验.*会自动重试.*doctor/],
    [['launch_provenance_missing'],/指定原安装渠道.*会话 ID/],
    [['launch_image_ambiguous'],/等 Claude 桌面端更新完成.*config set/],
    [['profile_version_unsupported','dedicated_mode_unsupported'],/更新到支持这个会话的版本.*重新打开会话/],
    [['original_home_changed','cwd_mismatch'],/改回原来的.*有意迁移.*重新打开会话/],
    [['session_not_found','session_ambiguous','session_invalid','resume_not_found','rollout_not_found'],/找回会话.*历史记录.*另选搭档/],
    [['invalid_config'],/修复.*配置文件/],
    [['owned_profile_unverified','restart_profile_unverified'],/告诉 Claude.*查原来的设置并决定怎么处理/],
    [['foreground_active','claude_foreground_active'],/回到正在接手这个会话的窗口/],
    [['claude_background_unavailable','claude_bypass_consent_required','claude_bypass_disabled','claude_inbound_restricted','claude_policy_unverified'],/确认后台运行、审批和收信设置/],
    [['session_changed','future_precondition'],/^运行 nxbridge status 查看原因$/]
  ];
  for(const host of ['claude','codex'] as const){
    const peer={...(host==='claude'?f.c:f.x),cwd:`Q:/原工程/${host} workspace`},
      incident=recovery.observe(peer,pair,undefined,{configHome:`Q:/原配置/${host} home`,executable:`Q:/原入口/${host} wrapper.cmd`});
    incident.state='blocked';
    for(const [codes,expected] of families)for(const code of codes){
      incident.preconditionCode=code;const step=recoveryUserStep(peer,incident)!;
      assert.match(step,expected,host+':'+code);assert.doesNotMatch(step,/请用户|AI/,host+':'+code);
                              
      if(['workspace_untrusted','trust_required'].includes(code))for(const part of [peer.cwd,incident.configHome,incident.proof.executable!,peer.host])assert.ok(step.includes(part),host+':'+code);
      if(['launch_profile_missing','profile_version_unsupported'].includes(code))assert.ok(step.includes(incident.proof.executable!));
      if(code==='profile_version_unsupported'||code==='launch_provenance_missing')assert.ok(step.includes(incident.sessionId));
      if(['session_not_found','invalid_config','original_home_changed'].includes(code))assert.ok(step.includes(incident.configHome),host+':'+code);
      if(/^(?:session_|resume_|rollout_)/.test(code))assert.doesNotMatch(step,/--resume|\bresume\s+\S|--last/);
    }
  }
});

test('未知启动结果覆盖陈旧前置失败，非阻塞状态清除用户步骤且不虚构路径',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),incident=new ManagedRecovery(f.store).observe(f.x,pair);
  incident.state='blocked';incident.preconditionCode='bootstrap_required';
  assert.equal(recoveryUserStep(f.x,incident),'Claude 会先试着运行一次；不行的话，在 Claude/Codex 之外的普通 PowerShell 里运行 nxbridge bootstrap');
  assert.equal(recoveryUserStep(f.c,incident),'在 Claude/Codex 之外的普通 PowerShell 里运行 nxbridge bootstrap');
  incident.state='unknown';
  const unknown=recoveryUserStep(f.x,incident)!;assert.match(unknown,/nxbridge status 核对上次启动结果.*不要再次启动或恢复会话/);assert.doesNotMatch(unknown,/bootstrap/);
  for(const state of ['pending','recovering','ready','exhausted','closed'] as const){incident.state=state;assert.equal(recoveryUserStep(f.x,incident),undefined);}
  incident.state='blocked';incident.preconditionCode='launch_profile_missing';incident.configHome='';delete incident.proof.executable;
  const missing=recoveryUserStep(f.x,incident)!;assert.match(missing,/原来的 codex 启动方式/);assert.doesNotMatch(missing,/undefined|~\//);
});

test('确定的启动前置失败跨重启和时钟变化保持暂停，只在输入变化后重新准入',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);let now=1000000,mono=0;
  let recovery=new ManagedRecovery(f.store,()=>now,()=>mono),incident=recovery.observe(f.x,pair,f.store.endpoint(f.x.id));
  recovery.blocked(incident,new BridgeError('profile_version_unsupported','原入口版本较旧'),{code:'profile_version_unsupported',fingerprint:'original-input'});
  assert.equal(incident.attempts.length,0);assert.equal(incident.nextAt,Number.MAX_SAFE_INTEGER);assert.equal(recovery.due(incident),false);
  now-=300000;mono+=3600000;recovery=new ManagedRecovery(f.store,()=>now,()=>mono);incident=recovery.observe(f.x,pair,f.store.endpoint(f.x.id));
  assert.equal(incident.nextAt,Number.MAX_SAFE_INTEGER);assert.equal(recovery.due(incident),false);
  recovery.rearm(incident);assert.equal(recovery.due(incident),true);assert.equal(recovery.get(f.x.id)?.launchPark,undefined);
});

test('恢复额度在启动前消费，按 30/120 秒退避，三次加一次跨实例保留',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);let now=1000000,mono=0;
  let recovery=new ManagedRecovery(f.store,()=>now,()=>mono),incident=recovery.observe(f.x,pair,f.store.endpoint(f.x.id));
  for(let index=0;index<3;index++){
    assert.equal(recovery.due(incident),true);recovery.claim(incident);assert.equal(recovery.get(f.x.id)!.attempts.length,index+1);
    recovery.settled(incident,new BridgeError('connection_reset','隔离的瞬时失败'));
    if(index<2){const wait=index===0?30000:120000;assert.equal(incident.nextAt,now+wait);now+=wait;}
  }
  assert.equal(recovery.due(incident),false);now+=120000;
  recovery=new ManagedRecovery(f.store,()=>now,()=>mono);incident=recovery.observe(f.x,pair,f.store.endpoint(f.x.id));
  assert.throws(()=>recovery.claim(incident),{code:'recovery_exhausted'});
  recovery.claim(incident,'用户报告本地代理已恢复');recovery.settled(incident);assert.equal(recovery.get(f.x.id)!.attempts.length,4);
  assert.throws(()=>recovery.claim(incident,'重复请求'),{code:'recovery_exhausted'});
  recovery.ready(f.x.id,true);for(let tick=0;tick<12;tick++){mono+=5000;now+=5000;recovery.ready(f.x.id,true);}assert.equal(recovery.get(f.x.id)!.state,'closed');
});

test('前置条件不花启动额度；启动结果丢失在重启后只允许核对',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);let now=1000000;
  let recovery=new ManagedRecovery(f.store,()=>now),incident=recovery.observe(f.x,pair,f.store.endpoint(f.x.id));
  recovery.blocked(incident,new BridgeError('workspace_untrusted','工程未信任'));assert.equal(incident.attempts.length,0);assert.equal(recovery.due(incident),false);
  now+=30000;assert.equal(recovery.due(incident),true);recovery.claim(incident);
  recovery=new ManagedRecovery(f.store,()=>now);incident=recovery.observe(f.x,pair,f.store.endpoint(f.x.id));
  assert.equal(incident.state,'unknown');assert.equal(incident.attempts[0]!.state,'unknown');assert.throws(()=>recovery.claim(incident),{code:'recovery_uncertain'});
  assert.equal(f.store.setting<any>(managedRecoveryKey(f.x.id)).proof.endpoint,f.store.endpoint(f.x.id)!.address);
});

test('六次每小时上限跨稳定关闭保留，向后跳时不会返还已用额度',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);let now=1000000,mono=0;
  const recovery=new ManagedRecovery(f.store,()=>now,()=>mono);
  for(let index=0;index<6;index++){
    const incident=recovery.observe(f.x,pair,f.store.endpoint(f.x.id));recovery.claim(incident);recovery.settled(incident);recovery.ready(f.x.id,true);for(let tick=0;tick<12;tick++){now+=5000;mono+=5000;recovery.ready(f.x.id,true);}
    assert.equal(recovery.get(f.x.id)!.state,'closed');
  }
  now-=300000;const incident=recovery.observe(f.x,pair,f.store.endpoint(f.x.id));assert.throws(()=>recovery.claim(incident),{code:'recovery_rate_limited'});
});

test('接管归还不改托管归属，活窗口不因期限归还，后台死亡可解除远程接管',async t=>{
  const f=fixture(t);f.store.pair(f.c.id,f.x.id);const peer=f.store.updatePeer(f.x.id,{owner:'nxbridge'}),endpoint=f.store.endpoint(peer.id)!;
  let states:('alive'|'dead'|'unknown')[]=['alive','alive'];const leases=new Takeovers(f.store,async()=>states,undefined,undefined,()=>false),lease=leases.begin(peer,endpoint);
  leases.claim(peer.id,lease.id,{pid:123,procStart:'foreground'});assert.equal(await leases.returned(peer.id,lease.id),false);assert.equal(f.store.peer(peer.id).owner,'nxbridge');
  states=['unknown','dead'];assert.equal(await leases.returned(peer.id,lease.id),true);assert.equal(leases.get(peer.id),null);
  const second=leases.begin(peer,endpoint);assert.equal(await leases.returned(peer.id,lease.id),false);assert.equal(leases.get(peer.id)!.id,second.id);
});

test('未知启动按退避只核对，适配器证明退出后继续；额外额度不受上次失败退避阻拦',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);let now=1000000;
  const recovery=new ManagedRecovery(f.store,()=>now),incident=recovery.observe(f.x,pair,f.store.endpoint(f.x.id));
  recovery.claim(incident);recovery.settled(incident,new BridgeError('timeout','原生结果未知'),true);
  assert.equal(recovery.due(incident),false);now=incident.nextAt;assert.equal(recovery.due(incident),true);
  assert.throws(()=>recovery.claim(incident),{code:'recovery_uncertain'});recovery.absent(incident);
  recovery.claim(incident);recovery.settled(incident,new Error('第二次失败'));now=incident.nextAt;
  recovery.claim(incident);recovery.settled(incident,new Error('第三次失败'));
  assert.ok(incident.nextAt>now);recovery.claim(incident,'代理已恢复');assert.equal(incident.attempts.at(-1)!.extra,true);
});

test('未登记前台代次时，CLI 确认窗口结束仍可归还；旧租约不能归还新接管',async t=>{
  const f=fixture(t);f.store.pair(f.c.id,f.x.id);const peer=f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  const leases=new Takeovers(f.store,async()=>['alive']),lease=leases.begin(peer,f.store.endpoint(peer.id));
  assert.equal(await leases.returned(peer.id,lease.id),false);assert.equal(await leases.returned(peer.id,lease.id,true),true);
  const fresh=leases.begin(peer,f.store.endpoint(peer.id));assert.equal(await leases.returned(peer.id,lease.id,true),false);assert.equal(leases.get(peer.id)!.id,fresh.id);
});

test('CLI 在登记前退出：未知观察退避，发现原生窗口后保留，证明缺席才归还',async t=>{
  const f=fixture(t);f.store.pair(f.c.id,f.x.id);const peer=f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  let observations=0,matches:({pid:number;procStart:string}|undefined)[]|undefined,session:'alive'|'dead'='alive';
  const leases=new Takeovers(f.store,async()=>['alive'],async()=>{observations++;return matches;},async()=>session),lease=leases.begin(peer,f.store.endpoint(peer.id),undefined,{pid:2147483647,procStart:'dead-controller'});
  lease.startedAt-=10000;f.store.setSetting('takeover:'+peer.id,lease);
  assert.equal(await leases.returned(peer.id,lease.id),false);assert.equal(observations,1);
  assert.equal(await leases.returned(peer.id,lease.id),false);assert.equal(observations,1);
  let current=leases.get(peer.id)!;current.nextObservation=0;f.store.setSetting('takeover:'+peer.id,current);matches=[{pid:123,procStart:'observed'}];
  assert.equal(await leases.returned(peer.id,lease.id),false);assert.deepEqual(leases.get(peer.id)!.foreground,matches[0]);
  current=leases.get(peer.id)!;delete current.foreground;current.nextObservation=0;f.store.setSetting('takeover:'+peer.id,current);matches=[];
  assert.equal(await leases.returned(peer.id,lease.id),false,'登记集合为空不能证明控制器交出的终端已退出');
  session='dead';current=leases.get(peer.id)!;current.nextObservation=0;f.store.setSetting('takeover:'+peer.id,current);
  assert.equal(await leases.returned(peer.id,lease.id),true);assert.equal(leases.get(peer.id),null);assert.equal(f.store.peer(peer.id).owner,'nxbridge');
});

test('同一故障更新结构化唤醒并在最终准入记录，普通故障告警被取代',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),peer=f.store.updatePeer(f.x.id,{owner:'nxbridge'}),engine=f.broker.managedRecovery;
  const incident=(lifecycle(f.broker) as any).recoveryIncident(peer);
  (lifecycle(f.broker) as any).publishRecovery(incident,peer,'首次核对失败');
  let notice=f.broker.deliveryHealth.get(pair.id,f.c.id,incident.noticeId)!;assert.equal(notice.wake,false);assert.equal(f.broker.deliveryHealth.peek(pair.id,f.c.id).length,0);
  for(let index=0;index<3;index++){incident.nextAt=0;f.store.setSetting(managedRecoveryKey(peer.id),incident);engine.claim(incident);engine.settled(incident,new Error('瞬时失败'));}
  (lifecycle(f.broker) as any).publishRecovery(incident,peer,'自动额度用尽');
  notice=f.broker.deliveryHealth.peek(pair.id,f.c.id)[0]!;assert.equal(notice.wake,true);assert.equal(notice.recovery!.wake,true);
  assert.ok(f.broker.deliveryHealth.prepare(pair.id,f.c.id,notice.id,notice.scope,false,true));assert.equal(engine.get(peer.id)!.wakeAdmitted,true);
  const latest=engine.get(peer.id)!;(lifecycle(f.broker) as any).publishRecovery(latest,peer,'重复观察');assert.equal(f.broker.deliveryHealth.peek(pair.id,f.c.id).length,0);
  const rows=f.store.setting<any>(deliveryHealthKey(pair.id,f.c.id)).notices;assert.equal(rows.filter((value:any)=>value.source==='managed_recovery').length,1);
});

test('仅发给 Claude 的 bootstrap 阻塞可被动取件，状态更新清除旧级别与动作且不消耗额度',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),peer=f.store.updatePeer(f.x.id,{owner:'nxbridge'}),engine=f.broker.managedRecovery,
    incident=(lifecycle(f.broker) as any).recoveryIncident(peer) as ManagedIncident;
  const pending=f.store.accept(pair,f.c,'故障期间保留的消息','bootstrap-held');
  engine.blocked(incident,new BridgeError('bootstrap_required','独立启动尚未核验'));
  const interrupted={turn_id:'original-turn',tools:['原工具'],children:['原子代理'],effects:'prior tool effects require history inspection; nothing replayed'};
  incident.notice!.interrupted=interrupted;const happened=incident.notice!.happened;
  const publish=(did:string)=>(lifecycle(f.broker) as any).publishRecovery(incident,peer,did),
    rows=()=>f.store.setting<{notices:RuntimeNotice[]}>(deliveryHealthKey(pair.id,f.c.id))!.notices,
    current=()=>rows().find(row=>row.id===incident.noticeId)!;
  publish('等待独立启动');
  let notice=f.broker.deliveryHealth.peek(pair.id,f.c.id)[0]!;
  assert.equal(notice.tier,3);assert.equal(notice.wake,false);assert.equal(notice.recovery!.wake,false);
  assert.match(notice.recovery!.ai_action!,/attempt nxbridge bootstrap once.*verifies independent launch before handover.*originating window.*ordinary PowerShell outside Claude\/Codex.*do not resend/);
  assert.equal(f.broker.deliveryHealth.prepare(pair.id,f.c.id,notice.id,notice.scope,false,true),undefined);
  assert.equal(engine.get(peer.id)!.noticeAdmitted,undefined);
  for(const [state,code] of [['blocked','workspace_untrusted'],['unknown','bootstrap_required'],['pending','bootstrap_required'],['ready','bootstrap_required']] as const){
    incident.state=state;incident.preconditionCode=code;publish('状态已更新');
    assert.equal(current().tier,undefined);assert.equal(f.broker.deliveryHealth.get(pair.id,f.c.id,incident.noticeId)!.tier,1);
    assert.equal(current().recovery!.ai_action,undefined);assert.equal(f.broker.deliveryHealth.peek(pair.id,f.c.id).length,0);
    if(state==='pending'||state==='ready')assert.equal(Object.hasOwn(current().recovery!,'user_step'),false);
    if(state==='unknown')assert.match(current().recovery!.user_step!,/nxbridge status/);
    assert.deepEqual(current().recovery!.interrupted,interrupted);assert.equal(current().recovery!.happened,happened);
  }
  incident.state='blocked';publish('仍待独立启动');notice=f.broker.deliveryHealth.peek(pair.id,f.c.id)[0]!;
  assert.equal(notice.tier,3);assert.ok(f.broker.deliveryHealth.prepare(pair.id,f.c.id,notice.id,notice.scope));
  const admitted=engine.get(peer.id)!;assert.equal(admitted.noticeAdmitted,true);assert.notEqual(admitted.wakeAdmitted,true);assert.equal(admitted.attempts.length,0);
  (lifecycle(f.broker) as any).publishRecovery(admitted,peer,'重复观察');assert.equal(f.broker.deliveryHealth.peek(pair.id,f.c.id).length,0);
  assert.equal(rows().length,1);assert.equal(f.store.messages().length,1);assert.equal(f.store.message(pending.id).state,'accepted');
  admitted.nextAt=0;f.store.setSetting(managedRecoveryKey(peer.id),admitted);engine.claim(admitted);engine.settled(admitted);
  (lifecycle(f.broker) as any).publishRecovery(admitted,peer,'原会话恢复完成');
  assert.equal(admitted.notice!.user_step,undefined);assert.match(admitted.notice!.ai_action!,/inspect original history, partial work and unresolved effects/);
  assert.equal(admitted.notice!.wake,true);assert.deepEqual(admitted.notice!.interrupted,interrupted);assert.equal(admitted.attempts.length,1);
});

test('发给 Codex 的 bootstrap 阻塞保留诊断，不提高级别或提供 Claude 执行动作',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),peer=f.store.updatePeer(f.c.id,{owner:'nxbridge'}),incident=(lifecycle(f.broker) as any).recoveryIncident(peer) as ManagedIncident;
  f.broker.managedRecovery.blocked(incident,new BridgeError('bootstrap_required','独立启动尚未核验'));
  (lifecycle(f.broker) as any).publishRecovery(incident,peer,'等待用户处理');
  const notice=f.broker.deliveryHealth.get(pair.id,f.x.id,incident.noticeId)!;
  assert.equal(notice.tier,1);assert.equal(notice.wake,false);assert.equal(notice.recovery!.ai_action,undefined);assert.ok(notice.recovery!.user_step);
  assert.equal(f.broker.deliveryHealth.peek(pair.id,f.x.id).length,0);assert.equal(incident.attempts.length,0);
});

test('托管仅重连不打开退出事件；已结束子代理不导致空闲恢复唤醒',async t=>{
  const f=fixture(t);f.store.pair(f.c.id,f.x.id);const peer=f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'offline'});
  assert.equal(await lifecycle(f.broker).recover(peer),true);assert.equal(f.broker.managedRecovery.get(peer.id),undefined);
});

test('恢复通知与状态都统计故障成员的入站消息，出站不确定消息独立保留',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),peer=f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  f.store.accept(pair,f.c,'入站一','incoming-one');f.store.accept(pair,f.c,'入站二','incoming-two');
  const uncertain=f.store.accept(pair,f.x,'出站未知','outgoing-unknown');f.store.claim(uncertain.id);f.store.transition(uncertain.id,'unknown');
  const incident=(lifecycle(f.broker) as any).recoveryIncident(peer);(lifecycle(f.broker) as any).publishRecovery(incident,peer,'只读核对失败');
  assert.deepEqual(f.broker.deliveryHealth.get(pair.id,f.c.id,incident.noticeId)!.recovery!.msgs,{held:2,uncertain:0});
  const status=lifecycle(f.broker).recoveryStatus(peer);assert.ok(status);assert.equal(status.held,2);assert.equal(status.uncertain,0);
});

test('休眠跨过稳定窗口仍需重新观察；时钟倒退只压缩等待，不退还额度',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);let now=1000000,mono=0;
  const recovery=new ManagedRecovery(f.store,()=>now,()=>mono),incident=recovery.observe(f.x,pair,f.store.endpoint(f.x.id));recovery.claim(incident);recovery.settled(incident);recovery.ready(f.x.id,true);
  now+=3600000;mono+=3600000;recovery.ready(f.x.id,true);assert.equal(recovery.get(f.x.id)!.state,'ready');
  for(let tick=0;tick<12;tick++){now+=5000;mono+=5000;recovery.ready(f.x.id,true);}assert.equal(recovery.get(f.x.id)!.state,'closed');
});

test('原托管会话在前台接续后，确认退出才归还后台，保留原会话、配对和消息',async t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),original=f.store.endpoint(f.x.id)!,pending=f.store.accept(pair,f.c,'原消息','custody');
  const peer=f.store.updatePeer(f.x.id,{owner:'user'}),input={host:'codex' as const,sessionId:peer.sessionId!,cwd:peer.cwd,name:peer.name,kind:'root' as const,permission:'prompting' as const,endpoint:{...original,mode:'embedded' as const,address:'',pid:123,procStart:'user-window',configHome:'/isolated/custody'}};
  f.store.saveEndpoint({...input.endpoint,peerId:peer.id,active:true,epoch:'user',seenAt:Date.now()});recordManagedAdoption(f.store,peer,input,original);
  let state:'alive'|'dead'='alive';const takeovers=(f.broker as any).takeovers as Takeovers;
  t.mock.method(takeovers as any,'probe',async()=>[state,state]);t.mock.method(takeovers as any,'alive',()=>false);const lease=takeovers.get(peer.id)!;
  assert.equal(await lifecycle(f.broker).returnTakeover(peer.id,lease.id),false);assert.equal(f.store.peer(peer.id).owner,'user');state='dead';
  assert.equal(await lifecycle(f.broker).returnTakeover(peer.id,lease.id),true);assert.equal(f.store.peer(peer.id).owner,'nxbridge');assert.equal(f.store.peer(peer.id).sessionId,peer.sessionId);assert.equal(f.store.pairFor(peer.id)!.id,pair.id);assert.equal(f.store.message(pending.id).state,'accepted');assert.equal(f.store.peer(peer.id).state,'offline');
});

test('远程接管支持 resume --last 和选择器；活窗口的周期检查不启动代次助手',async t=>{
  const f=fixture(t);f.store.pair(f.c.id,f.x.id);let probes=0;
  const leases=new Takeovers(f.store,async()=>{probes++;return['dead','alive'];},undefined,undefined,()=>true),lease=leases.begin(f.x,f.store.endpoint(f.x.id));
  for(const args of [['codex','--remote',lease.endpoint!.address,'resume','--last'],['codex','--remote='+lease.endpoint!.address,'resume']])assert.equal(foregroundMatchesTakeover(lease,'codex',args),true);
  assert.equal(foregroundMatchesTakeover(lease,'codex',['codex','--remote','another','resume','--last']),false);
  assert.equal(foregroundMatchesTakeover(lease,'codex',['codex','resume',lease.sessionId]),false);
  leases.claim(f.x.id,lease.id,{pid:123,procStart:'foreground'});
  for(let tick=0;tick<3;tick++)assert.equal(await leases.returned(f.x.id,lease.id),false);
  assert.equal(probes,0);
});
