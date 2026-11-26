import assert from 'node:assert/strict';
import test from 'node:test';
import type { HealthStorage } from '../broker/health/contracts.js';
import { CodexRecycle, codexRecycleFenced, type CodexRecycleOperation, type CodexRecyclePorts, type CodexRecycleWork } from '../broker/recovery/codex-recycle.js';
import { ManagedRecovery, recoveryUserStep, type ManagedIncident } from '../broker/recovery/managed-recovery.js';
import { BridgeError } from '../shared/errors.js';
import type { Pair, Peer } from '../types.js';

function fixture() {
  const values=new Map<string,unknown>();
  const storage:HealthStorage={
    setting<T>(key:string){return structuredClone(values.get(key)) as T|undefined;},
    setSetting(key,value){values.set(key,structuredClone(value));},
    transaction<T>(work:()=>T):T{
      const before=new Map(values);
      try{return work();}catch(error){values.clear();for(const [key,value] of before)values.set(key,value);throw error;}
    },
    event(){},
  };
  let now=1000;
  const makeRecovery=()=>new ManagedRecovery(storage,()=>now,()=>now);
  let recovery=makeRecovery(),engine=new CodexRecycle(recovery,()=>now);
  const peer={id:'member',sessionId:'original',host:'codex',owner:'nxbridge'} as Peer,pair={id:'pair'} as Pair;
  let incident=recovery.observe(peer,pair),alive=true,progressToken=false,launchExists=false,partner=true,authority=true;
  const old={runtimeId:'old',endpoint:'old-endpoint',pid:123,procStart:'start123'};
  const fresh={runtimeId:'new',endpoint:'new-endpoint',pid:234,procStart:'start234'};
  const profile={sourceId:'old',sessionId:'original',file:'original-rollout',settingsKey:'profile'};
  const ledger={held:['unsent'],uncertain:[{id:'original-input',senderId:'sender'}]};
  let work:CodexRecycleWork={activeTurnIds:[],progress:'idle',roles:[],unresolved:[]};
  const seen={notices:[] as string[],effects:[] as string[],published:[] as typeof ledger[],retired:0};
  const current=()=>recovery.get(peer.id)!;
  const sample=(trigger:Parameters<typeof engine.advance>[1],accept?:Parameters<typeof engine.advance>[3])=>engine.advance(incident,trigger,ports,accept);
  const want=(phase:CodexRecycleOperation['phase'])=>{
    const operation=current().recycle;
    assert.equal(operation?.phase,phase);return operation!;
  };
  const ports:CodexRecyclePorts={
    async observe(){return {kind:'available',old,profile,work};},
    guard(){if(!authority)throw new BridgeError('incident_changed','用户已接管');},
    partnerReachable(){return partner;},deliveredTurnCompletedSince(){return progressToken;},
    ledger(){return ledger;},notify(_incident,basis){seen.notices.push(basis.decision.revision);},
    persistProfile(){seen.effects.push('profile');assert.equal(want('preflight').phase,'preflight');},
    async inspectOld(){return alive?'alive':'dead';},
    async stop(_old,_deadline,guard,markSignal){guard();markSignal();want('stop_requested');seen.effects.push('stop');alive=false;},
    async retireOld(_old,guard){guard();seen.effects.push('retire');},
    async prepareLaunch(){return {runtimeId:fresh.runtimeId};},
    async launch(_intent,guard){guard();want('launch_requested');seen.effects.push('launch');launchExists=true;},
    async inspectLaunch(){return launchExists?{kind:'matched',fresh}:{kind:'unstarted'};},
    async verify(_fresh,_original,{markSubmitted,guard,resumeSubmitted}){
      guard();assert.equal(resumeSubmitted,false);markSubmitted();seen.effects.push('resume');
    },
    finish(_incident,ready){assert.equal(codexRecycleFenced(current()),false);seen.published.push(ready.attempt.ledger);},
  };
  return {
    ports,seen,old,fresh,profile,ledger,current,want,lastRecycle:()=>recovery.lastRecycle(peer.id),
    sample,run:async(trigger:Parameters<typeof engine.advance>[1]='native_thread_wedged',accept?:Parameters<typeof engine.advance>[3])=>{
      const first=await sample(trigger,accept),stage=current().recycle;
      if(stage?.phase==='observing'&&stage.basis)await sample(trigger);
      return first;
    },
    tick:(ms:number)=>{now+=ms;},
    work:(next:Partial<CodexRecycleWork>)=>{work={...work,...next};},
    authority:(next:boolean)=>{authority=next;},partner:(next:boolean)=>{partner=next;},
    close:()=>recovery.closeRecycle(current()),
    oldAlive:(next:boolean)=>{alive=next;},launchExists:(next:boolean)=>{launchExists=next;},
    restore:()=>{recovery=makeRecovery();engine=new CodexRecycle(recovery,()=>now);},
    nextIncident:(completed:boolean)=>{
      for(let i=0;i<=6;i++){recovery.ready(peer.id,true);now+=10000;}
      assert.equal(current().state,'closed');
      alive=true;launchExists=false;progressToken=completed;incident=recovery.observe(peer,pair);
    },
    incident:()=>incident,
  };
}

function accept(f:ReturnType<typeof fixture>,index=-1) {
  return {incidentId:f.incident().id,evidenceRevision:f.seen.notices.at(index)!,reason:'已核对工作，接受可能的中断。'};
}

test('确认很可能空闲后只回收一次，各阶段先保存并只结算一次原消息',async()=>{
  const f=fixture();
  await f.run();f.restore();await f.run();
  const ready=f.want('ready');
  assert.ok(ready.phase==='ready'&&ready.finalized);
  assert.deepEqual(f.seen.effects,['profile','stop','retire','launch','resume']);
  assert.deepEqual(f.seen.published,[f.ledger]);
  assert.deepEqual(f.seen.notices,[]);
  f.nextIncident(false);await f.run();
  f.want('awaiting_partner');assert.equal(f.seen.effects.filter(x=>x==='stop').length,1);
  await f.run('native_thread_wedged',accept(f));f.want('ready');
  f.nextIncident(true);await f.run();
  f.want('ready');assert.equal(f.seen.effects.filter(x=>x==='stop').length,3);
});

test('输出和时间变化沿用原确认，重新出现进展则撤销旧确认',async()=>{
  const f=fixture();f.work({activeTurnIds:['turn'],progress:'stalled',lastProgressAt:10});
  await f.run();const stale=accept(f);
  f.work({lastProgressAt:20});await f.run();assert.equal(f.seen.notices.length,1);
  f.work({progress:'progressing',lastProgressAt:30});
  assert.equal((await f.run('native_thread_wedged',stale)).kind,'stale');
  assert.equal(f.seen.notices.length,2);assert.equal(f.seen.effects.length,0);
  f.work({lastProgressAt:40});
  await f.run('native_thread_wedged',accept(f));f.want('ready');
  assert.equal(f.seen.notices.length,2);
});

test('持续进展保留旧决定，确认停滞十五分钟或重新推进时才更新',async()=>{
  const f=fixture();f.work({activeTurnIds:['turn'],progress:'unknown',lastProgressAt:1000});
  await f.run();
  const initial=f.want('awaiting_partner');
  assert.ok(initial.phase==='awaiting_partner'&&initial.basis.decision.work.progress==='progressing');
  const first=accept(f);
  f.tick(60000);f.work({lastProgressAt:61000});await f.run();
  f.tick(3*60000);await f.run();
  const progressing=f.want('awaiting_partner');
  assert.ok(progressing.phase==='awaiting_partner'&&progressing.basis.decision.work.progress==='progressing');
  assert.deepEqual(f.seen.notices,[first.evidenceRevision]);
  f.tick(12*60000);await f.run();
  const stalled=f.want('awaiting_partner');
  assert.ok(stalled.phase==='awaiting_partner'&&stalled.basis.decision.work.progress==='stalled');
  const stale=accept(f);f.work({lastProgressAt:961000});
  assert.equal((await f.run('native_thread_wedged',stale)).kind,'stale');
  assert.equal(f.seen.notices.length,3);
  f.tick(45*1000);await f.run();
  assert.equal(f.seen.notices.length,3);
  assert.deepEqual(f.seen.effects,[]);
});

test('正常卸载宽限期内未同意中断，恢复空闲后自然卸载且不发送停止信号',async()=>{
  const f=fixture();f.work({activeTurnIds:['turn'],progress:'progressing'});
  await f.run('loaded_unleased');
  f.want('awaiting_partner');assert.equal(f.seen.effects.length,0);
  f.work({activeTurnIds:[],progress:'idle'});
  assert.equal((await f.run('loaded_unleased')).kind,'natural');
  assert.equal(f.current().recycle,undefined);assert.equal(f.seen.effects.length,0);
});

test('有真实轮次与积压输入时，当前搭档同意可回收；工作改变后的旧同意不能执行',async()=>{
  const f=fixture();f.work({activeTurnIds:['old-turn'],progress:'stalled'});
  await f.run('loaded_unleased');const stale=accept(f);
  f.work({activeTurnIds:['new-turn'],progress:'progressing'});
  assert.equal((await f.run('loaded_unleased',stale)).kind,'stale');
  assert.equal(f.seen.effects.length,0);
  await f.run('loaded_unleased',accept(f));f.want('ready');
  assert.equal(f.seen.effects.filter(effect=>effect==='stop').length,1);
});

test('普通重连的停止前预检发现活动轮次消失时撤销批准并回到自然接续',async()=>{
  const f=fixture();f.work({activeTurnIds:['turn'],progress:'stalled'});
  await f.run('loaded_unleased');const acceptance=accept(f),observe=f.ports.observe;
  let observations=0;
  f.ports.observe=async trigger=>{
    if(++observations===2)f.work({activeTurnIds:[],progress:'unknown',unresolved:['native_work_unavailable']});
    return observe(trigger);
  };
  assert.equal((await f.run('loaded_unleased',acceptance)).kind,'stale');
  assert.equal(f.current().recycle,undefined);
  assert.deepEqual(f.seen.effects,[]);
  assert.equal(f.seen.notices.length,1);
});

for(const phase of ['首次观察','停止前预检'] as const)test(`普通重连的${phase}发现原设置无法核验时保留阻塞原因和用户步骤`,async()=>{
  const f=fixture();f.work({activeTurnIds:['turn'],progress:'stalled'});
  const preflight=phase==='停止前预检',observe=f.ports.observe;
  if(preflight)await f.run('loaded_unleased');
  const blocked={kind:'held',code:'owned_profile_unverified',reason:'原配置证据不足'} as const;
  let observations=0;
  f.ports.observe=async trigger=>preflight&&++observations===1?observe(trigger):blocked;
  await f.run('loaded_unleased',preflight?accept(f):undefined);
  if(preflight){f.want('observing');assert.equal(codexRecycleFenced(f.current()),false);await f.run('loaded_unleased');}
  const held=f.want('held');
  assert.ok(held.phase==='held');
  assert.equal(held.code,blocked.code);
  assert.equal(held.reason,blocked.reason);
  assert.equal(recoveryUserStep({host:'codex',cwd:process.cwd()},f.current()),'告诉 Claude，由它帮你查原来的设置并决定怎么处理');
  assert.deepEqual(f.seen.effects,[]);
  assert.equal(f.lastRecycle(),undefined);
  assert.equal(f.seen.notices.length,preflight?1:0);
});

test('普通重连在停止调用等待期间失去最后一条待投递消息时不发停止信号',async()=>{
  const f=fixture();f.work({activeTurnIds:['turn'],progress:'stalled'});
  await f.run('loaded_unleased');const stop=f.ports.stop;
  f.ports.stop=async(old,deadline,authorize,markSignal,force)=>{
    authorize();await Promise.resolve();f.ledger.held.length=0;
    await stop(old,deadline,authorize,markSignal,force);
  };
  await f.run('loaded_unleased',accept(f));
  f.want('observing');
  assert.equal(codexRecycleFenced(f.current()),false);
  assert.equal(f.lastRecycle(),undefined);
  assert.deepEqual(f.seen.effects,['profile']);
});

test('归属变化或原设置未核验时，当前确认不能启动回收',async()=>{
  const f=fixture();f.work({activeTurnIds:['turn'],progress:'progressing'});
  await f.run();f.authority(false);
  assert.equal((await f.run('native_thread_wedged',accept(f))).kind,'stale');
  assert.equal(f.seen.effects.length,0);
  f.authority(true);
  f.ports.observe=async()=>({kind:'held',code:'owned_profile_unverified',reason:'原配置证据不足'});
  assert.equal((await f.run('native_thread_wedged',accept(f))).kind,'stale');
  f.want('held');assert.equal(f.seen.effects.length,0);
});

test('搭档短暂不可达只记一次同一决策，空闲后仍可正常处理',async()=>{
  const f=fixture();f.work({progress:'unknown',roles:['tool']});
  await f.run();f.want('awaiting_partner');
  f.partner(false);await f.run();f.want('held');
  f.partner(true);await f.run();f.want('awaiting_partner');
  assert.equal(f.seen.notices.length,1);
  f.work({progress:'idle',roles:[]});await f.run();f.want('ready');
});

test('无搭档时仅用户可对当前证据明确接受可能的工作中断',async()=>{
  const f=fixture();f.work({progress:'progressing',activeTurnIds:['turn']});f.partner(false);
  await f.run();const held=f.want('held');assert.ok(held.phase==='held'&&held.basis);
  const acceptance={incidentId:f.incident().id,evidenceRevision:held.basis.decision.revision,reason:'明确接受工作可能中断。'};
  await f.run('native_thread_wedged',acceptance);f.want('held');
  await f.run('native_thread_wedged',{...acceptance,by:'user'});f.want('ready');
});

test('预检持久化失败不消费回收机会，重新观察后才允许停止',async()=>{
  const f=fixture(),persist=f.ports.persistProfile;
  f.ports.persistProfile=()=>{throw new BridgeError('incomplete_profile','保存预检快照前中断');};
  await f.run();const held=f.want('held');
  assert.ok(held.phase==='held'&&!held.paused);assert.deepEqual(f.seen.effects,[]);
  f.restore();f.ports.persistProfile=persist;await f.run();f.want('ready');
});

test('预检发现进展变化时产生新确认，未消费停止机会',async()=>{
  const f=fixture(),read=f.ports.observe;
  let reads=0;
  f.ports.observe=async trigger=>{
    if(++reads===2)f.work({progress:'progressing',activeTurnIds:['new-turn']});
    return read(trigger);
  };
  await f.run();f.want('observing');assert.deepEqual(f.seen.effects,[]);
});

test('停止请求持久化后协调器退出，仅沿用同一代次和停止期限',async()=>{
  const f=fixture(),stop=f.ports.stop;
  let deadline:number|undefined;
  f.ports.stop=async(_old,end,_guard,markSignal)=>{deadline=end;markSignal();throw new Error('停止结果未知');};
  await f.run();const held=f.want('held');
  assert.ok(held.phase==='held'&&held.paused?.phase==='stop_requested'&&held.paused.attempt.stopDeadline===deadline);
  f.restore();f.ports.stop=async()=>assert.fail('可能已提交的停止不能重发');
  f.oldAlive(false);await f.run();f.want('ready');assert.equal(f.seen.effects.filter(x=>x==='stop').length,0);

  const expired=fixture();expired.ports.stop=async(_old,_end,_guard,mark)=>{mark();throw new Error('停止结果未知');};
  await expired.run();expired.tick(15000);expired.restore();
  expired.ports.stop=async()=>assert.fail('不能延长原期限');
  await expired.run();const deadlineHold=expired.want('held');
  assert.ok(deadlineHold.phase==='held'&&deadlineHold.code==='codex_recycle_stop_deadline');
  assert.equal(codexRecycleFenced(expired.current()),false);
  await expired.run();expired.want('held');
});

test('停止入口已经提交时，即使随后失去授权也保留栅栏和原尝试供核对',async()=>{
  const f=fixture();let priorAttempt:string|undefined;
  f.ports.stop=async(_old,_deadline,authorize,markSignal)=>{
    markSignal();
    const stage=f.want('stop_requested');
    assert.ok(stage.phase==='stop_requested'&&stage.signal==='possibly_submitted');
    priorAttempt=stage.attempt.id;
    f.authority(false);authorize();
  };
  await f.run();
  let held=f.want('held');
  assert.ok(held.phase==='held'&&held.paused?.phase==='stop_requested'&&held.paused.attempt.id===priorAttempt);
  assert.equal(codexRecycleFenced(f.current()),true);
  assert.equal(f.lastRecycle(),1000);
  f.restore();await f.run();held=f.want('held');
  assert.ok(held.phase==='held'&&held.paused?.phase==='stop_requested'&&held.paused.signal==='possibly_submitted');
  assert.equal(codexRecycleFenced(f.current()),true);
  assert.deepEqual(f.seen.effects,['profile']);
});

test('原进程过时限仍存活只接受新的明确批准，旧批准与自动轮询不会再次停止',async()=>{
  const f=fixture(),stop=f.ports.stop;
  f.ports.stop=async(_old,_deadline,_guard,mark)=>{mark();throw new Error('停止结果未知');};
  await f.run();f.tick(15000);await f.run();
  const failed=f.want('held');assert.ok(failed.phase==='held'&&failed.basis);
  const approval={incidentId:f.incident().id,evidenceRevision:failed.basis.decision.revision,reason:'确认再次停止。',by:'user' as const};
  f.ports.stop=async()=>assert.fail('缺少新决定不得停止');
  await f.run();await f.run('native_thread_wedged',{...approval,evidenceRevision:'旧版本'});
  assert.equal(f.lastRecycle(),1000);
  f.ports.stop=stop;await f.run('native_thread_wedged',approval);f.want('ready');
  assert.equal(f.lastRecycle(),16000);
  assert.deepEqual(f.seen.effects,['profile','profile','stop','retire','launch','resume']);
});

test('停止额度使用可能提交信号时刻，意图写入后的先行完成不会返还额度',async()=>{
  const f=fixture(),stop=f.ports.stop;
  f.ports.stop=async(old,deadline,guard,mark,force)=>{f.tick(200);await stop(old,deadline,guard,mark,force);};
  await f.run();f.want('ready');assert.equal(f.lastRecycle(),1200);
  f.nextIncident(false);f.ports.deliveredTurnCompletedSince=at=>1100>at;
  await f.run();f.want('awaiting_partner');assert.equal(f.seen.effects.filter(effect=>effect==='stop').length,1);
});

test('替代进程已确定退出时保留账本交给普通托管启动',async()=>{
  const f=fixture(),inspect=f.ports.inspectLaunch;let state:'unstarted'|'absent'='unstarted',subscribed=false;
  f.ports.launch=async()=>{state='absent';};
  f.ports.inspectLaunch=async intent=>state==='absent'?{kind:'absent'}:inspect(intent);
  f.ports.recoveredSubscription=()=>subscribed?f.fresh:undefined;
  await f.run();const stage=f.want('handback');assert.ok(stage.phase==='handback'&&!stage.fresh);
  assert.equal(codexRecycleFenced(f.current()),false);assert.deepEqual(f.seen.published,[]);
  f.restore();subscribed=true;await f.run();f.want('ready');assert.deepEqual(f.seen.published,[f.ledger]);
});

test('信号提交前的临时权限制约不消耗额度，解除后允许本次自动恢复',async()=>{
  const f=fixture(),stop=f.ports.stop;
  f.ports.stop=async(_old,_deadline,guard)=>{f.authority(false);guard();};
  await f.run();f.want('observing');assert.equal(f.lastRecycle(),undefined);
  f.tick(16000);f.authority(true);f.ports.stop=stop;f.restore();await f.run();f.want('ready');
  assert.deepEqual(f.seen.effects,['profile','profile','stop','retire','launch','resume']);
});

for(const change of ['活动轮次已结束','短暂出现子进程','证据连接失败','原生已卸载'] as const)
test(`最后一次停止前观察发现${change}时返回观察，不消耗额度`,async()=>{
  const f=fixture(),observe=f.ports.observe,trigger=change==='原生已卸载'?'loaded_unleased':'native_thread_wedged';
  const approved=change==='活动轮次已结束'||change==='原生已卸载';
  if(approved){f.work({activeTurnIds:['turn'],progress:'stalled'});await f.run(trigger);}
  const acceptance=approved?accept(f):undefined,notices=f.seen.notices.length;
  let reads=0;
  f.ports.observe=async cause=>{
    if(++reads!==3)return observe(cause);
    if(change==='原生已卸载')return {kind:'natural'};
    if(change==='证据连接失败')return {kind:'held',code:'codex_recycle_evidence_unverified',reason:'连接未完成'};
    f.work(change==='活动轮次已结束'?{activeTurnIds:[],progress:'idle'}:{roles:['git'],progress:'unknown'});
    return observe(cause);
  };
  await f.sample(trigger,acceptance);
  f.want('observing');assert.equal(codexRecycleFenced(f.current()),false);
  assert.equal(f.lastRecycle(),undefined);assert.deepEqual(f.seen.effects,['profile']);assert.equal(f.seen.notices.length,notices);
});

test('核对替代进程期间授权消失时保留可见阻塞和原启动记录',async()=>{
  const f=fixture(),inspect=f.ports.inspectLaunch;
  f.ports.inspectLaunch=async launch=>{f.authority(false);return inspect(launch);};
  await f.run();
  const held=f.want('held');
  assert.ok(held.phase==='held'&&held.paused?.phase==='launch_requested');
  assert.equal(held.code,'incident_changed');
  assert.ok(held.reason);
  assert.equal(codexRecycleFenced(f.current()),true);
  assert.deepEqual(f.seen.effects,['profile','stop','retire']);
  f.authority(true);f.ports.inspectLaunch=inspect;f.restore();await f.run();f.want('ready');
  assert.deepEqual(f.seen.effects,['profile','stop','retire','launch','resume']);
});

test('旧进程退出后 broker 停止，恢复时沿用同一尝试继续启动',async()=>{
  const f=fixture(),retire=f.ports.retireOld;
  f.ports.retireOld=async(_old,guard)=>{f.authority(false);guard();};
  await f.run();const held=f.want('held');
  assert.ok(held.phase==='held'&&held.paused?.phase==='stopped'&&codexRecycleFenced(f.current()));
  assert.equal(codexRecycleFenced({...f.current(),state:'closed'}),false);
  f.tick(60000);f.authority(true);f.ports.retireOld=retire;f.restore();await f.run();f.want('ready');
  assert.deepEqual(f.seen.effects,['profile','stop','retire','launch','resume']);
});

test('等待期间关闭事件会解除栅栏，并阻止同一步骤的迟到副作用',async()=>{
  const f=fixture(),retire=f.ports.retireOld;
  f.ports.retireOld=async(old,guard,work)=>{await Promise.resolve();f.close();await retire(old,guard,work);};
  await f.run();assert.equal(f.current().state,'closed');assert.equal(codexRecycleFenced(f.current()),false);
  f.restore();await f.run();assert.deepEqual(f.seen.effects,['profile','stop']);
});

test('启动前权限暂停且没有启动登记，重启后只提交原启动意图一次',async()=>{
  const f=fixture(),launch=f.ports.launch;
  f.ports.launch=async(_intent,guard)=>{f.authority(false);guard();};
  await f.run();const held=f.want('held');
  assert.ok(held.phase==='held'&&held.paused?.phase==='launch_requested'&&codexRecycleFenced(f.current()));
  f.restore();f.authority(true);f.ports.launch=async(intent,guard,profile)=>{
    assert.equal(intent.runtimeId,held.paused?.phase==='launch_requested'&&held.paused.launch.runtimeId);return launch(intent,guard,profile);
  };
  await f.run();f.want('ready');assert.deepEqual(f.seen.effects,['profile','stop','retire','launch','resume']);
});

test('连续两轮证据不一致时既不唤醒搭档也不消耗停止额度',async()=>{
  const f=fixture(),observe=f.ports.observe;
  let reads=0;
  f.ports.observe=async trigger=>{
    if(++reads%2)f.work({roles:['git'],progress:'unknown'});else f.work({roles:['other'],progress:'unknown'});
    return observe(trigger);
  };
  for(let index=0;index<4;index++){
    await f.sample('native_thread_wedged');f.want('observing');assert.equal(codexRecycleFenced(f.current()),false);
  }
  assert.equal(f.lastRecycle(),undefined);assert.deepEqual(f.seen.effects,[]);assert.deepEqual(f.seen.notices,[]);
});

test('恢复回执无法带过 broker 重启时仅移交已证实的运行时，待普通订阅后结算原账本',async()=>{
  const f=fixture();let handedBack=0,subscribed=false;
  f.ports.verify=async(_fresh,_source,options)=>{
    if(!options.resumeSubmitted){options.markSubmitted();f.seen.effects.push('resume');}
    throw new BridgeError('codex_recycle_ack_missing','原订阅回执无法继续使用');
  };
  f.ports.prepareUnsubscribed=async fresh=>assert.deepEqual(fresh,f.fresh);
  f.ports.admitUnsubscribed=()=>{handedBack++;};
  f.ports.recoveredSubscription=()=>subscribed?f.fresh:undefined;
  await f.run();f.want('handback');assert.equal(codexRecycleFenced(f.current()),false);assert.deepEqual(f.seen.published,[]);
  f.restore();await f.run();f.want('handback');
  subscribed=true;await f.run();await f.run();f.want('ready');
  assert.equal(handedBack,1);assert.deepEqual(f.seen.published,[f.ledger]);
  assert.deepEqual(f.seen.effects,['profile','stop','retire','launch','resume']);assert.equal(f.seen.retired,0);
});

test('未知进程暂留时禁止替代启动，确认原进程退出后才能继续',async()=>{
  const f=fixture(),retire=f.ports.retireOld;
  f.work({roles:['tool'],progress:'unknown',unattested:[{pid:999,procStart:'old-tool',role:'tool'}]});
  await f.run();f.ports.retireOld=async()=>{throw new BridgeError('codex_recycle_old_holder','原子进程仍在运行');};
  await f.run('native_thread_wedged',accept(f));f.want('held');
  assert.equal(f.seen.effects.includes('launch'),false);
  f.restore();f.ports.retireOld=async(old,guard,work)=>{
    assert.deepEqual(work.unattested,[{pid:999,procStart:'old-tool',role:'tool'}]);await retire(old,guard,work);
  };
  await f.run();f.want('ready');
});

test('未知启动只核对原预约，不能另启第二个进程',async()=>{
  const f=fixture();
  const inspect=f.ports.inspectLaunch;let submitted=false;
  f.ports.inspectLaunch=async intent=>submitted&&!f.seen.effects.includes('confirm launch')?{kind:'unknown'}:inspect(intent);
  f.ports.launch=async()=>{submitted=true;f.seen.effects.push('unknown launch');throw new Error('启动回执丢失');};
  await f.run();f.want('held');f.restore();await f.run();f.want('held');
  f.launchExists(true);f.ports.inspectLaunch=inspect;await f.run();f.want('ready');
  assert.deepEqual(f.seen.effects,['profile','stop','retire','unknown launch','resume']);
});

test('未知恢复不重放并保留健康替代后台，原回执可继续核验一次',async()=>{
  const f=fixture();
  f.ports.verify=async(_fresh,_original,options)=>{
    if(!options.resumeSubmitted){options.markSubmitted();f.seen.effects.push('resume');}
    throw new BridgeError('codex_recycle_verify_unknown','恢复回执超时');
  };
  await f.run();f.want('held');assert.equal(codexRecycleFenced(f.current()),true);
  f.restore();f.ports.verify=async(_fresh,_source,options)=>assert.equal(options.resumeSubmitted,true);
  await f.run();f.want('ready');assert.equal(f.seen.retired,0);
  assert.equal(f.seen.effects.filter(x=>x==='resume').length,1);
  assert.deepEqual(f.seen.published,[f.ledger]);
});

test('提交结果时中断，仅重做原结果事务',async()=>{
  const f=fixture(),finish=f.ports.finish;
  f.ports.finish=()=>{throw new Error('结果提交前退出');};
  await assert.rejects(f.run(),/结果提交前退出/);
  const pending=f.want('ready');assert.ok(pending.phase==='ready'&&!pending.finalized);
  f.restore();f.ports.finish=finish;await f.run();f.want('ready');
  assert.deepEqual(f.seen.published,[f.ledger]);
  assert.deepEqual(f.seen.effects,['profile','stop','retire','launch','resume']);
});
