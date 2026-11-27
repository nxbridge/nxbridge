import test,{type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {appendFileSync,writeFileSync} from 'node:fs';
import {randomUUID} from 'node:crypto';
import {TurnEndCoordinator} from '../broker/turn-end.js';
import {TurnEndEvidence,turnNativeScope,type TurnEndStats} from '../broker/turn-end-evidence.js';
import {DeliveryHealth} from '../broker/health/delivery.js';
import type {BrokerHealthCoordinator} from '../broker/health/coordinator.js';
import type {CompletionHold} from '../broker/turn-end-text.js';
import {fixture,lifecycle} from './support/broker-fixture.js';
import {temporary} from './support/temporary.js';
import {saveManagedRuntime} from '../hosts/managed-runtimes.js';
import {managedMarker} from '../install/installation.js';
import {MOD_REVISION} from '../mod-revision.js';
import {StartupRoutes} from '../broker/startup-routes.js';
import {catchUpCodexTurnEvidence,readCodexTurnEvidence} from '../hosts/codex/codex-turn-evidence.js';
import {BridgeError,RuntimeRecoveryHold} from '../shared/errors.js';
import type {CodexAdapter} from '../hosts/codex/adapter.js';

function setup(t:TestContext,limit=4,startup?:StartupRoutes){
  t.mock.timers.enable({apis:['Date'],now:1000000});
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),health=new DeliveryHealth(f.store);
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});f.store.updatePeer(f.c.id,{state:'ready'});
  let hold:CompletionHold|undefined,owned=true;
  const file=path.join(temporary(t),'native.jsonl');
  const guard=new TurnEndCoordinator(f.store,health,{owned:()=>owned,source:()=>file,stats:()=>undefined,
    gate:(background,foreground,boundary)=>hold??(startup?.incomplete(f.x.id)?'native_state_unverified':f.store.pairFor(background.id)?.paused?'paused':
      background.id!==boundary&&f.store.peer(background.id).state!=='ready'?'background_busy':
      foreground.id!==boundary&&f.store.peer(foreground.id).state!=='ready'?'foreground_busy':undefined)},()=>limit);
  const bg=()=>f.store.peer(f.x.id),fg=()=>f.store.peer(f.c.id);
  const stats=(extra:Partial<TurnEndStats>={}):TurnEndStats=>({startedAt:Date.now(),covered:true,actions:0,workTools:0,...extra});
  const finish=(id:string,value:TurnEndStats|undefined=stats())=>{guard.begin(bg(),id);guard.complete(bg(),{turnId:id,completedAt:Date.now(),stats:value});guard.refresh(pair);};
  const pending=()=>health.peek(pair.id,f.c.id).filter(notice=>notice.source==='background_completed');
  const admit=()=>f.store.transaction(()=>{
    assert.ok(guard.ready(pair,true));const n=pending()[0];assert.ok(n);
    const prepared=health.prepare(pair.id,f.c.id,n.id,n.scope,false,true,true);assert.ok(prepared);
    guard.admitted(pair,prepared);return prepared;
  });
  return{...f,pair,health,guard,bg,fg,stats,finish,pending,admit,file,
    hold:(next:CompletionHold|undefined)=>{hold=next;},owned:(next:boolean)=>{owned=next;}};
}

test('后台无工具且没有最终正文的完成事实仍提醒；不伪造搭档消息，重复回执和重建不重发',t=>{
  const f=setup(t);f.finish('answer-without-tools');const notice=f.pending()[0]!;
  assert.equal(notice.readyOnly,true);assert.equal(notice.wake,true);assert.match(notice.message,/no work-tool calls/);
  assert.equal(f.health.prepare(f.pair.id,f.c.id,notice.id,notice.scope),undefined);
  f.admit();assert.equal(f.guard.status(f.bg()).state,'completed');
  f.guard.complete(f.bg(),{turnId:'answer-without-tools',completedAt:Date.now(),stats:f.stats()});f.guard.refresh(f.pair);
  assert.deepEqual(f.pending(),[]);assert.equal(f.store.messages().length,0);assert.equal(f.guard.budget.status(f.c.id).used,1);
});

function completedHistory(file:string,sessionId:string,turnId:string,startedAt:number,workAt:number,completedAt:number){
  const row=(type:string,payload:unknown,at:number)=>JSON.stringify({type,payload,timestamp:new Date(at).toISOString()});
  writeFileSync(file,[row('session_meta',{id:sessionId},startedAt-1),row('event_msg',{type:'task_started',turn_id:turnId},startedAt),
    row('response_item',{type:'function_call',name:'exec_command',call_id:'work'},workAt),
    row('event_msg',{type:'task_complete',turn_id:turnId},completedAt)].join('\n')+'\n','utf8');
}

async function startupCheck(guard:TurnEndCoordinator){
  guard.retryStartupChecks();
  const pending=guard as unknown as {startupChecks:Map<string,{work?:Promise<void>}>};
  await Promise.all([...pending.startupChecks.values()].map(state=>state.work));
}

for(const kind of ['long','oversized','invalid','rollback'] as const)test('冷启动仅剩同轮终态仍能核对完成，不补造计数：'+kind,async t=>{
  const f=setup(t),turnId='same-completed',at=Date.now(),row=(type:string,payload:unknown,time=at)=>JSON.stringify({type,payload,timestamp:new Date(time).toISOString()})+'\n';
  f.finish(turnId,f.stats({covered:false}));f.guard.recoverAtStartup(()=>true);
  assert.equal(f.guard.status(f.bg()).hold,'native_state_unverified');assert.deepEqual(f.pending(),[]);
  const padding=kind==='long'?row('event_msg',{type:'token_count',padding:'x'.repeat(2*1024*1024)}).repeat(5):
    kind==='oversized'?row('event_msg',{type:'token_count',padding:'x'.repeat(5*1024*1024)}):
    kind==='invalid'?'{"truncated":\n':row('event_msg',{type:'thread_rolled_back'});
  writeFileSync(f.file,row('session_meta',{id:f.x.sessionId})+row('event_msg',{type:'task_started',turn_id:turnId})+padding+
    row('event_msg',{type:'task_complete',turn_id:turnId},at+1),'utf8');
  await startupCheck(f.guard);
  const evidence=await readCodexTurnEvidence(f.file,f.x.sessionId!);
  assert.equal(evidence.caughtUp,true);assert.equal(evidence.latestTurnId,turnId);assert.equal(evidence.turns.length,0);
  assert.deepEqual(f.guard.startupStatus(),[]);assert.equal(f.guard.status(f.bg()).reason,'unverified');
  assert.equal(f.pending().length,1);assert.equal(f.guard.budget.status(f.c.id).used,0);
  f.admit();f.guard.refresh(f.pair);assert.deepEqual(f.pending(),[]);assert.equal(f.guard.budget.status(f.c.id).used,1);
});

test('启动核对连续读取同一次有界尾部的时间分片，不为每个分片等待重试周期',async t=>{
  const f=setup(t),turnId='cold-sliced',row=(payload:unknown)=>JSON.stringify({type:'event_msg',timestamp:new Date().toISOString(),payload})+'\n';
  f.finish(turnId,f.stats({covered:false}));
  writeFileSync(f.file,JSON.stringify({type:'session_meta',payload:{id:f.x.sessionId}})+'\n'+
    row({type:'task_started',turn_id:turnId})+row({type:'token_count',padding:'x'.repeat(1024*1024)}).repeat(10)+
    row({type:'task_complete',turn_id:turnId}),'utf8');
                                         
  let elapsed=0;t.mock.method(performance,'now',()=>elapsed+=60);
  f.guard.recoverAtStartup(()=>true);await startupCheck(f.guard);
  assert.deepEqual(f.guard.startupStatus(),[]);
  assert.equal(f.guard.status(f.bg()).turn_id,turnId);assert.equal(f.guard.status(f.bg()).reason,'unverified');
  assert.equal(f.pending().length,1);assert.equal(f.guard.budget.status(f.c.id).used,0);
});

test('启动分片追读遇到持续增长的原生记录仍止于一次尾部预算，交接取消后不再读下一片',async t=>{
  const f=setup(t),row=JSON.stringify({type:'event_msg',timestamp:new Date().toISOString(),payload:{type:'token_count',padding:'x'.repeat(128*1024)}})+'\n';
  writeFileSync(f.file,JSON.stringify({type:'session_meta',payload:{id:f.x.sessionId}})+'\n'+row.repeat(8),'utf8');
  let elapsed=0;t.mock.method(performance,'now',()=>elapsed+=60);
                                           
  const evidence=await catchUpCodexTurnEvidence(f.file,f.x.sessionId!,()=>{appendFileSync(f.file,row,'utf8');return true;});
  assert.equal(evidence.status,'supported');assert.equal(evidence.caughtUp,false);assert.equal(evidence.bytesRead,8*1024*1024);
  const cancelled=await catchUpCodexTurnEvidence(f.file,f.x.sessionId!,()=>false);
  assert.ok(cancelled.bytesRead<=128*1024,'取消后只允许已经开始的时间片结束');
});

test('最新轮次无法重建时只等待完成核对，新鲜运行轮次取消旧事实并撤掉核对状态',async t=>{
  const f=setup(t),at=Date.now();
  f.finish('old',f.stats({covered:false}));f.guard.recoverAtStartup(()=>true);
  writeFileSync(f.file,JSON.stringify({type:'session_meta',payload:{id:f.x.sessionId}})+'\n'+
    JSON.stringify({type:'event_msg',timestamp:new Date(at).toISOString(),payload:{type:'task_complete',turn_id:'newer-unreconstructed'}})+'\n','utf8');
  await startupCheck(f.guard);assert.equal(f.guard.startupStatus().length,1);assert.deepEqual(f.pending(),[]);
  f.guard.native(f.bg(),{turnId:'fresh-live',state:'running'});f.guard.refresh(f.pair);
  assert.deepEqual(f.guard.startupStatus(),[]);assert.equal(f.guard.status(f.bg()).state,'running');assert.deepEqual(f.pending(),[]);
});

test('已经看过的完成事实无需继续补证，不读取不可用历史也不重新提供提醒',async t=>{
  const f=setup(t);f.finish('already-observed');f.admit();
  const read=t.mock.method(f.guard as unknown as {read(peer:typeof f.x):Promise<boolean>},'read',async()=>{throw Error('不应读取已结清事实');});
  f.guard.recoverAtStartup(()=>true);await startupCheck(f.guard);f.guard.refresh(f.pair);
  assert.deepEqual(f.guard.startupStatus(),[]);assert.equal(read.mock.callCount(),0);assert.deepEqual(f.pending(),[]);
});

for(const missing of ['foreground','ownership'] as const)test('核对期间角色失效时保留完成门禁，恢复角色后实际折叠新事实：'+missing,async t=>{
  const f=setup(t),at=Date.now(),foreground=f.store.endpoint(f.c.id)!;
  f.finish('before-role-loss');
  completedHistory(f.file,f.x.sessionId!,'after-role-loss',at+1,at+2,at+3);
  f.guard.recoverAtStartup(()=>true);
  const checking=startupCheck(f.guard);
                                    
  if(missing==='foreground')f.store.deactivate(f.c.id,foreground.epoch);else f.owned(false);
  await checking;
  assert.equal(f.guard.startupStatus().length,1);
  assert.equal(f.store.setting<{turnId:string}>('turnEndFact:'+f.x.id)?.turnId,'before-role-loss');
  assert.equal(f.guard.native(f.bg(),{turnId:'after-role-loss',state:'running'}),false);
  assert.equal(f.guard.startupStatus().length,1,'没有应用的原生开始也不能撤下门禁');
  assert.equal(f.guard.budget.status(f.c.id).used,0);assert.deepEqual(f.pending(),[]);
  if(missing==='foreground')f.store.saveEndpoint(foreground);else f.owned(true);
  t.mock.timers.tick(5001);await startupCheck(f.guard);f.guard.refresh(f.pair);
  assert.deepEqual(f.guard.startupStatus(),[]);assert.equal(f.guard.status(f.bg()).turn_id,'after-role-loss');
  assert.equal(f.pending().length,1);assert.equal(f.guard.budget.status(f.c.id).used,0);
});

for(const unavailable of [false,true])for(const failed of [false,true])test('切换间隙结束的同一长轮次补回真实终态且不补造计数：'+unavailable+'/'+failed,async t=>{
  const f=setup(t),at=Date.now(),turnId='gap-turn',row=(payload:unknown,time=at)=>JSON.stringify({type:'event_msg',timestamp:new Date(time).toISOString(),payload})+'\n';
  f.guard.begin(f.bg(),turnId);
  if(unavailable)f.guard.native(f.bg(),{turnId,state:'ready'});
  writeFileSync(f.file,JSON.stringify({type:'session_meta',payload:{id:f.x.sessionId}})+'\n'+
    row({type:'task_started',turn_id:turnId})+row({type:'token_count',padding:'x'.repeat(2*1024*1024)},at+1).repeat(5)+
    row({type:'task_complete',turn_id:turnId,...(failed?{status:'failed',error:'原生轮次失败'}:{})},at+2),'utf8');
  let folded=false;for(let read=0;read<32&&!folded;read++)folded=await f.guard.scan(f.bg());
  assert.equal(folded,true);f.guard.refresh(f.pair);
  const fact=f.store.setting<{state:string;terminal?:string;stats?:TurnEndStats}>('turnEndFact:'+f.x.id)!;
  assert.equal(fact.stats,undefined);assert.equal(f.guard.status(f.bg()).turn_id,turnId);
  if(failed){
    assert.equal(fact.state,'unavailable');assert.equal(fact.terminal,'failed');assert.deepEqual(f.pending(),[]);
  }else{
    assert.equal(fact.state,'completed');assert.equal(f.guard.status(f.bg()).reason,'unverified');
    assert.equal(f.guard.status(f.bg()).completed_at,at+2);assert.equal(f.pending().length,1);
    f.admit();await f.guard.scan(f.bg());f.guard.refresh(f.pair);assert.deepEqual(f.pending(),[]);
  }
  assert.equal(f.guard.budget.status(f.c.id).used,failed?0:1);
});

test('补证恢复同轮新范围前不会复活旧通知，完整持久统计只更新汇报判定而不增加额度',async t=>{
  const f=setup(t),at=Date.now();
  f.finish('persisted',f.stats({covered:true,workTools:1,actions:1,lastWorkAt:at-1}));
  f.store.accept(f.pair,f.bg(),'该轮工作已汇报','reported-after-old-completion');
  f.guard.recoverAtStartup(()=>true);
  const endpoint=f.store.endpoint(f.x.id)!;f.store.deactivate(f.x.id,endpoint.epoch);f.store.saveEndpoint({...endpoint,epoch:'reconnected-scope'});
  f.guard.refresh(f.pair);assert.deepEqual(f.pending(),[]);assert.equal(f.guard.budget.status(f.c.id).used,0);
  writeFileSync(f.file,JSON.stringify({type:'session_meta',payload:{id:f.x.sessionId}})+'\n'+
    JSON.stringify({type:'event_msg',timestamp:new Date(at).toISOString(),payload:{type:'task_complete',turn_id:'persisted'}})+'\n','utf8');
  await startupCheck(f.guard);f.guard.refresh(f.pair);
  assert.deepEqual(f.guard.startupStatus(),[]);assert.equal(f.guard.status(f.bg()).reason,'reported');assert.deepEqual(f.pending(),[]);
  assert.equal(f.guard.budget.status(f.c.id).used,0);
});

for(const reported of [false,true])test(reported?'降级期间不提醒，补证发现已汇报后撤回旧未知事实且不扣额度':'降级期间不提醒，完整补证后只为未汇报工作提供一次完成唤醒',async t=>{
  const startup=new StartupRoutes(()=>{}),f=setup(t,4,startup);t.after(()=>startup.close());
  const began=Date.now()+1,work=Date.now()+2;
  startup.begin([f.x.id]);startup.fail(f.x.id);t.mock.timers.tick(3);
  if(reported)f.store.accept(f.pair,f.bg(),'工作已汇报','reported-before-complete');
  t.mock.timers.tick(1);f.finish('finished',f.stats({covered:false}));
  assert.equal(f.guard.status(f.bg()).hold,'native_state_unverified');assert.deepEqual(f.pending(),[]);
  assert.equal(f.guard.budget.status(f.c.id).used,0);
  completedHistory(f.file,f.x.sessionId!,'finished',began,work,Date.now());
  assert.equal(await f.guard.scan(f.bg()),true);f.guard.refresh(f.pair);
  assert.equal(f.guard.status(f.bg()).reason,reported?'reported':'work_after_message');
  assert.deepEqual(f.pending(),[]);assert.equal(f.guard.budget.status(f.c.id).used,0);
  startup.finish(f.x.id);f.guard.refresh(f.pair);f.guard.refresh(f.pair);
  assert.equal(f.pending().length,reported?0:1);
  if(!reported){f.admit();f.guard.refresh(f.pair);assert.equal(f.guard.budget.status(f.c.id).used,1);assert.deepEqual(f.pending(),[]);}
});

test('补证期间已经换轮时不能采用旧完成，新的运行事实不会变成陈旧唤醒',async t=>{
  const startup=new StartupRoutes(()=>{}),f=setup(t,4,startup);t.after(()=>startup.close());
  startup.begin([f.x.id]);f.finish('old',f.stats({covered:false}));f.guard.begin(f.bg(),'new');
  completedHistory(f.file,f.x.sessionId!,'old',Date.now()-3,Date.now()-2,Date.now()-1);
  assert.equal(await f.guard.scan(f.bg(),()=>false),false);
  startup.finish(f.x.id);f.guard.refresh(f.pair);
  assert.equal(f.guard.status(f.bg()).state,'running');assert.equal(f.guard.status(f.bg()).turn_id,'new');assert.deepEqual(f.pending(),[]);
});

test('补证中重新创建协调器时重建内存门禁，持久的未知完成在新补证前不能唤醒',async t=>{
  const first=new StartupRoutes(()=>{}),f=setup(t,4,first);t.after(()=>first.close());
  first.begin([f.x.id]);f.finish('durable-completion',f.stats({covered:false}));assert.deepEqual(f.pending(),[]);
  first.close();
  const resumed=new StartupRoutes(()=>{});t.after(()=>resumed.close());resumed.begin([f.x.id]);
  const guard=new TurnEndCoordinator(f.store,f.health,{owned:()=>true,source:()=>f.file,stats:()=>undefined,
    gate:()=>resumed.incomplete(f.x.id)?'native_state_unverified':undefined},()=>4);
  guard.refresh(f.pair);assert.deepEqual(f.pending(),[]);assert.equal(guard.budget.status(f.c.id).used,0);
  completedHistory(f.file,f.x.sessionId!,'durable-completion',Date.now()-3,Date.now()-2,Date.now()-1);
  assert.equal(await guard.scan(f.bg()),true);guard.refresh(f.pair);assert.deepEqual(f.pending(),[]);
  resumed.finish(f.x.id);guard.refresh(f.pair);
  assert.equal(f.pending().length,1);assert.equal(guard.status(f.bg()).reason,'work_after_message');
});

test('来源只接受一后台一用户的根配对；双用户、双后台和归属不明均不干预',t=>{
  const f=setup(t);f.owned(false);f.finish('unowned');assert.deepEqual(f.pending(),[]);
  f.owned(true);f.store.updatePeer(f.x.id,{owner:'user'});f.finish('two-users');assert.equal(f.guard.status(f.fg()).state,'not_applicable');
  f.store.updatePeer(f.x.id,{owner:'nxbridge'});f.store.updatePeer(f.c.id,{owner:'nxbridge'});f.finish('two-backgrounds');assert.deepEqual(f.pending(),[]);
});

test('后台 Stop 每轮最多拦一次；只读工作也算工作，发信后继续工作须汇报',t=>{
  const f=setup(t),message=f.store.accept(f.pair,f.fg(),'请接着处理','task');
  const input={turn_id:'source',stop_hook_active:false,stats:f.stats({partnerDriven:true,partnerMessageIds:[message.id]})};
  f.guard.begin(f.bg(),'source');const first=f.guard.checkStop(f.bg(),input);
  assert.match(first.block!,/no work-tool calls/);assert.ok(first.intervention_id);
  assert.deepEqual(f.guard.checkStop(f.bg(),input),{});assert.deepEqual(f.guard.checkStop(f.bg(),{...input,turn_id:'other',stop_hook_active:true}),{});
  const sent=f.store.accept(f.pair,f.bg(),'已开始','progress');f.guard.sent(f.bg());t.mock.timers.tick(1);
  const work=f.guard.checkStop(f.bg(),{turn_id:'read-work',stop_hook_active:false,stats:f.stats({workTools:1,actions:1,lastWorkAt:Date.now()})});
  assert.match(work.block!,/report/);assert.equal(f.store.lastMessageFacts(f.pair.id,f.x.id)?.id,sent.id);
});

test('前台 Stop 看见后台已完成时同轮提示；未完成、忙碌、审批和暂停均不误报',t=>{
  const f=setup(t);f.guard.begin(f.bg(),'running');
  const stop={turn_id:'fg',stop_hook_active:false,stats:f.stats(),background:{tasks:0,crons:0}};assert.deepEqual(f.guard.checkStop(f.fg(),stop),{});
  f.finish('done');f.guard.begin(f.fg(),'fg');f.store.updatePeer(f.c.id,{state:'busy'});f.guard.refresh(f.pair);
  assert.equal(f.guard.ready(f.pair),false);const tip=f.guard.checkStop(f.fg(),stop);assert.ok(tip.block);assert.deepEqual(f.pending(),[]);
  f.store.updatePeer(f.c.id,{state:'ready'});f.guard.complete(f.fg(),{turnId:'fg',completedAt:Date.now(),stats:f.stats(),interventions:[{id:tip.intervention_id!,inert:true}]});
  assert.equal(f.guard.status(f.fg()).state,'completed');
  for(const hold of ['pending_user','paused','runtime_unavailable','native_state_unverified'] as const){
    f.finish('held-'+hold);f.hold(hold);assert.deepEqual(f.guard.checkStop(f.fg(),{...stop,turn_id:hold}),{});assert.equal(f.guard.ready(f.pair,true),false);
  }
});

test('新轮次立刻撤回旧完成提醒，旧终态不能把正在运行的新轮次覆盖成完成',t=>{
  const f=setup(t);f.finish('old');assert.equal(f.pending().length,1);f.guard.begin(f.bg(),'new');
  f.guard.complete(f.bg(),{turnId:'old',completedAt:Date.now(),stats:f.stats()});f.guard.refresh(f.pair);
  assert.deepEqual(f.pending(),[]);assert.equal(f.guard.status(f.bg()).state,'running');assert.equal(f.guard.status(f.bg()).turn_id,'new');
  f.guard.native(f.bg(),{turnId:'new',state:'failed'});assert.equal(f.guard.status(f.bg()).state,'unavailable');
});

test('汇报之后直接结束不提醒，工作后未汇报和未知覆盖范围都给出中性完成事实',t=>{
  const f=setup(t),work=Date.now();t.mock.timers.tick(1);f.store.accept(f.pair,f.bg(),'工作结果','report');
  f.finish('reported',f.stats({workTools:1,actions:2,lastWorkAt:work}));assert.deepEqual(f.pending(),[]);assert.equal(f.guard.status(f.fg()).reason,'reported');
  t.mock.timers.tick(1);f.finish('unreported',f.stats({workTools:1,actions:1,lastWorkAt:Date.now()}));assert.equal(f.guard.status(f.fg()).reason,'work_after_message');
  f.guard.begin(f.bg(),'unknown');f.guard.complete(f.bg(),{turnId:'unknown',completedAt:Date.now()});f.guard.refresh(f.pair);
  assert.equal(f.guard.status(f.fg()).reason,'unverified');assert.match(f.pending()[0]!.message,/unverified/);
});

test('后台和前台 Stop 与完成唤醒共用 K 次额度，普通消息和承诺不补额',t=>{
  const f=setup(t);const input={stop_hook_active:false,stats:f.stats({partnerDriven:true})};
  for(let i=0;i<4;i++){
    const turn='turn-'+i;f.guard.begin(f.bg(),turn);assert.ok(f.guard.checkStop(f.bg(),{...input,turn_id:turn}).block);
    f.store.accept(f.pair,f.bg(),'正在处理','promise-'+i);f.guard.sent(f.bg());
    f.guard.complete(f.bg(),{turnId:turn,completedAt:Date.now(),stats:f.stats({actions:1})});
  }
  f.finish('exhausted');assert.equal(f.guard.status(f.fg()).state,'limited');assert.equal(f.guard.status(f.fg()).budget!.used,4);
  assert.deepEqual(f.guard.checkStop(f.fg(),{...input,turn_id:'foreground'}),{});assert.deepEqual(f.pending(),[]);
  const ordinary=f.store.accept(f.pair,f.fg(),'请继续','ordinary');assert.ok(ordinary);assert.equal(f.guard.budget.status(f.c.id).remaining,0);
  t.mock.timers.tick(1);f.guard.human(f.fg(),'human');f.guard.refresh(f.pair);assert.equal(f.pending().length,1);
});

test('连续无动作通知回合可精确退费，发信、工具动作、未知或错误轮次不会退费',t=>{
  const f=setup(t);
  for(let i=0;i<12;i++){
    f.finish('bg-'+i);const notice=f.admit();f.guard.entered(f.fg(),notice.id);f.guard.started(f.fg(),'fg-'+i,notice.id);
    f.guard.complete(f.fg(),{turnId:'fg-'+i,completedAt:Date.now(),stats:f.stats()});assert.equal(f.guard.budget.status(f.c.id).used,0);t.mock.timers.tick(60001);
  }
  f.finish('effectful');const notice=f.admit();f.guard.entered(f.fg(),notice.id);f.guard.started(f.fg(),'effectful-fg',notice.id);f.guard.sent(f.fg());
  f.guard.complete(f.fg(),{turnId:'effectful-fg',completedAt:Date.now(),stats:f.stats()});assert.equal(f.guard.budget.status(f.c.id).used,1);
});

test('明确提交前失败沿用同一预约重试，结果未知不重投；最后一份额度也允许原预约重试',t=>{
  const f=setup(t,1);f.finish('retry');const first=f.admit();
  f.store.transaction(()=>f.guard.notSubmitted(f.fg(),f.health.outcome(f.pair.id,f.c.id,first.id,'pending')));f.guard.refresh(f.pair);
  assert.equal(f.guard.ready(f.pair),true);assert.deepEqual(f.pending(),[],'提交前重试也遵守现有间隔');t.mock.timers.tick(60001);
  const second=f.admit();assert.equal(second.id,first.id);assert.equal(f.guard.budget.status(f.c.id).used,1);
  f.health.outcome(f.pair.id,f.c.id,second.id,'unknown');f.guard.refresh(f.pair);assert.deepEqual(f.pending(),[]);assert.equal(f.guard.budget.status(f.c.id).used,1);
});

test('队列标记只允许当前事实在同一原生范围消费一次，重复 hook 只接受相同轮次',t=>{
  const f=setup(t);f.finish('queued');const notice=f.admit();
  assert.equal(f.guard.consumeQueued(f.fg(),notice.id,'native-fg'),true);assert.equal(f.guard.consumeQueued(f.fg(),notice.id,'native-fg'),true);
  assert.equal(f.guard.consumeQueued(f.fg(),notice.id,'duplicate-new-turn'),false);
  f.guard.begin(f.bg(),'new-bg');assert.equal(f.guard.consumeQueued(f.fg(),notice.id,'native-fg'),false);
});

test('关闭自动干预仍保留完成事实和可读状态，不开启模型轮次',t=>{
  const f=setup(t,0);f.finish('disabled');assert.deepEqual(f.pending(),[]);
  const status=f.guard.status(f.fg());assert.equal(status.state,'limited');assert.equal(status.turn_id,'disabled');assert.match(status.user_text!,/配置里关闭/);
});

test('原生完整开始到终态支持无正文完成；未结束的文件尾部不产生完成通知',async t=>{
  const f=setup(t),row=(type:string,payload:object)=>JSON.stringify({timestamp:new Date().toISOString(),type,payload})+'\n';
  writeFileSync(f.file,row('session_meta',{id:f.x.sessionId})+row('event_msg',{type:'task_started',turn_id:'native'}),'utf8');
  await f.guard.scan(f.bg());f.guard.refresh(f.pair);assert.equal(f.guard.status(f.fg()).state,'running');assert.deepEqual(f.pending(),[]);
  appendFileSync(f.file,row('event_msg',{type:'task_complete',turn_id:'native'}),'utf8');await f.guard.scan(f.bg());f.guard.refresh(f.pair);
  assert.equal(f.guard.status(f.fg()).turn_id,'native');assert.equal(f.pending().length,1);assert.ok(!f.pending()[0]!.message.includes('final'));
});

test('Codex 无动作证明要求连续原生工具流；重连、未知条目或范围改变使证明失效',t=>{
  const f=setup(t),tracker=new TurnEndEvidence(),peer=f.bg(),endpoint=f.store.endpoint(peer.id)!;
  const observe=(method:string,item?:object)=>tracker.observe(peer,endpoint,{method,params:{threadId:peer.sessionId,turnId:'native',...(item?{item}:{})}});
  observe('turn/started');tracker.partner(peer,endpoint,'native');assert.equal(tracker.get(peer,endpoint)!.covered,true);
  observe('item/started',{id:'status',type:'mcpToolCall',server:'claude'});observe('item/completed',{id:'status',type:'mcpToolCall',server:'claude'});
  assert.equal(tracker.get(peer,endpoint)!.actions,1);assert.equal(tracker.get(peer,endpoint)!.workTools,0);
  observe('item/started',{id:'read',type:'commandExecution'});observe('item/completed',{id:'read',type:'commandExecution'});assert.equal(tracker.get(peer,endpoint)!.workTools,1);
  observe('item/started',{id:'future',type:'newUnknownAction'});assert.equal(tracker.get(peer,endpoint)!.covered,false);
  assert.equal(tracker.get(peer,{...endpoint,epoch:'other'}),undefined);assert.notEqual(turnNativeScope(peer,endpoint),turnNativeScope(peer,{...endpoint,address:'other'}));
});

test('Codex 自己的输入和 queue 来源均不补额度，真人新轮次补额后只裁剪安全的旧来源编号',async t=>{
  const f=setup(t);f.store.updatePeer(f.c.id,{owner:'nxbridge'});f.store.updatePeer(f.x.id,{owner:'user'});
  const foreground=f.bg(),endpoint=f.store.endpoint(f.x.id)!;
  f.guard.budget.admit(f.x.id,'prior-effect',turnNativeScope(foreground,endpoint));
  const own=await f.guard.origin(foreground,'queue'),row=(payload:object)=>JSON.stringify({timestamp:new Date().toISOString(),type:'event_msg',payload})+'\n';
  for(let index=0;index<300;index++)await f.guard.origin(foreground,'queue');
  t.mock.timers.tick(10);
  writeFileSync(f.file,JSON.stringify({type:'session_meta',payload:{id:foreground.sessionId}})+'\n','utf8');
  const append=(turn:string,input:string,trigger='user')=>appendFileSync(f.file,row({type:'task_started',turn_id:turn,turn_attribution:{turn_id:turn,turn_trigger:trigger}})+
    row({type:'item_completed',turn_id:turn,thread_id:foreground.sessionId,item:{type:'UserMessage',client_id:input}})+row({type:'task_complete',turn_id:turn}),'utf8');
  append('own-queue',own.clientUserMessageId);await f.guard.scan(foreground);assert.equal(f.guard.budget.status(f.x.id).used,1);
  t.mock.timers.tick(10);append('unattested-queue','native-queue-id','queue');await f.guard.scan(foreground);assert.equal(f.guard.budget.status(f.x.id).used,1);
  t.mock.timers.tick(10);append('human-input','native-composer-id');await f.guard.scan(foreground);assert.equal(f.guard.budget.status(f.x.id).used,0);
  const retained=f.store.setting<{ids:string[]}>('turnEndOrigins:'+foreground.id)!;
  assert.equal(retained.ids.includes(own.clientUserMessageId),false);assert.equal(retained.ids.length,300,'尚未进入原生轮次的编号仍须保留');
  t.mock.timers.tick(10);f.guard.budget.admit(f.x.id,'after-human',turnNativeScope(foreground,endpoint));
  await f.guard.scan(foreground);assert.equal(f.guard.budget.status(f.x.id).used,1,'裁剪后重扫旧轮次不能再补额');
});

function managedFixture(t:TestContext){
  const f=fixture(t),home=f.broker.home,pair=f.store.pair(f.c.id,f.x.id),previous=f.store.endpoint(f.x.id)!,runtimeId=randomUUID();
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'busy'});f.store.updatePeer(f.c.id,{state:'ready'});
  f.store.saveEndpoint({...previous,configHome:home,runtimeId});
  f.store.saveEndpoint({...f.store.endpoint(f.c.id)!,modRevision:MOD_REVISION});f.broker.foregroundHealth.observe(f.c,'ready','foreground');
  saveManagedRuntime(home,{id:runtimeId,owner:managedMarker(home),kind:'owned',state:'ready',dedicated:true,endpoint:previous.address,
    peerId:f.x.id,sessionId:f.x.sessionId!,pid:previous.pid!,procStart:previous.procStart!,configHome:home,createdAt:Date.now()});
  const native=(method:string,extra:object)=>f.codex.emit('native',{address:previous.address,message:{jsonrpc:'2.0',method,params:{threadId:f.x.sessionId,...extra}}});
  return{...f,pair,native,previous};
}

for(const inactive of [false,true])test('投递栅栏内只修复原 Codex 订阅，真实回执前不激活离线端点：'+inactive,async t=>{
  const f=managedFixture(t),life=lifecycle(f.broker),b=f.broker as unknown as {startupRoutes:StartupRoutes;catchUpOwnedRoutes(ids:Iterable<string>):Promise<void>};
  f.store.updatePeer(f.x.id,{state:'ready'});
  if(inactive)f.store.deactivate(f.x.id,f.store.endpoint(f.x.id)!.epoch);
  b.startupRoutes.begin([f.x.id]);
  let subscribed=false,enter!:()=>void,resume!:()=>void;
  const entered=new Promise<void>(resolve=>{enter=resolve;}),wait=new Promise<void>(resolve=>{resume=resolve;});
  t.after(()=>resume());
  t.mock.method(f.codex,'hasSubscription',()=>subscribed);
  t.mock.method(f.codex,'connect',async()=>({request:async()=>({thread:{id:f.x.sessionId,status:{type:'idle'},turns:[]}})}));
  const attach=f.codex.attach.bind(f.codex);
  f.codex.attach=async(peer,endpoint,...[options]:[{prepare?:()=>void}?])=>{
    options?.prepare?.();enter();await wait;options?.prepare?.();
    const result=await attach(peer,{...endpoint,active:true});subscribed=true;return result;
  };
  const recovery=life.schedulePairedRecovery(f.x.id,false);await entered;
  assert.equal(!!f.store.endpoint(f.x.id),!inactive,'订阅回执前不把历史端点冒充当前端点');
  await assert.rejects(life.prepare(f.store.peer(f.x.id)),{code:'startup_route_pending'});
  const sent:any=await f.broker.dispatch('send',{actor:{host:'claude',sessionId:f.c.sessionId,callId:'repair-lease'},message:'订阅修复期间排队'});
  assert.equal(f.store.message(sent.message_id).attempts,0);
  resume();await recovery;
  assert.equal(subscribed,true);assert.equal(b.startupRoutes.blocked(f.x.id),true);
  assert.equal(f.codex.ensured,0);assert.equal(f.codex.created.length,0);assert.equal(f.codex.released.length,0);
  assert.equal(f.codex.woken.length,0);assert.equal(f.codex.injected.length,0);assert.equal(f.broker.turnEnds.budget.status(f.c.id).used,0);
  await b.catchUpOwnedRoutes([f.x.id]);assert.deepEqual(b.startupRoutes.status(),[]);
});

for(const failed of [false,true])test('投递核对期间完成 T1 补证后，T2 原生终态覆盖旧事实且不提前扣额：'+(failed?'失败':'成功'),async t=>{
  const f=managedFixture(t),b=f.broker as unknown as {startupRoutes:StartupRoutes};
  const file=path.join(f.broker.home,'completion-before-delivery.jsonl'),bg=()=>f.store.peer(f.x.id);
  f.store.updatePeer(f.x.id,{state:'ready'});
  b.startupRoutes.begin([f.x.id]);
  const at=Date.now();
  f.broker.turnEnds.begin(bg(),'T1');
  f.broker.turnEnds.complete(bg(),{turnId:'T1',completedAt:at,stats:{startedAt:at-3,covered:true,actions:0,workTools:0}});
  f.store.setSetting('codexTranscript:'+f.x.id,{sessionId:f.x.sessionId,file});
  completedHistory(file,f.x.sessionId!,'T1',at-3,at-2,at-1);
  f.broker.turnEnds.recoverAtStartup(()=>true);await startupCheck(f.broker.turnEnds);
  assert.deepEqual(f.broker.turnEnds.startupStatus(),[]);
  assert.equal(f.broker.turnEnds.status(bg()).turn_id,'T1');
  f.broker.turnEnds.refresh(f.pair);
  assert.equal(f.broker.turnEnds.ready(f.pair,true),false);
  f.native('turn/started',{turn:{id:'T2',status:'inProgress'}});
  assert.equal(f.broker.turnEnds.status(bg()).turn_id,'T2');
  f.native('turn/completed',{turn:{id:'T2',status:failed?'failed':'completed'}});
  f.codex.emit('state',f.x.sessionId,'ready',f.previous.address);
  await new Promise(resolve=>setImmediate(resolve));
  f.broker.turnEnds.refresh(f.pair);
  assert.equal(f.broker.turnEnds.status(bg()).turn_id,'T2');
  assert.equal(f.broker.turnEnds.ready(f.pair,true),false);
  assert.equal(f.broker.turnEnds.budget.status(f.c.id).used,0);
  assert.equal(f.broker.deliveryHealth.peek(f.pair.id,f.c.id).some(n=>n.source==='background_completed'),false);
  b.startupRoutes.finish(f.x.id);f.broker.turnEnds.refresh(f.pair);
  const notices=f.broker.deliveryHealth.peek(f.pair.id,f.c.id).filter(n=>n.source==='background_completed');
  assert.equal(notices.length,failed?0:1);
  assert.equal(f.broker.turnEnds.ready(f.pair,true),!failed);
  if(!failed){
    assert.equal(f.broker.turnEnds.status(bg()).reason,'no_tools');
    const notice=notices[0]!,prepared=f.broker.deliveryHealth.prepare(f.pair.id,f.c.id,notice.id,notice.scope,false,true,true);
    assert.ok(prepared);f.broker.turnEnds.admitted(f.pair,prepared);
    assert.equal(f.broker.turnEnds.budget.status(f.c.id).used,1);
  }
});

test('交接路由丢失订阅时仍做普通复核，但空闲 L2 回收直到投递栅栏退出才执行',async t=>{
  const f=managedFixture(t),life=lifecycle(f.broker),b=f.broker as unknown as {startupRoutes:StartupRoutes};
  f.store.updatePeer(f.x.id,{state:'ready'});
  t.mock.method(f.codex,'hasSubscription',()=>false);
  const route=f.store.endpoint(f.x.id)!;
  const old={runtimeId:route.runtimeId!,endpoint:route.address,pid:route.pid!,procStart:route.procStart!};
  let observations=0,signals=0;
  Object.assign(f.codex,{recycleHost:()=>({
    observe:async()=>{observations++;return {kind:'available',old,profile:{sourceId:old.runtimeId,sessionId:f.x.sessionId!,file:path.join(f.broker.home,'recycle.jsonl'),settingsKey:'same'},
      work:{activeTurnIds:[],progress:'idle',roles:[],unresolved:[]}};},
    persistProfile(){},
    inspectOld:async()=> 'alive',
    stop:async(_old:unknown,_deadline:number,guard:()=>void,markSignal:()=>void)=>{guard();markSignal();signals++;throw new BridgeError('process_stop_pending','模拟原进程尚未退出');},
  } as unknown as ReturnType<CodexAdapter['recycleHost']>)});
  t.mock.method(life as unknown as {assertNoLiveSessionHolder(peer:typeof f.x):Promise<void>},'assertNoLiveSessionHolder',async()=>{});
  assert.equal(life.codexRecycle.noteHold(f.store.peer(f.x.id),new RuntimeRecoveryHold('native_thread_wedged','隔离的卡住信号')),true);
  b.startupRoutes.begin([f.x.id]);
  await life.schedulePairedRecovery(f.x.id,false);
  assert.ok(f.codex.attached.length>0,'弱恢复门禁仍允许核对原进程并接回原订阅');
  assert.equal(life.blocksDeliveryActivation(f.x.id),true);
  await assert.rejects(life.prepare(f.store.peer(f.x.id)),{code:'startup_route_pending'});
  await assert.rejects(life.acceptCodexRecycle(f.store.peer(f.x.id),{incidentId:'pending',evidenceRevision:'pending',reason:'隔离验收'}),{code:'startup_route_pending'});
  assert.equal(observations,0);assert.equal(signals,0);
  assert.equal(f.broker.deliveryHealth.peek(f.pair.id,f.c.id).length,0);
  assert.equal(f.broker.turnEnds.budget.status(f.c.id).used,0);
  b.startupRoutes.finish(f.x.id);
  await life.schedulePairedRecovery(f.x.id,false);
  assert.ok(observations>0);assert.equal(signals,1);
});

test('Codex 真实 hook 分发在原生终态之前拦 Stop，保持工作状态且只在成功终态后登记完成',async t=>{
  const f=managedFixture(t),{native,pair,previous}=f;
  f.codex.emit('turnOriginObserved',f.x.id,'codex-root','partner');native('turn/started',{turn:{id:'codex-root',status:'inProgress'}});
  await new Promise(resolve=>setImmediate(resolve));
  const stop=(active:boolean)=>f.broker.dispatch('hook',{session_id:f.x.sessionId,hook_event_name:'Stop',turn_id:'codex-root',stop_hook_active:active}) as Promise<any>;
  const blocked=await stop(false);assert.equal(blocked.decision,'block');assert.match(blocked.reason,/no work-tool calls/);
  assert.equal(f.store.peer(f.x.id).state,'busy');assert.equal(f.broker.turnEnds.status(f.x).state,'running');
  await stop(true);assert.equal(f.broker.turnEnds.status(f.x).state,'running','Stop 放行也不能代替原生成功终态');
  native('turn/completed',{turn:{id:'codex-root',status:'completed'}});f.codex.emit('state',f.x.sessionId,'ready',previous.address);
  await new Promise(resolve=>setImmediate(resolve));f.broker.turnEnds.refresh(pair);
  assert.equal(f.broker.turnEnds.status(f.x).turn_id,'codex-root');assert.equal(f.broker.turnEnds.status(f.x).reason,'no_tools');
});

test('前台未应用 Stop 提示时完成事实仍待提醒，不能被暂存预约吃掉',t=>{
  const f=setup(t);f.finish('background');f.guard.begin(f.fg(),'foreground');f.store.updatePeer(f.c.id,{state:'busy'});f.guard.refresh(f.pair);
  const tip=f.guard.checkStop(f.fg(),{turn_id:'foreground',stop_hook_active:false,stats:f.stats(),background:{tasks:0,crons:0}});assert.ok(tip.block);
  f.store.updatePeer(f.c.id,{state:'ready'});f.guard.complete(f.fg(),{turnId:'foreground',completedAt:Date.now(),stats:f.stats(),interventions:[]});f.guard.refresh(f.pair);
  assert.equal(f.guard.status(f.fg()).state,'pending');assert.equal(f.guard.budget.status(f.c.id).used,0);assert.equal(f.pending().length,1);
});

test('明确未提交的提醒最多定时重试一次，持续拒绝时等下一个真实前台忙完边界',t=>{
  const f=setup(t);f.finish('background');const first=f.admit();
  f.health.outcome(f.pair.id,f.c.id,first.id,'pending');f.guard.refresh(f.pair);
  t.mock.timers.tick(60001);const second=f.admit();assert.equal(second.id,first.id);
  f.health.outcome(f.pair.id,f.c.id,second.id,'pending');f.guard.refresh(f.pair);
  for(let index=0;index<10;index++){t.mock.timers.tick(60001);f.guard.refresh(f.pair);assert.equal(f.guard.ready(f.pair),false);}
  assert.equal(f.guard.status(f.fg()).hold,'notice_retry_held');assert.equal(f.health.get(f.pair.id,f.c.id,first.id)?.completionRetry?.offers,2);
  f.guard.begin(f.fg(),'independent');f.guard.complete(f.fg(),{turnId:'independent',completedAt:Date.now(),stats:f.stats()});f.guard.refresh(f.pair);
  assert.equal(f.guard.ready(f.pair),true);f.admit();assert.equal(f.health.outcome(f.pair.id,f.c.id,first.id,'pending')[0]?.completionRetry?.offers,1);
});

test('队列在模型采样前被暂时门禁拒绝后保留事实和原预约，新轮次则释放确定未提交的旧预约',t=>{
  const f=setup(t);f.finish('queued');const notice=f.admit();f.hold('pending_user');
  assert.equal(f.guard.consumeQueued(f.fg(),notice.id,'declined'),false);f.guard.refresh(f.pair);assert.equal(f.guard.status(f.fg()).state,'pending');
  f.hold(undefined);t.mock.timers.tick(60001);f.guard.refresh(f.pair);assert.equal(f.guard.ready(f.pair),true);assert.equal(f.guard.budget.status(f.c.id).used,1);
  f.guard.begin(f.bg(),'new-background');assert.equal(f.guard.budget.status(f.c.id).used,0);
});

test('后台暂时失去端点不会吞掉真人补额，也不会保留可占用唤醒通道的旧通知',t=>{
  const f=setup(t);f.finish('done');const endpoint=f.store.endpoint(f.x.id)!;
  const original=f.pending()[0]!.id;
  f.guard.budget.admit(f.c.id,'used',turnNativeScope(f.fg(),f.store.endpoint(f.c.id)!));
  f.store.deactivate(f.x.id);f.guard.refresh(f.pair);assert.deepEqual(f.pending(),[]);
  t.mock.timers.tick(1);f.guard.human(f.fg(),'during-restart');assert.equal(f.guard.budget.status(f.c.id).used,0);
  f.store.saveEndpoint({...endpoint,active:true});f.guard.refresh(f.pair);assert.deepEqual(f.pending().map(row=>row.id),[original]);
});

test('同一完成事实在门槛撤回、重新准入和协调器重建的轨迹中保持一个通知编号',async t=>{
  const gates=['paused','pending_user','native_state_unverified'] as const;
  for(let cut=0;cut<=gates.length;cut++)await t.test('重建点 '+cut,sub=>{
    const f=setup(sub);f.finish('one-completion');const noticeId=f.pending()[0]!.id;
    let guard=f.guard,hold:CompletionHold|undefined;
    const reopen=()=>new TurnEndCoordinator(f.store,f.health,{owned:()=>true,source:()=>f.file,stats:()=>undefined,gate:()=>hold},()=>4);
    const admit=()=>{
      const notice=f.pending()[0]!;assert.equal(notice.id,noticeId);assert.ok(guard.ready(f.pair,true));
      const prepared=f.health.prepare(f.pair.id,f.c.id,notice.id,notice.scope,false,true,true);assert.ok(prepared);
      guard.admitted(f.pair,prepared);return prepared;
    };
    for(const [index,gate] of gates.entries()){
      if(index===cut)guard=reopen();
      guard.refresh(f.pair);const notice=admit();assert.equal(guard.budget.status(f.c.id).used,1);
      guard.notSubmitted(f.fg(),f.health.outcome(f.pair.id,f.c.id,notice.id,'pending'));
      hold=gate;f.hold(gate);guard.refresh(f.pair);
      assert.deepEqual(f.pending(),[]);assert.equal(guard.budget.status(f.c.id).used,1,'暂缓同一事实仍保留原预约，不额外扣次');
      hold=undefined;f.hold(undefined);
                                            
      const turn='independent-'+index;guard.begin(f.fg(),turn);
      guard.complete(f.fg(),{turnId:turn,completedAt:Date.now(),stats:f.stats()});
      sub.mock.timers.tick(60001);guard.refresh(f.pair);
      assert.deepEqual(f.pending().map(entry=>entry.id),[noticeId]);
    }
    if(cut===gates.length)guard=reopen();
    const unknown=admit();f.health.outcome(f.pair.id,f.c.id,unknown.id,'unknown');guard=reopen();
    sub.mock.timers.tick(60001);guard.refresh(f.pair);
    assert.deepEqual(f.pending(),[]);assert.equal(guard.budget.status(f.c.id).used,1,'结果不明的提交保留扣额且不重新提供');
    assert.equal(f.store.setting<{noticeId:string}>('turnEndFact:'+f.x.id)?.noticeId,noticeId);
  });
});

test('核验为已汇报后保留精确原轮次的提醒编号，但不再提供提醒',t=>{
  const f=setup(t);f.guard.begin(f.bg(),'same-turn');
  f.guard.complete(f.bg(),{turnId:'same-turn',completedAt:Date.now()});f.guard.refresh(f.pair);
  const id=f.pending()[0]!.id;
  f.store.accept(f.pair,f.bg(),'已汇报','proof');
  f.guard.complete(f.bg(),{turnId:'same-turn',completedAt:Date.now(),stats:f.stats({workTools:1,actions:1,lastWorkAt:Date.now()-1})});f.guard.refresh(f.pair);
  assert.deepEqual(f.pending(),[]);assert.equal(f.guard.status(f.fg()).reason,'reported');
  assert.equal(f.store.setting<{noticeId?:string}>('turnEndFact:'+f.x.id)?.noticeId,id);
});

test('失败轮次不能被无错误字段的文件终态覆盖，重绑也不把已汇报的旧轮次改成未确认',async t=>{
  const f=setup(t),row=(payload:object)=>JSON.stringify({timestamp:new Date().toISOString(),type:'event_msg',payload})+'\n';
  f.guard.begin(f.bg(),'failed');f.guard.native(f.bg(),{turnId:'failed',state:'failed'});
  writeFileSync(f.file,JSON.stringify({type:'session_meta',payload:{id:f.x.sessionId}})+'\n'+row({type:'task_started',turn_id:'failed'})+row({type:'task_complete',turn_id:'failed'}),'utf8');
  await f.guard.scan(f.bg());assert.equal(f.guard.status(f.fg()).state,'unavailable');assert.deepEqual(f.pending(),[]);
  const at=Date.now();t.mock.timers.tick(1);f.store.accept(f.pair,f.bg(),'已汇报','reported');f.finish('reported',f.stats({actions:1,workTools:1,lastWorkAt:at}));
  const previous=f.store.endpoint(f.x.id)!;f.store.deactivate(f.x.id);
  f.store.saveEndpoint({...previous,epoch:'replacement'});f.guard.complete(f.bg(),{turnId:'reported',completedAt:Date.now()});f.guard.refresh(f.pair);
  assert.equal(f.guard.status(f.fg()).reason,'reported');assert.equal(f.guard.status(f.fg()).state,'completed');assert.deepEqual(f.pending(),[]);
});

test('实际完成门槛：空闲前台收到后台原生完成；暂缓的完成提醒不占用其他唤醒',async t=>{
  const f=managedFixture(t);
  f.native('turn/started',{turn:{id:'background',status:'inProgress'}});
  f.native('turn/completed',{turn:{id:'background',status:'completed'}});f.codex.emit('state',f.x.sessionId,'ready',f.previous.address);
  await new Promise(resolve=>setImmediate(resolve));f.broker.turnEnds.refresh(f.pair);
  assert.equal(f.broker.turnEnds.ready(f.pair),true);
  const notice=f.broker.deliveryHealth.peek(f.pair.id,f.c.id).find(n=>n.source==='background_completed');assert.ok(notice);
  f.broker.foregroundHealth.observe(f.c,'start','foreground');f.store.updatePeer(f.c.id,{state:'busy'});f.broker.turnEnds.begin(f.c,'foreground');
  f.broker.turnEnds.refresh(f.pair);assert.equal(f.broker.turnEnds.ready(f.pair),false);
  f.broker.deliveryHealth.enqueue(f.pair.id,f.c.id,{id:'independent-health',state:'pending',source:'turn_health',message:'Independent health fact',tier:4,wake:true});
  const internals=f.broker as unknown as {runtimeHealth:BrokerHealthCoordinator;wakeNotices(pair:typeof f.pair,peer:typeof f.c):{id:string}[]};
  const wakeNotices=internals.wakeNotices(f.pair,f.c);
  assert.deepEqual(wakeNotices.map(n=>n.id),['independent-health']);
  f.broker.foregroundHealth.observe(f.c,'ready','foreground');f.store.updatePeer(f.c.id,{state:'ready'});f.broker.turnEnds.refresh(f.pair);
  const prepared=internals.runtimeHealth.admitNotices(f.pair,f.c,[notice],notice.scope!,true,true);
  assert.equal(prepared.length,1);assert.equal(prepared[0]?.interventionIds?.[0],notice.id);
  assert.equal(f.broker.turnEnds.status(f.c).state,'completed');assert.equal(f.store.messages().length,0);
});

test('完成提醒按原顺序报告配对暂停、待人处理和原生端点状态，再考虑生命周期门槛',async t=>{
  const f=managedFixture(t),foreground=()=>f.store.peer(f.c.id);
  f.native('turn/started',{turn:{id:'background',status:'inProgress'}});
  f.native('turn/completed',{turn:{id:'background',status:'completed'}});
  f.codex.emit('state',f.x.sessionId,'ready',f.previous.address);
  await new Promise(resolve=>setImmediate(resolve));f.broker.turnEnds.refresh(f.pair);
  assert.equal(f.broker.turnEnds.ready(f.pair),true);
  const life=lifecycle(f.broker),hold=()=>f.broker.turnEnds.status(foreground()).hold;
  const retire=t.mock.method(life,'isRetiring',(id:string)=>id===f.x.id);
  assert.equal(hold(),'runtime_unavailable');retire.mock.restore();
  f.store.setSetting('managedSessionConflict:'+f.x.id,{conflict:true});
  assert.equal(hold(),'runtime_unavailable');
  f.store.setPaused(true,f.pair.id);assert.equal(hold(),'paused');f.store.setPaused(false,f.pair.id);
  f.store.updatePeer(f.x.id,{state:'needsHuman'});assert.equal(hold(),'pending_user');
  const endpoint=f.store.endpoint(f.x.id)!;
  f.store.saveEndpoint({...endpoint,pendingContext:true});assert.equal(hold(),'native_state_unverified');
  f.store.saveEndpoint(endpoint);f.store.updatePeer(f.x.id,{state:'ready'});
  f.store.setSetting('managedSessionConflict:'+f.x.id,null);
  const cancel=life.beginRestart(f.x.id);
  try{assert.equal(hold(),'runtime_unavailable');}finally{cancel();}
  assert.equal(hold(),undefined);assert.equal(f.broker.turnEnds.ready(f.pair),true);
});

test('实际完成门槛：后台先完成、前台后 Stop；过期或已结束的请求不扣额，应用回执才结清事实',async t=>{
  const f=managedFixture(t),turnId='foreground',at=Date.now();
  f.store.updatePeer(f.c.id,{state:'busy'});f.broker.foregroundHealth.observe(f.c,'start',turnId);f.broker.turnEnds.begin(f.c,turnId);
  const health={turnId,state:'running' as const,lastEventAt:at,lastProgressAt:at,tools:[]};
  f.store.saveEndpoint({...f.store.endpoint(f.c.id)!,nativeHealth:health});
  f.native('turn/started',{turn:{id:'background',status:'inProgress'}});
  f.native('turn/completed',{turn:{id:'background',status:'completed'}});f.codex.emit('state',f.x.sessionId,'ready',f.previous.address);
  await new Promise(resolve=>setImmediate(resolve));f.broker.turnEnds.refresh(f.pair);
  assert.equal(f.broker.turnEnds.ready(f.pair),false);
  const stats={startedAt:at,covered:true,actions:0,workTools:0},input={actor:{host:'claude',memberId:f.c.id,sessionId:f.c.sessionId!,callId:randomUUID()},turn_id:turnId,stop_hook_active:false,stats,background:{tasks:0,crons:0},deadline_at:at+4500};
  assert.deepEqual(await f.broker.dispatch('peer.turn.stop',{...input,deadline_at:at-1}),{});
  assert.equal(f.broker.turnEnds.budget.status(f.c.id).used,0);
  const tip=await f.broker.dispatch('peer.turn.stop',input) as {block?:string;intervention_id:string};assert.ok(tip.block);
  assert.equal(f.broker.turnEnds.status(f.c).state,'pending','返回 Stop 建议还不能证明前台见到了事实');
  f.store.saveEndpoint({...f.store.endpoint(f.c.id)!,nativeHealth:{...health,state:'ready',completion:'completed'}});
  f.broker.turnEnds.native(f.c,{turnId,state:'ready',completion:'completed'});
  f.broker.foregroundHealth.observe(f.c,'ready',turnId);f.store.updatePeer(f.c.id,{state:'ready'});f.broker.turnEnds.refresh(f.pair);
  assert.equal(f.broker.turnEnds.ready(f.pair),true,'前台已经空闲但完成回执缺失时，提醒不能永久扣住');
  assert.deepEqual(await f.broker.dispatch('peer.turn.stop',{...input,deadline_at:at-1,stop_hook_active:true,background:{tasks:1,crons:1}}),{confirmed:true});
  assert.equal(f.broker.turnEnds.status(f.c).state,'completed','已应用提示的回执不依赖后台任务归零或完成回执');
  f.store.saveEndpoint({...f.store.endpoint(f.c.id)!,nativeHealth:{...health,state:'ready',completion:'completed'}});
  assert.deepEqual(await f.broker.dispatch('peer.turn.stop',{...input,turn_id:turnId}),{});
  f.broker.turnEnds.complete(f.c,{turnId,completedAt:Date.now(),stats,interventions:[{id:tip.intervention_id,inert:true}]});
  f.store.updatePeer(f.c.id,{state:'ready'});f.broker.foregroundHealth.observe(f.c,'ready',turnId);f.broker.turnEnds.refresh(f.pair);
  assert.equal(f.broker.turnEnds.status(f.c).state,'completed');assert.equal(f.broker.turnEnds.budget.status(f.c.id).used,0);
});

for(const host of ['claude','codex'] as const)test(host+' 前台 Stop 已空闲但回执未结算时，不重复叫醒同一事实',t=>{
  const f=setup(t);
  if(host==='codex'){f.store.updatePeer(f.c.id,{owner:'nxbridge'});f.store.updatePeer(f.x.id,{owner:'user'});}
  const foreground=()=>f.store.peer(host==='claude'?f.c.id:f.x.id),background=()=>f.store.peer(host==='claude'?f.x.id:f.c.id);
  f.guard.begin(background(),'background');f.guard.complete(background(),{turnId:'background',completedAt:Date.now(),stats:f.stats()});
  f.guard.begin(foreground(),'foreground');f.store.updatePeer(foreground().id,{state:'busy'});f.guard.refresh(f.pair);
  const tip=f.guard.checkStop(foreground(),{turn_id:'foreground',stop_hook_active:false,stats:f.stats(),background:{tasks:0,crons:0}});assert.ok(tip.block);
  assert.equal(f.guard.status(foreground()).hold,'foreground_tip_pending');
  f.guard.confirmStop(foreground(),'foreground');
  f.store.updatePeer(foreground().id,{state:'ready'});f.guard.native(foreground(),{turnId:'foreground',state:'ready',completion:'completed'});f.guard.refresh(f.pair);
  assert.equal(f.guard.ready(f.pair,true),false);assert.deepEqual(f.health.peek(f.pair.id,foreground().id),[]);
  assert.equal(f.guard.budget.status(foreground().id).used,1);
  f.guard.confirmStop(foreground(),'foreground');f.guard.refresh(f.pair);
  assert.equal(f.guard.status(foreground()).state,'completed');assert.equal(f.guard.ready(f.pair,true),false);
  assert.equal(f.store.setting<{id:string}>('turnEndIntervention:'+foreground().id)?.id,tip.intervention_id);
  f.guard.complete(foreground(),{turnId:'foreground',completedAt:Date.now(),stats:f.stats()});
  assert.equal(f.guard.budget.status(foreground().id).used,0);assert.equal(f.guard.ready(f.pair,true),false);
});

for(const host of ['claude','codex'] as const)test(host+' 前台真正空闲后不等缺失的完成回执，迟到确认仍能撤回尚未提交的提醒',t=>{
  const f=setup(t);
  if(host==='codex'){f.store.updatePeer(f.c.id,{owner:'nxbridge'});f.store.updatePeer(f.x.id,{owner:'user'});}
  const foreground=()=>f.store.peer(host==='claude'?f.c.id:f.x.id),background=()=>f.store.peer(host==='claude'?f.x.id:f.c.id);
  f.guard.begin(background(),'background');f.guard.complete(background(),{turnId:'background',completedAt:Date.now(),stats:f.stats()});
  f.guard.begin(foreground(),'foreground');f.store.updatePeer(foreground().id,{state:'busy'});
  const tip=f.guard.checkStop(foreground(),{turn_id:'foreground',stop_hook_active:false,stats:f.stats(),background:{tasks:0,crons:0}});assert.ok(tip.block);
  f.store.updatePeer(foreground().id,{state:'ready'});f.guard.stopped(foreground(),'foreground');f.guard.refresh(f.pair);
  assert.equal(f.guard.ready(f.pair),true);assert.equal(f.guard.status(foreground()).hold,undefined);
  assert.equal(f.guard.budget.status(foreground().id).used,1,'未确认的停止建议保持扣额');
  assert.equal(f.guard.confirmStop(foreground(),'foreground'),true);f.guard.refresh(f.pair);
  assert.equal(f.guard.status(foreground()).state,'completed');assert.deepEqual(f.health.peek(f.pair.id,foreground().id),[]);
  const intervention=f.store.setting<{confirmed?:true;released?:true}>('turnEndIntervention:'+foreground().id)!;
  assert.equal(intervention.confirmed,true);assert.equal(intervention.released,true,'停止已确认和前台占用已释放是可以并存的事实');
  f.guard.complete(foreground(),{turnId:'foreground',completedAt:Date.now(),stats:f.stats()});
  assert.equal(f.guard.budget.status(foreground().id).used,0,'保留的原生元数据仍支持迟到的无动作证明');
});

test('前台 Stop 确认丢失后失败或直接开新轮次，不永久挡住提醒，也不把未知结果退费',t=>{
  const f=setup(t);f.finish('background');
  for(const terminal of ['failed','next-turn']){
    f.guard.begin(f.fg(),'foreground-'+terminal);f.store.updatePeer(f.c.id,{state:'busy'});f.guard.refresh(f.pair);
    assert.ok(f.guard.checkStop(f.fg(),{turn_id:'foreground-'+terminal,stop_hook_active:false,stats:f.stats(),background:{tasks:0,crons:0}}).block);
    if(terminal==='failed')f.guard.native(f.fg(),{turnId:'foreground-'+terminal,state:'failed'});
    else f.guard.begin(f.fg(),'next-real-turn');
    f.store.updatePeer(f.c.id,{state:'ready'});f.guard.refresh(f.pair);
    assert.equal(f.guard.ready(f.pair),true);assert.equal(f.guard.status(f.fg()).state,'pending');
  }
  assert.equal(f.guard.budget.status(f.c.id).used,2);
});

test('已经采样的同轮队列回执遇到门槛变化仍幂等，不能重新变成未提交预约',t=>{
  const f=setup(t);f.finish('background');const notice=f.admit();
  assert.equal(f.guard.consumeQueued(f.fg(),notice.id,'sampled'),true);f.hold('pending_user');
  assert.equal(f.guard.consumeQueued(f.fg(),notice.id,'sampled'),true);assert.equal(f.guard.consumeQueued(f.fg(),notice.id,'other'),false);
  f.guard.refresh(f.pair);f.hold(undefined);t.mock.timers.tick(60001);f.guard.refresh(f.pair);
  assert.equal(f.guard.ready(f.pair),false);assert.deepEqual(f.pending(),[]);
  f.guard.complete(f.fg(),{turnId:'sampled',completedAt:Date.now(),stats:f.stats({actions:1,workTools:1})});
  assert.equal(f.guard.budget.status(f.c.id).used,1);
});
