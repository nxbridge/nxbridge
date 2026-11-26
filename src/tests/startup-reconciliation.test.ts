import test,{type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import {appendFileSync,writeFileSync} from 'node:fs';
import path from 'node:path';
import {Broker,type Adapters} from '../broker/coordinator.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {Questions} from '../integration/mcp/questions.js';
import {Store} from '../storage/store.js';
import {lifecycle} from './support/broker-fixture.js';
import {FakeAdapter} from './support/fake-adapter.js';
import {recordFixtureLaunch} from './support/native-launch.js';
import {registration} from './support/registration.js';
import {temporaryDirectory} from './support/temporary.js';

function fixture(t:TestContext){
  const temp=temporaryDirectory(),home=temp.directory,filename=path.join(home,'nxbridge.db');
  const config={...DEFAULT_CONFIG,implicit:'never' as const,codexHome:home,claudeHome:home};
  let store=new Store(filename),broker:Broker|undefined;
  const reopen=()=>{
    store.close();store=new Store(filename);
    const codex=new FakeAdapter(),claude=new FakeAdapter();
    broker=new Broker(store,config,{codex,claude} as unknown as Adapters,home);
    return{store,broker,codex,claude};
  };
  t.after(async()=>{await broker?.stop();store.close();await temp.cleanup();});
  return{home,store,config,reopen};
}

function gate(){
  let release!:()=>void;
  const waiting=new Promise<void>(resolve=>{release=resolve;});
  return{release,waiting};
}

test('启动从已重开的数据库分开结算旧回执、在途投递和问题，单条故障不阻断其余记录',async t=>{
  const f=fixture(t),c=f.store.register(registration('claude','c')),x=f.store.register(registration('codex','x')),pair=f.store.pair(c.id,x.id);
  const interrupted=f.store.accept(pair,c,'提交中断的消息','interrupted'),failed=f.store.accept(pair,c,'模拟持久化故障','isolated-failure');
  for(const message of [interrupted,failed])assert.equal(f.store.claim(message.id),true);
  const receipt=f.store.accept(pair,c,'已有宿主回执的消息','legacy-receipt');
  f.store.transition(receipt.id,'runtimeQueued','宿主 PostToolUse 确认取件工具完成');
  const question=new Questions(f.store,f.config).create(pair,x,{host:'codex',sessionId:'x',callId:'question'},
    {questions:[{id:'next',header:'方向',question:'如何继续？',options:[{label:'甲',description:'说明甲'},{label:'乙',description:'说明乙'}]}]});
  const {store,broker,codex,claude}=f.reopen(),transition=store.transition.bind(store);
  store.transition=(id,...args)=>{
    if(id===failed.id&&args[0]==='unknown')throw Error('隔离消息写入失败');
    return transition(id,...args);
  };
  await broker.start();await broker.pump();
  assert.equal(store.message(receipt.id).state,'contextObserved');
  assert.equal(store.message(interrupted.id).state,'unknown');
  assert.equal(store.message(failed.id).state,'sending');
  assert.equal(store.question(String(question.request_id)).terminalReason,'broker_restarted');
  assert.ok(store.events().some(event=>event.type==='pump.failed'&&(event.data as {stage:string}).stage==='message_recover:'+failed.id));
  assert.ok(store.events().some(event=>event.type==='health.delivery.restart'&&(event.data as {recoveredSending:number}).recoveredSending===2));
  assert.ok([...codex.delivered,...claude.delivered].every(message=>message.id!==interrupted.id&&message.id!==failed.id));
});

test('启动按持久记录恢复待回收成员，慢回收不阻塞其他成员且不重新启动原会话',async t=>{
  const f=fixture(t),names=['slow','discovered','later','exhausted','disabled'] as const;
  const members=Object.fromEntries(names.map(name=>[name,f.store.updatePeer(f.store.register(registration('claude',name)).id,{owner:'nxbridge',...(name==='disabled'?{enabled:false}:{})})])) as Record<typeof names[number],ReturnType<Store['peer']>>;
  recordFixtureLaunch(f.store,f.home,members.discovered);
  const later={attempts:1,nextAt:Date.now()+600000,error:'等待下次期限'},exhausted={attempts:3,nextAt:Number.MAX_SAFE_INTEGER,error:'保留历史失败'};
  f.store.setSetting('processRetirements',{[members.slow.id]:{attempts:1,nextAt:0},[members.later.id]:later,[members.exhausted.id]:exhausted,[members.disabled.id]:{attempts:0,nextAt:0}});
  const {store,broker,claude}=f.reopen(),slowEntered=gate(),continueSlow=gate(),discoveredReleased=gate(),release=claude.release.bind(claude);
  claude.release=async peer=>{
    if(peer.id===members.slow.id){slowEntered.release();await continueSlow.waiting;throw Error('隔离回收失败');}
    await release(peer);if(peer.id===members.discovered.id)discoveredReleased.release();
  };
  try{
    await broker.start();await broker.pump();await slowEntered.waiting;await discoveredReleased.waiting;
    assert.equal(lifecycle(broker).isRetiring(members.slow.id),true);
    assert.deepEqual(claude.released.map(peer=>peer.id),[members.discovered.id]);
  }finally{continueSlow.release();}
  await lifecycle(broker).waitForReleases();
  const records=lifecycle(broker).retirements();
  assert.equal(records[members.discovered.id],undefined);
  assert.equal(records[members.slow.id]?.attempts,2);
  assert.match(records[members.slow.id]!.error!,/隔离回收失败/);
  assert.deepEqual(records[members.later.id],later);
  assert.deepEqual(records[members.exhausted.id],exhausted);
  assert.deepEqual(records[members.disabled.id],{attempts:0,nextAt:0});
  assert.deepEqual([...claude.created,...claude.woken],[]);
  assert.equal(store.peer(members.discovered.id).state,'offline');
});

test('重开后从原生日志重新观察未完成的子代理账目，独立活动不生成租约或已读证明',async t=>{
  const f=fixture(t),c=f.store.register(registration('claude','c')),x=f.store.register(registration('codex','x',{state:'busy'}));
  f.store.pair(c.id,x.id);
  const file=path.join(f.home,'root.jsonl'),append=(row:object)=>appendFileSync(file,JSON.stringify({timestamp:new Date().toISOString(),...row})+'\n','utf8');
  writeFileSync(file,'','utf8');append({type:'session_meta',payload:{id:x.sessionId}});append({type:'event_msg',payload:{type:'task_started',turn_id:'parent-turn'}});
  f.store.setSetting('codexTranscript:'+x.id,{sessionId:x.sessionId,file});
  const prior=new Broker(f.store,f.config,{codex:new FakeAdapter(),claude:new FakeAdapter()} as unknown as Adapters,f.home);
  prior.subagentCompletions.record(x,'child-session','child-turn','/root/child',file);
  append({type:'event_msg',payload:{type:'item_completed',thread_id:x.sessionId,item:{type:'SubAgentActivity',kind:'completed',id:'subagent-completed-child-turn',agent_thread_id:'child-session',agent_path:'/root/child'}}});
  append({type:'event_msg',payload:{type:'mcp_tool_call_begin',turn_id:'parent-turn'}});
  await prior.stop();
  const {store,broker,codex}=f.reopen();codex.hasSubscription=()=>false;
  const root=store.peer(x.id),completions=broker.subagentCompletions;
  assert.equal(completions.running(root),false);
  assert.equal(completions.status(root)?.needsWake,1);
  await broker.start();await broker.pump();
  assert.equal(completions.running(root),true);
  const status=completions.status(root)!;
  assert.equal(status.needsWake,1);assert.equal(status.unconfirmed,1);assert.equal(status.awaitingContext,1);assert.equal(status.wake,undefined);
  assert.deepEqual(codex.woken,[]);
});
