import test,{type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {Broker,type Adapters} from '../broker/coordinator.js';
import type {ParkedCustody} from '../broker/broker-custody.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {Questions} from '../integration/mcp/questions.js';
import {Store} from '../storage/store.js';
import {delay} from '../shared/time.js';
import {FakeAdapter} from './support/fake-adapter.js';
import {registration} from './support/registration.js';
import {temporaryDirectory} from './support/temporary.js';
import {ensureHome} from '../ipc/home.js';
import {listenBroker} from '../ipc/ipc.js';
import {waitForPartnerQuestion} from '../integration/mcp/question-wait.js';
import {once} from 'node:events';
import {WebSocketServer,type WebSocket} from 'ws';
import {CodexAdapter} from '../hosts/codex/adapter.js';
import {StartupRoutes} from '../broker/startup-routes.js';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {existsSync,writeFileSync} from 'node:fs';
import {readCodexHistory} from '../hosts/codex/codex-history.js';
import type {BrokerHealthCoordinator} from '../broker/health/coordinator.js';
import type {BrokerLifecycleCoordinator} from '../broker/lifecycle/coordinator.js';
import {turnNativeScope} from '../broker/turn-end-evidence.js';
import {saveManagedRuntime} from '../hosts/managed-runtimes.js';
import {managedMarker} from '../install/installation.js';
import {randomUUID} from 'node:crypto';

class HandoffAdapter extends FakeAdapter{
  pending=false;
  paused=false;
  failure?:Error;
  promoted:string[]=[];
  pauseHandoff(){this.paused=true;return{pending:()=>this.pending,resume:()=>{this.paused=false;}};}
  async prepareHandoff(){}
  parkHandoff():ParkedCustody{
    if(this.failure)throw this.failure;
    return{offers:[],live:()=>false,release:()=>{},close:()=>{}};
  }
  async acquireHandoff(){return{promoted:this.promoted,transferred:this.promoted.length,held:0};}
  async connect(address:string){
    this.connected.push(address);
    return{request:async(...args:any[])=>({thread:{id:args[1]?.threadId,status:{type:'idle'},turns:[]}})};
  }
}

function fixture(t:TestContext){
  const temp=temporaryDirectory(),home=temp.directory,file=path.join(home,'nxbridge.db'),store=new Store(file),codex=new HandoffAdapter(),claude=new FakeAdapter();
  const broker=new Broker(store,{...DEFAULT_CONFIG,implicit:'never'},{codex,claude} as unknown as Adapters,home);
  const c=store.register(registration('claude','c')),x=store.register(registration('codex','x')),pair=store.pair(c.id,x.id);
                                
  store.saveEndpoint({...store.endpoint(x.id)!,runtimeId:'custody-fixture'});
  t.after(async()=>{await broker.stop();store.close();await temp.cleanup();});
  const cx={host:'codex' as const,sessionId:'x',callId:'ask'};
  const question=()=>broker.dispatch('question.create',{actor:cx,questions:[{id:'one',header:'方向',question:'如何继续？',options:[{label:'甲',description:'说明甲'},{label:'乙',description:'说明乙'}]}]}) as Promise<{request_id:string;expires_at:string}>;
  return{home,file,broker,store,codex,claude,c,x,pair,cx,question};
}

const barrier=()=>{
  let open!:()=>void;
  const wait=new Promise<void>(resolve=>{open=resolve;});
  return {wait,open};
};

function holdNativeProbe(f:ReturnType<typeof fixture>,entered:ReturnType<typeof barrier>,resume:ReturnType<typeof barrier>){
  const health=(f.broker as unknown as {runtimeHealth:BrokerHealthCoordinator}).runtimeHealth,probe=health.probeTurnHealth.bind(health);
  let first=true;
  health.probeTurnHealth=async(peer,endpoint)=>{
    const value=await probe(peer,endpoint);
    if(first){first=false;entered.open();await resume.wait;}
    return value;
  };
}

test('封存等待公开请求和 Mod 的实际内层工作，丢弃迟到生产者写入',async t=>{
  const f=fixture(t),actual=barrier(),outer=barrier(),entered=barrier();
                                         
  const broker=f.broker as unknown as {dispatchRequest(method:string,raw:unknown):Promise<unknown>;dispatchAdmitted(method:string,raw:unknown):Promise<unknown>};
  const original=broker.dispatchRequest.bind(broker);
  broker.dispatchRequest=async(method,raw)=>{
    if(method==='fixture.mod'){
      await Promise.race([broker.dispatchAdmitted('fixture.inner',{}),outer.wait]);
      return{timedOut:true};
    }
    if(method==='fixture.inner'){entered.open();await actual.wait;f.store.setSetting('admitted-internal','settled');return{};}
    return original(method,raw);
  };
  const request=f.broker.dispatch('fixture.mod',{});await entered.wait;
  let committed=false;
  const handoff=f.broker.sealForHandoff({updateId:'sealed',signal:new AbortController().signal,commit:()=>{committed=true;}});
  try{
    outer.open();assert.deepEqual(await request,{timedOut:true});
    await assert.rejects(f.broker.dispatch('ping',{}),{code:'handoff_retry'});
    f.codex.emit('providerRetries',{address:'fake-address',budget:{value:1}});
    assert.equal(f.store.setting('providerRetries:fake-address'),undefined);
    assert.equal(committed,false);
  }finally{actual.open();}
  const parked=await handoff;parked.close();assert.equal(committed,true);
  assert.throws(()=>f.store.setSetting('late','forbidden'));
  assert.equal(f.codex.listenerCount('providerRetries'),0);
  await assert.rejects(f.broker.dispatch('ping',{}),{code:'stopping'});
  const read=new Store(f.file);try{assert.equal(read.setting('admitted-internal'),'settled');assert.equal(read.setting('late'),undefined);}finally{read.close();}
});

test('排空等待进行中的完成结算，新协调器接续后不能重复返还提醒次数',async t=>{
  const f=fixture(t),entered=barrier(),complete=barrier(),authority=f.c.id,scope='custody-scope',id='completion-during-drain';
  const budget=f.broker.turnEnds.budget;
  assert.ok(budget.admit(authority,id,scope));assert.ok(budget.started(authority,id,scope,'foreground-turn'));
  const broker=f.broker as unknown as {dispatchRequest(method:string,raw:unknown):Promise<unknown>};
  const original=broker.dispatchRequest.bind(broker);
  broker.dispatchRequest=async(method,raw)=>{
    if(method!=='fixture.completion')return original(method,raw);
    entered.open();await complete.wait;
    assert.ok(budget.completed(authority,id,scope,'foreground-turn',true));return{};
  };
  const completion=f.broker.dispatch('fixture.completion',{});await entered.wait;
  let committed=false;
  const handoff=f.broker.sealForHandoff({updateId:'budget-drain',signal:new AbortController().signal,commit:()=>{committed=true;}});
  try{
    await assert.rejects(f.broker.dispatch('ping',{}),{code:'handoff_retry'});
    assert.equal(committed,false);assert.equal(budget.status(authority).used,1);
  }finally{complete.open();}
  await completion;(await handoff).close();assert.equal(committed,true);
  const store=new Store(f.file),next=new Broker(store,DEFAULT_CONFIG,{codex:new HandoffAdapter(),claude:new FakeAdapter()} as unknown as Adapters,path.dirname(f.file));
  try{
    const resumed=next.turnEnds.budget;
    assert.deepEqual(resumed.status(authority),{used:0,limit:4,remaining:4,awaitingProof:0});
    assert.equal(resumed.completed(authority,id,scope,'foreground-turn',true),false);
    assert.equal(resumed.admit(authority,id,scope),undefined,'已经进入过宿主的提醒不能因返还次数再提交');
    assert.equal(store.setting<{version:number}>('completionWakeBudget:'+authority)?.version,2);
  }finally{await next.stop();store.close();}
});

for(const phase of ['reserved','retryable','started'] as const)test(`旧版 ${phase} 预约经过封存交接后由新版继续结算`,async t=>{
  const f=fixture(t),authority=f.c.id,scope='legacy-custody',id='legacy-completion',key='completionWakeBudget:'+authority;
  const legacy={version:1,epoch:'human-before-update',human:{id:'human-before-update',at:10},limit:4,used:1,
    reservations:[{id,scope,at:20,...(phase==='retryable'?{retryable:true as const}:{}),...(phase==='started'?{turnId:'bound-turn'}:{})}]};
  f.store.setSetting(key,legacy);
  (await f.broker.sealForHandoff({updateId:'legacy-budget-'+phase,signal:new AbortController().signal,commit:()=>{}})).close();
  const store=new Store(f.file),next=new Broker(store,DEFAULT_CONFIG,{codex:new HandoffAdapter(),claude:new FakeAdapter()} as unknown as Adapters,path.dirname(f.file));
  try{
    const budget=next.turnEnds.budget;
    assert.equal(budget.status(authority).used,1);assert.deepEqual(store.setting(key),legacy,'新版先读取，不提前改写旧记录');
    if(phase==='retryable')assert.deepEqual(budget.admit(authority,id,scope),{epoch:legacy.epoch});
    else assert.equal(budget.admit(authority,id,scope),undefined,'没有明确写入前失败证明时不重投');
    assert.ok(budget.started(authority,id,scope,'bound-turn'));
    assert.equal(budget.completed(authority,id,scope,'bound-turn',true),true);
    assert.equal(budget.status(authority).used,0);
    const saved=store.setting<{version:number;epoch:string;reservations:{state:string;turnId?:string}[]}>(key)!;
    assert.equal(saved.version,2);assert.equal(saved.epoch,legacy.epoch);assert.equal(saved.reservations[0]?.state,'refunded');
    assert.equal(saved.reservations[0]?.turnId,'bound-turn');
    assert.equal(budget.completed(authority,id,scope,'bound-turn',true),false);
  }finally{await next.stop();store.close();}
});

test('可回滚排空只唤醒支持重连的等待，恢复原接入和问题',async t=>{
  const f=fixture(t),created=await f.question(),abort=new AbortController();
  const q={actor:f.cx,request_id:created.request_id,handoff:1};
  const waiting=assert.rejects(f.broker.dispatch('question.wait',q),{code:'handoff_retry'});
  f.codex.pending=true;
  let committed=false;
  const seal=assert.rejects(f.broker.sealForHandoff({updateId:'aborted',signal:abort.signal,commit:()=>{committed=true;}}),{code:'handoff_drain_interrupted'});
  await waiting;assert.equal(f.codex.paused,true);assert.equal(f.store.question(created.request_id).status,'pending');
  await assert.rejects(f.broker.dispatch('question.cancel',q),{code:'handoff_retry'});
  abort.abort();await seal;
  assert.equal(committed,false);assert.equal(f.codex.paused,false);assert.equal(f.store.setting('questionContinuity'),undefined);
  assert.equal((await f.broker.dispatch('question.get',q) as any).expires_at,created.expires_at);
  await f.broker.dispatch('question.cancel',q);
});

test('普通关闭立即结束等待，维持非计划重启的问题失效行为',async t=>{
  const f=fixture(t),created=await f.question();
  const waiting=assert.rejects(f.broker.dispatch('question.wait',{actor:f.cx,request_id:created.request_id}),{code:'stopping'});
  await f.broker.stop();await waiting;
  assert.equal(f.store.question(created.request_id).terminalReason,'broker_restarted');
});

test('实际 IPC 问题等待跨封存后完成正常长轮询并继续等待同一答案',async t=>{
  const f=fixture(t),home=path.dirname(f.file),created=await f.question(),admitted=barrier();
  await ensureHome(home);
  const old=await listenBroker(home,async(method,params)=>{
    const work=f.broker.dispatch(method,params);
    if(method==='question.wait')admitted.open();
    return work;
  });
  t.after(old.close);
  const wait=waitForPartnerQuestion<{status:string;expires_at?:string;answers?:Record<string,unknown>}>({actor:f.cx,request_id:created.request_id},home);
                                   
  void wait.catch(()=>{});
  await admitted.wait;
  (await f.broker.sealForHandoff({updateId:'ipc-question',signal:new AbortController().signal,commit:()=>{}})).close();
  await old.close();await delay(300);
  const store=new Store(f.file),next=new Broker(store,DEFAULT_CONFIG,{codex:new HandoffAdapter(),claude:new FakeAdapter()} as unknown as Adapters,home);
  const roundtrip=barrier(),secondPoll=barrier();let calls=0,first:unknown;
  const listener=await listenBroker(home,async(method,params)=>{
    const index=++calls;if(index===2)secondPoll.open();
    const value=await next.dispatch(method,params);
    if(index===1){first=value;roundtrip.open();}return value;
  });
  try{
    next.questions.recoverAfterStart();
    await roundtrip.wait;await secondPoll.wait;
    assert.equal(calls,2,'长轮询应该完成一次，不应每两秒留下一个孤立等候');
    assert.equal((first as {status:string}).status,'pending');
    await next.dispatch('question.answer',{actor:{host:'claude',sessionId:'c',callId:'actual-answer'},input:{request_id:created.request_id,answers:{'如何继续？':'甲'},rationale:'沿用原问题。'}});
    const result=await wait;
    assert.equal(result.status,'answered');assert.equal(result.expires_at,created.expires_at);
    assert.deepEqual(result.answers,{one:{answers:['甲']}});
  }finally{await listener.close();await next.stop();store.close();}
});

for(const overflow of [false,true])test(`实际用户窗口连接在${overflow?'观察达到上限':'取消排空'}后保留审批、完成和读件回执`,async()=>{
  const temp=temporaryDirectory(),home=temp.directory,store=new Store(':memory:'),codex=new CodexAdapter(DEFAULT_CONFIG,home);
  const native=new WebSocketServer({host:'127.0.0.1',port:0});await once(native,'listening');
  const broker=new Broker(store,DEFAULT_CONFIG,{codex,claude:new FakeAdapter()} as unknown as Adapters,home);
  const endpoint='ws://127.0.0.1:'+(native.address() as {port:number}).port;
  let connected:WebSocket|undefined,active=false;const writes:string[]=[],observed:string[]=[];
  codex.on('native',({message})=>observed.push(message.method));
  native.on('connection',socket=>socket.on('message',bytes=>{
    const message=JSON.parse(bytes.toString());
    if(message.method==='initialize'&&message.params.clientInfo.name==='codex-tui')connected=socket;
    if(!message.method&&message.id==='native-approval')writes.push('answered');
    if(message.method==='thread/resume')writes.push('resume');
    if(!message.method||message.id===undefined)return;
    const result=message.method==='initialize'?{userAgent:'codex-tui/0.170.0 (test) (codex-tui; 0.170.0)'}:
      ['thread/read','thread/resume'].includes(message.method)?{thread:{id:'user-window',cwd:home,status:{type:active?'active':'idle'}}}:{data:[]};
    socket.send(JSON.stringify({id:message.id,result}));
  }));
  const preparing=barrier(),continueSeal=barrier(),abort=new AbortController();
  const secondPreparing=barrier(),continueSecondSeal=barrier(),abortSecond=new AbortController();
  codex.prepareHandoff=async()=>{preparing.open();await continueSeal.wait;};
  let sealing:Promise<unknown>|undefined,secondSealing:Promise<unknown>|undefined;
  try{
    const x=store.register(registration('codex','user-window',{cwd:home,endpoint:{mode:'app-server',address:endpoint,epoch:endpoint,pid:null,procStart:null}}));
    const c=store.register(registration('claude','partner')),pair=store.pair(c.id,x.id);
    const message=store.accept(pair,c,'原消息','test-receipt');store.claim(message.id);store.transition(message.id,'runtimeQueued');
    const rpc=await codex.attach(x,store.endpoint(x.id)!);
    assert.deepEqual(writes.splice(0),['resume']);
    sealing=assert.rejects(broker.sealForHandoff({updateId:'user-rollback',signal:abort.signal,commit:()=>assert.fail('排空不能提交')}),{code:overflow?'handoff_observations_busy':'handoff_drain_interrupted'});
    await preparing.wait;
    const send=(method:string,fields:Record<string,unknown>,id?:string)=>connected!.send(JSON.stringify({id,method,params:{threadId:x.sessionId,...fields}}));
    active=true;send('turn/started',{turn:{id:'real-turn'}});
    send('item/commandExecution/requestApproval',{turnId:'real-turn',itemId:'tool'},'native-approval');
    send('item/reasoning/textDelta',{turnId:'real-turn',delta:'较早的进展'});
    send('error',{turnId:'real-turn',willRetry:true,error:{message:'retry 1/3'}});
    for(let i=0;i<1050;i++)send('item/agentMessage/delta',{turnId:'real-turn',delta:'过程文本'});
    send('error',{turnId:'real-turn',willRetry:true,error:{message:'retry 2/3'}});
    await rpc.request('config/read');
    assert.equal(broker.health.get(endpoint,x.sessionId!)?.state,undefined);
    assert.deepEqual(observed,[]);
    send('item/completed',{turnId:'real-turn',item:{id:'get',type:'functionCallOutput',namespace:'claude',name:'receive_messages',output:JSON.stringify({messages:[{message_id:message.id}]})}});
    active=false;send('turn/completed',{turn:{id:'real-turn',status:'completed'}});
    if(overflow)send('thread/fixture-observation',{turnId:'real-turn',data:'x'.repeat(4*1024*1024)});
    await rpc.request('config/read');
    if(!overflow){assert.equal(store.message(message.id).state,'runtimeQueued');abort.abort();}
    else{
      codex.prepareHandoff=async()=>{secondPreparing.open();await continueSecondSeal.wait;};
      secondSealing=assert.rejects(broker.sealForHandoff({updateId:'next-drain',signal:abortSecond.signal,commit:()=>assert.fail('第二次排空不能提交')}),{code:'handoff_drain_interrupted'});
      void secondSealing.catch(()=>{});await Promise.race([secondPreparing.wait,secondSealing]);
    }
    continueSeal.open();await sealing;
    if(secondSealing){
      await assert.rejects(broker.dispatch('ping',{}),{code:'handoff_retry'});
      abortSecond.abort();continueSecondSeal.open();await secondSealing;
    }
    assert.deepEqual(observed,['turn/started','item/commandExecution/requestApproval','error','item/agentMessage/delta','error','item/completed','turn/completed',...(overflow?['thread/fixture-observation']:[])]);
    assert.equal(broker.health.get(endpoint,x.sessionId!)?.state,'ready',JSON.stringify(store.events().filter(e=>e.type==='pump.failed')));
    assert.equal(store.message(message.id).state,'contextObserved');
    assert.equal(codex.listenerCount('native')>0,true);assert.equal((await codex.connect(endpoint))===rpc,true);
    assert.deepEqual(writes,[]);
  }finally{
    abort.abort();continueSeal.open();await sealing?.catch(()=>{});
    abortSecond.abort();continueSecondSeal.open();await secondSealing?.catch(()=>{});
    await broker.stop();store.close();for(const socket of native.clients)socket.terminate();await new Promise<void>(resolve=>native.close(()=>resolve()));await temp.cleanup();
  }
});

test('旧等待中的问题延后交接，已签发的本地回执仍可完成排空',async t=>{
  const f=fixture(t),created=await f.question(),q={actor:f.cx,request_id:created.request_id};
  const legacy=f.broker.dispatch('question.wait',q);
  const options={updateId:'with-receipt',signal:new AbortController().signal,commit:()=>{}};
  await assert.rejects(f.broker.sealForHandoff(options),{code:'handoff_legacy_wait'});
  assert.equal(f.codex.paused,false);await f.broker.dispatch('question.cancel',q);await legacy;
  const receipts=(f.broker as unknown as {hookReceipts:Map<string,{ack:()=>Promise<void>;timer:NodeJS.Timeout}>}).hookReceipts;
  const id='025768e0-5dfe-491b-993a-d427f6fac30b',callback=barrier(),entered=barrier();
  receipts.set(id,{timer:setTimeout(()=>receipts.delete(id),1000),ack:async()=>{entered.open();await callback.wait;f.store.setSetting('receipt','settled');}});
  let finished=false;
  const handoff=f.broker.sealForHandoff(options).then(value=>{finished=true;return value;});
  const ack=f.broker.dispatch('hook.client.ack',{receipt:id});await entered.wait;
  await delay(30);assert.equal(finished,false);
  callback.open();await ack;(await handoff).close();
  const read=new Store(f.file);try{assert.equal(read.setting('receipt'),'settled');}finally{read.close();}
});

test('不可逆封存期间出错也关闭旧写入句柄，问题连续性只消费一次',async t=>{
  const f=fixture(t),created=await f.question();
  f.codex.failure=Error('原生停放失败');
  await assert.rejects(f.broker.sealForHandoff({updateId:'failed-after-commit',signal:new AbortController().signal,commit:()=>{}}),/原生停放失败/);
  assert.throws(()=>f.store.setSetting('late','forbidden'));
  const restarted=new Store(f.file);
  try{
    assert.equal(restarted.setting<{updateId:string}>('questionContinuity')?.updateId,'failed-after-commit');
    new Questions(restarted,DEFAULT_CONFIG).recoverAfterStart();
    assert.equal(restarted.question(created.request_id).status,'pending');
    assert.equal(new Date(restarted.question(created.request_id).expiresAt).toISOString(),created.expires_at);
    assert.equal(restarted.setting('questionContinuity'),null);
    new Questions(restarted,DEFAULT_CONFIG).recoverAfterStart();
    assert.equal(restarted.question(created.request_id).terminalReason,'broker_restarted');
  }finally{restarted.close();}
});

test('接班补证期间仍响应 IPC，新原生轮次胜过先前探针，控制请求立即重试',async t=>{
  const f=fixture(t);
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});
  const endpoint=f.store.endpoint(f.x.id)!;
  f.store.saveEndpoint({...endpoint,mode:'app-server',address:'隔离原生连接'});
  f.codex.promoted=[f.x.id];
  const raced=barrier(),resume=barrier();holdNativeProbe(f,raced,resume);
  const start=f.broker.start({offers:[],signal:new AbortController().signal,verify:async()=>false,resolve:async()=>{}});
  await raced.wait;
  try{
    assert.equal((await f.broker.dispatch('ping',{}) as any).pid,process.pid);
    await assert.rejects(f.question(),{code:'starting'});
    f.codex.emit('native',{address:'隔离原生连接',message:{method:'turn/started',params:{threadId:'x',turn:{id:'newer'}}}});
    assert.equal(f.store.peer(f.x.id).state,'ready');
  }finally{resume.open();}
  await start;assert.equal((await f.broker.dispatch('ping',{}) as any).pid,process.pid);assert.equal(f.store.peer(f.x.id).state,'busy');
});

test('慢路由认领不挡住查询和既有配对排队，控制与 hook 不等待认领，也不隐式创建搭档',async t=>{
  const f=fixture(t),entered=barrier(),resume=barrier(),serving=barrier();
  f.codex.acquireHandoff=async()=>{entered.open();await resume.wait;return{promoted:[],transferred:0,held:0};};
  const original=f.store.accept(f.pair,f.c,'原投递','original');f.store.claim(original.id);
  const start=f.broker.start({offers:[],signal:new AbortController().signal,verify:async()=>false,resolve:async()=>{}},undefined,serving.open);
  await serving.wait;await entered.wait;
  try{
    assert.equal((await f.broker.dispatch('ping',{}) as any).pid,process.pid);
    assert.equal(f.store.message(original.id).state,'unknown');
    const actor={host:'claude',sessionId:'c',callId:'startup-send'},input={actor,message:'认领期间保存的新消息'};
    const first:any=await f.broker.dispatch('send',input),again:any=await f.broker.dispatch('send',input);
    assert.equal(first.persisted,true);assert.equal(first.route_recovery_pending,true);assert.equal(first.message_id,again.message_id);
    assert.equal(f.store.message(first.message_id).attempts,0);
    assert.equal((await f.broker.dispatch('status',{actor}) as any).paired,true);
    await f.broker.dispatch('receive',{...actor,callId:'startup-receive'});
    for(const method of ['hook.client','broker.handoff.prepare','shutdown','codex.restart','codex.restart.target'])
      await assert.rejects(f.broker.dispatch(method,{}),{code:'starting'});
    f.store.register(registration('claude','unpaired'));
    await assert.rejects(f.broker.dispatch('send',{actor:{host:'claude',sessionId:'unpaired',callId:'new'},message:'不能创建新搭档'}),{code:'starting'});
    assert.equal(f.store.pairs().length,1);assert.equal(f.codex.created.length,0);assert.equal(f.claude.created.length,0);
    assert.deepEqual(f.codex.injected,[]);assert.deepEqual(f.codex.woken,[]);
  }finally{resume.open();await start;}
});

test('补证期间 hook 立即回退，稍后的原生历史结算回执；早到消息在补证结束前不提交',async t=>{
  const f=fixture(t),entered=barrier(),resume=barrier();
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});
  f.store.saveEndpoint({...f.store.endpoint(f.x.id)!,mode:'app-server',address:'unix://隔离接班连接'});f.codex.promoted=[f.x.id];
  const original=f.store.accept(f.pair,f.c,'hook 之前已提交','native-output');f.store.claim(original.id);
  const transcript=path.join(f.home,'native-hook-history.jsonl');
                                                       
  f.codex.reconcile=async peer=>(await readCodexHistory(transcript,peer.sessionId!)).receipts;
  holdNativeProbe(f,entered,resume);
  await ensureHome(f.home);
  const server=await listenBroker(f.home,(method,params)=>f.broker.dispatch(method,params));
  const start=f.broker.start({offers:[],signal:new AbortController().signal,verify:async()=>false,resolve:async()=>{}});
  await entered.wait;
  let queued:string|undefined;
  try{
    const at=new Date().toISOString(),history=[
      {type:'session_meta',payload:{id:f.x.sessionId}},
      {type:'event_msg',timestamp:at,payload:{type:'task_started',turn_id:'native-turn'}},
      {type:'response_item',timestamp:at,payload:{type:'function_call',namespace:'claude',name:'receive_messages',call_id:'native-receive'}},
      {type:'response_item',timestamp:at,payload:{type:'function_call_output',id:'native-tool-output',call_id:'native-receive',output:JSON.stringify({messages:[{message_id:original.id}]})}},
    ].map(row=>JSON.stringify(row)).join('\n')+'\n';
                                                             
    const program="import {hookClient} from "+JSON.stringify(new URL('../integration/hooks/hook-client.js',import.meta.url).href)+";\n"+
      "import {writeFileSync} from 'node:fs';\n"+
      "const output=await hookClient('codex',{hook_event_name:'Stop',session_id:'x'},()=>{throw Error('未提供回执');},async()=>{writeFileSync("+
      JSON.stringify(transcript)+","+JSON.stringify(history)+",'utf8');return{};});\nprocess.stdout.write(JSON.stringify(output));";
    const result=await promisify(execFile)(process.execPath,['--input-type=module','-e',program],{
      encoding:'utf8',windowsHide:true,timeout:5000,env:{...process.env,NXBRIDGE_HOME:f.home,NXBRIDGE_HOOK_STARTED_AT:String(Date.now())},
    });
    assert.deepEqual(JSON.parse(result.stdout),{});assert.equal(existsSync(transcript),true);assert.equal(f.store.message(original.id).state,'unknown');
    const saved:any=await f.broker.dispatch('send',{actor:{host:'claude',sessionId:'c',callId:'catchup-queue'},message:'补证期间新消息'});
    queued=saved.message_id;await f.broker.pump();
    assert.equal(f.store.message(queued!).attempts,0);assert.equal(f.codex.injected.length,0);assert.equal(f.codex.woken.length,0);
  }finally{
    resume.open();await start;await server.close();
  }
  assert.equal(f.store.message(original.id).state,'contextObserved');assert.equal(f.store.message(original.id).attempts,1);
  await f.broker.pump();assert.equal(f.store.message(queued!).attempts,1);
  assert.ok(f.codex.injected.some(call=>call.output.includes(queued!)));
});

test('原生历史超过启动等待预算后显式降级，新投递继续而未知消息不重放',async t=>{
  const f=fixture(t),entered=barrier(),resume=barrier();
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});f.store.saveEndpoint({...f.store.endpoint(f.x.id)!,mode:'app-server',address:'unix://慢历史连接'});
  f.codex.promoted=[f.x.id];
  const old=f.store.accept(f.pair,f.c,'旧未知投递','unknown-before');f.store.claim(old.id);
  holdNativeProbe(f,entered,resume);
  const start=f.broker.start({offers:[],signal:new AbortController().signal,verify:async()=>false,resolve:async()=>{}});
  await entered.wait;
  try{
    await start;
    const startup=(f.broker as unknown as {startupRoutes:StartupRoutes}).startupRoutes;startup.fail(f.x.id);
    const status:any=await f.broker.dispatch('status',{actor:{host:'claude',sessionId:'c',callId:'degraded'}});
    assert.ok(status.startupRoutes.some((route:any)=>route.phase==='degraded'));
    const sent:any=await f.broker.dispatch('send',{actor:{host:'claude',sessionId:'c',callId:'after-ceiling'},message:'降级后新消息'});
    await f.broker.pump();
    assert.equal(f.store.message(sent.message_id).attempts,1);assert.equal(f.store.message(old.id).state,'unknown');assert.equal(f.store.message(old.id).attempts,1);
    assert.equal(f.codex.injected.some(call=>call.output.includes(old.id)),false);
  }finally{resume.open();await start;}
  for(let i=0;i<20&&(f.broker as unknown as {startupCatchups:Map<string,unknown>}).startupCatchups.size;i++)await delay(10);
  assert.equal(f.store.message(old.id).state,'unknown');
});

test('首次回执核对只读到一片时，后续协调器对账沿同一游标继续核验且不重放未知消息',async t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});
  const f=fixture(t),found=f.store.accept(f.pair,f.c,'稍后能证明已接收','progress-found'),unknown=f.store.accept(f.pair,f.c,'仍无回执','progress-unknown');
  f.store.claim(found.id);f.store.claim(unknown.id);f.store.recover();
  const file=path.join(f.home,'progressive-receipts.jsonl'),call='progressive-call';
  writeFileSync(file,[
    {type:'session_meta',payload:{id:f.x.sessionId}},
    {type:'event_msg',payload:{type:'agent_message',message:'x'.repeat(10*1024*1024)}},
    {type:'event_msg',payload:{type:'task_started',turn_id:'progressive-turn'}},
    {type:'response_item',payload:{type:'function_call',namespace:'claude',name:'receive_messages',call_id:call}},
    {type:'response_item',payload:{type:'function_call_output',call_id:call,output:JSON.stringify({messages:[{message_id:found.id}]})}},
  ].map(row=>JSON.stringify(row)).join('\n')+'\n','utf8');
  f.codex.reconcile=async(peer,_endpoint,options:{since?:number}={})=>(await readCodexHistory(file,peer.sessionId!,{since:options.since})).receipts;
  assert.equal(await f.broker.reconcile(f.x.id),true);assert.equal(f.store.message(found.id).state,'unknown');
  for(let pass=0;pass<20&&f.store.message(found.id).state==='unknown';pass++)await f.broker.reconcile(f.x.id);
  assert.equal(f.store.message(found.id).state,'contextObserved');assert.equal(f.store.message(found.id).attempts,1);
  assert.equal(f.store.message(unknown.id).state,'unknown');assert.equal(f.store.message(unknown.id).attempts,1);
  assert.equal(f.codex.woken.length,0);assert.equal(f.codex.injected.length,0);assert.equal(f.codex.created.length,0);
});

test('普通重启的完成核对不阻止投递、全局回执对账和下一次交接，悬挂读取在封存后不写旧库',async t=>{
  const f=fixture(t),entered=barrier(),resume=barrier();
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});
  const runtimeId=randomUUID(),previous=f.store.endpoint(f.x.id)!;
  f.store.saveEndpoint({...previous,address:'unix://held-completion-source',configHome:f.home,runtimeId});
  const peer=f.store.peer(f.x.id),endpoint=f.store.endpoint(f.x.id)!;
                                          
  saveManagedRuntime(f.home,{id:runtimeId,owner:managedMarker(f.home),kind:'owned',state:'ready',dedicated:true,endpoint:endpoint.address,
    peerId:peer.id,sessionId:peer.sessionId!,pid:endpoint.pid!,procStart:endpoint.procStart!,configHome:f.home,createdAt:Date.now()});
  f.store.setSetting('turnEndFact:'+peer.id,{version:1,pairId:f.pair.id,sessionId:peer.sessionId,scope:turnNativeScope(peer,endpoint),state:'completed',turnId:'before-restart',at:Date.now(),reason:'unverified'});
  const reader=f.broker.turnEnds as unknown as {read(peer:typeof f.x,current?:()=>boolean):Promise<boolean>};
  t.mock.method(reader,'read',async()=>{entered.open();await resume.wait;return true;});
  const old=f.store.accept(f.pair,f.c,'旧回执','receipt-before-check');f.store.claim(old.id);
  f.codex.reconcile=async()=>[{messageId:old.id,turnId:'before-restart',itemId:'persisted-receipt'}];
  const start=f.broker.start();await Promise.all([start,entered.wait]);
  try{
    const status:any=await f.broker.dispatch('status',{actor:{host:'claude',sessionId:'c',callId:'normal-restart'}});
    assert.ok(status.startupRoutes.some((route:any)=>route.memberId===f.x.id&&route.phase==='completion_pending'));
    assert.equal(status.startupRoutes.some((route:any)=>route.phase==='pending'),false);
    const saved:any=await f.broker.dispatch('send',{actor:{host:'claude',sessionId:'c',callId:'normal-start-send'},message:'正常重启期间排队'});
    await f.broker.pump();await f.broker.reconcile();
    assert.equal(f.store.message(saved.message_id).attempts,1);assert.equal(f.store.message(old.id).state,'contextObserved');
    assert.equal(f.broker.turnEnds.startupStatus().length,1);
    const parked=await f.broker.sealForHandoff({updateId:'completion-still-checking',signal:new AbortController().signal,commit:()=>{}});parked.close();
  }finally{resume.open();await start;}
});

test('补证连续失败在硬期限前保持排队，降级后仍取通知、检查静默并处理原生完成，稍后成功才解除完成门禁',async t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});
  const f=fixture(t),b=f.broker as unknown as {startupRoutes:StartupRoutes;runtimeHealth:BrokerHealthCoordinator;catchUpOwnedRoutes(ids:Iterable<string>):Promise<void>};
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'busy'});f.store.updatePeer(f.c.id,{state:'ready'});
  b.startupRoutes.close();b.startupRoutes=new StartupRoutes(()=>{},250);b.startupRoutes.begin([f.x.id]);
  let reconciles=0;
  const probe=t.mock.method(b.runtimeHealth,'probeTurnHealth',async()=>{throw Error('原生读取暂时失败');});
  const old=f.store.accept(f.pair,f.c,'尚待核对的旧回执','failed-probe-receipt');f.store.claim(old.id);f.store.recover();
  f.codex.reconcile=async()=>{reconciles++;return[];};
  const availability=t.mock.method(b.runtimeHealth,'deliveryAvailability',async()=>({state:'reachable' as const,reasons:[],checked_at:Date.now()}));
  f.broker.deliveryHealth.enqueue(f.pair.id,f.c.id,{id:'stored-notice',message:'已保存的独立提醒',state:'pending',wake:false});
  const actor={host:'claude' as const,sessionId:'c',callId:'degraded-notices'};
  await b.catchUpOwnedRoutes([f.x.id]);t.mock.timers.tick(5001);await b.catchUpOwnedRoutes([f.x.id]);
  assert.equal(reconciles,2,'健康探针失败不应跳过消息回执补证');assert.equal(b.startupRoutes.blocked(f.x.id),true);
  assert.deepEqual(await b.runtimeHealth.prepareNotices(actor,undefined,false),[]);
  await b.runtimeHealth.checkTurnHealth();await b.runtimeHealth.checkDeliveryHealth();
  assert.equal(probe.mock.callCount(),2);assert.equal(availability.mock.callCount(),0);
  await delay(300);assert.equal(b.startupRoutes.status()[0]?.phase,'degraded');
  assert.ok((await b.runtimeHealth.prepareNotices(actor,undefined,false)).some(notice=>notice.id==='stored-notice'));
  t.mock.timers.tick(30001);await b.runtimeHealth.checkTurnHealth();await b.runtimeHealth.checkDeliveryHealth();
  assert.equal(probe.mock.callCount(),3,'降级仍检查原生任务是否静默');assert.ok(availability.mock.callCount()>=3);
  assert.equal(b.startupRoutes.incomplete(f.x.id),true);
  f.store.updatePeer(f.x.id,{state:'needsHuman'});
  f.codex.emit('native',{address:f.store.endpoint(f.x.id)!.address,message:{method:'turn/completed',params:{threadId:f.x.sessionId,turn:{id:'after-ceiling',status:'completed'}}}});
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(f.store.peer(f.x.id).state,'ready','降级期间的原生业务回调仍应执行');
  probe.mock.restore();t.mock.timers.tick(5001);await b.catchUpOwnedRoutes([f.x.id]);
  assert.deepEqual(b.startupRoutes.status(),[]);
});

for(const died of [false,true])test(died?'交接后运行时已退出的路由仍允许普通恢复，新订阅出现后补证解除栅栏':'交接路由的订阅暂缺时允许普通恢复，订阅稍后出现时由周期消息泵重试补证',async t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});
  const f=fixture(t),b=f.broker as unknown as {startupRoutes:StartupRoutes;startupCatchups:Map<string,unknown>;memberLifecycle:BrokerLifecycleCoordinator};
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});
  if(died)f.store.saveEndpoint({...f.store.endpoint(f.x.id)!,native:{pid:2147483647,procStart:'died-during-downtime'}});
  f.codex.promoted=[f.x.id];
  let subscribed=false;
  t.mock.method(f.codex,'hasSubscription',()=>subscribed);
  await f.broker.start({offers:[],signal:new AbortController().signal,verify:async()=>false,resolve:async()=>{}});assert.equal(b.startupRoutes.blocked(f.x.id),true);
  const recover=t.mock.method(b.memberLifecycle as unknown as {recoverPairedPeer(id:string,pending:boolean):Promise<boolean>},'recoverPairedPeer',async()=>true);
  await b.memberLifecycle.schedulePairedRecovery(f.x.id,false);assert.ok(recover.mock.callCount()>0);
                                         
  await f.broker.pump();
  if(died)f.store.saveEndpoint({...f.store.endpoint(f.x.id)!,native:{pid:process.pid,procStart:'recovered-native-generation'}});
  subscribed=true;t.mock.timers.tick(5001);await f.broker.pump();
  for(let i=0;i<20&&b.startupCatchups.size;i++)await delay(10);
  assert.deepEqual(b.startupRoutes.status(),[]);
  assert.equal(f.codex.created.length,0);
});

test('未接上的旧路由沿用普通健康和原生回调，完成检查单独等待且不取得新的写入权限',async t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});
  const f=fixture(t),b=f.broker as unknown as {startupRoutes:StartupRoutes;runtimeHealth:BrokerHealthCoordinator};
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'busy'});f.store.updatePeer(f.c.id,{state:'ready'});
  f.store.saveEndpoint({...f.store.endpoint(f.x.id)!,runtimeId:undefined});
  t.mock.method(f.codex,'hasSubscription',()=>false);
  const endpoint=f.store.endpoint(f.x.id)!;
  f.store.setSetting('turnEndFact:'+f.x.id,{version:1,pairId:f.pair.id,sessionId:f.x.sessionId,scope:turnNativeScope(f.x,endpoint),state:'completed',turnId:'old-legacy',at:Date.now(),reason:'unverified'});
  const probe=t.mock.method(b.runtimeHealth,'probeTurnHealth',async()=>undefined);
  t.mock.method(b.runtimeHealth,'deliveryAvailability',async()=>({state:'reachable' as const,reasons:[],checked_at:Date.now()}));
  await f.broker.start();await f.broker.pump();
  assert.equal(b.startupRoutes.blocked(f.x.id),false);assert.equal(b.startupRoutes.incomplete(f.x.id),false);
  assert.equal(f.broker.turnEnds.startupStatus()[0]?.phase,'completion_pending');
  b.runtimeHealth.resetTurnCheck();await b.runtimeHealth.checkTurnHealth();assert.ok(probe.mock.callCount()>0);
  f.broker.deliveryHealth.enqueue(f.pair.id,f.c.id,{id:'legacy-independent',message:'独立健康提醒',state:'pending',wake:false});
  const notices=await b.runtimeHealth.prepareNotices({host:'claude',sessionId:'c',callId:'legacy-notices'},undefined,false);
  assert.ok(notices.some(notice=>notice.id==='legacy-independent'));
  f.store.updatePeer(f.x.id,{state:'needsHuman'});
  f.codex.emit('native',{address:f.store.endpoint(f.x.id)!.address,message:{method:'turn/completed',params:{threadId:f.x.sessionId,turn:{id:'legacy-end',status:'completed'}}}});
  await new Promise(resolve=>setImmediate(resolve));assert.equal(f.store.peer(f.x.id).state,'ready');
  assert.equal(f.codex.created.length,0);assert.equal(f.codex.released.length,0);assert.equal(f.codex.woken.length,0);assert.equal(f.codex.injected.length,0);
  assert.deepEqual(b.startupRoutes.status(),[]);
});

test('公开管道创建时屏蔽不兼容参数，创建后恢复且子进程仍看到用户原环境',{skip:process.platform!=='win32'},async t=>{
  const f=fixture(t),previous=process.env.NODE_PENDING_PIPE_INSTANCES;
  await ensureHome(f.home);
  let server:Awaited<ReturnType<typeof listenBroker>>|undefined;
  try{
    for(const value of [undefined,'32']){
      if(value===undefined)delete process.env.NODE_PENDING_PIPE_INSTANCES;else process.env.NODE_PENDING_PIPE_INSTANCES=value;
      server=await listenBroker(f.home,async()=>({ok:true}));
      assert.equal(process.env.NODE_PENDING_PIPE_INSTANCES,value);
      const result=await promisify(execFile)(process.execPath,['-p','JSON.stringify(process.env.NODE_PENDING_PIPE_INSTANCES??null)'],{encoding:'utf8',windowsHide:true});
      assert.equal(JSON.parse(result.stdout),value??null);
      await server.close();server=undefined;
    }
  }finally{await server?.close();if(previous===undefined)delete process.env.NODE_PENDING_PIPE_INSTANCES;else process.env.NODE_PENDING_PIPE_INSTANCES=previous;}
});

test('历史补证栅栏到达硬期限后只降级一次，迟到成功会撤下可见恢复状态',async()=>{
  const degraded:string[]=[],routes=new StartupRoutes(id=>{degraded.push(id);},20);
  try{
    routes.begin(['slow']);assert.equal(routes.blocked('slow'),true);
    await delay(35);assert.equal(routes.blocked('slow'),false);assert.equal(routes.status()[0]?.phase,'degraded');assert.deepEqual(degraded,['slow']);
    routes.fail('slow');assert.deepEqual(degraded,['slow']);routes.finish('slow');assert.deepEqual(routes.status(),[]);
  }finally{routes.close();}
});
