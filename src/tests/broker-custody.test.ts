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
}

function fixture(t:TestContext){
  const temp=temporaryDirectory(),home=temp.directory,file=path.join(home,'nxbridge.db'),store=new Store(file),codex=new HandoffAdapter(),claude=new FakeAdapter();
  const broker=new Broker(store,{...DEFAULT_CONFIG,implicit:'never'},{codex,claude} as unknown as Adapters,home);
  const c=store.register(registration('claude','c')),x=store.register(registration('codex','x')),pair=store.pair(c.id,x.id);
  t.after(async()=>{await broker.stop();store.close();await temp.cleanup();});
  const cx={host:'codex' as const,sessionId:'x',callId:'ask'};
  const question=()=>broker.dispatch('question.create',{actor:cx,questions:[{id:'one',header:'方向',question:'如何继续？',options:[{label:'甲',description:'说明甲'},{label:'乙',description:'说明乙'}]}]}) as Promise<{request_id:string;expires_at:string}>;
  return{file,broker,store,codex,claude,c,x,pair,cx,question};
}

const barrier=()=>{
  let open!:()=>void;
  const wait=new Promise<void>(resolve=>{open=resolve;});
  return {wait,open};
};

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

test('接班补证期间的新原生轮次使并行扫描证据过时，业务仍被拒绝',async t=>{
  const f=fixture(t);
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'ready'});
  const endpoint=f.store.endpoint(f.x.id)!;
  f.store.saveEndpoint({...endpoint,mode:'app-server',address:'隔离原生连接'});
  f.codex.promoted=[f.x.id];
  const raced=barrier(),resume=barrier(),scan=f.broker.turnEnds.scan.bind(f.broker.turnEnds);
  f.broker.turnEnds.scan=async(peer,current)=>{
    if(!current)return scan(peer);
    assert.equal(current(),true);raced.open();await resume.wait;assert.equal(current(),false);
  };
  const start=f.broker.start({offers:[],signal:new AbortController().signal,verify:async()=>false,resolve:async()=>{}});
  await raced.wait;
  try{
    await assert.rejects(f.broker.dispatch('ping',{}),{code:'handoff_retry'});
    f.codex.emit('native',{address:'隔离原生连接',message:{method:'turn/started',params:{threadId:'x',turn:{id:'newer'}}}});
    assert.equal(f.store.peer(f.x.id).state,'ready');
  }finally{resume.open();}
  await start;assert.equal((await f.broker.dispatch('ping',{}) as any).pid,process.pid);
});
