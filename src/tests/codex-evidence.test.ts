import test from 'node:test';
import assert from 'node:assert/strict';
import { RpcClient, clearRpcFaults, type RpcMessage } from '../ipc/rpc.js';
import { readCodexThread, readCodexItems } from '../hosts/codex/codex-evidence.js';
import {TurnHealthTracker} from '../broker/health/turn-health.js';
import { CodexNativeRpc } from '../hosts/codex/codex-native-rpc.js';
import { CodexOwnedRoutes, type OwnedCodexRoute } from '../hosts/codex/codex-owned-routes.js';
import { RuntimeRecoveryHold } from '../shared/errors.js';
import { CodexAdapter } from '../hosts/codex/adapter.js';
import { DEFAULT_CONFIG } from '../config/defaults.js';
import { temporary } from './support/temporary.js';

function mockRpc(handle:(message:RpcMessage)=>unknown,endpoint?:string){
  const requests:RpcMessage[]=[];const rpc=new RpcClient(message=>{requests.push(message);queueMicrotask(()=>{try{rpc.receive({id:message.id,result:handle(message)});}catch(error){rpc.receive({id:message.id,error:error as any});}});},()=>{},endpoint);
  return{rpc,requests};
}

test('创建回执在正式登记前只允许初始化，核验完成后才能提交工作',async t=>{
  const adapter=new CodexAdapter(DEFAULT_CONFIG,temporary(t)),routes=(adapter as any).ownedRoutes as CodexOwnedRoutes,calls:string[]=[];
  const route={peer:{id:'member',host:'codex',owner:'nxbridge',sessionId:'new'},endpoint:{peerId:'member',mode:'app-server',address:'test://created',epoch:'created',runtimeId:'runtime',pid:123,procStart:'native',active:true}} as OwnedCodexRoute;
  const runtime={id:'runtime',peerId:'member',endpoint:route.endpoint.address,pid:123,procStart:'native'};
  let selected:OwnedCodexRoute|undefined;
  routes.bind(()=>selected,()=>selected?.peer.sessionId??undefined);
  const rpc=new CodexNativeRpc(message=>{calls.push(message.method!);queueMicrotask(()=>rpc.receive({id:message.id,result:{ok:true}}));},()=>{},route.endpoint.address);
  t.after(()=>adapter.close());(adapter as any).clients.set(route.endpoint.address,rpc);
  (adapter as any).installThreadGuard(rpc,route.endpoint.address,(id:string,method:string)=>(adapter as any).observationPermit(route.endpoint.address,id,rpc,(adapter as any).ownedNativeScope(runtime),method));
  routes.created(route,rpc);
  assert.equal((adapter as any).ownedNativeScope(runtime).root,'new');
  await rpc.request('thread/settings/update',{threadId:'new'});await rpc.request('thread/name/set',{threadId:'new'});
  await assert.rejects(rpc.request('turn/start',{threadId:'new'}),{code:'owned_subscription_waiting'});
  selected={peer:{...route.peer},endpoint:{...route.endpoint,bindingEpoch:'registered'}};
  assert.equal(routes.adoptCreation(selected,rpc),false);assert.equal(routes.permits(selected,rpc),false);
  await assert.rejects(rpc.request('turn/start',{threadId:'new'}),{code:'owned_subscription_waiting'});
  routes.creationReady(route,rpc);assert.equal(routes.adoptCreation(selected,rpc),true);
  await rpc.request('turn/start',{threadId:'new'});
  assert.deepEqual(calls,['thread/settings/update','thread/name/set','turn/start']);
});

test('创建回执不转移给其他连接、成员或原生代次，登记后也不能再次提升旧回执',t=>{
  const routes=new CodexOwnedRoutes(),rpc=new RpcClient(()=>{},()=>{}),other=new RpcClient(()=>{},()=>{});t.after(()=>{rpc.close();other.close();});
  const route={peer:{id:'member',host:'codex',owner:'nxbridge',sessionId:'new'},endpoint:{peerId:'member',mode:'app-server',address:'test://created',epoch:'created',runtimeId:'runtime',pid:123,procStart:'native',active:true}} as OwnedCodexRoute;
  const runtime={id:'runtime',peerId:'member',pid:123,procStart:'native'};
  routes.created(route,rpc);
  assert.equal(routes.nativeRoot(route.endpoint.address,other,runtime),undefined);
  assert.equal(routes.nativeRoot(route.endpoint.address,rpc,{...runtime,peerId:'other'}),undefined);
  assert.equal(routes.nativeRoot(route.endpoint.address,rpc,{...runtime,procStart:'replaced'}),undefined);
  assert.equal(routes.creationIdentity(route.endpoint.address,'other',rpc,runtime),undefined);
  routes.creationReady(route,rpc);
  assert.equal(routes.adoptCreation(route,other),false);
  assert.equal(routes.adoptCreation({peer:{...route.peer,id:'other'},endpoint:{...route.endpoint,peerId:'other'}},rpc),false);
  assert.equal(routes.adoptCreation({peer:route.peer,endpoint:{...route.endpoint,procStart:'replaced'}},rpc),false);
  assert.equal(routes.adoptCreation(route,rpc),true);
  assert.equal(routes.adoptCreation({peer:route.peer,endpoint:{...route.endpoint,bindingEpoch:'later'}},rpc),false);
  routes.revoke(route.endpoint.address,'new',rpc);assert.equal(routes.permits(route,rpc),false);
});

test('外部后台的创建回执只授予当前连接的线程许可，不授予进程代次权限',t=>{
  const routes=new CodexOwnedRoutes(),rpc=new RpcClient(()=>{},()=>{}),other=new RpcClient(()=>{},()=>{});t.after(()=>{rpc.close();other.close();});
  const route={peer:{id:'member',host:'codex',owner:'nxbridge',sessionId:'external'},endpoint:{peerId:'member',mode:'app-server',address:'test://external',epoch:'external',active:true,pid:null,procStart:null}} as OwnedCodexRoute;
  routes.creationReady(route,rpc);
  assert.equal(routes.adoptCreation({...route,endpoint:{...route.endpoint,bindingEpoch:'registered'}},other),false);
  const registered={...route,endpoint:{...route.endpoint,bindingEpoch:'registered'}};
  assert.equal(routes.adoptCreation(registered,rpc),true);assert.equal(routes.permits(registered,rpc),true);
  assert.equal(routes.permits(registered,other),false);
  assert.equal(routes.nativeHolds(route.endpoint.address,'external',rpc,{id:'fabricated',pid:123,procStart:'native'}),false);
  rpc.close();assert.equal(routes.permits(registered,rpc),false);
});

function delayedNative(){
  const sent:RpcMessage[]=[];let ready=false;
  const rpc=new CodexNativeRpc(message=>{
    sent.push(message);
    const result=message.method==='thread/loaded/list'?{data:['root']}:message.method==='thread/turns/list'?{data:[]}:
      ready?{thread:{id:'root'}}:undefined;
    if(result)queueMicrotask(()=>rpc.receive({id:message.id,result}));
  },()=>{},'test://passive-native-reply');
  return{rpc,sent,ready:()=>{ready=true;}};
}

test('被动迟到回执只保留有限旧请求，丢弃与断线都不能调用旧成功回调',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});
  const sent:RpcMessage[]=[],observed:number[]=[],rpc=new RpcClient(message=>sent.push(message),()=>{});
  t.after(()=>rpc.close());
  for(let index=0;index<65;index++){
    const failed=assert.rejects(rpc.request('read',{},{timeout:1,onLateSuccess:()=>observed.push(index)}),{code:'timeout'});
    t.mock.timers.tick(1);await failed;
  }
  rpc.receive({id:sent[0]!.id,result:{}});assert.deepEqual(observed,[]);
  rpc.receive({id:sent[64]!.id,result:{}});assert.deepEqual(observed,[64]);
  rpc.close();rpc.receive({id:sent[63]!.id,result:{}});assert.deepEqual(observed,[64]);
});

test('慢读取的原请求迟到成功仅解除自己的等待，不新增队列探针',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});
  const f=delayedNative(),permit={};t.after(()=>f.rpc.close());f.rpc.threadGuard=()=>permit;
  const original=f.rpc.request('thread/read',{threadId:'root'},{timeout:750});
  const failed=assert.rejects(original,{code:'native_thread_unconfirmed'});
  t.mock.timers.tick(750);await failed;await new Promise<void>(resolve=>setImmediate(resolve));
  await assert.rejects(f.rpc.request('thread/read',{threadId:'root'}),{code:'native_thread_wedged'});
  const before=f.sent.length;
  f.rpc.receive({id:f.sent[0]!.id,result:{thread:{id:'root'}}});
  assert.equal(f.sent.length,before,'迟到回执不能主动查询原线程');
  await assert.rejects(original,{code:'native_thread_unconfirmed'},'原请求的超时结果不被改写');
  f.ready();assert.equal((await f.rpc.request('thread/read',{threadId:'root'})).thread.id,'root');
});

test('并发超时逐项核对迟到成功，写入仍为未知且不重发',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});
  const f=delayedNative(),permit={};t.after(()=>f.rpc.close());f.rpc.threadGuard=()=>permit;
  const read=f.rpc.request('thread/read',{threadId:'root'},{timeout:750}),write=f.rpc.request('turn/start',{threadId:'root'},{timeout:750,mutation:true});
  const failures=Promise.all([assert.rejects(read,{code:'native_thread_unconfirmed'}),assert.rejects(write,{code:'timeout',uncertain:true})]);
  t.mock.timers.tick(750);await failures;await new Promise<void>(resolve=>setImmediate(resolve));
  const before=f.sent.length;
  f.rpc.receive({id:f.sent[0]!.id,result:{thread:{id:'root'}}});
  await assert.rejects(f.rpc.request('thread/read',{threadId:'root'}),{code:'native_thread_wedged'});
  f.rpc.receive({id:f.sent[1]!.id,result:{turn:{id:'original-turn'}}});
  assert.equal(f.sent.length,before);
  await assert.rejects(write,{code:'timeout',uncertain:true},'原写入结果仍需既有回执核对');
  f.ready();assert.equal((await f.rpc.request('thread/read',{threadId:'root'})).thread.id,'root');
  assert.equal(f.sent.filter(message=>message.method==='turn/start').length,1);
});

for(const changed of ['permit','connection'])test('原请求迟到成功不解除后来代次的等待：'+changed,async t=>{
  t.mock.timers.enable({apis:['setTimeout']});
  const f=delayedNative();t.after(()=>f.rpc.close());let permit={};f.rpc.threadGuard=()=>permit;
  const first=assert.rejects(f.rpc.request('thread/read',{threadId:'root'},{timeout:750}),{code:'native_thread_unconfirmed'});
  t.mock.timers.tick(750);await first;await new Promise<void>(resolve=>setImmediate(resolve));
  let current=f;
  if(changed==='connection'){
    f.rpc.close();current=delayedNative();t.after(()=>current.rpc.close());current.rpc.threadGuard=()=>permit;
    const next=assert.rejects(current.rpc.request('thread/read',{threadId:'root'},{timeout:750}),{code:'native_thread_unconfirmed'});
    t.mock.timers.tick(750);await next;await new Promise<void>(resolve=>setImmediate(resolve));
  }else permit={};
  const before=current.sent.length;
  f.rpc.receive({id:f.sent[0]!.id,result:{thread:{id:'root'}}});
  await assert.rejects(current.rpc.request('thread/read',{threadId:'root'}),{code:'native_thread_wedged'});
  assert.equal(current.sent.length,before);
});

test('同线程控制超时后保留写入不确定性，重复核验只用独立接口直到确认卸载',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});
  const sent:RpcMessage[]=[];let now=0,wedged=true,loaded=true;
  const rpc=new CodexNativeRpc(message=>{
    sent.push(message);
    const reply=message.method==='thread/loaded/list'?{data:loaded?['root','other']:['other']}:message.method==='thread/turns/list'?{data:[]}:!wedged||message.params?.threadId==='other'?{thread:{id:message.params.threadId}}:undefined;
    if(reply)queueMicrotask(()=>rpc.receive({id:message.id,result:reply}));
  },()=>{},'test://owned-native-queue',()=>now);
  t.after(()=>rpc.close());
  const original=assert.rejects(rpc.request('turn/start',{threadId:'root'},{timeout:10,mutation:true}),{code:'timeout',uncertain:true});
  t.mock.timers.tick(10);await original;
  await new Promise<void>(resolve=>setImmediate(resolve));
  await assert.rejects(rpc.request('thread/queue/add',{threadId:'root'},{mutation:true,beforeSend:()=>assert.fail('本地门禁不能标记已提交')}),{code:'native_thread_wedged',uncertain:false,phase:'prepare'});
  assert.equal(sent.filter(message=>message.params?.threadId==='root'&&message.method!=='thread/turns/list').length,1);
  assert.equal((await rpc.request('thread/read',{threadId:'other'})).thread.id,'other');
  now+=30001;wedged=false;
  rpc.threadGuard=(_method,id)=>{if(id==='root')throw new RuntimeRecoveryHold('lease_lost','原生订阅已失效。');};
  await assert.rejects(rpc.request('turn/start',{threadId:'root'},{mutation:true}),{code:'lease_lost',phase:'prepare'});
  assert.equal(sent.filter(message=>message.method==='thread/read'&&message.params?.threadId==='root').length,0);
  rpc.threadGuard=undefined;
  for(let attempt=0;attempt<4;attempt++){
    now+=30001;
    await assert.rejects(rpc.request('turn/start',{threadId:'root'},{mutation:true}),{code:'native_thread_wedged',uncertain:false});
  }
  assert.equal(sent.filter(message=>message.method==='thread/read'&&message.params?.threadId==='root').length,0);
  assert.equal(sent.filter(message=>message.method==='turn/start').length,1);
  let absent=0;rpc.on('threadAbsent',id=>{assert.equal(id,'root');absent++;});
  loaded=false;now+=30001;
  await assert.rejects(rpc.request('turn/start',{threadId:'root'},{mutation:true}),{code:'native_thread_wedged',uncertain:false});
  assert.equal(absent,1);assert.equal(sent.filter(message=>message.method==='turn/start').length,1);
  assert.equal((await rpc.request('thread/read',{threadId:'root'})).thread.id,'root');
});

for(const method of ['thread/unloaded','thread/closed','thread/archived','thread/status/changed'])test('短超时探测停止追加线程请求，'+method+' 解除等待后仍核验新订阅',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});let allow=false;
  const sent:RpcMessage[]=[];
  const rpc=new CodexNativeRpc(message=>{
    sent.push(message);
    if(allow||message.method==='thread/loaded/list'||message.method==='thread/turns/list')queueMicrotask(()=>rpc.receive({id:message.id,result:{data:['root'],thread:{id:'root'}}}));
  },()=>{},'test://native-unload-reset');
  t.after(()=>rpc.close());
  const timedOut=assert.rejects(rpc.request('thread/read',{threadId:'root'},{timeout:750}),{code:'native_thread_unconfirmed'});
  t.mock.timers.tick(750);await timedOut;
  await new Promise<void>(resolve=>setImmediate(resolve));
  for(let attempt=0;attempt<3;attempt++)await assert.rejects(rpc.request('thread/read',{threadId:'root'},{timeout:750}),{code:'native_thread_wedged'});
  assert.equal(sent.filter(message=>message.method==='thread/read').length,1);
  allow=true;
  rpc.receive({method,params:{threadId:'root',status:{type:'notLoaded'}}});
  rpc.threadGuard=()=>{throw new RuntimeRecoveryHold('owned_subscription_waiting','没有新的订阅回执。');};
  await assert.rejects(rpc.request('thread/resume',{threadId:'root'}),{code:'owned_subscription_waiting'});
  rpc.threadGuard=undefined;
  assert.equal((await rpc.request('thread/read',{threadId:'root'})).thread.id,'root');
});

test('旧连接和旧路由的迟到回执不能撤销或签发当前路由的许可',t=>{
  const routes=new CodexOwnedRoutes(),old=new RpcClient(()=>{},()=>{}),fresh=new RpcClient(()=>{},()=>{});
  t.after(()=>{old.close();fresh.close();});
  const route:OwnedCodexRoute={peer:{id:'p',host:'codex',owner:'nxbridge',sessionId:'root',kind:'root',name:'root',cwd:'/cwd',parentId:null,permission:'unknown',state:'ready',updatedAt:0},
    endpoint:{peerId:'p',address:'test://route',epoch:'1',runtimeId:'runtime',mode:'app-server',pid:12,procStart:'first',active:true,seenAt:0}};
  let current=route;
  routes.bind(()=>current);
  routes.subscribed(route,old);
  const identity=routes.leaseIdentity(route,old);
  assert.equal(routes.permits(route,old),true);
  current={...route,endpoint:{...route.endpoint,epoch:'2',procStart:'second'}};
  routes.subscribed(current,fresh);
  assert.notEqual(routes.leaseIdentity(current,fresh),identity);
  routes.subscribed(route,old);
  routes.revoke(route.endpoint.address,'root',old);
  assert.equal(routes.permits(route,old),false);
  assert.equal(routes.permits(current,fresh),true);
  const oldOwner=current;
  current={...current,peer:{...current.peer,owner:'user'}};
  assert.equal(routes.permits(current,fresh),false);
  assert.equal(routes.permits(oldOwner,fresh),false);
});

for(const state of ['idle','active'])test('尚未物化的 '+state+' 线程保留元数据，首次输入后重新读取轮次且不触发协议熔断',async t=>{
  let materialized=false;const endpoint='test://not-materialized/'+state;
  const {rpc,requests}=mockRpc(({method,params})=>{
    if(method==='thread/read')return{thread:{id:'fresh',status:{type:state}}};
    assert.equal(method,'thread/turns/list');assert.equal(params.threadId,'fresh');
    if(!materialized)throw{code:-32600,message:'thread fresh is not materialized yet; thread/turns/list is unavailable before first user message'};
    return{data:[{id:'first-real-turn',status:'completed'}]};
  },endpoint);t.after(()=>{rpc.close();clearRpcFaults(endpoint);});
  const initial=await readCodexThread(rpc,'fresh',{progress:true});assert.equal(initial.status?.type,state);assert.deepEqual(initial.turns,[]);assert.equal(initial.progressEvidence,state==='active'?false:undefined);
  materialized=true;assert.equal((await readCodexThread(rpc,'fresh')).turns?.at(-1)?.id,'first-real-turn');assert.equal(requests.filter(value=>value.method==='thread/turns/list').length,2);assert.equal(rpc.metrics.rejected,0);
});

test('其他线程的未物化错误及普通无效请求不被当成本线程的空历史',async t=>{
  for(const message of ['thread other is not materialized yet','invalid request']){
    const {rpc}=mockRpc(({method})=>{if(method==='thread/read')return{thread:{id:'target',status:{type:'idle'}}};throw{code:-32600,message};});t.after(()=>rpc.close());
    await assert.rejects(readCodexThread(rpc,'target'),{code:'-32600',message});
  }
});

test('原生运行状态和参数错误不熔断后续读取，只有缺少方法会短暂停止同类请求',async t=>{
  for(const code of [-32600,-32602,-32601]){
    const endpoint='test://rpc-error/'+code;let failed=true;
    const {rpc,requests}=mockRpc(()=>{if(failed)throw{code,message:code===-32600?'thread not loaded':'native error'};return{ready:true};},endpoint);
    t.after(()=>{rpc.close();clearRpcFaults(endpoint);});
    await assert.rejects(rpc.request('thread/read',{threadId:'target'}),{code:String(code)});failed=false;
    if(code===-32601){await assert.rejects(rpc.request('thread/read',{threadId:'target'}),{code:String(code)});assert.equal(requests.length,1);}
    else{assert.deepEqual(await rpc.request('thread/read',{threadId:'target'}),{ready:true});assert.equal(requests.length,2);}
  }
});

test('长轮次概要不变时只读取最新一项，持续的新工具进展不误报停滞，入站条目不冒充进展',async t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});let item:any={id:'0',type:'agentMessage',text:'实际进展'};
  const {rpc,requests}=mockRpc(({method,params})=>{
    if(method==='thread/read'){assert.equal(params.includeTurns,false);return{thread:{id:'t',status:{type:'active'}}};}
    if(method==='thread/turns/list'){assert.equal(params.limit,1);assert.equal(params.itemsView,'summary');return{data:[{id:'turn',status:'inProgress',items:[]}]};}
    assert.equal(method,'thread/items/list');assert.equal(params.limit,1);assert.equal(params.turnId,'turn');assert.equal(params.sortDirection,'desc');return{data:[{turnId:'turn',item}]};
  });t.after(()=>rpc.close());
  const health=new TurnHealthTracker(),read=async()=>health.read('endpoint',await readCodexThread(rpc,'t',{progress:true}));await read();
  for(let i=1;i<=20;i++){t.mock.timers.tick(60000);item={id:String(i),type:'mcpToolCall',status:'completed'};assert.equal((await read())?.state,'running');}
  const progress=health.get('endpoint','t')!.lastProgressAt;t.mock.timers.tick(60000);item={id:'incoming',type:'functionCallOutput',namespace:'claude',name:'receive_messages'};await read();assert.equal(health.get('endpoint','t')!.lastProgressAt,progress);
  assert.equal(requests.filter(request=>request.method==='thread/items/list').length,22);
});

test('最新条目读取不支持时明确保留进展未知，不重试该方法或回退全历史',async t=>{
  const {rpc,requests}=mockRpc(({method})=>{
    if(method==='thread/read')return{thread:{id:'t',status:{type:'active'}}};
    if(method==='thread/turns/list')return{data:[{id:'turn',status:'inProgress',items:[]}]};
    throw{code:-32601,message:'method not found'};
  });t.after(()=>rpc.close());
  assert.equal((await readCodexThread(rpc,'t',{progress:true})).progressEvidence,false);
  assert.equal((await readCodexThread(rpc,'t',{progress:true})).progressEvidence,false);
  assert.equal(requests.filter(request=>request.method==='thread/items/list').length,1);
  assert.ok(requests.filter(request=>request.method==='thread/read').every(request=>request.params.includeTurns===false));
});

test('轮次概要单次共享，不拉取全量；线程不支持不污染其他线程或条目方法',async t=>{
  const {rpc,requests}=mockRpc(({method,params})=>{if(method==='thread/read'){assert.equal(params.includeTurns,false);return{thread:{id:params.threadId}};}if(method==='thread/turns/list'){assert.equal(params.itemsView,'summary');if(params.threadId==='old')throw{code:-32000,message:'list_turns is not supported yet'};return{data:[{id:'new'},{id:'old'}]};}return{data:[]};});t.after(()=>rpc.close());
  await Promise.all([readCodexThread(rpc,'old'),readCodexThread(rpc,'old')]);assert.equal(requests.filter(v=>v.method==='thread/read').length,1);
  await readCodexThread(rpc,'old');assert.equal(requests.filter(v=>v.method==='thread/turns/list').length,1);
  assert.deepEqual((await readCodexThread(rpc,'new',{turnId:'old'})).turns?.map(v=>v.id),['old','new']);
  assert.equal((await readCodexItems(rpc,'old')).complete,true);
});

test('条目从最新页读取；超预算不推进边界或声称缺席，后续新增仍可读',async t=>{
  let ids=['0'];const {rpc}=mockRpc(({params})=>{const start=Number(params.cursor??0),data=ids.slice(start,start+16).map(id=>({item:{id}}));return{data,nextCursor:start+16<ids.length?String(start+16):null};});t.after(()=>rpc.close());
  assert.equal((await readCodexItems(rpc,'thread')).complete,true);
  ids=Array.from({length:40},(_,i)=>String(40-i)).concat('0');let result=await readCodexItems(rpc,'thread');assert.equal(result.complete,false);assert.equal(result.items.length,32);
  ids.unshift('41');result=await readCodexItems(rpc,'thread');assert.equal(result.items[0].item.id,'41');assert.equal(result.complete,false);
  ids=['42','0'];result=await readCodexItems(rpc,'thread');assert.equal(result.complete,true);assert.deepEqual(result.items.map(v=>v.item.id),['42']);
});

test('RPC 大响应只隔离所属读取；套接字断开不连带熔断，写超时仍保持不确定',async t=>{
  t.mock.timers.enable({apis:['Date'],now:1000000});const endpoint='test://rpc-budget';clearRpcFaults(endpoint);const sent:RpcMessage[]=[];
  const rpc=new RpcClient(m=>sent.push(m),()=>{},endpoint);t.after(()=>rpc.close());
  const large=rpc.request('thread/items/list',{threadId:'big'},{maxBytes:8});rpc.receive({id:sent.at(-1)!.id,result:{}},{bytes:9,parseMs:1});await assert.rejects(large,{code:'response_too_large'});
  await assert.rejects(rpc.request('thread/items/list',{threadId:'big'}),{code:'response_too_large'});
  const other=rpc.request('thread/items/list',{threadId:'small'});rpc.receive({id:sent.at(-1)!.id,result:{ok:true}},{bytes:7,parseMs:1});assert.deepEqual(await other,{ok:true});
  const read=rpc.request('thread/read',{threadId:'small'}),write=rpc.request('thread/inject_items',{threadId:'small'},{mutation:true});rpc.disconnected('关闭1009','payload_too_large');
  await assert.rejects(read,{code:'payload_too_large',uncertain:false});await assert.rejects(write,{code:'payload_too_large',uncertain:true});
  const next=new RpcClient(m=>queueMicrotask(()=>next.receive({id:m.id,result:true})),()=>{},endpoint);t.after(()=>next.close());assert.equal(await next.request('thread/read',{threadId:'small'}),true);
  t.mock.timers.tick(900000);assert.equal(await next.request('thread/items/list',{threadId:'big'}),true);
});
