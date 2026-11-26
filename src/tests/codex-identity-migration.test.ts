import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import {fixture, lifecycle} from './support/broker-fixture.js';
import { registration } from './support/registration.js';
import { temporary } from './support/temporary.js';
import { FakeAdapter } from './support/fake-adapter.js';
import {BridgeError} from '../shared/errors.js';
import {type Endpoint,type Peer} from '../types.js';
import { runtimeFor, saveManagedRuntime } from '../hosts/managed-runtimes.js';
import { RpcClient } from '../ipc/rpc.js';
import { managedMarker } from '../install/installation.js';
import { processIdentity } from '../process/process.js';
import { once } from 'node:events';
import { WebSocketServer } from 'ws';
import { CodexAdapter, connectAppServer } from '../hosts/codex/adapter.js';
import { Broker, type Adapters } from '../broker/coordinator.js';
import { Store } from '../storage/store.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import { formatGlobalStatus, formatStatus } from '../integration/mcp/presentation.js';
import path from 'node:path';
import { appendFileSync, writeFileSync } from 'node:fs';

test('冷恢复的关闭证据来自实际 WebSocket 结束事件',async t=>{
  const home=temporary(t),adapter=new CodexAdapter(DEFAULT_CONFIG,home),server=new WebSocketServer({host:'127.0.0.1',port:0});
  t.after(async()=>{adapter.close();for(const socket of server.clients)socket.terminate();await new Promise<void>(resolve=>server.close(()=>resolve()));});
  await once(server,'listening');
  server.on('connection',socket=>socket.on('message',payload=>{
    const request=JSON.parse(payload.toString());
    if(request.method==='initialize')socket.send(JSON.stringify({id:request.id,result:{userAgent:'codex-tui/0.166.0 (fixture)'}}));
  }));
  const endpoint='ws://127.0.0.1:'+(server.address() as {port:number}).port;
  const rpc=await connectAppServer(endpoint,DEFAULT_CONFIG,undefined,home,{readOnly:true});
  const websocket=[...server.clients][0]!,remoteClosed=once(websocket,'close');
                           
  rpc.disconnected();assert.equal(rpc.closed,true);assert.equal(websocket.readyState,websocket.OPEN);
  assert.equal(await (adapter as any).closeColdResumeTransport(rpc),true);
  await remoteClosed;
});

for(const seal of [false,true])test('原生连接迟关闭时'+(seal?'停止协调器后不写证据':'仍记录实际关闭证据'),async t=>{
  const home=temporary(t),adapter=new CodexAdapter(DEFAULT_CONFIG,home),server=new WebSocketServer({host:'127.0.0.1',port:0});
  t.after(async()=>{adapter.close();for(const socket of server.clients)socket.terminate();await new Promise<void>(resolve=>server.close(()=>resolve()));});
  await once(server,'listening');
  server.on('connection',socket=>socket.on('message',payload=>{
    const request=JSON.parse(payload.toString());
    if(request.method==='initialize')socket.send(JSON.stringify({id:request.id,result:{userAgent:'codex-tui/0.166.0 (fixture)'}}));
  }));
  const endpoint='ws://127.0.0.1:'+(server.address() as {port:number}).port;
  const rpc=await connectAppServer(endpoint,DEFAULT_CONFIG,undefined,home,{readOnly:true}),websocket=[...server.clients][0]!;
  websocket.pause();const remoteClosed=once(websocket,'close');
  const setTimer=setTimeout,wait=t.mock.method(globalThis,'setTimeout',(callback:()=>void,ms?:number)=>setTimer(callback,ms===1500?0:ms));
  let evidence=false;let observed!:()=>void;const observation=new Promise<void>(resolve=>observed=resolve);
  try{assert.equal(await (adapter as any).closeColdResumeTransport(rpc,()=>{evidence=true;observed();}),false);}
  finally{wait.mock.restore();}
  assert.equal(evidence,false);if(seal)adapter.close();
  websocket.resume();await remoteClosed;
  if(seal)assert.equal(await (adapter as any).closeColdResumeTransport(rpc),true);else await observation;
  assert.equal(evidence,!seal);
});

test('冷恢复只从已退出的协调器代次或正常启动兼容的旧记录取得重试许可',async t=>{
  const home=temporary(t),owner=await processIdentity(process.pid),id='original',runtime={id:randomUUID(),peerId:'member',sessionId:id,owner:managedMarker(home),kind:'owned' as const,state:'ready' as const,endpoint:'unix://original',pid:1,procStart:'native-1',createdAt:1};
  const marker={sessionId:id,pid:runtime.pid,procStart:runtime.procStart};
  const ordinary=new CodexAdapter(DEFAULT_CONFIG,home),sameProcess=new CodexAdapter(DEFAULT_CONFIG,home),started=new CodexAdapter(DEFAULT_CONFIG,home,owner);
  t.after(()=>{ordinary.close();sameProcess.close();started.close();});
  const read=()=>runtimeFor(home,{id:runtime.id})!;
  const plan=(adapter:CodexAdapter)=>(adapter as any).prepareColdResume(read(),id);
  saveManagedRuntime(home,{...runtime,coldResumeUnconfirmed:{...marker,attempts:1,broker:{pid:owner.pid,startedAt:owner.startedAt},connectionId:'unchanged-owner'}});
  await assert.rejects(plan(ordinary),{code:'owned_subscription_waiting'});
  await assert.rejects(plan(sameProcess),{code:'owned_subscription_waiting'});
  await assert.rejects(plan(started),{code:'owned_subscription_waiting'});
  saveManagedRuntime(home,{...runtime,coldResumeUnconfirmed:{...marker,attempts:1,broker:{pid:owner.pid,startedAt:'prior-generation'},connectionId:'exited-owner'}});
  assert.equal((await plan(started)).attempt,2);assert.equal(read().value.coldResumeUnconfirmed?.transportClosed,true);
  saveManagedRuntime(home,{...runtime,coldResumeUnconfirmed:marker});
  await assert.rejects(plan(ordinary),{code:'owned_subscription_waiting'});
  assert.equal((await plan(started)).attempt,2);assert.equal(read().value.coldResumeUnconfirmed?.transportClosed,true);
  saveManagedRuntime(home,{...runtime,coldResumeUnconfirmed:{...marker,attempts:2,transportClosed:true}});
  await assert.rejects(plan(started),{code:'owned_subscription_waiting'});
});

for(const outcome of ['无关闭证据','重试成功','两次失败'] as const)test('托管迁移冷恢复按代次保留关闭证据与尝试次数：'+outcome,async t=>{
  const home=temporary(t),store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home);
  t.after(()=>{adapter.close();store.close();});
  const original=store.register(registration('codex','migration-root'));store.updatePeer(original.id,{owner:'nxbridge'});
  const peer=store.peer(original.id),endpoint=store.endpoint(peer.id)!,id=peer.sessionId!,file=path.join(home,'migration.jsonl');
  endpoint.nativeSession={transcriptPath:file};
  const settings={model:'fixture',reasoning_effort:'high',service_tier:'default',approval_policy:'never',approvals_reviewer:'user',permission_profile:{type:'disabled'}};
  writeFileSync(file,[{type:'session_meta',payload:{id}},{type:'turn_context',payload:settings}].map(row=>JSON.stringify(row)+'\n').join(''),'utf8');
  const runtime={id:randomUUID(),peerId:peer.id,sessionId:id,owner:managedMarker(home),kind:'owned' as const,state:'ready' as const,endpoint:endpoint.address,pid:1,procStart:'native-1',createdAt:1};
  saveManagedRuntime(home,runtime);
  const client=(state:'timeout'|'loaded'|'success')=>{
    const sent:string[]=[];let wireOpen=true,lateReply!:()=>boolean;
    const rpc=new RpcClient(message=>{sent.push(message.method!);queueMicrotask(()=>{
      if(message.method==='thread/loaded/list')rpc.receive({id:message.id,result:{data:state==='loaded'?[id]:[]}});
      else if(message.method==='thread/resume'){
        assert.equal(message.params.threadId,id);assert.equal(message.params.model,settings.model);assert.equal(message.params.serviceTier,'default');
        const reply=()=>{const subscribed=wireOpen;rpc.receive({id:message.id,result:{thread:{id,path:file,status:{type:'idle'}},model:settings.model,reasoningEffort:'high',serviceTier:'default',approvalPolicy:'never',approvalsReviewer:'user'}});return subscribed;};
        if(state==='success')reply();else lateReply=reply;
      }else if(message.method==='thread/settings/update'){
        appendFileSync(file,JSON.stringify({type:'event_msg',payload:{type:'thread_settings_applied',thread_id:id,thread_settings:settings}})+'\n','utf8');
        rpc.receive({id:message.id,result:{}});
      }else assert.fail('恢复核验不应发送输入：'+message.method);
    });},()=>{wireOpen=false;});
    t.after(()=>rpc.close());
    const request=rpc.request.bind(rpc);
    t.mock.method(rpc,'request',(method:string,params?:unknown,options?:Parameters<typeof rpc.request>[2])=>request(method,params,method==='thread/resume'?{...options,timeout:20}:options));
    return{rpc,sent,late:()=>lateReply()};
  };
  const acquire=(host:CodexAdapter,rpc:RpcClient)=>(host as any).acquireMigrationRoot(rpc,runtime,peer,endpoint,()=>{});
  const proof=(host:CodexAdapter)=>t.mock.method(host as any,'closeColdResumeTransport',async(rpc:RpcClient,onClosed:()=>void)=>{rpc.close();onClosed();return true;});
  if(outcome!=='无关闭证据')proof(adapter);
  const first=client('timeout');
  await assert.rejects(acquire(adapter,first.rpc),{code:'timeout'});
  assert.equal(first.rpc.closed,true);assert.equal(first.late(),false);
  assert.deepEqual(first.sent,['thread/loaded/list','thread/resume']);
  const pending=()=>runtimeFor(home,{id:runtime.id})!.value.coldResumeUnconfirmed;
  assert.equal(pending()?.attempts,1);assert.equal(pending()?.transportClosed,outcome==='无关闭证据'?undefined:true);
  assert.equal(pending()?.broker?.pid,process.pid);assert.ok(pending()?.broker?.startedAt);assert.ok(pending()?.connectionId);
  const restarted=new CodexAdapter(DEFAULT_CONFIG,home);t.after(()=>restarted.close());
  if(outcome==='无关闭证据'){
    const fresh=client('timeout');await assert.rejects(acquire(restarted,fresh.rpc),{code:'owned_subscription_waiting'});assert.deepEqual(fresh.sent,[]);return;
  }
  proof(restarted);
  const loaded=client('loaded');await assert.rejects(acquire(restarted,loaded.rpc),{code:'owned_subscription_waiting'});
  assert.deepEqual(loaded.sent,['thread/loaded/list']);assert.equal(pending()?.attempts,1);
  const second=client(outcome==='重试成功'?'success':'timeout');
  if(outcome==='重试成功'){
    await acquire(restarted,second.rpc);
    assert.deepEqual(second.sent,['thread/loaded/list','thread/resume','thread/settings/update']);assert.equal(pending(),undefined);
  }else{
    await assert.rejects(acquire(restarted,second.rpc),{code:'owned_subscription_waiting'});
    assert.deepEqual(second.sent,['thread/loaded/list','thread/resume']);assert.equal(pending()?.attempts,2);assert.equal(second.late(),false);
    const fresh=client('timeout');await assert.rejects(acquire(adapter,fresh.rpc),{code:'owned_subscription_waiting'});assert.deepEqual(fresh.sent,[]);
  }
});

for(const state of ['runtimeQueued','unknown','sending','held','partial'] as const)test('身份迁移只凭完整原生缺席证据保留未知，不重投旧消息：'+state,async t=>{
  const f=fixture(t),home=f.broker.home,pair=f.store.pair(f.c.id,f.x.id),endpoint=f.store.endpoint(f.x.id)!,history=path.join(home,'history.jsonl');
  f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  saveManagedRuntime(home,{id:randomUUID(),peerId:f.x.id,owner:managedMarker(home),kind:'owned',state:'ready',endpoint:endpoint.address,pid:process.pid,procStart:(await processIdentity(process.pid)).startedAt,createdAt:Date.now()});
  writeFileSync(history,JSON.stringify({type:'session_meta',payload:{id:'x'}})+'\n'+(state==='partial'?'{':''),'utf8');
  const stranded=f.store.accept(pair,f.c,'旧任务已由另一请求处理，不能重发','stranded');f.store.claim(stranded.id);if(state!=='sending')f.store.transition(stranded.id,state==='partial'?'runtimeQueued':state);
  const fresh=f.store.accept(pair,f.c,'新的未提交消息只投递一次','fresh');let migrated=false;
  f.codex.attachFailure=new BridgeError('codex_identity_conflict','旧身份');f.codex.connect=async()=>{if(!migrated)throw new BridgeError('codex_identity_conflict','旧身份');return{request:async()=>({})};};
  (f.codex as any).restart=async(peer:Peer,old:Endpoint,guard:()=>void,options:any)=>{
    await options.verifyDelivery({id:'x',path:history,turns:[]});guard();migrated=true;f.codex.attachFailure=undefined;
    f.store.adopt(peer.id,registration('codex','x',{state:'ready',endpoint:{...old,address:'unix://migrated',epoch:'migrated'}}));return{oldPid:1,pid:2,resumed:{}};
  };
  await f.broker.pump();await f.broker.pump();
  const allowed=state==='unknown'||state==='runtimeQueued';assert.equal(migrated,allowed);assert.equal(f.store.message(stranded.id).state,allowed?'unknown':state==='partial'?'runtimeQueued':state);assert.equal(f.store.message(stranded.id).attempts,1);
  assert.equal(f.codex.delivered.some(m=>m.id===stranded.id),false);assert.equal(f.store.message(fresh.id).state,allowed?'contextObserved':'accepted');assert.equal(f.codex.created.length,0);
  if(allowed)assert.equal(f.codex.delivered.filter(m=>m.id===fresh.id).length,1);
});

for(const polluted of [false,true])for(const legacy of [false,true])test('托管原生调用在 broker 重启后恢复同代次路由，不把后台误建为前台'+(polluted?'（已有错误 embedded 记录）':'')+(legacy?'（旧 MCP 无类型）':''),async t=>{
  const f=fixture(t),home=f.broker.home,pair=f.store.pair(f.c.id,f.x.id),identity=await processIdentity(process.pid),endpoint={...f.store.endpoint(f.x.id)!,procStart:identity.startedAt};
  f.store.saveEndpoint(endpoint);f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'offline',launchFailure:{at:Date.now(),message:'旧错误归属'}});f.store.deactivate(f.x.id);
  saveManagedRuntime(home,{id:randomUUID(),peerId:f.x.id,owner:managedMarker(home),kind:'owned',state:'ready',endpoint:endpoint.address,pid:process.pid,procStart:identity.startedAt,createdAt:Date.now()});
  const ingress={mode:'embedded' as const,address:'',epoch:'native-ingress',pid:process.pid,procStart:identity.startedAt,processKind:'app-server' as const,codexPermission:{sandbox:'danger-full-access',approval:'bypass' as const}};
  if(polluted)f.store.saveEndpoint({...ingress,peerId:f.x.id,seenAt:Date.now(),active:true});
  const message=f.store.accept(pair,f.c,'保持原配对和消息账本','ingress');
  const result=await f.broker.dispatch('mcp.register',registration('codex','x',{endpoint:{...ingress,...(legacy?{processKind:undefined}:{})}})) as Peer;
  assert.equal(result.id,f.x.id);assert.equal(result.launchFailure,undefined);assert.equal(result.state,'unknown');assert.equal(result.permission,'bypass');
  assert.equal(f.store.endpoint(f.x.id)?.mode,'app-server');assert.equal(f.store.endpoint(f.x.id)?.address,endpoint.address);assert.equal(f.store.bindings.current(f.x.id),undefined);
  assert.equal(f.store.endpoints(f.x.id).some(e=>e.active&&e.mode==='embedded'),false);assert.equal(f.store.pairFor(f.x.id)?.id,pair.id);assert.equal(f.store.peers().filter(p=>p.sessionId==='x').length,1);
  assert.equal(f.store.message(message.id).attempts,0);assert.equal(f.codex.created.length,0);assert.equal(f.codex.ensured,0);
});

for(const phase of ['startup','maintenance','disabled'] as const)test('没有原生调用的托管成员也能恢复旧路由并继续已接受消息：'+phase,async t=>{
  const f=fixture(t),home=f.broker.home,pair=f.store.pair(f.c.id,f.x.id),identity=await processIdentity(process.pid),endpoint={...f.store.endpoint(f.x.id)!,procStart:identity.startedAt};
  f.store.saveEndpoint({...endpoint,active:false});f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'offline',enabled:phase!=='disabled',launchFailure:{at:Date.now(),message:'旧错误归属'}});
  f.store.saveEndpoint({peerId:f.x.id,mode:'embedded',address:'',epoch:'old-ingress',pid:process.pid,procStart:identity.startedAt,processKind:'app-server',seenAt:Date.now(),active:true});
  saveManagedRuntime(home,{id:randomUUID(),peerId:f.x.id,owner:managedMarker(home),kind:'owned',state:'ready',endpoint:endpoint.address,pid:process.pid,procStart:identity.startedAt,createdAt:Date.now()});
  const message=f.store.accept(pair,f.c,'空闲时恢复，不要求目标调用 MCP','idle-recovery'),before=f.store.endpoints(f.x.id);
  if(phase==='startup')await f.broker.start();await f.broker.pump();
  if(phase==='disabled'){assert.deepEqual(f.store.endpoints(f.x.id),before);assert.equal(f.store.message(message.id).state,'accepted');assert.equal(f.store.message(message.id).attempts,0);}
  else{assert.equal(f.store.endpoint(f.x.id)?.mode,'app-server');assert.equal(f.store.peer(f.x.id).launchFailure,undefined);assert.equal(f.store.message(message.id).state,'contextObserved');assert.equal(f.codex.delivered.filter(m=>m.id===message.id).length,1);}
  assert.equal(f.store.pairFor(f.x.id)?.id,pair.id);assert.equal(f.codex.created.length,0);assert.equal(f.codex.ensured,0);
});

for(const inactive of [false,true])test('旧专用身份冲突先保持消息，忙碌不停止，原生空闲后自动恢复原 ID 再投递'+(inactive?'（升级时观察端点已停用）':''),async t=>{
  const f=fixture(t),home=f.broker.home,pair=f.store.pair(f.c.id,f.x.id),endpoint=f.store.endpoint(f.x.id)!;
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'busy'});
  saveManagedRuntime(home,{id:randomUUID(),peerId:f.x.id,owner:managedMarker(home),kind:'owned',state:'ready',endpoint:endpoint.address,pid:process.pid,procStart:(await processIdentity(process.pid)).startedAt,createdAt:Date.now()});
  if(inactive){f.store.deactivate(f.x.id);f.store.updatePeer(f.x.id,{state:'offline'});}
  let migrated=false,restarts=0,busy=true;
  f.codex.attachFailure=new BridgeError('codex_identity_conflict','旧身份');
  f.codex.connect=async()=>{if(!migrated)throw new BridgeError('codex_identity_conflict','旧身份');return{request:async()=>({})};};
  (f.codex as any).restart=async(peer:Peer,old:Endpoint,guard:()=>void,options:any)=>{
    restarts++;guard();assert.equal(options.identityMigration,true);assert.equal(options.requesterSession,'c');
    if(busy)throw new BridgeError('session_busy','原生轮次仍忙碌');
    migrated=true;f.codex.attachFailure=undefined;f.store.adopt(peer.id,registration('codex',peer.sessionId!,{state:'ready',endpoint:{...old,address:'unix://migrated',epoch:'migrated'}}));
    return{endpoint:'unix://migrated',oldPid:1,pid:2,resumed:{}};
  };
  const message=f.store.accept(pair,f.c,'升级期间只保留原消息','migration');
  await f.broker.pump();assert.equal(restarts,1);assert.equal(f.codex.injected.length,0);assert.equal(f.store.message(message.id).attempts,0);assert.equal(f.store.message(message.id).state,'accepted');
  assert.equal(f.store.peer(f.x.id).launchFailure,undefined,'等待原生空闲是身份迁移保持状态，不是后台启动失败');
  assert.match((f.broker.status() as any).capabilities.identityIssues[0].message,/仍忙碌/);
  busy=false;lifecycle(f.broker).clearRetryDelay(f.x.id);f.store.updatePeer(f.x.id,{state:'offline'});
  await f.broker.pump();await f.broker.pump();
  assert.equal(restarts,2);assert.equal(f.store.peer(f.x.id).sessionId,'x');assert.equal(f.codex.created.length,0);
  assert.equal(f.codex.delivered.filter(m=>m.id===message.id).length,1);assert.equal(f.store.message(message.id).attempts,1);
  assert.deepEqual((f.broker.status() as any).capabilities.identityIssues,[]);
});

for(const mode of ['remote','user','unproven','shared'] as const)test('身份冲突 '+mode+' 端点保持消息并显示原因，绝不重启或降级投递',async t=>{
  const f=fixture(t),home=f.broker.home,pair=f.store.pair(f.c.id,f.x.id),endpoint=f.store.endpoint(f.x.id)!;
  f.store.updatePeer(f.x.id,{owner:mode==='user'?'user':'nxbridge'});
  if(mode==='remote')f.store.saveEndpoint({...endpoint,address:'wss://remote.example'});
  if(mode==='shared'){
    saveManagedRuntime(home,{id:randomUUID(),peerId:f.x.id,owner:managedMarker(home),kind:'owned',state:'ready',endpoint:endpoint.address,pid:process.pid,procStart:(await processIdentity(process.pid)).startedAt,createdAt:Date.now()});
    f.store.register(registration('codex','other',{endpoint:{...endpoint,epoch:'other'}}));
  }
  f.codex.connect=async()=>{throw new BridgeError('codex_identity_conflict','旧 originator 不可改写');};
  (f.codex as any).restart=async()=>assert.fail('不能停止非独占已证明归属的宿主');
  const message=f.store.accept(pair,f.c,'保留直到安全重载','hold');await f.broker.pump();
  assert.equal(f.store.message(message.id).state,'accepted');assert.equal(f.store.message(message.id).attempts,0);assert.equal(f.codex.injected.length,0);assert.equal(f.codex.woken.length,0);
  const status=await f.broker.dispatch('status',{actor:{host:'claude',sessionId:'c',callId:'status'},model_view:true}) as any;
  assert.ok(status.integrationIssues.some((issue:any)=>issue.code==='codex_identity_conflict'&&issue.message.includes('投递已暂停')));
});

test('真实 WebSocket 远程端点固定旧 originator 时只握手，不发业务 RPC，模型状态明确暂停原因',async t=>{
  const server=new WebSocketServer({host:'127.0.0.1',port:0});await once(server,'listening');
  const address='ws://127.0.0.1:'+(server.address() as {port:number}).port,methods:string[]=[];
  server.on('connection',socket=>socket.on('message',raw=>{
    const message=JSON.parse(raw.toString());methods.push(message.method);
    if(message.id!==undefined)socket.send(JSON.stringify({id:message.id,result:{userAgent:'nxbridge/0.166.0 (test; test) test (codex-tui; 0.166.0)'}}));
  }));
  const home=temporary(t),store=new Store(':memory:'),codex=new CodexAdapter(DEFAULT_CONFIG,home),broker=new Broker(store,DEFAULT_CONFIG,{codex,claude:new FakeAdapter()} as unknown as Adapters,home);
  t.after(async()=>{await broker.stop();store.close();for(const socket of server.clients)socket.terminate();await new Promise<void>(resolve=>server.close(()=>resolve()));});
  const c=store.register(registration('claude','remote-source')),x=store.register(registration('codex','remote-target',{endpoint:{mode:'app-server',address,epoch:address,pid:null,procStart:null}})),pair=store.pair(c.id,x.id),message=store.accept(pair,c,'远程不能重启时保留','remote-real');
  store.updatePeer(x.id,{owner:'nxbridge'});await broker.pump();
  assert.equal(store.message(message.id).state,'accepted');assert.equal(store.message(message.id).attempts,0);
  assert.deepEqual(methods,['initialize','initialized','initialize']);
  const result=await broker.dispatch('status',{actor:{host:'claude',sessionId:c.sessionId,callId:'status'},model_view:true}) as any;
  assert.ok(result.integrationIssues.some((issue:any)=>issue.code==='codex_identity_conflict'&&issue.message.includes('投递已暂停')));
  assert.match(formatGlobalStatus(broker.status()),/投递已暂停/);
  assert.match(formatStatus(broker.status({host:'claude',sessionId:c.sessionId!,callId:'status'})),/投递已暂停/);
});
