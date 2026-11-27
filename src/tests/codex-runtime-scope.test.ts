import test, {type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {writeFileSync} from 'node:fs';
import {randomUUID} from 'node:crypto';
import {assertCodexRuntimeScope, forgetAbsentChildControls, probeCodexChildWork} from '../hosts/codex/codex-restart.js';
import {CodexAdapter, type CodexThread} from '../hosts/codex/adapter.js';
import {observeCodexThreadIdentity} from '../hosts/codex/codex-evidence.js';
import {removeManagedRuntime, runtimeFor, saveManagedRuntime, type ManagedRuntime} from '../hosts/managed-runtimes.js';
import {managedMarker} from '../install/installation.js';
import {RpcClient, type RpcMessage} from '../ipc/rpc.js';
import {processIdentity} from '../process/process.js';
import {BridgeError} from '../shared/errors.js';
import {atomicJson,readJson} from '../shared/files.js';
import {DEFAULT_CONFIG,DEFAULT_RETENTION} from '../config/defaults.js';
import {type Endpoint,type Peer} from '../types.js';
import {modelError} from '../integration/mcp/model-output.js';
import { fixture } from './support/broker-fixture.js';
import { registration } from './support/registration.js';
import { temporary } from './support/temporary.js';

function native(t:TestContext,values:CodexThread[],loaded=values.map(thread=>thread.id)){
  const threads=new Map(values.map(thread=>[thread.id,thread])),queue=new Map<string,unknown[]>(),terminals=new Map<string,unknown[]>(),calls:string[]=[];
  let silent:string|undefined;
  let terminalReply:Pick<RpcMessage,'result'|'error'>|undefined;
  const rpc=new RpcClient(message=>{
    calls.push(message.method!);const id=message.params?.threadId,thread=threads.get(id);
    if(message.method===silent)return;
    if(message.method==='thread/backgroundTerminals/list'&&terminalReply){queueMicrotask(()=>rpc.receive({id:message.id,...terminalReply}));return;}
    let result:unknown;
    switch(message.method){
      case'thread/loaded/list':result={data:loaded};break;
      case'thread/list':result={data:values};break;
      case'thread/read':result={thread};break;
      case'thread/turns/list':if(thread?.ephemeral){queueMicrotask(()=>rpc.receive({id:message.id,error:{code:-32600,message:'ephemeral threads do not support thread/turns/list'}}));return;}result={data:[...(thread?.turns??[])].reverse()};break;
      case'thread/queue/list':result={data:queue.get(id)??[]};break;
      case'thread/backgroundTerminals/list':result={data:terminals.get(id)??[]};break;
      default:assert.fail('不应调用原生变更：'+message.method);
    }
    queueMicrotask(()=>rpc.receive({id:message.id,result}));
  },()=>{});t.after(()=>rpc.close());
  const home=temporary(t),runtime:ManagedRuntime={id:randomUUID(),peerId:'member',owner:managedMarker(home),kind:'owned',state:'ready',endpoint:'private',pid:1,procStart:'native-1',createdAt:1};
  saveManagedRuntime(home,runtime);
  const scope=(generation=runtime)=>assertCodexRuntimeScope(rpc,'root',{
    observed:ids=>forgetAbsentChildControls(home,generation,id=>ids.has(id)),
    probe:id=>probeCodexChildWork(home,generation,id,async()=>loaded.includes(id),(method,beforeSend)=>rpc.request(method,{threadId:id,limit:1},{timeout:20,beforeSend}),()=>{}),
  });
  return{rpc,home,runtime,threads,queue,terminals,calls,scope,silence:(method?:string)=>{silent=method;},replyTerminals:(reply:typeof terminalReply)=>{terminalReply=reply;}};
}
const child=(id:string,parent='root',status='idle'):CodexThread=>({id,cwd:'/project',parentThreadId:parent,status:{type:status}});

test('重启范围及缺失父辈的补查都只读状态库，分页仍覆盖完整父链',async t=>{
  let catalogs=0;
  const rpc=new RpcClient(message=>{
    let result:unknown;
    if(message.method==='thread/loaded/list')result={data:['root','grandchild']};
    else{
      assert.equal(message.method,'thread/list');
      assert.equal(message.params.useStateDbOnly,true,'范围核验不能触发原生 JSONL 修补');
      assert.equal(message.params.ancestorThreadId,'root','缺失父辈也要覆盖原根的所有层级后代');
      catalogs++;
      result=message.params.cursor?{data:[child('parent')]}:{data:[child('grandchild','parent')],nextCursor:'parent-page'};
    }
    queueMicrotask(()=>rpc.receive({id:message.id,result}));
  },()=>{});t.after(()=>rpc.close());
  await assertCodexRuntimeScope(rpc,'root',{inspectOnly:true});
  assert.equal(catalogs,3,'已加载子线程先命中，缺失父辈再按原生分页补查');
});

test('普通目录隐藏的纯工具子代理按祖先查询，外来根仍从全局目录识别',async t=>{
  const descendants=[child('parent'),child('grandchild','parent')],loaded=['root','grandchild','foreign'];
  const rpc=new RpcClient(message=>{
    const data=message.method==='thread/loaded/list'?loaded
      :message.params.ancestorThreadId?descendants:[{id:'foreign',cwd:'/project',status:{type:'idle'}}];
    if(message.method==='thread/list')assert.equal(message.params.useStateDbOnly,true);
    queueMicrotask(()=>rpc.receive({id:message.id,result:{data}}));
  },()=>{});t.after(()=>rpc.close());
  await assert.rejects(assertCodexRuntimeScope(rpc,'root',{inspectOnly:true}),{code:'shared_runtime'});
  loaded.pop();
  await assertCodexRuntimeScope(rpc,'root',{inspectOnly:true});
});

test('已加载子线程不在状态库时保持未知，连接上的旧空闲身份也不能放行',async t=>{
  const f=native(t,[],['root','missing']);
  await assert.rejects(assertCodexRuntimeScope(f.rpc,'root',{inspectOnly:true}),{code:'restart_verification_failed'});
  observeCodexThreadIdentity(f.rpc,child('missing'));
  await assert.rejects(assertCodexRuntimeScope(f.rpc,'root',{inspectOnly:true}),{code:'subagent_active'});
});

test('原生父链覆盖嵌套子代理及未加载的中间父线程，全部核验只读',async t=>{
  const f=native(t,[child('parent'),child('grandchild','parent'),child('old','root','notLoaded')],['root','grandchild','old']);
  await f.scope();
  assert.ok(f.calls.includes('thread/backgroundTerminals/list'));assert.ok(f.calls.every(method=>/^(thread\/(loaded\/list|list|read|turns\/list|queue\/list|backgroundTerminals\/list))$/.test(method)));
  f.threads.get('grandchild')!.status={type:'active'};
  await assert.rejects(f.scope(),{code:'subagent_active'});
});

for(const [name,thread,code] of [
  ['其他根会话',{id:'other',cwd:'/project',status:{type:'idle'}},'shared_runtime'],
  ['不支持轮次历史的临时根会话',{id:'ephemeral-root',cwd:'/project',ephemeral:true,status:{type:'idle'}},'shared_runtime'],
  ['其他 fork',{id:'fork',cwd:'/project',forkedFromId:'root',status:{type:'idle'}},'shared_runtime'],
  ['缺失父链',{id:'orphan',cwd:'/project',threadSource:'subagent',status:{type:'idle'}},'restart_verification_failed'],
  ['自环父链',child('cycle','cycle'),'restart_verification_failed'],
  ['审批中的子代理',{...child('approval'),status:{type:'active',activeFlags:['waitingForApproval']}},'subagent_active'],
  ['元数据空闲但轮次未结束',{...child('turn'),turns:[{id:'in-progress',status:'inProgress'}]},'subagent_active'],
  ['状态未知的子代理',{...child('unknown'),status:undefined},'subagent_active'],
] satisfies [string,CodexThread,string][])test('宿主范围不放行'+name,async t=>{
  const f=native(t,[thread]);await assert.rejects(f.scope(),{code});
});

test('自有临时子代理只在原生状态空闲且没有排队和后台任务时允许停止',async t=>{
  const thread={...child('ephemeral-child'),ephemeral:true},f=native(t,[thread]);await f.scope();
  assert.equal(f.calls.includes('thread/turns/list'),false);
  thread.status={type:'active'};await assert.rejects(f.scope(),{code:'subagent_active'});
  thread.status={type:'idle'};f.queue.set(thread.id,[{id:'queued'}]);await assert.rejects(f.scope(),{code:'delivery_unsettled'});
});

test('子代理排队、后台终端和未落盘末轮分别阻止停止，不清除原生工作',async t=>{
  const f=native(t,[child('child')]);
  f.queue.set('child',[{id:'queued'}]);await assert.rejects(f.scope(),{code:'delivery_unsettled'});
  f.queue.clear();f.terminals.set('child',[{processId:'still-running'}]);await assert.rejects(f.scope(),{code:'session_busy'});
  f.terminals.clear();const thread=f.threads.get('child')!;thread.turns=[{id:'last-child-turn',status:'completed'}];
  await assert.rejects(f.scope(),{code:'history_not_persisted'});
  thread.path=path.join(temporary(t),'child.jsonl');writeFileSync(thread.path,JSON.stringify({type:'event_msg',payload:{type:'task_complete',turn_id:'last-child-turn'}})+'\n','utf8');
  await f.scope();
});

test('无子线程订阅时只用全局目录和加载列表，原生确认卸载后才放行',async t=>{
  const loaded=['root','child'],f=native(t,[child('child')],loaded);
  await assert.rejects(assertCodexRuntimeScope(f.rpc,'root'),{code:'subagent_active'});
  assert.ok(f.calls.every(method=>method==='thread/list'||method==='thread/loaded/list'));
  loaded.pop();await assertCodexRuntimeScope(f.rpc,'root');
});

test('子代理工作探针超时后，反复核验不再提交控制请求，原生卸载后清除预约',async t=>{
  const loaded=['root','child'],f=native(t,[child('child')],loaded);
  f.silence('thread/backgroundTerminals/list');let stopped=0;
  const stop=async()=>{await f.scope();stopped++;};
  await assert.rejects(stop(),{code:'subagent_control_unverified'});
  assert.ok(runtimeFor(f.home,{id:f.runtime.id})?.value.childControlChecks?.pending.child);
  const calls=f.calls.length;
  await assert.rejects(stop(),{code:'subagent_control_unverified'});
  assert.ok(f.calls.slice(calls).every(method=>['thread/loaded/list','thread/list','thread/turns/list'].includes(method)));
  assert.equal(stopped,0);
  loaded.pop();await stop();assert.equal(stopped,1);
  assert.equal(runtimeFor(f.home,{id:f.runtime.id})?.value.childControlChecks,undefined);
});

test('收到明确的子代理忙碌回执会清除预约，旧代次不能更新新宿主记录',async t=>{
  const f=native(t,[child('child')]);
  f.terminals.set('child',[{processId:'running'}]);
  await assert.rejects(f.scope(),{code:'session_busy'});
  assert.equal(runtimeFor(f.home,{id:f.runtime.id})?.value.childControlChecks,undefined);
  f.terminals.clear();f.silence('thread/queue/list');
  await assert.rejects(f.scope(),{code:'subagent_control_unverified'});
  const replacement={...runtimeFor(f.home,{id:f.runtime.id})!.value,procStart:'native-2'};
  saveManagedRuntime(f.home,replacement);
  assert.throws(()=>forgetAbsentChildControls(f.home,f.runtime,()=>false),{code:'runtime_changed'});
  f.silence();await f.scope(replacement);
  assert.equal(runtimeFor(f.home,{id:f.runtime.id})?.value.childControlChecks,undefined);
});

for(const [label,reply] of [
  ['缺少有效列表',{result:{data:{}}}],
  ['分页不完整',{result:{data:[],nextCursor:'more'}}],
  ['游标无效',{result:{data:[],nextCursor:12}}],
  ['原生方法不可用',{error:{code:-32601,message:'Method not found'}}],
  ['原生错误',{error:{code:-32600,message:'Thread not found'}}],
] satisfies [string,Pick<RpcMessage,'result'|'error'>][])test('子代理回执'+label+'时保留预约，重复核验只使用独立证据',async t=>{
  const f=native(t,[child('child')]);
  f.replyTerminals(reply);
  await assert.rejects(f.scope(),{code:'subagent_control_unverified'});
  assert.ok(runtimeFor(f.home,{id:f.runtime.id})?.value.childControlChecks?.pending.child);
  const count=f.calls.length;
  f.replyTerminals(undefined);
  await assert.rejects(f.scope(),{code:'subagent_control_unverified'});
  assert.ok(f.calls.slice(count).every(method=>['thread/loaded/list','thread/list','thread/turns/list'].includes(method)));
});

test('旧版托管记录保留未确认子代理探针，完整加载列表确认卸载后清除',async t=>{
  const loaded=['root','child'],f=native(t,[child('child')],loaded);
  removeManagedRuntime(runtimeFor(f.home,{id:f.runtime.id})!);
  const file=path.join(f.home,'run','codex-runtime.json'),raw={peerId:f.runtime.peerId,endpoint:f.runtime.endpoint,pid:f.runtime.pid,procStart:f.runtime.procStart,createdAt:f.runtime.createdAt};
  atomicJson(file,raw);
  const legacy=runtimeFor(f.home,{id:'legacy'})!.value;
  await f.scope(legacy);
  f.silence('thread/queue/list');
  await assert.rejects(f.scope(legacy),{code:'subagent_control_unverified'});
  assert.ok(runtimeFor(f.home,{id:'legacy'})?.value.childControlChecks?.pending.child);
  const count=f.calls.length;
  await assert.rejects(f.scope(runtimeFor(f.home,{id:'legacy'})!.value),{code:'subagent_control_unverified'});
  assert.ok(f.calls.slice(count).every(method=>['thread/loaded/list','thread/list','thread/turns/list'].includes(method)));
  loaded.pop();await f.scope(legacy);
  assert.deepEqual(readJson(file),raw);
});

function bridge(t:TestContext){
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  const endpoint=f.store.endpoint(f.x.id)!,first=f.store.register(registration('codex','child',{kind:'subagent',parentId:'x',endpoint:{...endpoint,epoch:'child'}})),second=f.store.register(registration('codex','grandchild',{kind:'subagent',parentId:'child',endpoint:{...endpoint,epoch:'grandchild'}}));
  f.codex.observe=async()=>{};return{...f,pair,endpoint,first,second};
}

for(const partner of [false,true])test((partner?'搭档':'普通')+'重启接受自有空闲子代理，停止时只停用原宿主代次',async t=>{
  const f=bridge(t),input=partner?{actor:{host:'claude',sessionId:'c',callId:'restart'},partner:true}:{};
  f.store.updatePeer(f.first.id,{state:'paused'});
  (f.codex as any).restart=async(peer:Peer,endpoint:Endpoint,guard:()=>void)=>{
    guard();f.codex.emit('runtimeStopped',endpoint);
    f.store.adopt(peer.id,registration('codex','x',{endpoint:{...endpoint,address:'new-runtime',epoch:'new-runtime'}}));
    return{endpoint:'new-runtime',oldPid:1,pid:2,resumed:{model:'test'}};
  };
  assert.equal((await f.broker.dispatch('codex.restart.target',input) as any).peer_id,f.x.id);
  assert.equal((await f.broker.dispatch('codex.restart',input) as any).restarted,true);
  assert.equal(f.store.endpoint(f.first.id),undefined);assert.equal(f.store.endpoint(f.second.id),undefined);
  assert.equal(f.store.peer(f.first.id).state,'offline');assert.equal(f.store.peer(f.first.id).owner,'user');assert.equal(f.store.endpoint(f.x.id)?.address,'new-runtime');
  assert.equal(f.store.pairs().length,1);assert.deepEqual((f.broker as any).pairAnomalies.issues(f.first.id),[]);
});

test('子代理的活动、桥接投递和晚到任务均阻止重启，其他根线程仍报共享宿主',async t=>{
  const f=bridge(t);let stopped=0;(f.codex as any).restart=async()=>{stopped++;};
  for(const state of ['busy','needsHuman','unknown','starting'] as const){f.store.updatePeer(f.first.id,{state});await assert.rejects(f.broker.dispatch('codex.restart',{}),{code:'subagent_active'});}
  f.store.updatePeer(f.first.id,{state:'ready'});
  const message=f.store.accept(f.pair,f.c,'子代理未确认的既有投递','child-delivery');
  (f.store as any).db.prepare('UPDATE messages SET recipient=? WHERE id=?').run(f.first.id,message.id);
  f.store.deactivate(f.first.id);await assert.rejects(f.broker.dispatch('codex.restart',{}),{code:'delivery_unsettled'});assert.equal(f.store.message(message.id).attempts,0);
  f.store.transition(message.id,'contextObserved');
  f.store.setSetting('codexWake:'+f.first.id,{scope:'child',anchor:'pending',attempts:1,at:1});await assert.rejects(f.broker.dispatch('codex.restart',{}),{code:'delivery_unsettled'});f.store.setSetting('codexWake:'+f.first.id,null);
  const foreign=f.store.register(registration('codex','other-root',{endpoint:{...f.endpoint,epoch:'foreign'}}));
  await assert.rejects(f.broker.dispatch('codex.restart',{}),{code:'shared_runtime'});f.store.deactivate(foreign.id);
  (f.codex as any).restart=async(_peer:Peer,_endpoint:Endpoint,guard:()=>void)=>{f.codex.emit('turnStarted',f.second.sessionId,f.endpoint.address);guard();stopped++;};
  await assert.rejects(f.broker.dispatch('codex.restart',{}),{code:'subagent_active'});assert.equal(stopped,0);assert.equal(f.store.peer(f.x.id).state,'ready');
});

for(const partner of [false,true])test((partner?'搭档':'普通')+'重启兼容既有三个已卸载子代理，保留发往 Claude 的历史未知消息',async t=>{
  const f=bridge(t),third=f.store.register(registration('codex','third-child',{kind:'subagent',parentId:'x',endpoint:{...f.endpoint,epoch:'third-child'}}));
  f.store.updatePeer(f.first.id,{state:'paused'});
  const unknown=f.store.accept(f.pair,f.x,'既有发往 Claude 的未知投递','root-outbound');f.store.transition(unknown.id,'unknown');
  const childUnknown=f.store.accept(f.pair,f.x,'既有子代理发往 Claude 的未知投递','child-outbound');
  (f.store as any).db.prepare('UPDATE messages SET sender=? WHERE id=?').run(f.first.id,childUnknown.id);f.store.transition(childUnknown.id,'unknown');
  const before=[f.store.message(unknown.id),f.store.message(childUnknown.id)];
  const adapter=new CodexAdapter(DEFAULT_CONFIG,temporary(t));t.after(()=>adapter.close());
  const rpc=native(t,[child('child','x','notLoaded'),child('grandchild','child','notLoaded'),child('third-child','x','notLoaded')]).rpc;
  adapter.connect=async()=>rpc;adapter.on('register',(input,memberId)=>f.codex.emit('register',input,memberId));adapter.on('state',(...args)=>f.codex.emit('state',...args));
  f.codex.observe=(peer,endpoint)=>adapter.observe(peer,endpoint);
  (f.codex as any).restart=async(peer:Peer,endpoint:Endpoint,guard:()=>void)=>{guard();f.codex.emit('runtimeStopped',endpoint);f.store.adopt(peer.id,registration('codex','x',{endpoint:{...endpoint,address:'replacement',epoch:'replacement'}}));return{endpoint:'replacement',oldPid:1,pid:2,resumed:{model:'test'}};};
  const input=partner?{actor:{host:'claude',sessionId:'c',callId:'restart'},partner:true}:{};
  assert.equal((await f.broker.dispatch('codex.restart',input) as any).restarted,true);
  for(const peer of [f.first,f.second,third]){assert.equal(f.store.endpoint(peer.id),undefined);assert.equal(f.store.peer(peer.id).state,'offline');}
  assert.deepEqual([f.store.message(unknown.id),f.store.message(childUnknown.id)],before);
});

for(const state of ['dead','alive','unknown'] as const)test('地址复用的旧子端点进程代次为 '+state+' 时只回收已确认退出的记录',async t=>{
  const f=bridge(t),identity=state==='alive'?await processIdentity(process.pid):undefined;
  const stale=f.store.register(registration('codex','stale-child',{kind:'subagent',parentId:'x',endpoint:{...f.endpoint,epoch:'stale-child',runtimeId:'old-runtime',pid:identity?.pid??2147483647,procStart:identity?.startedAt??(state==='dead'?'old-generation':null)}}));
  let stopped=false;(f.codex as any).restart=async(_peer:Peer,_endpoint:Endpoint,guard:()=>void)=>{guard();stopped=true;return{oldPid:1,pid:2,resumed:{model:'test'}};};
  if(state==='dead'){assert.equal((await f.broker.dispatch('codex.restart',{}) as any).restarted,true);assert.equal(f.store.endpoint(stale.id),undefined);assert.equal(f.store.peer(stale.id).state,'offline');}
  else{await assert.rejects(f.broker.dispatch('codex.restart',{}),{code:'shared_runtime'});assert.ok(f.store.endpoint(stale.id));}
  assert.equal(stopped,state==='dead');
});

test('升级刷新停用未加载子代理，原生结束通知和再次启动保持端点生命周期',async t=>{
  const f=bridge(t),observed:string[]=[];
  const identity=await processIdentity(process.pid);
  for(const peer of [f.first,f.second])for(const endpoint of f.store.endpoints(peer.id))f.store.saveEndpoint({...endpoint,procStart:identity.startedAt});
  f.store.updatePeer(f.first.id,{state:'paused'});
  let observedFirst!:()=>void;
  const observedTarget=new Promise<void>(resolve=>observedFirst=resolve);
  f.codex.observe=async(peer,endpoint)=>{observed.push(peer.id);f.codex.emit('register',registration('codex',peer.sessionId!,{kind:'subagent',parentId:peer.parentId??undefined,state:'offline',endpoint}),peer.id);if(peer.id===f.first.id)observedFirst();};
  await f.broker.start();await observedTarget;assert.ok(observed.includes(f.first.id));assert.equal(f.store.endpoint(f.first.id),undefined);assert.ok(f.store.endpoint(f.x.id));
  const adapter=new CodexAdapter(DEFAULT_CONFIG,temporary(t));t.after(()=>adapter.close());
  adapter.on('state',(sessionId,state,address)=>f.codex.emit('state',sessionId,state,address));
  f.store.register(registration('codex','child',{kind:'subagent',parentId:'x',endpoint:{...f.endpoint,epoch:'child'}}));
  (adapter as any).notify(f.endpoint.address,{method:'thread/status/changed',params:{threadId:'child',status:{type:'notLoaded'}}});assert.equal(f.store.endpoint(f.first.id),undefined);
  f.store.register(registration('codex','child',{kind:'subagent',parentId:'x',endpoint:{...f.endpoint,epoch:'child'}}));
  f.store.updatePeer(f.first.id,{state:'paused'});f.codex.emit('turnStarted','child',f.endpoint.address);assert.equal(f.store.peer(f.first.id).state,'busy');
  (adapter as any).notify(f.endpoint.address,{method:'thread/closed',params:{threadId:'child'}});assert.equal(f.store.endpoint(f.first.id),undefined);
});

test('原宿主退出不撤销其他代次或其他地址端点，离线子代理按既有保留策略回收',async t=>{
  const f=bridge(t),changed=f.store.register(registration('codex','changed-generation',{kind:'subagent',parentId:'x',endpoint:{...f.endpoint,epoch:'changed',procStart:'new-generation'}}));
  const other=f.store.register(registration('codex','other-address',{kind:'subagent',parentId:'x',endpoint:{...f.endpoint,address:'other-address',epoch:'other'}}));
  f.codex.emit('runtimeStopped',f.endpoint);assert.ok(f.store.endpoint(changed.id));assert.ok(f.store.endpoint(other.id));
  f.store.updatePeer(f.first.id,{updatedAt:0});f.store.updatePeer(f.second.id,{updatedAt:0});
  f.store.maintain({peerMaxIdleMs:1},Date.now()+DEFAULT_RETENTION.historyMaxAgeMs+1000);
  assert.equal(f.store.getPeer(f.first.id),undefined);assert.equal(f.store.getPeer(f.second.id),undefined);assert.ok(f.store.getPeer(f.x.id));
});

test('子代理活动与其他根宿主错误有明确模型处理步骤',()=>{
  assert.match(modelError(new BridgeError('subagent_active','中文')).message,/own subagent.*retry once.*no loop/);
  assert.match(modelError(new BridgeError('shared_runtime','中文')).message,/another Codex root session.*nothing stopped.*tell user/);
});
