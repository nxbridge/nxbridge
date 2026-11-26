import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { WebSocketServer } from 'ws';
import { CodexAdapter } from '../hosts/codex/adapter.js';
import { exitedOwnedCodexRoute } from '../hosts/codex/codex-owned-routes.js';
import { Broker, type Adapters } from '../broker/coordinator.js';
import { Store } from '../storage/store.js';
import {BridgeError,RuntimeRecoveryHold} from '../shared/errors.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import { FakeAdapter } from './support/fake-adapter.js';
import {fixture, lifecycle} from './support/broker-fixture.js';
import { registration } from './support/registration.js';
import { temporary } from './support/temporary.js';
import {atomicJson} from '../shared/files.js';
import { claudeChildFile } from '../hosts/claude/claude-child.js';
import { managedMarker } from '../install/installation.js';
import { runtimeFor, saveManagedRuntime } from '../hosts/managed-runtimes.js';
import { recordManagedSessionLaunch } from '../hosts/launch-profiles.js';
import { processIdentity } from '../process/process.js';
import { HEALTH_WAKE_NOTICE } from '../types.js';

async function completePump(work:Promise<unknown>):Promise<void>{
  let timer:NodeJS.Timeout|undefined;try{await Promise.race([work,new Promise<never>((_resolve,reject)=>{timer=setTimeout(()=>reject(Error('慢恢复阻塞了独立消息泵')),5000);})]);}finally{clearTimeout(timer);}
}

for(const pending of [false,true])test('统一恢复调度'+(pending?'有待发消息':'无待发消息')+'每轮只恢复一次且不重发未知结果',async t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),endpoint=f.store.endpoint(f.c.id)!;
  const unknown=f.store.accept(pair,f.x,'已提交但结果未知','unknown');f.store.claim(unknown.id);f.store.transition(unknown.id,'unknown');
  const queued=pending?f.store.accept(pair,f.x,'等待同会话恢复','queued'):undefined;
  f.store.updatePeer(f.c.id,{owner:'nxbridge',state:'offline'});f.store.deactivate(f.c.id);
  let attempts=0;t.mock.method(lifecycle(f.broker),'recover',async()=>{attempts++;f.store.saveEndpoint({...endpoint,active:true});f.store.updatePeer(f.c.id,{state:'ready'});return true;});
  await Promise.all([lifecycle(f.broker).observeManagedExits(),f.broker.pump()]);
  assert.equal(attempts,1);assert.equal(f.store.message(unknown.id).state,'unknown');
  assert.deepEqual(f.claude.delivered.map(message=>message.id),queued?[queued.id]:[]);
});

for(const backoff of ['protocol','transient','delivery-clock'] as const)test('待发成员掉线后恢复不等待投递退避：'+backoff,async t=>{
  t.mock.timers.enable({apis:['Date'],now:100000});
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),endpoint=f.store.endpoint(f.x.id)!;
  const queued=f.store.accept(pair,f.c,'等待原会话恢复','queued'),unknown=f.store.accept(pair,f.c,'结果不明不能重发','unknown');f.store.claim(unknown.id);f.store.transition(unknown.id,'unknown');
  f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'offline'});f.store.deactivate(f.x.id);
  if(backoff==='delivery-clock')lifecycle(f.broker).delayRetry(f.x.id,30000);
  else{
    const incident=(f.broker as any).incidents.failed(f.store.peer(f.x.id),endpoint,new BridgeError(backoff==='protocol'?'-32600':'disconnected','旧代次投递失败'),[queued.id]);
    if(backoff==='protocol')assert.equal(incident.retryAt-incident.lastFailureAt,900000,'最坏的十五分钟投递退避也不能延后下一次恢复调度');
  }
  let attempts=0;t.mock.method(lifecycle(f.broker),'recover',async()=>{attempts++;f.store.saveEndpoint({...endpoint,active:true});f.store.updatePeer(f.x.id,{state:'ready'});return true;});
  await f.broker.pump();assert.equal(attempts,1,'首次 pump 即恢复，正常空闲定时器至多等待五秒');assert.equal(f.store.peer(f.x.id).state,'ready');
  assert.equal(f.codex.woken.length,0,'旧代次的投递仍遵守原有退避');assert.equal(f.store.message(queued.id).state,'accepted');assert.equal(f.store.message(unknown.id).state,'unknown');
});

test('独立恢复预算仍阻止尚未到期的后台启动',async t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);f.store.accept(pair,f.x,'等待恢复预算','queued');
  const peer=f.store.updatePeer(f.c.id,{owner:'nxbridge',state:'offline'});f.store.deactivate(f.c.id);
  const incident=f.broker.managedRecovery.observe(peer,pair);f.broker.managedRecovery.settled(incident,new Error('上次恢复暂时失败'));
  t.mock.method(lifecycle(f.broker),'prepare',async()=>{assert.fail('恢复预算尚未到期');});await f.broker.pump();assert.equal(f.store.peer(f.c.id).state,'offline');
});

for(const stage of ['failed','removed','before-register','legacy','legacy-live','legacy-active','legacy-foreign','unproven','alive'] as const)test('同会话恢复只跳过已确认退出的历史运行时：'+stage,async t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),peer=f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'offline'});
  t.mock.method(f.codex,'routeExited',(...[target,address,launch]:Parameters<FakeAdapter['routeExited']>)=>exitedOwnedCodexRoute(f.broker.home,{peer:target,endpoint:address},launch));
  const legacy=stage.startsWith('legacy'),alive=stage==='alive'||stage==='legacy-live';
  const identity=await processIdentity(process.pid),runtimeId=randomUUID(),native={pid:identity.pid,procStart:alive?identity.startedAt:identity.startedAt+'-retired'};
  const old={...f.store.endpoint(peer.id)!,...native,runtimeId,configHome:f.broker.home,active:!legacy||stage==='legacy-active'};
  f.store.saveEndpoint(old);
  const runtime={id:runtimeId,peerId:peer.id,sessionId:peer.sessionId!,owner:managedMarker(f.broker.home),kind:'owned' as const,state:'failed' as const,endpoint:old.address,configHome:old.configHome,...native,createdAt:1};
  const proof={sessionId:peer.sessionId,runtimeId,endpoint:old.address,configHome:old.configHome,...native};
  const key='managedLaunchProof:'+peer.id;
  if(stage==='failed'||stage==='alive')saveManagedRuntime(f.broker.home,runtime);
  if(legacy)recordManagedSessionLaunch(f.broker.home,stage==='legacy-foreign'?'another-session':peer.sessionId!,peer.cwd,{...runtime,state:'ready',dedicated:true,executable:process.execPath});
  else if(stage!=='unproven')f.store.setSetting(key,proof);
  const replacement={...runtime,id:randomUUID(),endpoint:'unix://replacement',state:'starting' as const,createdAt:2};
  if(stage==='before-register')saveManagedRuntime(f.broker.home,replacement);
  const queued=f.store.accept(pair,f.c,'等待恢复后的原投递','unused'),oldUnknown=f.store.accept(pair,f.c,'提交状态不明','uncertain');f.store.claim(oldUnknown.id);f.store.transition(oldUnknown.id,'unknown');
  let interrupted=stage==='before-register';
  const normal=f.codex.attach.bind(f.codex);
  const attach=t.mock.method(f.codex,'attach',async(target:Parameters<FakeAdapter['attach']>[0],endpoint:Parameters<FakeAdapter['attach']>[1])=>{
    if(endpoint.address===old.address)throw new RuntimeRecoveryHold('owned_route_unverified','历史端点不再拥有原生订阅');
    if(interrupted){interrupted=false;throw new RuntimeRecoveryHold('owned_subscription_waiting','替代后台已启动，登记尚未完成');}
    assert.equal(target.sessionId,peer.sessionId);return normal(target,{...endpoint,runtimeId:replacement.id});
  });
  const ensure=t.mock.method(f.codex,'ensureEndpoint',async(...[_cwd,id,_requester,sessionId,home,beforeLaunch]:Parameters<FakeAdapter['ensureEndpoint']>)=>{
    assert.equal(id,peer.id);assert.equal(sessionId,peer.sessionId);assert.equal(home,old.configHome);
    if(stage==='before-register')assert.equal(runtimeFor(home!,{peerId:id})?.value.id,replacement.id);
    else await beforeLaunch?.();
    return replacement.endpoint;
  });
  const manager=lifecycle(f.broker);
  if(stage==='unproven'||alive||stage==='legacy-active'||stage==='legacy-foreign'){
    await assert.rejects(manager.prepare(peer),{code:'owned_route_unverified'});
    assert.equal(ensure.mock.callCount(),0);assert.equal(attach.mock.callCount(),1);assert.equal(f.store.endpoint(peer.id)?.runtimeId,old.active?runtimeId:undefined);
  }else{
    if(stage==='before-register'){
      await assert.rejects(manager.prepare(peer),{code:'owned_subscription_waiting'});
      assert.deepEqual(f.store.setting(key),proof);
    }
    await manager.prepare(peer);
    assert.equal(f.store.endpoint(peer.id)?.address,replacement.endpoint);assert.equal(f.store.peer(peer.id).sessionId,peer.sessionId);
    assert.equal(ensure.mock.callCount(),stage==='before-register'?2:1);
    assert.ok(attach.mock.calls.every(call=>call.arguments[1].address===replacement.endpoint));
  }
  assert.equal(f.store.message(queued.id).attempts,0);assert.equal(f.store.message(oldUnknown.id).state,'unknown');assert.equal(f.codex.woken.length,0);
});

test('投递仍被阻塞时健康后台照常完成一分钟稳定恢复观察',async t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),endpoint=f.store.endpoint(f.x.id)!;f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  const peer=f.store.peer(f.x.id),queued=f.store.accept(pair,f.c,'等待投递策略恢复','queued');let now=0;
  const recovery=f.broker.managedRecovery;t.mock.method(recovery as any,'now',()=>now);t.mock.method(recovery as any,'monotonic',()=>now);
  const incident=recovery.observe(peer,pair,endpoint);recovery.settled(incident);(f.broker as any).incidents.failed(peer,endpoint,new BridgeError('-32600','投递协议仍被阻塞'),[queued.id]);
  for(;now<=60000;now+=5000)await(lifecycle(f.broker) as any).recoverPairedPeer(peer.id,true);
  assert.equal(recovery.get(peer.id)?.state,'closed');assert.equal(f.store.message(queued.id).state,'accepted');
});

for(const stage of ['pending_windows','pump'])test('路由阶段退避不阻止成员恢复：'+stage,async t=>{
  const f=fixture(t);f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.c.id,{owner:'nxbridge',state:'offline'});f.store.deactivate(f.c.id);
  for(let n=0;n<3;n++)f.store.faults.failed(stage,new Error('路由探测失败'));
  let attempts=0;t.mock.method(lifecycle(f.broker),'recover',async()=>{attempts++;return false;});
  await f.broker.pump();if(stage==='pump')await Promise.all((f.broker as any).schedulePairedRecoveries().values());assert.equal(attempts,1);assert.equal(f.store.faults.ready(stage),false);
});

test('一组配对恢复未结束时事件重入不扫描，定时调度仍恢复另一组且合并同成员',async t=>{
  const f=fixture(t),other=f.store.register(registration('claude','other')),otherPartner=f.store.register(registration('codex','other-partner'));f.store.pair(other.id,otherPartner.id);f.store.pair(f.c.id,f.x.id);f.store.updatePeer(other.id,{owner:'nxbridge'});f.store.updatePeer(f.c.id,{owner:'nxbridge',state:'offline'});f.store.deactivate(f.c.id);
  let entered!:()=>void,release!:()=>void;const started=new Promise<void>(resolve=>entered=resolve),gate=new Promise<void>(resolve=>release=resolve),attempts:string[]=[];
  t.mock.method(lifecycle(f.broker),'recover',async(peer:{id:string})=>{attempts.push(peer.id);if(peer.id===f.c.id){entered();await gate;}return false;});
  t.mock.method((f.broker as any).windowRecovery,'retry',async()=>{await gate;});
  const first=f.broker.pump();let tick:Promise<unknown>|undefined;
  try{
    await started;
                                             
    await lifecycle(f.broker).recoveryWork(other.id);f.store.updatePeer(other.id,{state:'offline'});f.store.deactivate(other.id);
    const facts=t.mock.method(f.store,'pendingFacts'),before=facts.mock.callCount();assert.equal(f.broker.pump(),first);assert.equal(facts.mock.callCount(),before,'进行中的事件消息泵不能扫描待发消息');
    tick=Promise.all((f.broker as any).schedulePairedRecoveries().values());
    await new Promise<void>(resolve=>setImmediate(resolve));assert.deepEqual(attempts,[f.c.id,other.id]);
  }finally{release();await Promise.all([first,tick]);}
});

test('一组配对有待发消息且恢复缓慢时消息泵仍处理新来信，恢复后自动投递原消息一次',async t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),endpoint=f.store.endpoint(f.c.id)!,other=f.store.register(registration('claude','other')),sender=f.store.register(registration('codex','other-sender')),otherPair=f.store.pair(other.id,sender.id);
  const queued=f.store.accept(pair,f.x,'等待慢恢复的原消息','slow');f.store.updatePeer(f.c.id,{owner:'nxbridge',state:'offline'});f.store.deactivate(f.c.id);
  let release!:()=>void,delivered!:()=>void;const gate=new Promise<void>(resolve=>release=resolve),resumed=new Promise<void>(resolve=>delivered=resolve);let attempts=0;
  t.mock.method(lifecycle(f.broker),'recover',async()=>{attempts++;await gate;f.store.saveEndpoint({...endpoint,active:true});f.store.updatePeer(f.c.id,{state:'ready'});return true;});
  const deliver=f.claude.deliver.bind(f.claude);t.mock.method(f.claude,'deliver',async(...args:Parameters<typeof deliver>)=>{const result=await deliver(...args);if(args[2].id===queued.id)delivered();return result;});
  const first=f.broker.pump(),recovery=lifecycle(f.broker).recoveryWork(f.c.id);let second:Promise<void>|undefined;
  try{
    await completePump(first);assert.equal((f.broker as any).pumping,undefined);assert.equal(f.store.message(queued.id).state,'accepted');assert.equal(f.store.message(queued.id).attempts,0);
    const incoming=f.store.accept(otherPair,sender,'恢复期间另一组配对的新来信','independent');second=f.broker.pump();await completePump(second);
    assert.equal(f.store.message(incoming.id).state,'runtimeQueued');assert.equal(attempts,1);assert.equal(f.store.message(queued.id).attempts,0);
    assert.equal(lifecycle(f.broker).retryPending(f.c.id),false);assert.equal((f.broker as any).incidents.current(f.store.peer(f.c.id),endpoint),undefined);
    release();await completePump(resumed);await(f.broker as any).pumping;assert.equal(f.store.message(queued.id).state,'runtimeQueued');assert.equal(lifecycle(f.broker).isRestoring(f.c.id),false);
    assert.deepEqual(f.claude.delivered.map(message=>message.id),[incoming.id,queued.id]);
  }finally{release();await Promise.all([first,second,recovery]);}
});

test('路由提前失败后异步恢复故障仍由成员边界接收，消息保留且不会触发重试环',async t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),queued=f.store.accept(pair,f.x,'路由失败时保留原消息','route-failed');f.store.updatePeer(f.c.id,{owner:'nxbridge',state:'offline'});f.store.deactivate(f.c.id);
  let release!:()=>void;const gate=new Promise<void>(resolve=>release=resolve);let attempts=0;
  t.mock.method(lifecycle(f.broker),'recover',async()=>{attempts++;await gate;throw Error('异步恢复故障');});t.mock.method((f.broker as any).windowRecovery,'retry',async()=>{throw Error('路由阶段故障');});
  const pumping=f.broker.pump(),recovery=lifecycle(f.broker).recoveryWork(f.c.id);
  try{
    await completePump(pumping);assert.equal((f.broker as any).pumping,undefined);release();assert.equal(await recovery,false);
    await new Promise<void>(resolve=>setImmediate(resolve));assert.equal(attempts,1);assert.equal(f.store.message(queued.id).state,'accepted');assert.equal(f.store.message(queued.id).attempts,0);assert.equal(lifecycle(f.broker).isRestoring(f.c.id),false);
    assert.ok(f.store.events().some(event=>event.type==='pump.failed'&&(event.data as any).stage==='member_recovery:'+f.c.id));
  }finally{release();await recovery;}
});

for(const unpaired of [false,true])test('路由期间新出现的在线目标不把空恢复检查当作后台恢复：'+(unpaired?'旧快照已解除配对':'新配对直接投递'),async t=>{
  const f=fixture(t),pump=t.mock.method(f.broker,'pump'),defer=t.mock.method(lifecycle(f.broker),'deferRecoveryDelivery'),pending=f.store.pendingFacts.bind(f.store);let messageId='',stale:ReturnType<typeof pending>|undefined;
  t.mock.method(f.store,'pendingFacts',(peerId?:string)=>stale??pending(peerId));
  t.mock.method((f.broker as any).windowRecovery,'retry',async()=>{
    if(messageId)return;
    const pair=f.store.pair(f.c.id,f.x.id),message=f.store.accept(pair,f.x,'在线目标的新来信','new-route');messageId=message.id;
    if(unpaired){stale=pending();f.store.unpair(pair.id);}
  });
  await completePump(f.broker.pump());await new Promise<void>(resolve=>setImmediate(resolve));
  assert.equal(pump.mock.callCount(),1,'空检查不能自行追加消息泵');assert.equal(defer.mock.callCount(),0);assert.equal(lifecycle(f.broker).isRestoring(f.c.id),false);
  assert.equal(f.store.message(messageId).state,unpaired?'accepted':'runtimeQueued');assert.equal(f.claude.delivered.length,unpaired?0:1);
  if(unpaired){await completePump(f.broker.pump());await new Promise<void>(resolve=>setImmediate(resolve));assert.equal(pump.mock.callCount(),2);assert.equal(f.store.message(messageId).attempts,0);}
});

for(const phase of ['stopped','handoff','user'] as const)test('统一恢复准入按 Claude 前台状态处理：'+phase,{skip:process.platform!=='linux'},async t=>{
  const f=fixture(t);f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.c.id,{owner:'nxbridge',state:'offline'});f.store.deactivate(f.c.id);
  mkdirSync(path.join(f.broker.home,'run'),{recursive:true});atomicJson(claudeChildFile(f.broker.home,f.c.id),{version:1,peerId:f.c.id,sessionId:f.c.sessionId,cwd:f.c.cwd,address:'test',phase,hasSession:true,updatedAt:Date.now()});
  let attempts=0;t.mock.method(lifecycle(f.broker),'recover',async()=>{attempts++;return false;});
  await(lifecycle(f.broker) as any).recoverPairedPeer(f.c.id,false);assert.equal(attempts,phase==='stopped'?1:0);
});

for(const state of ['paused','needsHuman'] as const)test('统一恢复准入保留人工暂停：'+state,async t=>{
  const f=fixture(t);f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.c.id,{owner:'nxbridge',state});f.store.deactivate(f.c.id);
  t.mock.method(lifecycle(f.broker),'recover',async()=>{assert.fail('人工暂停不能触发恢复');});await(lifecycle(f.broker) as any).recoverPairedPeer(f.c.id,true);
});

for(const replaced of [false,true])test('用户观察通道异步重连'+(replaced?'不能重新激活已替换端点':'保留连接期间更新的原生证据'),async t=>{
  const f=fixture(t),endpoint={...f.store.endpoint(f.x.id)!,bindingEpoch:'context-a',active:false};f.store.saveEndpoint(endpoint);
  let entered!:()=>void,finish!:()=>void;
  const started=new Promise<void>(resolve=>entered=resolve),gate=new Promise<void>(resolve=>finish=resolve);
  t.mock.method(f.codex,'connect',async()=>{entered();await gate;return{request:async()=>({})};});
  const pending=lifecycle(f.broker).recover(f.x);await started;
  const latest={...endpoint,epoch:replaced?'replacement':endpoint.epoch,active:replaced,seenAt:endpoint.seenAt+1,codexPermission:{sandbox:'workspace-write'},nativeHealth:{turnId:'turn',lastProgressAt:200,lastEventAt:200,state:'ready' as const,tools:[]}};
  f.store.saveEndpoint(latest);finish();assert.equal(await pending,!replaced);
  const current=f.store.endpoint(f.x.id)!;assert.equal(current.epoch,latest.epoch);assert.deepEqual(current.nativeHealth,latest.nativeHealth);assert.deepEqual(current.codexPermission,latest.codexPermission);assert.equal(current.bindingEpoch,'context-a');
  if(replaced)assert.equal(f.store.endpoints(f.x.id).find(e=>e.epoch===endpoint.epoch)!.active,false);
});

test('notLoaded 清除订阅缓存，恢复合并并发，断线重连沿用原线程且不重发 unknown',async t=>{
  t.mock.timers.enable({apis:['Date'],now:Date.now()});
  const server=new WebSocketServer({host:'127.0.0.1',port:0});await once(server,'listening');
  const address=`ws://127.0.0.1:${(server.address() as {port:number}).port}`,methods:{method:string;params:any}[]=[];
  const home=temporary(t),sessionId='same-thread',file=path.join(home,'history.jsonl'),runtimeId=randomUUID(),native=await processIdentity(process.pid);
  const row=(type:string,payload:unknown)=>JSON.stringify({type,payload})+'\n';
  const settings={permission_profile:{type:'disabled'},approval_policy:'never',approvals_reviewer:'user',model:'gpt-6.1-sol',reasoning_effort:'max',service_tier:'priority'};
  writeFileSync(file,row('session_meta',{id:sessionId})+row('turn_context',settings),'utf8');
  let status='notLoaded';
  server.on('connection',socket=>socket.on('message',raw=>{
    const request=JSON.parse(raw.toString());methods.push(request);
    if(request.id===undefined)return;
    if(request.method==='thread/resume')status='idle';
    if(request.method==='thread/settings/update')appendFileSync(file,row('event_msg',{type:'thread_settings_applied',thread_id:sessionId,thread_settings:settings}),'utf8');
    const thread={id:sessionId,path:file,cwd:process.cwd(),status:{type:status}};
    const result=request.method==='initialize'?{userAgent:'codex-tui/0.166.0 (test; test) test (codex-tui; 0.166.0)'}:request.method==='thread/loaded/list'?{data:status==='notLoaded'?[]:[sessionId]}:request.method==='thread/resume'?{thread,model:settings.model,reasoningEffort:settings.reasoning_effort,serviceTier:settings.service_tier,approvalPolicy:'never',approvalsReviewer:'user'}:request.method==='thread/read'?{thread}:request.method==='turn/start'?{turn:{id:'completion-wake'}}:{};
    socket.send(JSON.stringify({jsonrpc:'2.0',id:request.id,result}));
  }));
  const config={...DEFAULT_CONFIG,implicit:'never' as const},store=new Store(':memory:'),claude=new FakeAdapter();
  let codex=new CodexAdapter(config,home),broker=new Broker(store,config,{codex,claude} as unknown as Adapters,home);
  t.after(async()=>{await broker.stop();store.close();for(const socket of server.clients)socket.terminate();await new Promise<void>(r=>server.close(()=>r()));});
  const c=store.register(registration('claude','c')),x=store.register(registration('codex',sessionId,{cwd:process.cwd(),endpoint:{address,epoch:address,mode:'app-server',pid:native.pid,procStart:native.startedAt,runtimeId,nativeSession:{transcriptPath:file}}}));
  store.updatePeer(x.id,{owner:'nxbridge'});const pair=store.pair(c.id,x.id),endpoint=store.endpoint(x.id)!;
  saveManagedRuntime(home,{id:runtimeId,peerId:x.id,sessionId,owner:managedMarker(home),kind:'owned',state:'ready',pid:native.pid,procStart:native.startedAt,endpoint:address,createdAt:Date.now()});
  t.mock.method(codex as any,'verifyColdResumeRuntime',async()=>{});
  status='active';
  const unleased=await codex.connect(address);
  for(const method of ['thread/read','thread/resume','thread/queue/add','thread/settings/update','thread/unsubscribe','thread/delete','turn/start']){
    await assert.rejects(unleased.request(method,{threadId:sessionId},{mutation:method!=='thread/read'}),{code:'owned_subscription_waiting',phase:'prepare',uncertain:false});
    assert.equal(methods.some(value=>value.method===method),false);
  }
  await assert.rejects(codex.release(store.peer(x.id),endpoint),{code:'owned_subscription_waiting'});
  status='notLoaded';
                                      
  const restoreMember=async()=>{const pumping=broker.pump(),recovering=lifecycle(broker).recoveryWork(x.id);await Promise.all([pumping,recovering]);};
  await Promise.all([codex.attach(store.peer(x.id),endpoint),codex.attach(store.peer(x.id),endpoint)]);
  assert.equal(methods.filter(m=>m.method==='thread/resume').length,1);
  status='active';store.updatePeer(x.id,{state:'offline'});await restoreMember();
  assert.equal(store.peer(x.id).state,'busy');assert.equal(methods.filter(m=>m.method==='thread/resume').length,1);
  assert.equal(methods.some(m=>m.method==='thread/read'),true);
  status='notLoaded';const unloaded=once(codex,'state');for(const socket of server.clients)socket.send(JSON.stringify({method:'thread/status/changed',params:{threadId:x.sessionId,status:{type:'notLoaded'}}}));await unloaded;
  assert.equal(store.peer(x.id).state,'offline');
  await restoreMember();assert.equal(store.peer(x.id).state,'ready');assert.equal(methods.filter(m=>m.method==='thread/resume').length,2);
  const message=store.accept(pair,c,'结果不明，不可自动重发','uncertain');store.claim(message.id);store.transition(message.id,'unknown');
  status='notLoaded';const disconnected=once(codex,'disconnected');for(const socket of server.clients)socket.terminate();await disconnected;
  assert.equal(store.endpoint(x.id),undefined);await restoreMember();
  assert.equal(store.peer(x.id).sessionId,'same-thread');assert.equal(store.peer(x.id).state,'ready');assert.equal(store.message(message.id).state,'unknown');
  assert.equal(methods.some(m=>m.method==='thread/start'||m.method==='turn/start'),false);
  assert.ok(methods.filter(m=>m.method==='thread/resume').every(m=>m.params.threadId==='same-thread'));

                                             
  const ordinary=store.accept(pair,c,'等待原线程重新订阅','ordinary-after-restart');
  broker.subagentCompletions.record(store.peer(x.id),'child','child-turn',undefined,undefined);
  await broker.stop();
  codex=new CodexAdapter(config,home);broker=new Broker(store,config,{codex,claude} as unknown as Adapters,home);
  t.mock.method(codex as any,'verifyColdResumeRuntime',async()=>{});
  t.mock.method(codex as any,'completionNative',async()=>{});
  const before=methods.length;
  assert.equal(store.peer(x.id).state,'ready');
  assert.equal(store.endpoint(x.id)?.active,true);
  await restoreMember();
  const held=(await broker.dispatch('status',{}) as any).peers.find((p:any)=>p.id===x.id);
  assert.match(held.childCompletions.hold.reason,/刚重启.*核对主线程的进展/);
  assert.equal(held.childCompletions.hold.observing,true);
  assert.equal(held.childCompletions.wake,undefined);
  assert.equal(store.setting('codexWake:'+x.id),undefined);
  assert.equal(store.message(ordinary.id).attempts,0);
  assert.equal(broker.managedRecovery.get(x.id)?.attempts.length,0);
  assert.ok(methods.slice(before).some(m=>m.method==='thread/loaded/list'));
  assert.ok(methods.slice(before).every(m=>m.params?.threadId!==sessionId),'持有端点但无订阅时，不能进入线程串行队列');

  status='notLoaded';t.mock.timers.tick(31000);
  await restoreMember();
  await completePump(broker.pump());
  const requests=methods.slice(before),resumed=requests.filter(m=>m.method==='thread/resume');
  assert.equal(resumed.length,1);
  assert.deepEqual([resumed[0]!.params.threadId,resumed[0]!.params.model,resumed[0]!.params.config.model_reasoning_effort,resumed[0]!.params.serviceTier],
    [sessionId,'gpt-6.1-sol','max','priority']);
  assert.equal(requests.filter(m=>m.method==='turn/start').length,1);
  assert.equal(requests.some(m=>m.method==='thread/start'||m.method==='thread/queue/add'),false);
  assert.equal(store.message(ordinary.id).attempts,0);
  const actor={host:'codex',sessionId,memberId:x.id,callId:'consume-after-restart'};
  const prompt=await broker.dispatch('hook',{actor,session_id:sessionId,hook_event_name:'UserPromptSubmit',prompt:HEALTH_WAKE_NOTICE,turn_id:'completion-wake'}) as any;
  assert.match(prompt.hookSpecificOutput.additionalContext,/child-turn/);
  const receipt=await broker.dispatch('receive',actor) as any;
  assert.deepEqual(receipt.messages.map((m:any)=>m.message_id),[ordinary.id]);
  assert.deepEqual((await broker.dispatch('receive',{...actor,callId:'repeat-consume'}) as any).messages,[]);
  await completePump(broker.pump());
  assert.equal(methods.slice(before).filter(m=>m.method==='turn/start').length,1);
  assert.equal(store.message(ordinary.id).attempts,1);
  assert.equal(broker.managedRecovery.get(x.id)?.attempts.length,0);
});
