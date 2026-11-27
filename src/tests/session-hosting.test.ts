import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { appendFileSync, writeFileSync } from 'node:fs';
import { RpcClient } from '../ipc/rpc.js';
import { Store } from '../storage/store.js';
import { readSessionProfile, resumeSession, snapshotLiveSession, verifyCodexResumeAck, verifyRestartProfileCurrent, planTakenOverCodexResume } from '../hosts/codex/codex-session-profile.js';
import { closeHostedWindow, hostingGate, hostingKey, hostingRecord, hostingStatus, SessionTransfers, writeHosting, profileVersionSupported } from '../broker/recovery/session-hosting.js';
import { WindowRecovery } from '../broker/recovery/window-recovery.js';
import { hostingArguments } from '../app/commands/hosting-command.js';
import { managedCodexRegistration } from '../hosts/managed-registration.js';
import { saveManagedRuntime, runtimeFor, removeManagedRuntime } from '../hosts/managed-runtimes.js';
import { managedMarker } from '../install/installation.js';
import {BridgeError} from '../shared/errors.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {fixture, lifecycle} from './support/broker-fixture.js';
import { registration } from './support/registration.js';
import { temporary } from './support/temporary.js';
import { FakeAdapter } from './support/fake-adapter.js';
import { CodexAdapter } from '../hosts/codex/adapter.js';
import { Broker, type Adapters } from '../broker/coordinator.js';
import { bootstrapPolicyRejected, restoreBootstrapKey } from '../hosts/codex/codex-isolation.js';
import { selectHostLaunch } from '../hosts/host-launch.js';
import { processIdentity } from '../process/process.js';
import { Takeovers } from '../broker/recovery/takeover.js';

const row=(type:string,payload:unknown)=>JSON.stringify({type,payload})+'\n';

test('持有者快照中的进程已退出时继续核验，未知身份或代次变化仍阻止接管',async t=>{
  const {store,x}=fixture(t),home=temporary(t);let failure:unknown=new BridgeError('process_missing','快照后退出');
  const transfers=new SessionTransfers(store,DEFAULT_CONFIG,home,{holder:async()=> 'dead',prepare:async()=>{},stop:async()=>{},enabled:()=>{},processes:async()=>[{pid:987654,startedAt:'old'}],identity:async()=>{if(failure)throw failure;return{pid:987654,parentPid:1,startedAt:'new',name:'codex.exe'};}});
  await(transfers as any).assertNoExternalHolder(x);
  failure=new BridgeError('process_identity_unknown','查询失败');await assert.rejects((transfers as any).assertNoExternalHolder(x),{code:'process_identity_unknown'});
  failure=undefined;await assert.rejects((transfers as any).assertNoExternalHolder(x),{code:'holder_unknown'});
});
const profile={type:'managed',file_system:{type:'restricted',entries:[{path:{type:'special',value:{kind:'root'}},access:'none'}]},network:'restricted'};

for(const kind of ['identity','incomplete','truncated','changed'] as const)test('原会话历史不足和已确认设置矛盾使用不同的阻塞原因：'+kind,async t=>{
  const home=temporary(t),file=path.join(home,'profile-classification.jsonl'),id='profile-classification';
  const settings={cwd:home,model:'model-a',model_provider:'fixture',reasoning_effort:'high',service_tier:'priority',approval_policy:'never',approvals_reviewer:'user',permission_profile:profile};
  const start=row('session_meta',{id})+row('turn_context',settings),full=start+row('turn_context',settings);
  writeFileSync(file,full,'utf8');const original=await readSessionProfile(file,id);assert.equal(original.verified,true);assert.equal(original.complete,true);
  if(kind==='identity')writeFileSync(file,row('session_meta',{id:'other'})+row('turn_context',settings),'utf8');
  else if(kind==='truncated')writeFileSync(file,start,'utf8');
  else if(kind==='changed')appendFileSync(file,row('turn_context',{...settings,approval_policy:'on-request'}),'utf8');
  const current=kind==='incomplete'?{...original,complete:false}:undefined;
  await assert.rejects(verifyRestartProfileCurrent(original,{exact:true,...(current?{current}:{})}),{code:kind==='changed'?'restart_profile_changed':'restart_profile_unverified'});
});

test('权限读取按持久顺序覆盖、增量跟随半行并拒绝错误身份',async t=>{
  const home=temporary(t),file=path.join(home,'history.jsonl');
  writeFileSync(file,row('session_meta',{id:'original',cwd:home})+row('turn_context',{model:'model-a',effort:'low',sandbox_policy:{type:'read-only'},approval_policy:'never'})+row('event_msg',{type:'thread_settings_applied',thread_id:'original',thread_settings:{model:'model-b',reasoning_effort:'high',permission_profile:profile,approval_policy:'on-request',approvals_reviewer:'user'}}),'utf8');
  const first=await readSessionProfile(file,'original');assert.equal(first.verified,true);assert.equal(first.settings.model,'model-b');assert.equal(first.settings.reasoningEffort,'high');assert.equal(first.settings.sandbox,undefined);
  const next=row('turn_context',{effort:'medium'});appendFileSync(file,next.slice(0,-2),'utf8');
  const partial=await readSessionProfile(file,'original');assert.equal(partial.complete,false);assert.equal(partial.settings.reasoningEffort,'high');
  appendFileSync(file,next.slice(-2),'utf8');assert.equal((await readSessionProfile(file,'original')).settings.reasoningEffort,'medium');
  assert.equal((await readSessionProfile(file,'another')).verified,false);
});

test('恢复在第一条原生请求中限制权限，并核验新落盘的完整 profile',async t=>{
  const home=temporary(t),file=path.join(home,'history.jsonl'),calls:any[]=[];
  writeFileSync(file,row('session_meta',{id:'original',cwd:home})+row('turn_context',{cwd:home,model:'model-a',effort:'low',approval_policy:'on-request',approvals_reviewer:'user',permission_profile:profile}),'utf8');
  const rpc=new RpcClient(message=>{
    calls.push(message);
    if(message.method==='thread/settings/update')appendFileSync(file,row('event_msg',{type:'thread_settings_applied',thread_id:'original',thread_settings:{permission_profile:profile,approval_policy:'on-request',approvals_reviewer:'user'}}),'utf8');
    queueMicrotask(()=>rpc.receive({id:message.id,result:message.method==='thread/resume'?{thread:{id:'original',path:file},cwd:home,model:'model-a',reasoningEffort:'low',approvalPolicy:'on-request',approvalsReviewer:'user'}:{}}));
  },()=>{});t.after(()=>rpc.close());
  const restored=await resumeSession(rpc,'original',{file,cwd:home});
  assert.equal(calls[0].method,'thread/resume');assert.equal(calls[0].params.config['permissions.nxbridge_transfer'].filesystem[':root'],'none');
  assert.equal(restored.policy.source,'persisted');assert.deepEqual(restored.policy.unknown,[]);assert.ok(calls.every(c=>!c.method.startsWith('turn/')));
});

test('窗口关闭保留原配对和 accepted；发送只保存一次，等待后不重复弹选择',async t=>{
  const {store,broker,c,x,codex}=fixture(t),pair=store.pair(c.id,x.id);
  closeHostedWindow(store,x);const actor={host:'claude' as const,sessionId:c.sessionId!,callId:'closed-send'};
  const first=await broker.dispatch('send',{actor,message:'原会话任务'}) as any;
  assert.equal(first.status,'selection_required');assert.equal(first.reason,'hosting');assert.equal(first.persisted,true);assert.equal(store.messages().length,1);
  assert.equal(first.hosting.phase,'window_closed');
  assert.equal(store.message(first.message_id).state,'accepted');await broker.pump();assert.equal(codex.delivered.length,0);
  const repeated=await broker.dispatch('send',{actor,message:'原会话任务'}) as any;assert.equal(repeated.message_id,first.message_id);assert.equal(store.messages().length,1);
  await broker.dispatch('pair.selection.commit',{actor,selection_id:first.selection_id,action:'wait'});
  const next=await broker.dispatch('send',{actor:{...actor,callId:'after-wait'},message:'继续等待'}) as any;
  assert.equal(next.status,'held');assert.equal(store.pairFor(x.id)?.id,pair.id);assert.equal(hostingStatus(store,store.peer(x.id))?.preference,'user');
});

for(const mode of ['embedded','app-server'] as const)test('释放后通过 '+mode+' 重入接续同一成员、配对和未确认状态',async t=>{
  const store=new Store(':memory:');t.after(()=>store.close());const home=temporary(t),cwd=home;
  const c=store.register(registration('claude','c',{cwd})),x=store.register(registration('codex','original',{cwd,endpoint:{mode:'app-server',address:'old',epoch:'old',pid:10,procStart:'old',configHome:home}})),pair=store.pair(c.id,x.id);
  writeHosting(store,x,{background:false,released:true,phase:'awaiting_window'},home);closeHostedWindow(store,x);
  const pending=store.accept(pair,c,'待发','pending'),unknown=store.accept(pair,c,'未知','unknown');store.claim(unknown.id);store.transition(unknown.id,'unknown');
  const recovery=new WindowRecovery(store,async()=>['alive'],undefined,async()=> 'dead');
  const joined=await recovery.register(registration('codex','original',{cwd,endpoint:{mode,address:'new',epoch:'new',pid:20,procStart:'new',configHome:home}}));
  assert.equal(joined.id,x.id);assert.equal(store.pairFor(x.id)?.id,pair.id);assert.equal(hostingGate(store,joined),false);
  assert.equal(hostingRecord(store,joined)?.released,false);assert.equal(store.message(pending.id).state,'accepted');assert.equal(store.message(unknown.id).state,'unknown');
});

test('Codex 同线程但不同 home 不按 session ID 合并',async t=>{
  const store=new Store(':memory:');t.after(()=>store.close());const home=temporary(t);
  const old=store.register(registration('codex','same',{endpoint:{mode:'embedded',address:'',epoch:'old',pid:10,procStart:'old',configHome:home}}));
  const recovery=new WindowRecovery(store,async()=>['alive','dead']);
  const fresh=await recovery.register(registration('codex','same',{endpoint:{mode:'embedded',address:'',epoch:'new',pid:20,procStart:'new',configHome:path.join(home,'other')}}));
  assert.notEqual(fresh.id,old.id);
});

test('打开中的 adopt 只记录意愿，未知持有者不启动后台',async t=>{
  const store=new Store(':memory:');t.after(()=>store.close());const home=temporary(t),c=store.register(registration('claude','c')),x=store.register(registration('codex','x',{endpoint:{...registration('codex','x').endpoint,configHome:home}}));store.pair(c.id,x.id);
  let state:'alive'|'unknown'='alive',starts=0;
  const transfers=new SessionTransfers(store,DEFAULT_CONFIG,home,{holder:async()=>state,prepare:async()=>{starts++;},stop:async()=>{},enabled:()=>{}});
  await transfers.adopt(x);assert.equal(starts,0);assert.equal(store.peer(x.id).owner,'user');assert.equal(hostingRecord(store,x)?.background,true);assert.equal(hostingGate(store,x),false);
  state='unknown';await assert.rejects(transfers.adopt(x),{code:'holder_unknown'});assert.equal(starts,0);
});

test('专用后台 MCP 在端点激活前登记仍使用 starting 启动证明，不能成为用户窗口',async t=>{
  const store=new Store(':memory:');t.after(()=>store.close());const home=temporary(t),c=store.register(registration('claude','c')),x=store.register(registration('codex','same'));store.pair(c.id,x.id);store.updatePeer(x.id,{owner:'nxbridge'});store.deactivate(x.id);
  saveManagedRuntime(home,{id:randomUUID(),owner:managedMarker(home),kind:'owned',state:'starting',endpoint:'ws://127.0.0.1:1234',createdAt:Date.now(),peerId:x.id,sessionId:'same',pid:123,procStart:'generation',configHome:home});
  const input=registration('codex','same',{endpoint:{mode:'embedded',address:'',epoch:'mcp',pid:123,procStart:'generation',configHome:home}});
  store.updatePeer(x.id,{state:'busy'});
  const joined=await managedCodexRegistration(store,home,input);assert.equal(joined?.id,x.id);assert.equal(store.peer(x.id).owner,'nxbridge');assert.equal(store.peer(x.id).state,'busy');assert.equal(store.peers().length,2);assert.equal(store.bindings.current(x.id),undefined);
  await managedCodexRegistration(store,home,{...input,state:'ready',endpoint:{...input.endpoint,mode:'app-server'}});assert.equal(store.peer(x.id).state,'ready');
  await assert.rejects(managedCodexRegistration(store,home,{...input,sessionId:'other'}),{code:'managed_proof_unverified'});assert.equal(store.peer(x.id).sessionId,'same');assert.equal(hostingGate(store,store.peer(x.id)),false);
});

test('托管参数拒绝歧义，版本比较处理正式版和预发布数字',()=>{
  assert.deepEqual(hostingArguments(['codex','--session','same']),{session_id:'same'});
  for(const args of [['claude'],['codex','--home'],['codex','--pair','a','--member','b'],['codex','--session','a','--session','b']])assert.throws(()=>hostingArguments(args),{code:'invalid_arguments'});
  assert.equal(profileVersionSupported('0.166.0','0.166.0'),true);assert.equal(profileVersionSupported('0.166.0','0.167.0-alpha.1'),false);assert.equal(profileVersionSupported('0.167.0-alpha.10','0.167.0-alpha.2'),true);
});

test('关闭托管表单只取消选择，下一次发送仍询问，消息不丢失',async t=>{
  const {store,broker,c,x}=fixture(t);store.pair(c.id,x.id);closeHostedWindow(store,x);
  const actor={host:'claude' as const,sessionId:c.sessionId!,callId:'cancel-hosting'};
  const first=await broker.dispatch('send',{actor,message:'第一条'}) as any;
  assert.ok(first.allowed_actions.includes('cancel'));
  const cancelled=await broker.dispatch('pair.selection.commit',{actor,selection_id:first.selection_id,action:'cancel'}) as any;
  assert.equal(cancelled.status,'held');assert.equal(cancelled.message_id,first.message_id);assert.equal(cancelled.persisted,true);assert.ok(cancelled.hosting);
  assert.equal(hostingRecord(store,x)?.released,undefined);
  const next=await broker.dispatch('send',{actor:{...actor,callId:'after-cancel'},message:'第二条'}) as any;
  assert.equal(next.status,'selection_required');assert.equal(next.reason,'hosting');
  assert.ok(store.messages().every(message=>message.state==='accepted'&&message.attempts===0));
});

for(const owner of ['user','nxbridge'] as const)test(owner+' 的首次连接不通过恢复请求改变加载中的会话设置',async t=>{
  const home=temporary(t),store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home),calls:any[]=[];
  t.after(()=>{adapter.close();store.close();});
  const peer=store.register(registration('codex','loaded'));store.updatePeer(peer.id,{owner});const endpoint=store.endpoint(peer.id)!;
  if(owner==='nxbridge')saveManagedRuntime(home,{id:randomUUID(),peerId:peer.id,sessionId:'loaded',owner:managedMarker(home),kind:'owned',state:'ready',dedicated:true,pid:endpoint.pid!,procStart:endpoint.procStart!,endpoint:endpoint.address,createdAt:Date.now()});
  const rpc=new RpcClient(message=>{calls.push(message);queueMicrotask(()=>rpc.receive({id:message.id,result:message.method==='thread/loaded/list'?{data:['loaded']}:{thread:{id:'loaded',cwd:peer.cwd,status:{type:'idle'}}}}));},()=>{});
  adapter.connect=async()=>rpc;t.after(()=>rpc.close());
  (adapter as any).clients.set(endpoint.address,rpc);
  if(owner==='nxbridge')await assert.rejects(adapter.attach(store.peer(peer.id),endpoint,{verify:true}),{code:'owned_subscription_waiting'});
  else await adapter.attach(store.peer(peer.id),endpoint,{verify:true});
  assert.deepEqual(calls.filter(call=>call.method==='thread/resume').map(call=>call.params),owner==='user'?[{threadId:'loaded',excludeTurns:true}]:[]);
  assert.equal(calls.some(call=>call.method==='thread/settings/update'),false);
});

test('接手核验期间持有者变化或用户选择等待，均不能启动第二个运行时',async t=>{
  for(const action of ['reopen','wait'] as const){
    const store=new Store(':memory:');t.after(()=>store.close());const home=temporary(t),c=store.register(registration('claude','c')),x=store.register(registration('codex','x',{endpoint:{...registration('codex','x').endpoint,configHome:home}}));store.pair(c.id,x.id);
    let finish!:(state:'dead')=>void,starts=0;const holder=new Promise<'dead'>(resolve=>finish=resolve);
    const transfers=new SessionTransfers(store,DEFAULT_CONFIG,home,{holder:()=>holder,prepare:async()=>{starts++;},stop:async()=>{},enabled:()=>{}});
    const adopting=assert.rejects(transfers.adopt(x),{code:'session_changed'});
    let waiting:Promise<unknown>|undefined;
    if(action==='reopen')store.saveEndpoint({...store.endpoint(x.id)!,epoch:'reopened',seenAt:Date.now()});else waiting=transfers.wait(store.peer(x.id));
    finish('dead');await adopting;await waiting;assert.equal(starts,0);assert.equal(store.peer(x.id).owner,'user');
    if(action==='wait')assert.equal(hostingStatus(store,store.peer(x.id))?.phase,'awaiting_window');
    else assert.ok(hostingRecord(store,x)!.nextAt!>Date.now());
  }
});

test('释放尚未就绪的启动预约先完成账本清理，再报告等待用户窗口',async t=>{
  const store=new Store(':memory:');t.after(()=>store.close());const home=temporary(t),c=store.register(registration('claude','c')),x=store.register(registration('codex','x'));store.pair(c.id,x.id);store.updatePeer(x.id,{owner:'nxbridge'});
  saveManagedRuntime(home,{id:randomUUID(),peerId:x.id,owner:managedMarker(home),kind:'owned',state:'starting',endpoint:'ws://127.0.0.1:1234',createdAt:Date.now()});
  let cleaned=0;const transfers=new SessionTransfers(store,DEFAULT_CONFIG,home,{holder:async()=> 'dead',prepare:async()=>{},stop:async()=>assert.fail('不能严格停止无原生 PID 的预约'),enabled:()=>{},cleanup:async()=>{assert.equal(hostingStatus(store,store.peer(x.id))?.phase,'releasing');removeManagedRuntime(runtimeFor(home,{peerId:x.id})!);cleaned++;}});
  const result=await transfers.release(store.peer(x.id)) as any;assert.equal(cleaned,1);assert.equal(result.hosting.phase,'awaiting_window');assert.equal(runtimeFor(home,{peerId:x.id}),undefined);
});

test('外部后台只按原生代次判断会话持有者，容器遗留不阻止原窗口恢复',async t=>{
  const f=fixture(t),home=temporary(t),identity=await processIdentity(process.pid);
  f.store.pair(f.c.id,f.x.id);writeHosting(f.store,f.x,{background:true},home);
  const runtime={id:randomUUID(),peerId:randomUUID(),owner:managedMarker(home),kind:'owned' as const,state:'ready' as const,endpoint:'unix://retired-container',createdAt:Date.now(),configHome:home,pid:2147483647,procStart:'dead',wrapperPid:identity.pid,wrapperProcStart:identity.startedAt};saveManagedRuntime(home,runtime);
  let loads=0;const transfers=new SessionTransfers(f.store,DEFAULT_CONFIG,home,{holder:async()=> 'dead',prepare:async()=>{},stop:async()=>{},enabled:()=>{},processes:async()=>[],loaded:async()=>{loads++;return'unknown';}});
  await(transfers as any).assertNoExternalHolder(f.x);assert.equal(loads,0);assert.ok(runtimeFor(home,{id:runtime.id}));
  saveManagedRuntime(home,{...runtime,pid:identity.pid,procStart:identity.startedAt,wrapperPid:-1,wrapperProcStart:'unknown'});
  await assert.rejects((transfers as any).assertNoExternalHolder(f.x),{code:'holder_unknown'});assert.equal(loads,1);
});

test('释放已退出的原生后台允许打开原会话，未知容器记录继续保留',async t=>{
  const f=fixture(t),home=temporary(t);f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  const runtime={id:randomUUID(),peerId:f.x.id,owner:managedMarker(home),kind:'owned' as const,state:'ready' as const,endpoint:'unix://unknown-cleanup',createdAt:Date.now(),pid:2147483647,procStart:'dead',wrapperPid:-1,wrapperProcStart:'unknown'};saveManagedRuntime(home,runtime);
  const transfers=new SessionTransfers(f.store,DEFAULT_CONFIG,home,{holder:async()=> 'dead',prepare:async()=>{},stop:async()=>assert.fail('原生代次已退出，不得再停止旧端点'),cleanup:async()=>assert.fail('窗口恢复不等待未知容器回收'),enabled:()=>{}});
  const result=await transfers.release(f.store.peer(f.x.id)) as any;
  assert.equal(result.hosting.phase,'awaiting_window');assert.equal(f.store.peer(f.x.id).owner,'user');assert.equal(runtimeFor(home,{id:runtime.id})!.value.state,'failed');assert.equal(runtimeFor(home,{id:runtime.id})!.value.wrapperPid,-1);
});

test('托管子代理不阻塞根成员，错误根线程卸载后解除冲突',async t=>{
  const f=fixture(t),home=f.broker.home;f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  const endpoint=f.store.endpoint(f.x.id)!;saveManagedRuntime(home,{id:randomUUID(),peerId:f.x.id,owner:managedMarker(home),kind:'owned',state:'ready',endpoint:endpoint.address,createdAt:Date.now()});
  f.codex.emit('register',registration('codex','child',{kind:'subagent',parentId:f.x.sessionId!,endpoint:{...endpoint,epoch:'child'}}));
  assert.equal(hostingGate(f.store,f.store.peer(f.x.id)),false);assert.equal(f.store.find('codex','child')?.kind,'subagent');
  f.codex.emit('register',registration('codex','wrong-root',{endpoint:{...endpoint,epoch:'wrong'}}));
  assert.equal(hostingGate(f.store,f.store.peer(f.x.id)),true);
  f.codex.emit('state','wrong-root','offline',endpoint.address);assert.equal(hostingGate(f.store,f.store.peer(f.x.id)),false);
});

for(const valid of [true,false])test('当前连接已有订阅时可继续比较中断核验的持久证据：'+(valid?'完成核验':'不匹配继续暂存'),async t=>{
  const home=temporary(t),file=path.join(home,'history.jsonl'),store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home);t.after(()=>{adapter.close();store.close();});
  writeFileSync(file,row('session_meta',{id:'interrupted'})+row('turn_context',{permission_profile:profile,approval_policy:'on-request',approvals_reviewer:'user',model:'fixture-model',reasoning_effort:'high',service_tier:'priority'}),'utf8');
  const original=await readSessionProfile(file,'interrupted');
  appendFileSync(file,row('event_msg',{type:'thread_settings_applied',thread_id:'interrupted',thread_settings:{permission_profile:valid?profile:{type:'disabled'}}}),'utf8');
  const peer=store.register(registration('codex','interrupted'));store.updatePeer(peer.id,{owner:'nxbridge'});const endpoint=store.endpoint(peer.id)!;
  Object.assign(endpoint,{runtimeId:randomUUID(),pid:process.pid,procStart:'fixture-native'});
  saveManagedRuntime(home,{id:endpoint.runtimeId!,peerId:peer.id,owner:managedMarker(home),kind:'owned',state:'ready',dedicated:true,pid:endpoint.pid!,procStart:endpoint.procStart!,endpoint:endpoint.address,createdAt:Date.now(),sessionVerification:{sessionId:'interrupted',profile:original}});
  const calls:string[]=[],rpc=new RpcClient(message=>{calls.push(message.method!);queueMicrotask(()=>rpc.receive({id:message.id,result:{thread:{id:'interrupted',path:file,status:{type:'idle'}},model:'fixture-model',reasoningEffort:'high',serviceTier:'priority',approvalPolicy:'on-request',approvalsReviewer:'user'}}));},()=>{});adapter.connect=async()=>rpc;t.after(()=>rpc.close());
  (adapter as any).clients.set(endpoint.address,rpc);
  (adapter as any).ownedRoutes.subscribed({peer:store.peer(peer.id),endpoint},rpc);
  if(valid)await adapter.attach(store.peer(peer.id),endpoint,{verify:true});else await assert.rejects(adapter.attach(store.peer(peer.id),endpoint,{verify:true}),{code:'policy_verification_failed'});
  assert.equal(!!runtimeFor(home,{peerId:peer.id})?.value.sessionVerification,!valid);assert.equal(calls.includes('thread/settings/update'),false);
});

for(const loaded of [false,true])test('托管故障恢复和协调器中断后重连均独立核验 null 档位投影：'+(loaded?'已加载':'冷恢复'),async t=>{
  const home=temporary(t),sessionId='opaque-recovery',file=path.join(home,'history.jsonl'),store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home);t.after(()=>{adapter.close();store.close();});
  const settings={permission_profile:profile,approval_policy:'on-request',approvals_reviewer:'user',model:'fixture-model',reasoning_effort:'high',service_tier:'opaque-tier-a'};
  const applied=()=>row('event_msg',{type:'thread_settings_applied',thread_id:sessionId,thread_settings:settings});
  writeFileSync(file,row('session_meta',{id:sessionId})+applied(),'utf8');
  const original=await readSessionProfile(file,sessionId),peer=store.register(registration('codex',sessionId));store.updatePeer(peer.id,{owner:'nxbridge'});
  const endpoint=store.endpoint(peer.id)!,runtimeId=randomUUID();
  Object.assign(endpoint,{runtimeId,pid:process.pid,procStart:'fixture-native',nativeSession:{transcriptPath:file}});
  saveManagedRuntime(home,{id:runtimeId,peerId:peer.id,owner:managedMarker(home),kind:'owned',state:'ready',pid:endpoint.pid!,procStart:endpoint.procStart!,endpoint:endpoint.address,createdAt:Date.now(),...(loaded?{sessionVerification:{sessionId,profile:original}}:{})});
  const calls:{method:string;params:any}[]=[];
  const rpc=new RpcClient(message=>{
    calls.push({method:message.method!,params:message.params});
    if(message.method==='thread/settings/update'){
      assert.equal(Object.hasOwn(message.params,'serviceTier'),false);
      appendFileSync(file,applied(),'utf8');
    }
    queueMicrotask(()=>rpc.receive({id:message.id,result:message.method==='thread/loaded/list'?{data:loaded?[sessionId]:[]}:{thread:{id:sessionId,path:file,status:{type:'idle'}},model:'fixture-model',reasoningEffort:'high',serviceTier:null,approvalPolicy:'on-request',approvalsReviewer:'user'}}));
  },()=>{});adapter.connect=async()=>rpc;t.after(()=>rpc.close());
  (adapter as any).clients.set(endpoint.address,rpc);
  t.mock.method(adapter as any,'verifyColdResumeRuntime',async()=>{});
  if(loaded){
    await assert.rejects(adapter.attach(store.peer(peer.id),endpoint),{code:'owned_subscription_waiting',phase:'prepare'});
    assert.deepEqual(calls.map(call=>call.method),['thread/loaded/list']);
    assert.ok(runtimeFor(home,{id:runtimeId})!.value.sessionVerification);
    return;
  }
  await adapter.attach(store.peer(peer.id),endpoint);
  assert.equal(runtimeFor(home,{id:runtimeId})!.value.sessionVerification,undefined);
  assert.equal(calls.some(call=>call.method==='turn/start'),false);
  assert.ok(calls.some(call=>call.method==='thread/settings/update'));
  if(!loaded)assert.equal(calls.find(call=>call.method==='thread/resume')!.params.serviceTier,'opaque-tier-a');
});

for(const state of ['初查仍加载','复查重新加载','原生通知重新加载','仅已完成子代理保持加载'] as const)test('普通来信只恢复已确认卸载的原线程：'+state,async t=>{
  const home=temporary(t),sessionId='ordinary-owned-cold',file=path.join(home,'history.jsonl'),store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home);t.after(()=>{adapter.close();store.close();});
  const settings={permission_profile:{type:'disabled'},approval_policy:'never',approvals_reviewer:'user',model:'gpt-6.1-sol',reasoning_effort:'max',service_tier:'priority'};
  const applied=()=>row('event_msg',{type:'thread_settings_applied',thread_id:sessionId,thread_settings:settings});
  writeFileSync(file,row('session_meta',{id:sessionId})+applied(),'utf8');
  const peer=store.register(registration('codex',sessionId));store.updatePeer(peer.id,{owner:'nxbridge',model:'gpt-6.1-sol',effort:'max'});const endpoint=store.endpoint(peer.id)!;
  const runtimeId=randomUUID();Object.assign(endpoint,{runtimeId,pid:process.pid,procStart:'fixture-native',nativeSession:{transcriptPath:file}});
  saveManagedRuntime(home,{id:runtimeId,peerId:peer.id,sessionId,owner:managedMarker(home),kind:'owned',state:'ready',pid:endpoint.pid!,procStart:endpoint.procStart!,endpoint:endpoint.address,createdAt:Date.now()});
  const calls:{method:string;params:any}[]=[],child='completed-but-open';let lists=0;
  const rpc=new RpcClient(message=>{
    calls.push({method:message.method!,params:message.params});
    const method=message.method;
    assert.ok(['thread/loaded/list','thread/resume','thread/settings/update','turn/start'].includes(method!));
    if(method==='thread/loaded/list')lists++;
    else assert.equal(message.params?.threadId,sessionId);
    if(message.method==='thread/settings/update')appendFileSync(file,applied(),'utf8');
    if(message.method==='turn/start')assert.equal(runtimeFor(home,{id:runtimeId})!.value.sessionVerification,undefined,'首条输入前必须结清恢复核验');
    const loaded=state==='初查仍加载'?[sessionId]:state==='复查重新加载'&&lists===2?[sessionId,child]:[child];
    const result=method==='thread/loaded/list'?{data:loaded}:method==='thread/resume'?{thread:{id:sessionId,path:file,status:{type:'idle'}},model:message.params.model??'gpt-6.2-astra',reasoningEffort:message.params.config?.model_reasoning_effort??'low',serviceTier:message.params.serviceTier??'opaque-tier-a',approvalPolicy:'never',approvalsReviewer:'user',sandbox:{type:'dangerFullAccess'}}:method==='turn/start'?{turn:{id:'ordinary-input'}}:{};
    queueMicrotask(()=>{
      if(state==='原生通知重新加载'&&method==='thread/loaded/list'&&lists===2)
        (adapter as any).notify(endpoint.address,{method:'thread/status/changed',params:{threadId:sessionId,status:{type:'idle'}}});
      rpc.receive({id:message.id,result});
    });
  },()=>{});adapter.connect=async()=>rpc;t.after(()=>rpc.close());
  (adapter as any).clients.set(endpoint.address,rpc);
  const checks={pid:process.pid,procStart:'fixture-native',pending:{[child]:'unconfirmed-child-probe'}};
  t.mock.method(adapter as any,'verifyColdResumeRuntime',async()=>{
    const {value}=runtimeFor(home,{id:runtimeId})!;saveManagedRuntime(home,{...value,childControlChecks:checks});
  });
  if(state==='初查仍加载'){
    await assert.rejects(adapter.wake(store.peer(peer.id),endpoint),{code:'owned_subscription_waiting'});
    assert.deepEqual(calls.map(call=>call.method),['thread/loaded/list']);
    await adapter.discoverLoaded(endpoint.address,rpc);
    (adapter as any).notify(endpoint.address,{method:'turn/started',params:{threadId:sessionId}});
    assert.deepEqual(calls.map(call=>call.method),['thread/loaded/list','thread/loaded/list']);
    return;
  }
  if(state!=='仅已完成子代理保持加载'){
    await assert.rejects(adapter.wake(store.peer(peer.id),endpoint),{code:'owned_cold_resume_waiting'});
    assert.deepEqual(calls.map(call=>call.method),['thread/loaded/list','thread/loaded/list']);
    assert.equal(runtimeFor(home,{id:runtimeId})!.value.sessionVerification,undefined);
    return;
  }
  await adapter.wake(store.peer(peer.id),endpoint);
  assert.deepEqual(calls.slice(0,3).map(call=>call.method),['thread/loaded/list','thread/loaded/list','thread/resume']);
  assert.deepEqual(runtimeFor(home,{id:runtimeId})!.value.childControlChecks,checks);
  const resumed=calls.find(call=>call.method==='thread/resume')!;
  assert.equal(resumed.params.threadId,sessionId);assert.equal(resumed.params.model,'gpt-6.1-sol');assert.equal(resumed.params.config.model_reasoning_effort,'max');assert.equal(resumed.params.serviceTier,'priority');
  assert.ok(calls.findIndex(call=>call.method==='turn/start')>calls.findIndex(call=>call.method==='thread/settings/update'));
});

for(const proof of ['缺失','其他成员','缺少进程代次'] as const)test('托管根线程缺少完整运行时归属时保持等待：'+proof,async t=>{
  const home=temporary(t),store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home);t.after(()=>{adapter.close();store.close();});
  const peer=store.register(registration('codex','owned-no-proof'));store.updatePeer(peer.id,{owner:'nxbridge'});const endpoint=store.endpoint(peer.id)!;
  if(proof!=='缺失')saveManagedRuntime(home,{id:randomUUID(),peerId:proof==='其他成员'?randomUUID():peer.id,owner:managedMarker(home),kind:'owned',state:'ready',endpoint:endpoint.address,createdAt:Date.now()});
  const rpc=new RpcClient(()=>assert.fail('归属无法核实时不能请求原生线程'),()=>{});
  adapter.connect=async()=>rpc;(adapter as any).clients.set(endpoint.address,rpc);
  await assert.rejects(adapter.attach(store.peer(peer.id),endpoint),{code:'owned_route_unverified',phase:'prepare'});
});

test('一个托管线程的对账未返回时，协调器和另一托管线程仍能启动',async t=>{
  const {store,broker,c,x,codex}=fixture(t);
  const peer=store.register(registration('codex','另一托管线程',{state:'ready'})),partner=store.register(registration('claude','另一搭档'));
  store.pair(c.id,x.id);store.pair(partner.id,peer.id);
  for(const member of [x,peer])store.updatePeer(member.id,{owner:'nxbridge',state:'ready'});
  let release!:()=>void,healthyReady!:()=>void,slowStarted!:()=>void;
  const pending=new Promise<void>(resolve=>release=resolve),healthy=new Promise<void>(resolve=>healthyReady=resolve),slow=new Promise<void>(resolve=>slowStarted=resolve);
  (codex as any).reconcileOwnedRoute=({peer:member}:{peer:typeof x})=>{
    if(member.id===x.id){slowStarted();return pending;}
    healthyReady();return Promise.resolve();
  };
  const starting=broker.start();
  try{await Promise.all([starting,slow,healthy]);}
  finally{release();}
});

test('原配置目录不符或门禁写入失败时保留精确核验证据，修复后继续原会话',async t=>{
  const home=temporary(t),file=path.join(home,'history.jsonl'),sessionId='fixture-recovery',store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home);
  const broker=new Broker(store,DEFAULT_CONFIG,{codex:adapter,claude:new FakeAdapter()} as unknown as Adapters,home);
  t.after(async()=>{await broker.stop();store.close();});
  const peer=store.register(registration('codex',sessionId));store.updatePeer(peer.id,{owner:'nxbridge'});
  const partner=store.register(registration('claude','fixture-partner'));store.pair(partner.id,peer.id);
  const endpoint=store.endpoint(peer.id)!;
  writeFileSync(file,row('session_meta',{id:sessionId})+row('turn_context',{permission_profile:profile,model:'fixture-model',reasoning_effort:'high',service_tier:'priority',approval_policy:'on-request',approvals_reviewer:'user'}),'utf8');
  const settings=await readSessionProfile(file,sessionId),live={model:'fixture-model',reasoningEffort:'high',serviceTier:'priority',approvalPolicy:'on-request',approvalsReviewer:'user',activePermissionProfile:{id:'fixture-permissions'}};
  appendFileSync(file,row('event_msg',{type:'thread_settings_applied',thread_id:sessionId,thread_settings:{permission_profile:profile}}),'utf8');
  const runtimeId=randomUUID(),verification={sessionId,profile:{...settings,live},restartSourceId:randomUUID()};
  Object.assign(endpoint,{runtimeId,pid:process.pid,procStart:'fixture-native'});store.saveEndpoint(endpoint);
  saveManagedRuntime(home,{id:runtimeId,peerId:peer.id,sessionId,configHome:home,owner:managedMarker(home),kind:'owned',state:'ready',pid:endpoint.pid!,procStart:endpoint.procStart!,endpoint:endpoint.address,createdAt:Date.now(),sessionVerification:verification});
  const resumeCalls:string[]=[];
  const rpc=new RpcClient(message=>{if(message.method==='thread/resume')resumeCalls.push(message.method);queueMicrotask(()=>rpc.receive({id:message.id,result:{thread:{id:sessionId,path:file,status:{type:'idle'}},...live}}));},()=>{});
  let detectedHome=temporary(t);
  adapter.connect=async()=>{(adapter as any).configHomes.set(endpoint.address,detectedHome);return rpc;};t.after(()=>rpc.close());
  (adapter as any).clients.set(endpoint.address,rpc);
  (adapter as any).ownedRoutes.subscribed({peer:store.peer(peer.id),endpoint},rpc);
  await assert.rejects(adapter.attach(store.peer(peer.id),endpoint,{verify:true}),{code:'original_home_changed'});
  assert.equal(resumeCalls.length,0);assert.equal(hostingGate(store,store.peer(peer.id)),true);
  detectedHome=home;
  const setSetting=store.setSetting.bind(store);let failCommit=true;
  store.setSetting=(key,value)=>{if(key===hostingKey(peer.id)&&(value as {verified?:boolean}).verified){assert.ok(runtimeFor(home,{id:runtimeId})?.value.sessionVerification);if(failCommit)throw new BridgeError('fixture_write_failed','模拟核验门禁写入失败');}setSetting(key,value);};
  await assert.rejects(adapter.attach(store.peer(peer.id),endpoint,{verify:true}),{code:'fixture_write_failed'});
  assert.equal(hostingGate(store,store.peer(peer.id)),true);assert.ok(runtimeFor(home,{id:runtimeId})?.value.sessionVerification);
  failCommit=false;
  await adapter.attach(store.peer(peer.id),endpoint,{verify:true});
  assert.equal(hostingGate(store,store.peer(peer.id)),false);assert.equal(runtimeFor(home,{id:runtimeId})?.value.sessionVerification,undefined);
  assert.equal(resumeCalls.length,2);
});

for(const changed of ['model','reasoningEffort','serviceTier','all'] as const)test('只在接手订阅中采用原历史已确认的模型设置：'+changed,async t=>{
  const home=temporary(t),id='takeover-model',file=path.join(home,'history.jsonl');
  const saved={cwd:home,runtime_workspace_roots:[home],model:'model-a',model_provider:'fixture',reasoning_effort:'high',service_tier:'priority',
    approval_policy:'never',approvals_reviewer:'user',permission_profile:{type:'disabled'},
    collaboration_mode:{mode:'default',settings:{model:'model-a',reasoning_effort:'high',developer_instructions:null}}};
  writeFileSync(file,row('session_meta',{id})+row('turn_context',saved),'utf8');
  const original=await readSessionProfile(file,id),updated={...saved,
    ...(['model','all'].includes(changed)?{model:'model-b'}:{}),...(['reasoningEffort','all'].includes(changed)?{reasoning_effort:'medium'}:{}),
    ...(['serviceTier','all'].includes(changed)?{service_tier:'flex'}:{})};
  updated.collaboration_mode={...saved.collaboration_mode,settings:{...saved.collaboration_mode.settings,model:updated.model,reasoning_effort:updated.reasoning_effort}};
  appendFileSync(file,row('event_msg',{type:'thread_settings_applied',thread_id:id,thread_settings:updated}),'utf8');
  const ack={thread:{id,path:file,status:{type:'idle'}},cwd:home,runtimeWorkspaceRoots:[home],model:updated.model,modelProvider:'fixture',
    reasoningEffort:updated.reasoning_effort,serviceTier:updated.service_tier,approvalPolicy:'never',approvalsReviewer:'user'};
  const planned=planTakenOverCodexResume(id,original,await readSessionProfile(file,id),home);
  assert.equal(planned.args.model,updated.model);assert.equal((planned.args.config as any).model_reasoning_effort,updated.reasoning_effort);
  assert.equal(planned.profile.settings.serviceTier,updated.service_tier);assert.equal(planned.args.approvalPolicy,'never');
  assert.equal(planned.profile.settings.collaborationMode.settings.developer_instructions,null);
  await assert.rejects(verifyCodexResumeAck(original,ack,{cwd:home}));
  const verified=await verifyCodexResumeAck(original,ack,{cwd:home,adoptLiveModelSettings:true});
  assert.equal(verified.profile.settings.model,updated.model);assert.equal(verified.profile.settings.reasoningEffort,updated.reasoning_effort);
  assert.equal(verified.profile.settings.serviceTier,updated.service_tier);assert.equal(verified.policy.approval,'never');
  assert.deepEqual(original.settings.permissionProfile,{type:'disabled'});assert.equal(original.settings.reasoningEffort,'high');
});

for(const field of ['mode','developer_instructions'] as const)test('接手的模型变更不能顺带采用协作模式或开发者指令：'+field,async t=>{
  const home=temporary(t),id='takeover-collaboration',file=path.join(home,'history.jsonl');
  const saved={cwd:home,runtime_workspace_roots:[home],model:'model-a',model_provider:'fixture',reasoning_effort:'high',service_tier:'priority',
    approval_policy:'never',approvals_reviewer:'user',permission_profile:{type:'disabled'},
    collaboration_mode:{mode:'default',settings:{model:'model-a',reasoning_effort:'high',developer_instructions:null}}};
  writeFileSync(file,row('session_meta',{id})+row('turn_context',saved),'utf8');const original=await readSessionProfile(file,id);
  const collaboration={mode:field==='mode'?'plan':'default',settings:{...saved.collaboration_mode.settings,reasoning_effort:'medium',
    developer_instructions:field==='developer_instructions'?'changed instructions':null}};
  appendFileSync(file,row('event_msg',{type:'thread_settings_applied',thread_id:id,thread_settings:{...saved,reasoning_effort:'medium',collaboration_mode:collaboration}}),'utf8');
  const ack={thread:{id,path:file,status:{type:'idle'}},cwd:home,runtimeWorkspaceRoots:[home],model:'model-a',modelProvider:'fixture',
    reasoningEffort:'medium',serviceTier:'priority',approvalPolicy:'never',approvalsReviewer:'user'};
  await assert.rejects(verifyCodexResumeAck(original,ack,{cwd:home,adoptLiveModelSettings:true}),{code:'restart_profile_changed'});
});

for(const changed of ['model','reasoningEffort','serviceTier'] as const)test('只在 ACK 出现的模型设置不能成为接手依据：'+changed,async t=>{
  const home=temporary(t),id='takeover-unpersisted',file=path.join(home,'history.jsonl');
  writeFileSync(file,row('session_meta',{id})+row('turn_context',{cwd:home,runtime_workspace_roots:[home],model:'model-a',model_provider:'fixture',reasoning_effort:'high',
    service_tier:'priority',approval_policy:'never',approvals_reviewer:'user',permission_profile:{type:'disabled'}}),'utf8');
  const original=await readSessionProfile(file,id),ack={thread:{id,path:file,status:{type:'idle'}},cwd:home,runtimeWorkspaceRoots:[home],model:'model-a',
    modelProvider:'fixture',reasoningEffort:'high',serviceTier:'priority',approvalPolicy:'never',approvalsReviewer:'user',
    [changed]:changed==='model'?'model-b':changed==='reasoningEffort'?'medium':'flex'};
  const planned=planTakenOverCodexResume(id,original,await readSessionProfile(file,id),home);
  assert.equal(planned.args.model,'model-a');assert.equal((planned.args.config as any).model_reasoning_effort,'high');assert.equal(planned.profile.settings.serviceTier,'priority');
  await assert.rejects(verifyCodexResumeAck(original,ack,{cwd:home,adoptLiveModelSettings:true}),{code:'takeover_profile_unverified'});
});

for(const failure of ['policy','unrecorded'] as const)for(const pending of [false,true])test('接手订阅的失败原因可见且核验证据保留：'+failure+'/'+pending,async t=>{
  const home=temporary(t),id='takeover-policy',file=path.join(home,'history.jsonl'),store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home);
  const broker=new Broker(store,DEFAULT_CONFIG,{codex:adapter,claude:new FakeAdapter()} as unknown as Adapters,home);
  t.after(async()=>{await broker.stop();store.close();});
  const peer=store.register(registration('codex',id,{cwd:home})),partner=store.register(registration('claude','takeover-partner'));store.pair(partner.id,peer.id);
  store.updatePeer(peer.id,{owner:'nxbridge',state:'ready'});
  const saved={cwd:home,runtime_workspace_roots:[home],model:'model-a',model_provider:'fixture',reasoning_effort:'high',service_tier:'priority',
    approval_policy:'never',approvals_reviewer:'user',permission_profile:{type:'disabled'}};
  writeFileSync(file,row('session_meta',{id})+row('turn_context',saved),'utf8');const original=await readSessionProfile(file,id);
  if(failure==='policy')appendFileSync(file,row('event_msg',{type:'thread_settings_applied',thread_id:id,thread_settings:{...saved,reasoning_effort:'medium',approval_policy:'on-request'}}),'utf8');
  const endpoint=store.endpoint(peer.id)!,runtimeId=randomUUID();Object.assign(endpoint,{runtimeId,pid:process.pid,procStart:'fixture-native',nativeSession:{transcriptPath:file}});store.saveEndpoint(endpoint);
  saveManagedRuntime(home,{id:runtimeId,peerId:peer.id,sessionId:id,configHome:home,owner:managedMarker(home),kind:'owned',state:'ready',pid:endpoint.pid!,procStart:endpoint.procStart!,
    endpoint:endpoint.address,createdAt:Date.now(),...(pending?{sessionVerification:{sessionId:id,profile:original}}:{})});
  let verifyingEvents=0;adapter.on('sessionVerifying',()=>{verifyingEvents++;});
  const calls:string[]=[],rpc=new RpcClient(message=>{
    calls.push(message.method!);queueMicrotask(()=>rpc.receive({id:message.id,result:message.method==='thread/loaded/list'?{data:[id]}:
      {thread:{id,path:file,status:{type:'idle'}},cwd:home,runtimeWorkspaceRoots:[home],model:'model-a',modelProvider:'fixture',reasoningEffort:'medium',
        serviceTier:'priority',approvalPolicy:failure==='policy'?'on-request':'never',approvalsReviewer:'user'}}));
  },()=>{});t.after(()=>rpc.close());adapter.connect=async()=>rpc;(adapter as any).clients.set(endpoint.address,rpc);
                                      
  if(!pending)writeFileSync(file,row('session_meta',{id})+row('turn_context',saved),'utf8');
  const request=rpc.request.bind(rpc);t.mock.method(rpc,'request',async(...args:Parameters<typeof rpc.request>)=>{
    if(!pending&&failure==='policy'&&args[0]==='thread/resume')appendFileSync(file,row('event_msg',{type:'thread_settings_applied',thread_id:id,thread_settings:{...saved,reasoning_effort:'medium',approval_policy:'on-request'}}),'utf8');
    return request(...args);
  });
  const code=failure==='policy'?'policy_verification_failed':'takeover_profile_unverified';
  await assert.rejects(adapter.attach(store.peer(peer.id),endpoint,{subscribeLoaded:async()=>()=>{}}),{code});
  const status=hostingStatus(store,store.peer(peer.id));assert.equal(status?.phase,'blocked');assert.ok(status&&'reason' in status);assert.equal(status.reason,code);
  assert.match(status.user_step!,failure==='policy'?/approvalPolicy.*权限改回/:/设置改动记进会话记录/);assert.equal(adapter.hasSubscription(store.peer(peer.id),endpoint),false);
  assert.equal(!!runtimeFor(home,{id:runtimeId})!.value.sessionVerification,pending);assert.deepEqual(calls,['thread/loaded/list','thread/resume']);
  if(pending||failure==='unrecorded'){
    await assert.rejects(adapter.attach(store.peer(peer.id),endpoint,{subscribeLoaded:async()=>()=>{}}),{code});
    assert.equal(hostingStatus(store,store.peer(peer.id))?.phase,'blocked');assert.equal(verifyingEvents,0,'相同失败重试不能清掉原原因、短暂显示为核验中');
    assert.deepEqual(calls,['thread/loaded/list','thread/resume','thread/loaded/list','thread/resume']);
  }
  if(failure==='unrecorded'){
    appendFileSync(file,row('event_msg',{type:'thread_settings_applied',thread_id:id,thread_settings:{...saved,reasoning_effort:'medium'}}),'utf8');
    await adapter.attach(store.peer(peer.id),endpoint,{subscribeLoaded:async()=>()=>{}});
    assert.equal(hostingStatus(store,store.peer(peer.id))?.phase,'background');assert.equal(runtimeFor(home,{id:runtimeId})!.value.sessionVerification,undefined);
    assert.equal(adapter.hasSubscription(store.peer(peer.id),store.endpoint(peer.id)!),true);
    assert.deepEqual(calls,['thread/loaded/list','thread/resume','thread/loaded/list','thread/resume','thread/loaded/list','thread/resume']);
  }
});

for(const failure of ['timeout','busy','guard','plain','stale'] as const)test('接手的暂时连接失败不变成权限失败，也不阻断原窗口取件：'+failure,async t=>{
  const home=temporary(t),id='takeover-transient',file=path.join(home,'history.jsonl'),store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home);
  const broker=new Broker(store,DEFAULT_CONFIG,{codex:adapter,claude:new FakeAdapter()} as unknown as Adapters,home);
  t.after(async()=>{await broker.stop();store.close();});
  const peer=store.register(registration('codex',id,{cwd:home})),partner=store.register(registration('claude','transient-partner'));store.pair(partner.id,peer.id);
  store.updatePeer(peer.id,{owner:'nxbridge',state:'ready'});
  writeFileSync(file,row('session_meta',{id})+row('turn_context',{cwd:home,runtime_workspace_roots:[home],model:'model-a',model_provider:'fixture',reasoning_effort:'high',
    service_tier:'priority',approval_policy:'never',approvals_reviewer:'user',permission_profile:{type:'disabled'}}),'utf8');
  const endpoint=store.endpoint(peer.id)!,runtimeId=randomUUID();Object.assign(endpoint,{runtimeId,pid:process.pid,procStart:'fixture-native',nativeSession:{transcriptPath:file}});store.saveEndpoint(endpoint);
  saveManagedRuntime(home,{id:runtimeId,peerId:peer.id,sessionId:id,configHome:home,owner:managedMarker(home),kind:'owned',state:'ready',pid:endpoint.pid!,procStart:endpoint.procStart!,
    endpoint:endpoint.address,createdAt:Date.now()});
  const replacement=new RpcClient(()=>{},()=>{});t.after(()=>replacement.close());
  const rpc=new RpcClient(message=>queueMicrotask(()=>{
    rpc.receive({id:message.id,result:message.method==='thread/loaded/list'?{data:[id]}:{thread:{id,path:file,status:{type:'idle'}},cwd:home,
      runtimeWorkspaceRoots:[home],model:'model-a',modelProvider:'fixture',reasoningEffort:'medium',serviceTier:'priority',approvalPolicy:'never',approvalsReviewer:'user'}});
    if(failure==='stale'&&message.method==='thread/resume')queueMicrotask(()=>(adapter as any).clients.set(endpoint.address,replacement));
  }),()=>{});t.after(()=>rpc.close());adapter.connect=async()=>rpc;(adapter as any).clients.set(endpoint.address,rpc);
  const fault=failure==='plain'?new Error('临时连接中断'):new BridgeError(failure==='busy'?'-32001':failure==='guard'?'takeover_unverified':'timeout','临时连接未就绪');
  const request=rpc.request.bind(rpc);t.mock.method(rpc,'request',async(...args:Parameters<typeof rpc.request>)=>{
    if(args[0]==='thread/resume'&&failure!=='stale'){args[2]?.beforeSend?.();throw fault;}return request(...args);
  });
  let failedEvents=0,verifyingEvents=0;adapter.on('sessionVerificationFailed',()=>{failedEvents++;});adapter.on('sessionVerifying',()=>{verifyingEvents++;});
  await assert.rejects(adapter.attach(store.peer(peer.id),endpoint,{subscribeLoaded:async()=>()=>{if(failure==='guard')throw fault;}}),
    failure==='stale'?{code:'owned_subscription_waiting'}:error=>error===fault);
  assert.equal(failedEvents,0);assert.equal(verifyingEvents,0);assert.equal(hostingGate(store,store.peer(peer.id)),false);
  assert.equal(hostingRecord(store,store.peer(peer.id))?.verificationError,undefined);
});

for(const outcome of ['ended','replaced','success'] as const)test('接管结束或替换后不复活迟到核验失败，已发请求的有效成功仍接纳：'+outcome,async t=>{
  const home=temporary(t),id='takeover-late',file=path.join(home,'history.jsonl'),store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home);
  const broker=new Broker(store,DEFAULT_CONFIG,{codex:adapter,claude:new FakeAdapter()} as unknown as Adapters,home);
  t.after(async()=>{await broker.stop();store.close();});
  const peer=store.register(registration('codex',id,{cwd:home})),partner=store.register(registration('claude','late-partner'));store.pair(partner.id,peer.id);
  store.updatePeer(peer.id,{owner:'nxbridge',state:'ready'});
  const saved={cwd:home,runtime_workspace_roots:[home],model:'model-a',model_provider:'fixture',reasoning_effort:'high',service_tier:'priority',
    approval_policy:'never',approvals_reviewer:'user',permission_profile:{type:'disabled'}};
  writeFileSync(file,row('session_meta',{id})+row('turn_context',saved),'utf8');
  const endpoint=store.endpoint(peer.id)!,runtimeId=randomUUID();Object.assign(endpoint,{runtimeId,pid:process.pid,procStart:'fixture-native',nativeSession:{transcriptPath:file}});store.saveEndpoint(endpoint);
  saveManagedRuntime(home,{id:runtimeId,peerId:peer.id,sessionId:id,configHome:home,owner:managedMarker(home),kind:'owned',state:'ready',pid:endpoint.pid!,procStart:endpoint.procStart!,
    endpoint:endpoint.address,createdAt:Date.now()});
  let foregroundAlive=true;
  const takeovers=new Takeovers(store,async rows=>rows.map(value=>value.pid===91231&&!foregroundAlive?'dead':'alive'),undefined,undefined,()=>foregroundAlive);
  const lease=takeovers.begin(store.peer(peer.id),endpoint);takeovers.claim(peer.id,lease.id,{pid:91231,procStart:'old-window'});
  let failedEvents=0;adapter.on('sessionVerificationFailed',()=>{failedEvents++;});
  const rpc=new RpcClient(message=>{void(async()=>{
    if(message.method==='thread/loaded/list'){rpc.receive({id:message.id,result:{data:[id]}});return;}
                                         
    foregroundAlive=false;assert.equal(await takeovers.returned(peer.id,lease.id),true);
    if(outcome==='replaced'){const next=takeovers.begin(store.peer(peer.id),endpoint);takeovers.claim(peer.id,next.id,{pid:91232,procStart:'new-window'});}
    rpc.receive({id:message.id,result:{thread:{id,path:file,status:{type:'idle'}},cwd:home,runtimeWorkspaceRoots:[home],model:'model-a',modelProvider:'fixture',
      reasoningEffort:outcome==='success'?'high':'medium',serviceTier:'priority',approvalPolicy:'never',approvalsReviewer:'user'}});
  })().catch(error=>rpc.receive({id:message.id,error:{code:-32000,message:String(error)}}));},()=>{});
  t.after(()=>rpc.close());adapter.connect=async()=>rpc;(adapter as any).clients.set(endpoint.address,rpc);
  const attaching=adapter.attach(store.peer(peer.id),endpoint,{subscribeLoaded:()=>takeovers.codexSubscriptionGuard(store.peer(peer.id),endpoint)});
  if(outcome==='success')await attaching;else await assert.rejects(attaching,{code:'takeover_profile_unverified'});
  assert.equal(failedEvents,0);assert.equal(hostingRecord(store,store.peer(peer.id))?.verificationError,undefined);
  assert.equal(adapter.hasSubscription(store.peer(peer.id),store.endpoint(peer.id)!),outcome==='success');
});

test('普通冷恢复的核验门禁成功后解除，显式接管仍保留自己的门禁',t=>{
  const f=fixture(t);f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  const event={memberId:f.x.id,sessionId:f.x.sessionId};
  f.codex.emit('sessionVerifying',event);assert.equal(hostingGate(f.store,f.store.peer(f.x.id)),true);assert.equal(hostingStatus(f.store,f.store.peer(f.x.id))?.phase,'verifying');
  f.codex.emit('sessionPolicy',{...event,policy:{source:'persisted'}});assert.equal(hostingGate(f.store,f.store.peer(f.x.id)),false);assert.equal(hostingStatus(f.store,f.store.peer(f.x.id))?.phase,'background');
  writeHosting(f.store,f.store.peer(f.x.id),{phase:'adopting'});f.codex.emit('sessionVerifying',event);f.codex.emit('sessionPolicy',{...event,policy:{source:'persisted'}});
  assert.equal(hostingGate(f.store,f.store.peer(f.x.id)),true);assert.equal(hostingStatus(f.store,f.store.peer(f.x.id))?.phase,'adopting');
});

test('失败核验显示阻塞，用户重新打开或释放后不继承后台门禁，旧代次成功不能解除新门禁',t=>{
  const f=fixture(t);f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  const event={memberId:f.x.id,sessionId:f.x.sessionId,runtimeId:'new-runtime',endpoint:'unix://new'};
  f.codex.emit('sessionVerifying',event);f.codex.emit('sessionPolicy',{...event,runtimeId:'old-runtime',policy:{source:'persisted'}});assert.equal(hostingGate(f.store,f.store.peer(f.x.id)),true);
  f.codex.emit('sessionVerificationFailed',{...event,error:{code:'policy_verification_failed',message:'请在原窗口确认设置'}});
  const failed=hostingStatus(f.store,f.store.peer(f.x.id));assert.equal(failed?.phase,'blocked');assert.equal(failed&&'reason' in failed?failed.reason:undefined,'policy_verification_failed');assert.equal(failed?.verifying,false);
  f.store.updatePeer(f.x.id,{owner:'user'});assert.equal(hostingGate(f.store,f.store.peer(f.x.id)),false);assert.equal(hostingStatus(f.store,f.store.peer(f.x.id))?.phase,'foreground');
  writeHosting(f.store,f.store.peer(f.x.id),{released:true});assert.equal(hostingRecord(f.store,f.store.peer(f.x.id))?.verifying,false);assert.equal(hostingRecord(f.store,f.store.peer(f.x.id))?.verificationRuntimeId,undefined);
});

test('已登记为托管的非独占旧运行时缺少空闲证明时保留原消息',async t=>{
  const home=temporary(t),file=path.join(home,'legacy.jsonl'),store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home),broker=new Broker(store,DEFAULT_CONFIG,{codex:adapter,claude:new FakeAdapter()} as unknown as Adapters,home);t.after(async()=>{await broker.stop();store.close();});
  writeFileSync(file,row('session_meta',{id:'legacy'})+row('turn_context',{permission_profile:profile,approval_policy:'on-request',approvals_reviewer:'user'}),'utf8');
  const initial=registration('codex','legacy');initial.endpoint.epoch=initial.endpoint.address;
  const peer=store.register(initial);store.updatePeer(peer.id,{owner:'nxbridge'});const endpoint=store.endpoint(peer.id)!;
  const partner=store.register(registration('claude','legacy-partner')),pair=store.pair(partner.id,peer.id);
  endpoint.runtimeId=randomUUID();store.saveEndpoint(endpoint);
  saveManagedRuntime(home,{id:endpoint.runtimeId,peerId:peer.id,sessionId:'legacy',owner:managedMarker(home),kind:'owned',state:'ready',dedicated:false,pid:endpoint.pid!,procStart:endpoint.procStart!,endpoint:endpoint.address,createdAt:Date.now()});
  let loaded=false;const calls:string[]=[],rpc=new RpcClient(message=>{
    calls.push(message.method!);if(message.method==='thread/resume')loaded=true;
    if(message.method==='thread/settings/update')appendFileSync(file,row('event_msg',{type:'thread_settings_applied',thread_id:'legacy',thread_settings:{permission_profile:profile,approval_policy:'on-request',approvals_reviewer:'user'}}),'utf8');
    queueMicrotask(()=>rpc.receive({id:message.id,result:message.method==='thread/loaded/list'?{data:[]}:{thread:{id:'legacy',cwd:peer.cwd,path:file,status:{type:loaded?'idle':'notLoaded'}},cwd:peer.cwd,approvalPolicy:'on-request',approvalsReviewer:'user'}}));
  },()=>{});adapter.connect=async()=>rpc;t.after(()=>rpc.close());
  (adapter as any).clients.set(endpoint.address,rpc);
  const message=store.accept(pair,partner,'核验完成后继续原消息','legacy-message');
  await assert.rejects(adapter.attach(store.peer(peer.id),endpoint),{code:'owned_cold_resume_waiting',phase:'prepare'});
  assert.deepEqual(calls,['thread/loaded/list']);assert.equal(runtimeFor(home,{peerId:peer.id})?.value.sessionVerification,undefined);
  assert.equal(store.message(message.id).state,'accepted');
});

test('临时命名权限的实时快照带完整定义，重启继续使用同名原策略',async t=>{
  const home=temporary(t),file=path.join(home,'temporary-profile.jsonl'),id='temporary-profile';
  const settings={permission_profile:profile,approval_policy:'on-request',approvals_reviewer:'user',model:'fixture-model',reasoning_effort:'high',service_tier:'default',cwd:home};
  writeFileSync(file,row('session_meta',{id})+row('turn_context',settings),'utf8');
  const calls:any[]=[],rpc=new RpcClient(message=>{calls.push(message);
    if(message.method==='thread/settings/update')appendFileSync(file,row('event_msg',{type:'thread_settings_applied',thread_id:id,thread_settings:settings}),'utf8');
    queueMicrotask(()=>rpc.receive({id:message.id,result:{thread:{id,path:file,status:{type:'idle'}},model:settings.model,reasoningEffort:settings.reasoning_effort,cwd:home,serviceTier:'default',approvalPolicy:'on-request',approvalsReviewer:'user',activePermissionProfile:{id:'nxbridge_transfer'}}}));
  },()=>{});t.after(()=>rpc.close());
  const snapshot=await snapshotLiveSession(rpc,id,file);calls.length=0;await resumeSession(rpc,id,{profile:snapshot,cwd:home});
  assert.equal(calls[0].params.permissions,'nxbridge_transfer');assert.deepEqual(calls[0].params.config['permissions.nxbridge_transfer'],{filesystem:{':root':'none'},network:{enabled:false}});
});

test('共享 daemon 持有者核验使用原 home 和原生代次，已加载与无法观察均阻止接管',async t=>{
  const f=fixture(t),home=temporary(t);f.store.pair(f.c.id,f.x.id);writeHosting(f.store,f.x,{},home);
  const runtime={pid:1234,procStart:'native-generation'},ops=(f.broker as any).transfers.ops;
  for(const state of ['alive','dead','unknown'] as const){
    (f.broker as any).daemons.scan=async(configHome:string,expected:unknown,fresh:boolean)=>{assert.equal(configHome,home);assert.deepEqual(expected,runtime);assert.equal(fresh,true);return state==='unknown'?undefined:{threads:state==='alive'?[{id:f.x.sessionId}]:[]};};
    assert.equal(await ops.daemon(f.x,runtime),state);
  }
});

test('未落盘的中断核验只对无未结算投递的独占线程自动冷恢复',async t=>{
  const f=fixture(t),home=f.broker.home;f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  const endpoint=f.store.endpoint(f.x.id)!,original={sessionId:f.x.sessionId!,offset:1,complete:true,verified:true,meta:{},settings:{permissionProfile:profile}};
  saveManagedRuntime(home,{id:randomUUID(),peerId:f.x.id,owner:managedMarker(home),kind:'owned',state:'ready',dedicated:true,endpoint:endpoint.address,createdAt:Date.now(),sessionVerification:{sessionId:f.x.sessionId!,profile:original}});
  f.codex.attachFailure=new BridgeError('policy_verification_incomplete','缺少新落盘证据');let restarts=0;
  (f.codex as any).restart=async(peer:any,_endpoint:any,guard:()=>void,options:any)=>{guard();assert.equal(options.verificationRecovery,true);restarts++;f.codex.emit('sessionPolicy',{memberId:peer.id,sessionId:peer.sessionId,policy:{source:'persisted'}});f.codex.emit('register',registration('codex',peer.sessionId,{endpoint:{...endpoint,address:'unix://verified',epoch:'verified'}}),peer.id);return{oldPid:1,pid:2};};
  f.codex.emit('sessionVerifying',{memberId:f.x.id,sessionId:f.x.sessionId});await lifecycle(f.broker).prepare(f.store.peer(f.x.id));
  assert.equal(restarts,1);assert.equal(hostingGate(f.store,f.store.peer(f.x.id)),false);assert.equal(f.store.endpoint(f.x.id)?.address,'unix://verified');
});

for(const inbound of [true,false])test('中断权限核验的重启门禁按目标入站未结算投递判断：'+(inbound?'阻止':'出站不阻止'),async t=>{
  const f=fixture(t),home=f.broker.home;const pair=f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.x.id,{owner:'nxbridge'});
  const endpoint=f.store.endpoint(f.x.id)!,original={sessionId:f.x.sessionId!,offset:1,complete:true,verified:true,meta:{},settings:{permissionProfile:profile}};
  saveManagedRuntime(home,{id:randomUUID(),peerId:f.x.id,owner:managedMarker(home),kind:'owned',state:'ready',dedicated:true,endpoint:endpoint.address,createdAt:Date.now(),sessionVerification:{sessionId:f.x.sessionId!,profile:original}});
  const message=f.store.accept(pair,inbound?f.c:f.x,'未知投递','uncertain');f.store.claim(message.id);f.store.transition(message.id,'unknown');
  f.codex.attachFailure=new BridgeError('policy_verification_incomplete','缺少新落盘证据');f.codex.ensureEndpoint=async()=>endpoint.address;let restarts=0;
  (f.codex as any).restart=async(peer:any,_endpoint:any,guard:()=>void)=>{guard();restarts++;f.codex.emit('sessionPolicy',{memberId:peer.id,sessionId:peer.sessionId,policy:{source:'persisted'}});return{oldPid:1,pid:2};};
  if(inbound)await assert.rejects(lifecycle(f.broker).prepare(f.store.peer(f.x.id)),{code:'delivery_unsettled'});else await lifecycle(f.broker).prepare(f.store.peer(f.x.id));
  assert.equal(restarts,inbound?0:1);assert.equal(f.store.message(message.id).state,'unknown');assert.equal(f.store.message(message.id).attempts,1);
});

test('启动权限不支持只认原生明确拒绝，不把超时、配置损坏或认证失败固化',()=>{
  for(const stderr of ['unknown field permissions.nxbridge_bootstrap','unsupported permissions.nxbridge_bootstrap','nxbridge_bootstrap does not exist'])assert.equal(bootstrapPolicyRejected({cause:{code:1,stderr}}),true);
  for(const error of [{cause:{code:'ETIMEDOUT',stderr:'unknown nxbridge_bootstrap'}},{cause:{code:1,signal:'SIGTERM',stderr:'invalid default_permissions'}},{cause:{code:1,stderr:'unknown field default_permissions'}},{cause:{code:1,stderr:'invalid config.toml line 5'}},{cause:{code:1,stderr:'TOML parse error: unexpected field default_permissions'}},{cause:{code:1,stderr:'authentication failed'}},new Error('command -c default_permissions failed')])assert.equal(bootstrapPolicyRejected(error),false);
});

test('恢复已登记路由只读核对用户根会话与分叉，其余路由沿用订阅前核验',async t=>{
  const f=fixture(t),adapter=new CodexAdapter(f.broker.config,f.broker.home),endpoint=f.store.endpoint(f.x.id)!;
  t.after(()=>adapter.close());
  const observed=t.mock.method(adapter,'observe',async()=>{});
  let guards=0,optionsReads=0;
  const attachRoute:CodexAdapter['attach']=async(_peer,_endpoint,options)=>{
    assert.equal(options?.verify,true);options?.prepare?.();return new RpcClient(()=>{},()=>{});
  };
  const attach=t.mock.method(adapter,'attach',attachRoute);
  const options=()=>{optionsReads++;return{verify:true,prepare:()=>{guards++;}};};
  await adapter.restoreRegisteredRoute({...f.x,owner:'user',kind:'root'},endpoint,options);
  await adapter.restoreRegisteredRoute({...f.x,owner:'user',kind:'fork'},endpoint,options);
  assert.equal(observed.mock.callCount(),2);assert.equal(attach.mock.callCount(),0);assert.equal(optionsReads,0);
  await adapter.restoreRegisteredRoute({...f.x,owner:'nxbridge',kind:'root'},endpoint,options);
  await adapter.restoreRegisteredRoute({...f.x,owner:'user',kind:'subagent'},endpoint,options);
  assert.equal(observed.mock.callCount(),2);assert.equal(attach.mock.callCount(),2);
  assert.equal(optionsReads,2);assert.equal(guards,2);
});

test('不支持的原生入口保持暂存，入口更新后自动解除旧预检等待并重新核验',async t=>{
  const f=fixture(t,{codexBinary:process.execPath}),home=f.broker.home;f.store.pair(f.c.id,f.x.id);f.store.updatePeer(f.x.id,{owner:'nxbridge',state:'offline'});const endpoint=f.store.endpoint(f.x.id)!;f.store.deactivate(f.x.id);
  const adapter=new CodexAdapter(f.broker.config,home);t.after(()=>adapter.close());
  f.codex.bootstrapProbeKey=adapter.bootstrapProbeKey.bind(adapter);
  const selected=selectHostLaunch('codex',f.broker.config,{home,cwd:f.x.cwd,sessionId:f.x.sessionId!,configHome:home}),key=restoreBootstrapKey(selected);
  writeHosting(f.store,f.store.peer(f.x.id),{reason:'bootstrap_policy_unsupported',bootstrapProbeKey:key,nextAt:Number.MAX_SAFE_INTEGER},home);
  let prepares=0;lifecycle(f.broker).prepare=async()=>{prepares++;f.store.saveEndpoint({...endpoint,active:true});f.store.updatePeer(f.x.id,{state:'ready'});};
  assert.equal(await lifecycle(f.broker).recover(f.store.peer(f.x.id)),false);assert.equal(prepares,0);
  writeHosting(f.store,f.store.peer(f.x.id),{bootstrapProbeKey:'previous-binary'});
  assert.equal(await lifecycle(f.broker).recover(f.store.peer(f.x.id)),true);assert.equal(prepares,1);assert.equal(hostingRecord(f.store,f.store.peer(f.x.id))?.bootstrapProbeKey,undefined);assert.equal(hostingGate(f.store,f.store.peer(f.x.id)),false);
});
