import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import path from 'node:path';
import {mkdirSync,unlinkSync,writeFileSync,symlinkSync} from 'node:fs';
import {createHash,randomUUID} from 'node:crypto';
import {fixture,lifecycle} from './support/broker-fixture.js';
import {temporary} from './support/temporary.js';
import {registration} from './support/registration.js';
import {processIdentity} from '../process/process.js';
import {probePeerRuntime} from '../broker/health/probe.js';
import {deliverClaudeInbox} from '../hosts/claude/claude-inbox.js';
import {saveNativeOwnership} from '../process/owned-native.js';
import {atomicJson} from '../shared/files.js';
import {BridgeError,DeliveryError} from '../shared/errors.js';
import type {Endpoint,Peer,Message} from '../types.js';
import type {RuntimeAvailability} from '../broker/health/contracts.js';
import {TransportMigrations} from '../broker/recovery/transport-migration.js';

const missing=():RuntimeAvailability=>({state:'blocked',nativeState:'alive',deliveryIssue:'claude_inbox_missing',reasons:['Claude native process alive; inbox socket pathname missing'],checked_at:Date.now()});
const health=(now=Date.now())=>({turnId:'completed',state:'ready' as const,tools:[],lastEventAt:now,lastProgressAt:now});

test('原生 socket 消失时准确报告仍存活的代次，拒绝在提交前写入；不可信替换不走恢复分支',{skip:process.platform==='win32'},async t=>{
  const home=temporary(t),address=path.join(home,'inbox.sock'),native=await processIdentity(process.pid);
  mkdirSync(path.join(home,'sessions'));atomicJson(path.join(home,'sessions',process.pid+'.json'),{sessionId:'same-session',procStart:native.startedAt});
  const peer={id:'socket-window',host:'claude',sessionId:'same-session',cwd:home,name:'Claude',owner:'user',state:'ready',permission:'prompting',kind:'root',parentId:null,updatedAt:Date.now()} satisfies Peer;
  const endpoint={...registration('claude',peer.sessionId).endpoint,peerId:peer.id,address,configHome:home,pid:process.pid,procStart:native.startedAt,active:true,seenAt:Date.now()} satisfies Endpoint;
  const server=net.createServer(socket=>socket.destroy());
  t.after(async()=>{if(server.listening)await new Promise<void>(resolve=>server.close(()=>resolve()));});
  await new Promise<void>(resolve=>server.listen(address,resolve));
  assert.equal((await probePeerRuntime(peer,endpoint)).state,'reachable');
  unlinkSync(address);
  const result=await probePeerRuntime(peer,endpoint);
  assert.equal(result.state,'blocked');assert.equal(result.nativeState,'alive');assert.equal(result.deliveryIssue,'claude_inbox_missing');
  const message={id:'unsent',pairId:'pair',from:peer.id,to:peer.id,body:'尚未提交的任务',inReplyTo:null,via:null,callId:'missing',state:'accepted',createdAt:Date.now(),updatedAt:Date.now(),evidence:null,error:null,attempts:0} satisfies Message;
  await assert.rejects(deliverClaudeInbox(peer,endpoint,message,peer,()=>home),{code:'claude_inbox_missing',uncertain:false});
                                          
  await new Promise<void>(resolve=>server.close(()=>resolve()));
  writeFileSync(address,'保留替换文件','utf8');
  assert.equal((await probePeerRuntime(peer,endpoint)).deliveryIssue,undefined);
  await assert.rejects(deliverClaudeInbox(peer,endpoint,message,peer,()=>home),{code:'invalid_inbox',uncertain:false});
  unlinkSync(address);symlinkSync(path.join(home,'does-not-exist'),address);
  assert.equal((await probePeerRuntime(peer,endpoint)).deliveryIssue,undefined);
  await assert.rejects(deliverClaudeInbox(peer,endpoint,message,peer,()=>home),{code:'invalid_inbox',uncertain:false});
  assert.equal((await probePeerRuntime(peer,{...endpoint,procStart:native.startedAt+'-old'})).deliveryIssue,undefined,'PID 代次不符时不能声称只是路径丢失');
});

test('通道丢失保留用户窗口状态及 Mod 只读能力，发送者立即得到具体动作',async t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),old=f.store.endpoint(f.c.id)!;
  f.store.saveEndpoint({...old,modRevision:'socket-test'});
  f.claude.probe=async()=>missing();
  const pending=f.store.accept(pair,f.x,'尚未提交的任务','pending');
  await f.broker.pump();
  assert.equal(f.store.endpoint(f.c.id)?.active,true);assert.equal(f.store.peer(f.c.id).state,'ready');
  assert.equal(f.store.message(pending.id).state,'accepted');assert.equal(f.store.message(pending.id).attempts,0);assert.equal(f.claude.released.length,0);
  const runtime=(f.broker as any).runtimeHealth,incident=runtime.incidentViews([f.c.id])[0];
  assert.equal(incident.layer,'transport');assert.equal(incident.action.owner,'user');assert.match(incident.action.text,/--resume c/);
  const again=await runtime.probeRuntime(f.store.peer(f.c.id),true);assert.equal(again.nativeState,'alive');
  assert.equal(runtime.incidentViews([f.c.id])[0].attempts,1,'只读探测不伪造新的发送尝试');
  const binding=f.store.bindings.current(f.c.id)!;assert.ok(binding);
  const status=await(f.broker as any).modRead({host:'claude',memberId:f.c.id,sessionId:'c',callId:'status',binding:{key:binding.key,epoch:binding.epoch}},'status',{sessionId:'c',native:{pid:old.pid,procStart:old.procStart},revision:'socket-test'},old.epoch);
  assert.equal(status.self.state,'ready');assert.equal(status.incidents[0].code,'claude_inbox_missing');
  assert.match(runtime.incidentNotes([f.c.id])[0].text,/--resume c/);
  assert.match(runtime.incidentNotes([f.c.id],f.c.id)[0].text,/Your Claude inbox.*cannot receive partner/);
  assert.match(f.broker.deliveryHealth.peek(pair.id,f.x.id)[0]?.message??'',/Claude partner inbox.*--resume c/);
  for(const self of [false,true]){
    const actor=self?{host:'claude',memberId:f.c.id,sessionId:'c',callId:'model-self',binding:{key:binding.key,epoch:binding.epoch}}:{host:'codex',sessionId:'x',callId:'model-partner'};
    const view=await f.broker.dispatch('status',{actor,model_view:true}) as any;
    const problem=view.incidents.find((item:any)=>item.code==='claude_inbox_missing');
    assert.equal(Object.hasOwn(problem,'userText'),false);assert.doesNotMatch(problem.action.text,/本窗口/);
    assert.match(problem.message,self?/Your Claude inbox/:/Claude partner inbox/);
  }
  f.broker.incidents.succeeded(f.c,f.store.endpoint(f.c.id));
  const recovered=await f.broker.dispatch('status',{actor:{host:'codex',sessionId:'x',callId:'model-recovered'},model_view:true}) as any;
  assert.match(recovered.incidents[0].message,/transport is responding again/);
});

function ownedFixture(t:Parameters<typeof fixture>[0]){
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),peer=f.store.updatePeer(f.c.id,{owner:'nxbridge'}),endpoint=f.store.endpoint(peer.id)!;
  f.store.saveEndpoint({...endpoint,nativeHealth:health(),nativeBackground:{tasks:0,crons:0,observedAt:Date.now()}});
  saveNativeOwnership({version:1,id:randomUUID(),home:endpoint.configHome!,peerId:peer.id,configHome:endpoint.configHome!,phase:'bound',createdAt:Date.now(),sessionId:peer.sessionId!,handle:'owned-test',native:{pid:endpoint.pid!,startedAt:endpoint.procStart!}});
  let stopped=0;
  (f.claude as any).preflightRestart=async()=>{};
  (f.claude as any).assertIdleWorkload=async()=>{};
  (f.claude as any).release=async(_peer:Peer,guard:(agent:unknown)=>Promise<void>)=>{await guard({status:'idle',pid:endpoint.pid});stopped++;};
  f.claude.probe=async()=>missing();
  return{...f,pair,peer,endpoint,stopped:()=>stopped};
}

test('自有原生后台在安全空闲点只恢复同一会话一次，并补发十七条待发消息',async t=>{
  const f=ownedFixture(t),messages=Array.from({length:17},(_,i)=>f.store.accept(f.pair,f.x,'积压消息 '+i,'queued-'+i));
  let restored=false;
  f.claude.probe=async()=>restored?{state:'reachable',nativeState:'alive',reasons:[],checked_at:Date.now()}:missing();
  const discover=f.claude.discover.bind(f.claude);
  f.claude.discover=async sessionId=>{restored=true;const input=await discover(sessionId);return{...input!,endpoint:{...input!.endpoint,configHome:f.endpoint.configHome,epoch:'new-inbox',procStart:'new-generation'}};};
  await lifecycle(f.broker).schedulePairedRecovery(f.peer.id,true);
  assert.equal(f.stopped(),1);assert.deepEqual(f.claude.woken.map(peer=>peer.sessionId),['c']);
  assert.equal(f.store.peer(f.peer.id).sessionId,'c');assert.equal(f.store.pairFor(f.peer.id)?.id,f.pair.id);
  assert.equal(f.broker.managedRecovery.get(f.peer.id)?.attempts.length,1);
  await f.broker.pump();
  assert.equal(f.claude.delivered.length,17);assert.deepEqual(f.claude.delivered.map(message=>message.id),messages.map(message=>message.id));
  assert.ok(messages.every(message=>f.store.message(message.id).attempts===1));
  await lifecycle(f.broker).schedulePairedRecovery(f.peer.id,false);assert.equal(f.stopped(),1);
});

test('未知发送、定时任务和缺少安全快照的旧子进程不能被自动停止或重放',async t=>{
  for(const mode of ['unknown','cron','child'] as const){
    const f=ownedFixture(t),message=f.store.accept(f.pair,f.x,'保留原任务',mode);
    if(mode==='unknown'){f.store.claim(message.id);f.store.transition(message.id,'unknown');}
    if(mode==='cron')f.store.saveEndpoint({...f.store.endpoint(f.peer.id)!,nativeBackground:{tasks:0,crons:1,observedAt:Date.now()}});
    if(mode==='child'){
      const id=createHash('sha256').update(f.peer.id).digest('hex').slice(0,16);
      mkdirSync(path.join(f.endpoint.configHome!,'run'),{recursive:true});
      atomicJson(path.join(f.endpoint.configHome!,'run','claude-child-'+id+'.json'),{version:1,peerId:f.peer.id,sessionId:f.peer.sessionId,phase:'active',address:'test-only',cwd:f.peer.cwd,hasSession:true,updatedAt:Date.now()});
    }
    await lifecycle(f.broker).schedulePairedRecovery(f.peer.id,true);
    assert.equal(f.stopped(),0,mode);assert.equal(f.claude.woken.length,0,mode);assert.equal(f.claude.delivered.length,0,mode);
    assert.equal(f.store.endpoint(f.peer.id)?.active,true,mode);
    assert.equal(f.broker.managedRecovery.get(f.peer.id)?.state,'blocked',mode);
    assert.match((f.broker as any).runtimeHealth.incidentViews([f.peer.id])[0].userText,/自动恢复没有成功/,mode);
    assert.equal(f.store.message(message.id).state,mode==='unknown'?'unknown':'accepted');
  }
});

test('自有后台恢复失败沿用既有三次加一次预算，旧用户窗口重新登记后未知发送不重放',async t=>{
  const f=ownedFixture(t);f.claude.fail=new BridgeError('test_launch_failure','隔离启动失败');
  for(let i=0;i<4;i++){
    const current=f.broker.managedRecovery.get(f.peer.id);if(current){current.nextAt=0;f.store.setSetting('managedRecovery:'+f.peer.id,current);}
                                      
    f.store.saveEndpoint({...f.endpoint,active:true,nativeHealth:health(),nativeBackground:{tasks:0,crons:0,observedAt:Date.now()}});f.store.updatePeer(f.peer.id,{state:'ready'});
    await lifecycle(f.broker).schedulePairedRecovery(f.peer.id,true);
  }
  assert.equal(f.stopped(),3);assert.equal(f.broker.managedRecovery.get(f.peer.id)?.attempts.length,3);assert.equal(f.broker.managedRecovery.get(f.peer.id)?.state,'exhausted');
  const incident=f.broker.managedRecovery.get(f.peer.id)!;
  await f.broker.dispatch('partner.recover',{actor:{host:'codex',sessionId:'x',callId:'extra'},action:'retry',incident_id:incident.id,reason:'隔离环境中的启动条件已恢复'});
  assert.equal(f.stopped(),4);assert.equal(f.broker.managedRecovery.get(f.peer.id)?.attempts.filter(attempt=>attempt.extra).length,1);
  await f.broker.dispatch('partner.recover',{actor:{host:'codex',sessionId:'x',callId:'same-extra'},action:'retry',incident_id:incident.id,reason:'重复请求只返回已有结果'});
  assert.equal(f.stopped(),4);
  const g=fixture(t),pair=g.store.pair(g.c.id,g.x.id),old=g.store.accept(pair,g.x,'旧结果不明','old'),fresh=g.store.accept(pair,g.x,'安全待发','fresh');
  g.store.claim(old.id);g.store.transition(old.id,'unknown');
  g.store.adopt(g.c.id,{...registration('claude','c'),endpoint:{...g.store.endpoint(g.c.id)!,epoch:'reregistered',procStart:'new-generation'}});
  await g.broker.pump();
  assert.equal(g.store.message(old.id).state,'unknown');assert.equal(g.store.message(old.id).attempts,1);
  assert.deepEqual(g.claude.delivered.map(message=>message.id),[fresh.id]);
});

test('回执路径被替换时明确报告冲突位置及人工动作，不能伪装成已经发送',t=>{
  const f=fixture(t),ep=f.store.endpoint(f.c.id),incident=f.broker.incidents.failed(f.c,ep,new DeliveryError('reply_socket_conflict','Claude 回执套接字地址已被其他文件占用：/isolated/reply.sock'));
  assert.equal(incident.state,'blocked');assert.equal(incident.layer,'transport');assert.equal(incident.causeClass,'configuration');
  assert.equal(incident.action?.owner,'user');assert.match(incident.message,/\/isolated\/reply.sock/);assert.doesNotMatch(incident.userText,/\/isolated/);
});

test('只保护确定的丢失通道，其他拒绝沿用原行为；停止前再次忙碌时不终止后台',async t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id);f.store.accept(pair,f.x,'无效路径不作为清理故障','invalid');
  f.claude.probe=async()=>({state:'blocked',nativeState:'alive',reasons:['invalid inbox'],checked_at:Date.now()});
  await f.broker.pump();assert.equal(f.store.peer(f.c.id).state,'offline');assert.equal(f.store.endpoint(f.c.id),undefined);
  const g=ownedFixture(t);let stopped=false;
  (g.claude as any).release=async(_peer:Peer,guard:(agent:unknown)=>void)=>{g.store.updatePeer(g.peer.id,{state:'busy'});await guard({status:'idle',pid:g.endpoint.pid});stopped=true;};
  await lifecycle(g.broker).schedulePairedRecovery(g.peer.id,true);
  assert.equal(stopped,false);assert.equal(g.claude.woken.length,0);assert.equal(g.store.endpoint(g.peer.id)?.active,true);assert.equal(g.store.peer(g.peer.id).state,'busy');
});

test('仍健康的旧 Claude inbox 在安全空闲时自动迁到持久地址，迁移不占故障恢复额度',{skip:process.platform==='win32'},async t=>{
  const f=ownedFixture(t),old=path.join(f.endpoint.configHome!,'legacy','inbox.sock'),fresh=path.join(f.endpoint.configHome!,'run','ci-current.sock');
  f.store.saveEndpoint({...f.store.endpoint(f.peer.id)!,address:old});
  f.claude.probe=async()=>({state:'reachable',nativeState:'alive',reasons:[],checked_at:Date.now()});
  const discover=f.claude.discover.bind(f.claude);
  f.claude.discover=async id=>{const value=await discover(id);return{...value!,endpoint:{...value!.endpoint,address:fresh,configHome:f.endpoint.configHome,epoch:'persistent',procStart:'persistent-generation'}};};
  await lifecycle(f.broker).schedulePairedRecovery(f.peer.id,false);
  assert.equal(f.stopped(),1);assert.equal(f.store.peer(f.peer.id).sessionId,f.peer.sessionId);assert.equal(f.store.pairFor(f.peer.id)?.id,f.pair.id);
  assert.equal(lifecycle(f.broker).transportMigrations.get(f.peer.id)?.state,'ready');
  assert.equal(f.broker.managedRecovery.get(f.peer.id),undefined);
  await lifecycle(f.broker).schedulePairedRecovery(f.peer.id,false);assert.equal(f.stopped(),1);
  const status=f.broker.status() as any;assert.equal(status.peers.find((peer:any)=>peer.id===f.peer.id).transport_migration.state,'ready');
});

test('可选迁移预检失败保留可用原会话，配置不支持新地址时不停止也不消费额度',{skip:process.platform==='win32'},async t=>{
  const f=ownedFixture(t);f.store.saveEndpoint({...f.store.endpoint(f.peer.id)!,address:path.join(f.endpoint.configHome!,'legacy','inbox.sock')});
  f.claude.probe=async()=>({state:'reachable',nativeState:'alive',reasons:[],checked_at:Date.now()});
  (f.claude as any).preflightRestart=async()=>{throw new BridgeError('claude_inbox_migration_unsupported','旧宿主不支持持久收件参数');};
  await lifecycle(f.broker).schedulePairedRecovery(f.peer.id,false);
  assert.equal(f.stopped(),0);assert.equal(f.store.peer(f.peer.id).state,'ready');assert.equal(f.store.endpoint(f.peer.id)?.active,true);
  const value=lifecycle(f.broker).transportMigrations.get(f.peer.id)!;assert.equal(value.state,'unsupported');assert.equal(value.attempts,0);
  assert.equal(f.broker.managedRecovery.get(f.peer.id),undefined);
});

for(const phase of ['stopping','launching'] as const)test('迁移在 '+phase+' 阶段中断后，原进程缺席由普通恢复接回原会话并保留原配置',async t=>{
  const f=ownedFixture(t),life=lifecycle(f.broker),value=life.transportMigrations.observe(f.peer,f.pair.id,f.endpoint,['legacy'])!;
  life.transportMigrations.rememberProfile(value,{model:'old-effective-model',effort:'low'});
  life.transportMigrations.claim(value,f.peer,f.endpoint,f.pair.id);if(phase==='launching')life.transportMigrations.launching(value);
  (life as {transportMigrations:TransportMigrations}).transportMigrations=new TransportMigrations(f.store);
  f.store.deactivate(f.peer.id);f.store.updatePeer(f.peer.id,{state:'offline'});
  let baseline:unknown;const wake=f.claude.wake.bind(f.claude);
  (f.claude as any).wake=async(peer:Peer,endpoint:Endpoint,signal:AbortSignal,guard:()=>Promise<void>,affinity:any)=>{baseline=affinity.modelProfile;await wake(peer,endpoint,signal,guard);};
  f.claude.probe=async()=>({state:'reachable',nativeState:'alive',reasons:[],checked_at:Date.now()});
  const discover=f.claude.discover.bind(f.claude);
  f.claude.discover=async id=>{const result=await discover(id);return{...result!,endpoint:{...result!.endpoint,address:path.join(f.endpoint.configHome!,'run','ci-recovered.sock'),configHome:f.endpoint.configHome,epoch:'recovered',procStart:'recovered'}};};
  await life.schedulePairedRecovery(f.peer.id,false);
  assert.deepEqual(baseline,{model:'old-effective-model',effort:'low'});assert.equal(f.claude.woken.length,1);
  assert.equal(f.store.peer(f.peer.id).sessionId,f.peer.sessionId);assert.equal(f.store.pairFor(f.peer.id)?.id,f.pair.id);
  assert.equal(f.broker.managedRecovery.get(f.peer.id)?.attempts.length,1);assert.equal(life.transportMigrations.get(f.peer.id)?.attempts,1);
  await life.schedulePairedRecovery(f.peer.id,false);assert.equal(f.claude.woken.length,1);assert.equal(life.transportMigrations.get(f.peer.id)?.state,'ready');
});

test('迁移中断后的替代进程待核验时给出明确未知恢复状态，不重复启动或只留离线提示',async t=>{
  const f=ownedFixture(t),life=lifecycle(f.broker),value=life.transportMigrations.observe(f.peer,f.pair.id,f.endpoint,['legacy'])!;
  life.transportMigrations.claim(value,f.peer,f.endpoint,f.pair.id);life.transportMigrations.launching(value);
  (life as {transportMigrations:TransportMigrations}).transportMigrations=new TransportMigrations(f.store);
  f.store.deactivate(f.peer.id);f.store.updatePeer(f.peer.id,{state:'offline'});f.claude.fail=new BridgeError('claude_start_pending','上次原生启动的结果尚未核验');
  await life.schedulePairedRecovery(f.peer.id,false);
  assert.equal(f.claude.woken.length,0);assert.equal(f.broker.managedRecovery.get(f.peer.id)?.state,'unknown');
  assert.equal(f.broker.managedRecovery.get(f.peer.id)?.attempts.length,0);
  const status=f.broker.status() as any,peer=status.peers.find((peer:any)=>peer.id===f.peer.id);
  assert.equal(peer.recovery.state,'unknown');assert.match(peer.recovery.user_step,/核对上次启动结果.*不要再次启动/);
  assert.equal(peer.transport_migration.attempts,1);
});

test('迁移中断后替代会话已经核验登记时复用它，不再创建第二个后台',async t=>{
  const f=ownedFixture(t),life=lifecycle(f.broker),value=life.transportMigrations.observe(f.peer,f.pair.id,f.endpoint,['legacy'])!;
  life.transportMigrations.claim(value,f.peer,f.endpoint,f.pair.id);life.transportMigrations.launching(value);
  (life as {transportMigrations:TransportMigrations}).transportMigrations=new TransportMigrations(f.store);
  f.store.adopt(f.peer.id,{...registration('claude',f.peer.sessionId!),endpoint:{...f.endpoint,address:path.join(f.endpoint.configHome!,'run','ci-existing.sock'),epoch:'existing-replacement',procStart:'existing-replacement'}});
  f.claude.probe=async()=>({state:'reachable',nativeState:'alive',reasons:[],checked_at:Date.now()});
  await life.schedulePairedRecovery(f.peer.id,false);
  assert.equal(f.claude.woken.length,0);assert.equal(f.claude.created.length,0);assert.equal(life.transportMigrations.get(f.peer.id)?.state,'ready');
  assert.equal(f.broker.managedRecovery.get(f.peer.id),undefined);
});
