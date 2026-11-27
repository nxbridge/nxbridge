import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {appendFileSync,mkdirSync,writeFileSync} from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import {stringify} from 'smol-toml';
import {Broker,type Adapters} from '../broker/coordinator.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {CodexAdapter,connectAppServer} from '../hosts/codex/adapter.js';
import {readTakeoverPolicy} from '../hosts/codex/codex-takeover-permission.js';
import {sameTakeoverModelSettings} from '../hosts/takeover-permission.js';
import {selectHostLaunch} from '../hosts/host-launch.js';
import {runtimeFor} from '../hosts/managed-runtimes.js';
import {ensureHome} from '../ipc/home.js';
import {runCli} from '../process/host-command.js';
import {Store} from '../storage/store.js';
import {FakeAdapter} from '../tests/support/fake-adapter.js';
import {releaseNativeTestLaunches} from '../tests/support/native-launch.js';
import {registration} from '../tests/support/registration.js';
import {temporaryDirectory} from '../tests/support/temporary.js';

test('原生空闲后台只恢复原审批方式、审批人和沙箱权限，保留后来模型设置',{timeout:90000},async t=>{
  const temporary=temporaryDirectory(),root=temporary.directory,home=path.join(root,'bridge'),codexHome=path.join(root,'codex');
  const sessionId=randomUUID(),timestamp=new Date().toISOString(),date=timestamp.slice(0,10).split('-');
  const history=path.join(codexHome,'sessions',...date,'rollout-'+timestamp.slice(0,19).replaceAll(':','-')+'-'+sessionId+'.jsonl');
  mkdirSync(path.dirname(history),{recursive:true});
  await ensureHome(home);
  const model='gpt-5.1-codex',provider='isolated_permissions',permissions='takeover_original';
  let providerRequests=0;
  const providerTrap=http.createServer((_request,response)=>{providerRequests++;response.writeHead(500);response.end();});
  await new Promise<void>(resolve=>providerTrap.listen(0,'127.0.0.1',resolve));
  const defaults={...DEFAULT_CONFIG,implicit:'never' as const,codexHome,...(process.env.NXBRIDGE_TEST_CODEX_BINARY?{codexBinary:process.env.NXBRIDGE_TEST_CODEX_BINARY}:{})};
  const launch=selectHostLaunch('codex',defaults,{home,cwd:root,configHome:codexHome});
  const config={...defaults,codexCommand:[launch.executable.command,...launch.executable.prefix]};
  const store=new Store(':memory:'),codex=new CodexAdapter(config,home),broker=new Broker(store,config,{codex,claude:new FakeAdapter()} as unknown as Adapters,home);
  let window:Awaited<ReturnType<typeof connectAppServer>>|undefined;
  t.after(async()=>{
    window?.close();await broker.stop();store.close();
    providerTrap.closeAllConnections();await new Promise<void>(resolve=>providerTrap.close(()=>resolve()));
    assert.deepEqual(await releaseNativeTestLaunches(home),[]);await temporary.cleanup();
  });
  const cliVersion=(await runCli('codex',['--version'],{launch,timeout:8000})).trim().match(/^codex-cli (\S+)$/)?.[1];
  assert.ok(cliVersion);
  const definition={filesystem:{':root':'read'},network:{enabled:false}};
  const writeConfig=(policy:typeof definition)=>writeFileSync(path.join(codexHome,'config.toml'),stringify({model,model_provider:provider,model_reasoning_effort:'low',
    approval_policy:'on-request',approvals_reviewer:'auto_review',default_permissions:permissions,permissions:{[permissions]:policy},
    check_for_update_on_startup:false,web_search:'disabled',analytics:{enabled:false},features:{apps:false,remote_plugin:false,code_mode:false},
    model_providers:{[provider]:{name:'隔离权限验收',base_url:'http://127.0.0.1:'+(providerTrap.address() as {port:number}).port+'/v1',wire_api:'responses',requires_openai_auth:false}}}),'utf8');
  writeConfig(definition);
  const originalSandbox={type:'managed',file_system:{type:'restricted',entries:[{path:{type:'special',value:{kind:'root'}},access:'read'}]},network:'restricted'};
  const event=(type:string,payload:unknown)=>JSON.stringify({timestamp,type,payload})+'\n';
                                       
  writeFileSync(history,event('session_meta',{id:sessionId,session_id:sessionId,timestamp,cwd:root,originator:'codex-tui',cli_version:cliVersion,source:'cli',model_provider:provider,runtime_workspace_roots:[root]})
    +event('event_msg',{type:'thread_settings_applied',thread_id:sessionId,thread_settings:{model,model_provider_id:provider,reasoning_effort:'low',service_tier:'default',
      approval_policy:'on-request',approvals_reviewer:'auto_review',permission_profile:originalSandbox,active_permission_profile:{id:permissions},
      cwd:root,runtime_workspace_roots:[root],collaboration_mode:{mode:'default',settings:{model,reasoning_effort:'low',developer_instructions:null}},disabled_plugin_ids:[]}}),'utf8');
  const seed=await readTakeoverPolicy(history,sessionId);
  const member=store.placeholder('codex',root),partner=store.register(registration('claude','permission-partner',{cwd:root}));
  const pair=store.pair(partner.id,member.id);
  const address=await codex.ensureEndpoint(root,member.id,undefined,sessionId,codexHome);
  const runtime=runtimeFor(home,{peerId:member.id,endpoint:address})?.value;
  assert.ok(runtime?.dedicated&&runtime.pid&&runtime.procStart);
  store.adopt(member.id,registration('codex',sessionId,{cwd:root,state:'offline',endpoint:{mode:'app-server',address,epoch:runtime.id,runtimeId:runtime.id,pid:runtime.pid,procStart:runtime.procStart,configHome:codexHome}}));
  const peer=()=>store.peer(member.id),endpoint=()=>store.endpoint(member.id)!;
  let rpc=await codex.attach(peer(),endpoint());
  assert.equal(codex.hasSubscription(peer(),endpoint()),true);
  const original=await codex.captureTakeoverPermission(peer(),endpoint());
  assert.ok(original.offset>seed.evidence.offset,'开窗基线必须来自原生实际写入');
  assert.deepEqual(original.policy,{approval:'on-request',reviewer:'auto_review',sandbox:originalSandbox,active:{id:permissions}});
  assert.deepEqual(original.named,{definition});

  window=await connectAppServer(address,config,undefined,home);
  const wire=await window.request('thread/resume',{threadId:sessionId,excludeTurns:true});
  assert.equal(wire.thread.id,sessionId);assert.equal(wire.thread.status.type,'idle');
  await window.request('thread/settings/update',{threadId:sessionId,approvalPolicy:'never',approvalsReviewer:'user',sandboxPolicy:{type:'dangerFullAccess'},
    model:'gpt-5.2',effort:'high',serviceTier:'fast'},{mutation:true});
  const changed=await readTakeoverPolicy(history,sessionId,original.offset);
  window.close();window=undefined;
  const closed=await codex.observeTakeoverPermission(peer(),original);
  assert.deepEqual(closed.policy,{approval:'never',reviewer:'user',sandbox:{type:'disabled'},active:null});
  const profile=(settings:Record<string,unknown>)=>[settings.model,settings.modelProvider,settings.reasoningEffort,settings.serviceTier];
  assert.deepEqual(profile(changed.profile.settings),['gpt-5.2',provider,'high','priority']);
  let probeSubmissions=0;
  const submitting=()=>{probeSubmissions++;};
  await assert.rejects(codex.probeTakeoverPermission(peer(),endpoint(),closed,()=>{throw new Error('探测未取得发送许可');},submitting),/探测未取得发送许可/);
  assert.equal(probeSubmissions,0);
  let probed=await codex.probeTakeoverPermission(peer(),endpoint(),closed,()=>{},submitting);
  assert.equal(probeSubmissions,1);
  assert.ok(probed.offset>closed.offset);
  assert.deepEqual(probed.policy,closed.policy);
  assert.ok(sameTakeoverModelSettings(probed,closed));
  const submitted:number[]=[];
  const restore=()=>codex.restoreTakeoverPermission(peer(),endpoint(),original,closed,()=>{},intent=>submitted.push(intent.before.offset));

                                               
  assert.deepEqual(await releaseNativeTestLaunches(home),[]);
  const replacementAddress=await codex.ensureEndpoint(root,member.id,undefined,sessionId,codexHome);
  const replacement=runtimeFor(home,{peerId:member.id,endpoint:replacementAddress})?.value;
  assert.ok(replacement?.dedicated&&replacement.pid&&replacement.procStart);
  assert.notEqual(replacement.id,runtime.id);
  store.deactivate(member.id);
  store.adopt(member.id,registration('codex',sessionId,{cwd:root,state:'ready',endpoint:{mode:'app-server',address:replacement.endpoint,epoch:replacement.endpoint,runtimeId:replacement.id,pid:replacement.pid,procStart:replacement.procStart,configHome:codexHome}}));
  rpc=await codex.attach(peer(),endpoint());

                                   
  const beforeOpen=(await readTakeoverPolicy(history,sessionId)).evidence;
  const generation=codex.takeoverProbeGeneration(peer(),endpoint());
  assert.ok(generation);
  const stateKey='takeoverPermission:'+member.id,changeId=randomUUID();
  store.setSetting(stateKey,{sessionId,inFlight:{choice:{id:changeId,original,final:beforeOpen,state:'failed',
    error:'restore_unconfirmed',requestPair:pair.id},intent:{at:Date.now()-12000,before:beforeOpen},
    probe:{generation,attempts:3,nextAt:Date.now()+30000}}});
  const opened=await broker.dispatch('open',{host:'codex',peer_id:member.id}) as {leaseId:string};
  probed=(await readTakeoverPolicy(history,sessionId)).evidence;
  assert.ok(probed.offset>beforeOpen.offset);
  assert.deepEqual(probed.policy,closed.policy);
  assert.ok(sameTakeoverModelSettings(probed,closed));
  const reconciled=store.setting<{inFlight?:unknown;choice?:{error?:string}}>(stateKey);
  assert.equal(reconciled?.inFlight,undefined);assert.equal(reconciled?.choice?.error,'restore_not_applied');
  assert.deepEqual(await broker.dispatch('open.return',{peer_id:member.id,session_id:sessionId,lease_id:opened.leaseId,foreground_ended:true}),{returned:true});

  writeConfig({...definition,filesystem:{':root':'write'}});
  await assert.rejects(restore(),{code:'policy_not_representable'});
  assert.deepEqual(submitted,[]);assert.equal((await codex.observeTakeoverPermission(peer(),original)).offset,probed.offset);
  writeConfig(definition);
                                    
  appendFileSync(history,event('turn_context',{model:'stale-model',effort:'low',service_tier:'default'}),'utf8');
  const applied=await restore();
  assert.equal(submitted.length,1);assert.ok(submitted[0]!>probed.offset);assert.ok(applied.offset>submitted[0]!);
  assert.deepEqual(applied.policy,original.policy);
  const durable=await readTakeoverPolicy(history,sessionId,closed.offset);
  assert.deepEqual(durable.evidence.policy,original.policy);
  assert.deepEqual(profile(durable.profile.settings),profile(changed.profile.settings));
  const {thread}=await rpc.request('thread/read',{threadId:sessionId,includeTurns:true});
  assert.equal(thread.id,sessionId);assert.equal(thread.status.type,'idle');assert.deepEqual(thread.turns,[]);
  assert.equal(runtimeFor(home,{peerId:member.id,endpoint:replacement.endpoint})?.value.procStart,replacement.procStart);
  assert.equal(providerRequests,0);
});
