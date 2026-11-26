import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import path from 'node:path';
import net from 'node:net';
import { randomUUID } from 'node:crypto';
import { closeSync, mkdirSync, openSync, readFileSync, writeFileSync } from 'node:fs';
import { readModelBody } from './model-http-fixture.js';
import { stringify } from 'smol-toml';
import { CodexAdapter, connectAppServer } from '../hosts/codex/adapter.js';
import { Broker, type Adapters } from '../broker/coordinator.js';
import { Store } from '../storage/store.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {delay} from '../shared/time.js';
import {ensureHome} from '../ipc/home.js';
import {selectHostLaunch} from '../hosts/host-launch.js';
import {launchIndependentWindows} from '../process/windows/windows-background.js';
import {spawnPosixSupervisor} from '../process/posix/posix-supervisor.js';
import { managedRuntimes } from '../hosts/managed-runtimes.js';
import { loadedCodexThreads } from '../hosts/codex/codex-restart.js';
import { BridgeError } from '../shared/errors.js';
import { prepareProcessRegistry, rememberOwnedChild } from '../process/owned-processes.js';
import { releaseNativeTestLaunches } from '../tests/support/native-launch.js';
import { closeHostedWindow, hostingStatus, type SessionTransfers } from '../broker/recovery/session-hosting.js';
import { FakeAdapter } from '../tests/support/fake-adapter.js';
import { registration } from '../tests/support/registration.js';
import { temporaryDirectory } from '../tests/support/temporary.js';
import { readSessionProfile, normalizedPermission } from '../hosts/codex/codex-session-profile.js';
import { ownedProcessState } from '../process/process-stop.js';

test('原生托管新建清理、首轮、释放和再托管保持同一会话、配对和权限',{timeout:180000},async t=>{
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge'),codexhome=path.join(temp.directory,'codex');
  mkdirSync(codexhome);await ensureHome(home);
  const config={...DEFAULT_CONFIG,implicit:'never' as const,codexHome:codexhome,...(process.env.NXBRIDGE_TEST_CODEX_BINARY?{codexBinary:process.env.NXBRIDGE_TEST_CODEX_BINARY}:{})};
  const store=new Store(':memory:'),codex=new CodexAdapter(config,home),broker=new Broker(store,config,{codex,claude:new FakeAdapter()} as unknown as Adapters,home);
  const transfers=(broker as unknown as {transfers:SessionTransfers}).transfers;
  let requests=0;const model=http.createServer(async(req,res)=>{
    try{
      const data=await readModelBody(req);
      if(!req.url?.includes('/responses')){res.writeHead(200,{'content-type':'application/json'});res.end('{"data":[]}');return;}
      JSON.parse(data.toString());requests++;
      const item={type:'message',id:'transfer-answer',role:'assistant',status:'completed',content:[{type:'output_text',text:'隔离的原会话答案'}]},id='transfer-response';
      res.writeHead(200,{'content-type':'text/event-stream'});
      for(const event of [{type:'response.created',response:{id,status:'in_progress',output:[]}},{type:'response.output_item.added',output_index:0,item},{type:'response.output_item.done',output_index:0,item},{type:'response.completed',response:{id,status:'completed',output:[item],usage:{input_tokens:1,output_tokens:1,total_tokens:2}}}])res.write('data: '+JSON.stringify(event)+'\n\n');
      res.end();
    }catch(error){res.writeHead(500);res.end(String(error));}
  });
  t.after(async()=>{await broker.stop();assert.deepEqual(await releaseNativeTestLaunches(home,()=>true),[]);model.closeAllConnections();await new Promise<void>(resolve=>model.close(()=>resolve()));store.close();await temp.cleanup();});
  await new Promise<void>(resolve=>model.listen(0,'127.0.0.1',resolve));
  const originalConfig=stringify({model:'gpt-5.1-codex',model_reasoning_effort:'low',model_provider:'transfer_test',approval_policy:'on-request',sandbox_mode:'read-only',check_for_update_on_startup:false,web_search:'disabled',analytics:{enabled:false},features:{apps:false,remote_plugin:false,code_mode:false},model_providers:{transfer_test:{name:'隔离托管检查',base_url:'http://127.0.0.1:'+(model.address() as {port:number}).port+'/v1',wire_api:'responses',requires_openai_auth:false}}});
  writeFileSync(path.join(codexhome,'config.toml'),originalConfig,'utf8');
  const wait=async(predicate:()=>boolean|Promise<boolean>)=>{const end=Date.now()+12000;while(!await predicate()){assert.ok(Date.now()<end,'隔离托管检查等待超时');await delay(50);}};
  const x=store.placeholder('codex',temp.directory),c=store.register(registration('claude','transfer-claude',{cwd:temp.directory})),pair=store.pair(c.id,x.id);
  const address=await codex.ensureEndpoint(temp.directory,x.id),creating=await codex.connect(address),request=creating.request.bind(creating);
  const failure=new BridgeError('fixture_name_failed','隔离夹具在命名提交前拒绝登记');
  let abandoned:string|undefined;const methods:{name:string;threadId?:string}[]=[];
  const failName=t.mock.method(creating,'request',async(method:string,params?:{threadId?:string},options?:Parameters<typeof creating.request>[2])=>{
    methods.push({name:method,threadId:params?.threadId});
    if(method==='thread/name/set')throw failure;
    const response=await request(method,params,options);
    if(method==='thread/start')abandoned=response.thread.id;
    return response;
  });
  try{await assert.rejects(codex.create(x,undefined,undefined,'bypass'),error=>error===failure);}finally{failName.mock.restore();}
  assert.ok(abandoned);assert.equal(store.peer(x.id).sessionId,null);assert.equal(requests,0);
  for(const name of ['thread/settings/update','thread/name/set','thread/delete'])assert.ok(methods.some(call=>call.name===name&&call.threadId===abandoned),JSON.stringify(methods));
  assert.deepEqual([...await loadedCodexThreads(creating)],[]);
  const modelProfile=codex.modelProfile;
  codex.modelProfile=member=>member.id===x.id&&!member.sessionId?{...modelProfile(member),serviceTier:'default'}:modelProfile(member);
  const created=await codex.create(x,undefined,undefined,'bypass');assert.notEqual(created.sessionId,abandoned);assert.equal(requests,0);
  assert.equal(created.permission,'bypass');assert.equal(created.endpoint.codexPermission?.sandbox,'danger-full-access');
  store.adopt(x.id,created);const peer=store.peer(x.id),id=peer.sessionId!,endpoint=store.endpoint(x.id)!,rpc=await codex.attach(peer,endpoint),completed=new Set<string>();
  assert.equal(rpc,creating);assert.equal(codex.hasSubscription(peer,endpoint),true);
  const beforeInput=await rpc.request('thread/read',{threadId:id,includeTurns:false});assert.equal(beforeInput.thread.name,x.name);
  rpc.on('notification',m=>{if(m.method==='turn/completed')completed.add(m.params.turn.id);});
  let firstTurn:string|undefined;
  await codex.wake(peer,endpoint,undefined,undefined,'只返回隔离的原会话答案',id=>firstTurn=id);
  assert.ok(firstTurn);await wait(()=>completed.has(firstTurn!)&&requests===1);
  const file=(await rpc.request('thread/read',{threadId:id,includeTurns:false})).thread.path;
  await wait(async()=>{const profile=await readSessionProfile(file,id);return profile.verified&&profile.settings.approvalPolicy==='never'&&profile.settings.serviceTier==='default';});
  const initialOffset=(await readSessionProfile(file,id)).settingsOffset??0;
  await rpc.request('thread/settings/update',{threadId:id,sandboxPolicy:{type:'workspaceWrite',writableRoots:[temp.directory],networkAccess:false},effort:'low'});
  await wait(async()=>{
    const profile=await readSessionProfile(file,id),permission=normalizedPermission(profile.settings.permissionProfile);
    return (profile.settingsOffset??0)>initialOffset&&permission?.type==='managed'&&permission.network==='restricted'&&permission.file_system?.entries?.some((entry:{access:string})=>entry.access==='write');
  });
  const profile=await readSessionProfile(file,id),old=managedRuntimes(home).find(r=>r.value.peerId===x.id)!.value;
  const upgraded=new CodexAdapter(config,home);t.after(()=>upgraded.close());
  await assert.rejects(upgraded.attach(store.peer(x.id),endpoint,{verify:true}),{code:'owned_subscription_waiting'});
  assert.equal((await readSessionProfile(file,id)).settingsOffset,profile.settingsOffset,'升级连接不能改写已加载会话的设置');
  assert.equal(await ownedProcessState({pid:old.pid!,startedAt:old.procStart!}),'alive');upgraded.close();
  const message=store.accept(pair,c,'托管切换期间的待发任务','pending-transfer');
  const released=await transfers.release(store.peer(x.id)) as any;
  assert.equal(released.hosting.phase,'awaiting_window');assert.equal(await ownedProcessState({pid:old.pid!,startedAt:old.procStart!}),'dead');
  assert.equal(store.pairFor(x.id)?.id,pair.id);assert.equal(store.message(message.id).state,'accepted');
  assert.deepEqual(normalizedPermission((await readSessionProfile(file,id)).settings.permissionProfile),normalizedPermission(profile.settings.permissionProfile),'释放应保留实时权限');
  const portServer=net.createServer();await new Promise<void>(resolve=>portServer.listen(0,'127.0.0.1',resolve));
  const plainAddress='ws://127.0.0.1:'+(portServer.address() as net.AddressInfo).port;await new Promise<void>(resolve=>portServer.close(()=>resolve()));
  const plainLaunch=randomUUID(),launch=selectHostLaunch('codex',config,{home,cwd:temp.directory}),args=[...launch.executable.prefix,'--no-daemon','app-server','--listen',plainAddress],logfile=path.join(home,'run','plain-app-server.log');
  await prepareProcessRegistry(home);
  if(process.platform==='win32')await launchIndependentWindows({home,executable:launch.executable.command,args,cwd:launch.cwd,env:launch.env,logfile,role:'codex-runtime',launchId:plainLaunch,configHome:codexhome});
  else{
    const output=openSync(logfile,'a',0o600);
    try{
      const child=spawnPosixSupervisor(launch.executable.command,args,{cwd:launch.cwd,env:launch.env,stdio:['ignore',output,output],waitForNative:true,home});
      assert.ok(await rememberOwnedChild(home,child,'codex-runtime',{configHome:codexhome,posixGroup:true},plainLaunch));
    }finally{closeSync(output);}
  }
  let plainRpc:Awaited<ReturnType<typeof connectAppServer>>|undefined;
  await wait(async()=>{try{plainRpc=await connectAppServer(plainAddress,config,undefined,home);return true;}catch{return false;}});
  const plainSettings=await plainRpc!.request('thread/resume',{threadId:id,excludeTurns:true,config:{thread_unload_delay_secs:0}});
  await plainRpc!.close();assert.deepEqual(await releaseNativeTestLaunches(home,record=>record.proof.launchId===plainLaunch),[]);
                                               
  const shared=await codex.ensureEndpoint(temp.directory,undefined,undefined,id,codexhome);await codex.connect(shared);
  await codex.resumeForeground(shared,codexhome);
  const remote=await connectAppServer(shared,config,undefined,home);t.after(()=>remote.close());
  const foregroundSettings=await remote.request('thread/resume',{threadId:id,excludeTurns:true,config:{thread_unload_delay_secs:0}});
  for(const key of ['sandbox','activePermissionProfile','approvalPolicy','approvalsReviewer','model','modelProvider','reasoningEffort'])assert.deepEqual(foregroundSettings[key],plainSettings[key],'remote 与同配置原生恢复语义应一致：'+key);
  assert.equal(foregroundSettings.sandbox.type,'readOnly','用户原生客户端按相同配置恢复时使用自己的 read-only 默认值');
  await remote.request('thread/settings/update',{threadId:id,effort:'low'});
  await wait(async()=>normalizedPermission((await readSessionProfile(file,id)).settings.permissionProfile)?.file_system?.entries?.length===1);
  const foregroundProfile=await readSessionProfile(file,id);
  await wait(()=>store.peer(x.id).state==='ready'&&!!store.endpoint(x.id));
  assert.equal(store.peers().filter(p=>p.host==='codex'&&p.sessionId===id).length,1);assert.equal(store.peer(x.id).owner,'user');
  assert.equal(hostingStatus(store,store.peer(x.id))?.phase,'foreground');assert.equal(requests,1,'恢复和发现不能向模型发送提示词');
  const consent=await transfers.adopt(store.peer(x.id)) as any;assert.equal(consent.hosting.reason,'holder_active');assert.equal(store.peer(x.id).owner,'user');
  const foreground=store.endpoint(x.id)!;remote.close();await codex.release(store.peer(x.id),foreground);await wait(async()=>await codex.sessionState(store.peer(x.id),foreground)==='dead');closeHostedWindow(store,store.peer(x.id));
  const adopted=await transfers.adopt(store.peer(x.id)) as {status:string;hosting?:{phase?:string;reason?:string}};
  assert.equal(adopted.status,'paired',JSON.stringify(adopted));assert.equal(adopted.hosting?.phase,'background',JSON.stringify(adopted));
  const adoptedPeer=store.peer(x.id),adoptedEndpoint=store.endpoint(x.id);
  assert.equal(adoptedPeer.owner,'nxbridge');assert.equal(adoptedPeer.sessionId,id);assert.equal(store.pairFor(x.id)?.id,pair.id);assert.equal(requests,1);
  assert.ok(adoptedEndpoint?.runtimeId);assert.notEqual(adoptedEndpoint.runtimeId,old.id);assert.equal(codex.hasSubscription(adoptedPeer,adoptedEndpoint),true);
  const restored=await readSessionProfile(file,id);
  assert.deepEqual(normalizedPermission(restored.settings.permissionProfile),normalizedPermission(foregroundProfile.settings.permissionProfile));
  assert.equal(restored.settings.model,foregroundProfile.settings.model);assert.equal(restored.settings.reasoningEffort,foregroundProfile.settings.reasoningEffort);
  assert.match(readFileSync(file,'utf8'),/隔离的原会话答案/);assert.equal(readFileSync(path.join(codexhome,'config.toml'),'utf8'),originalConfig);
});
