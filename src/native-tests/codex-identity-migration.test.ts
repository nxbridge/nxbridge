import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import net from 'node:net';
import path from 'node:path';
import { Duplex } from 'node:stream';
import { closeSync, existsSync, mkdirSync, openSync, readFileSync, writeFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { readModelBody } from './model-http-fixture.js';
import WebSocket from 'ws';
import { stringify } from 'smol-toml';
import { CodexAdapter, codexPermission, connectAppServer } from '../hosts/codex/adapter.js';
import { Broker, type Adapters } from '../broker/coordinator.js';
import { Store } from '../storage/store.js';
import { RpcClient } from '../ipc/rpc.js';
import {BridgeError} from '../shared/errors.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {delay} from '../shared/time.js';
import {ensureHome,socketDirectory} from '../ipc/home.js';
import {spawnCli} from '../process/host-command.js';
import { selectHostLaunch } from '../hosts/host-launch.js';
import { managedMarker } from '../install/installation.js';
import { managedRuntimes, saveManagedRuntime, createRuntimeToken, runtimeDirectory } from '../hosts/managed-runtimes.js';
import { requireDedicatedCodex } from '../hosts/codex/codex-isolation.js';
import { liveProcess, nativeDescendant } from '../process/process.js';
import { declareOwnedNative, prepareProcessRegistry, rememberOwnedChild, rememberOwnedHost } from '../process/owned-processes.js';
import { launchIndependentWindows } from '../process/windows/windows-background.js';
import { spawnPosixSupervisor } from '../process/posix/posix-supervisor.js';
import { FakeAdapter } from '../tests/support/fake-adapter.js';
import { registration } from '../tests/support/registration.js';
import { temporaryDirectory } from '../tests/support/temporary.js';
import { releaseNativeTestLaunches } from '../tests/support/native-launch.js';

test('旧 nxbridge 身份的真实后台：无订阅不触碰已加载线程，卸载后迁移同一 ID 并使用官方 UA',{timeout:150000},async t=>{
  const temp=temporaryDirectory(),root=temp.directory,home=path.join(root,'bridge'),codexHome=path.join(root,'codex'),launchId=randomUUID();
  mkdirSync(codexHome);await ensureHome(home);
  const config={...DEFAULT_CONFIG,codexHome,...(process.env.NXBRIDGE_TEST_CODEX_BINARY?{codexBinary:process.env.NXBRIDGE_TEST_CODEX_BINARY}:{})};
  const codex=new CodexAdapter(config,home),store=new Store(':memory:'),broker=new Broker(store,config,{codex,claude:new FakeAdapter()} as unknown as Adapters,home);
  const requests:{ua:string;body:any}[]=[],errors:string[]=[];let release!:()=>void,old:RpcClient|undefined;
  const held=new Promise<void>(resolve=>release=resolve);
  const model=http.createServer(async(req,res)=>{try{
    if(!req.url?.includes('/responses')){res.writeHead(200,{'content-type':'application/json'});res.end('{"data":[]}');return;}
    const raw=await readModelBody(req);
    requests.push({ua:String(req.headers['user-agent']),body:JSON.parse(raw.toString())});const number=requests.length;if(number===1)await held;
    const id='migration-'+number,item={id:'msg-'+id,type:'message',role:'assistant',status:'completed',content:[{type:'output_text',text:'MIGRATION_HISTORY_'+number}]};
    res.writeHead(200,{'content-type':'text/event-stream'});
    for(const event of [{type:'response.created',response:{id,status:'in_progress',output:[]}},{type:'response.output_item.added',output_index:0,item},{type:'response.output_item.done',output_index:0,item},{type:'response.completed',response:{id,status:'completed',output:[item],usage:{input_tokens:1,output_tokens:1,total_tokens:2}}}])res.write('data: '+JSON.stringify(event)+'\n\n');res.end();
  }catch(error){errors.push(String(error));res.writeHead(500);res.end(String(error));}});
  await new Promise<void>(resolve=>model.listen(0,'127.0.0.1',resolve));
  t.after(async()=>{
    release();old?.close();await broker.stop();
                                                      
    assert.deepEqual(await releaseNativeTestLaunches(home,record=>record.role==='codex-runtime'||record.proof.launchId===launchId),[]);
    model.closeAllConnections();await new Promise<void>(resolve=>model.close(()=>resolve()));store.close();await temp.cleanup();
  });
  writeFileSync(path.join(codexHome,'config.toml'),stringify({model:'gpt-5.1-codex',model_provider:'local_migration',model_reasoning_effort:'low',approval_policy:'never',sandbox_mode:'danger-full-access',thread_unload_delay_secs:0,web_search:'disabled',analytics:{enabled:false},features:{apps:false,remote_plugin:false,daemon_auto_start:false,code_mode:false},model_providers:{local_migration:{name:'本地迁移验收',base_url:'http://127.0.0.1:'+(model.address() as any).port+'/v1',wire_api:'responses',requires_openai_auth:false,request_max_retries:0,stream_max_retries:0}}}),'utf8');
  const launch=selectHostLaunch('codex',config,{home,cwd:root});
  const capability=await requireDedicatedCodex(launch);let address:string,token:string|undefined,authentication:string[]=[];
  if(capability.authenticatedWebsocket){
    const listener=net.createServer();await new Promise<void>(resolve=>listener.listen(0,'127.0.0.1',resolve));
    address='ws://127.0.0.1:'+(listener.address() as net.AddressInfo).port;await new Promise<void>(resolve=>listener.close(()=>resolve()));
    mkdirSync(runtimeDirectory(home),{recursive:true});
    const tokenFile=createRuntimeToken(home,{id:launchId,owner:managedMarker(home),kind:'owned',state:'starting',endpoint:address,authenticatedLoopback:true,createdAt:Date.now()});
    token=readFileSync(tokenFile,'utf8').trim();authentication=['--ws-auth','capability-token','--ws-token-file',tokenFile];
  }else address='unix://'+path.join(await socketDirectory(home),'old-identity.sock');
  const args=[...launch.executable.prefix,'--no-daemon','app-server','--listen',address,...authentication],logfile=path.join(home,'run','old-identity.log');
  await prepareProcessRegistry(home);
  const wrapper=process.platform==='win32'?await launchIndependentWindows({home,executable:launch.executable.command,args,cwd:root,env:launch.env,logfile,role:'codex-runtime',launchId,configHome:codexHome}):await(async()=>{
    const fd=openSync(logfile,'a',0o600);
    try{
      const child=spawnPosixSupervisor(launch.executable.command,args,{cwd:root,env:launch.env,stdio:['ignore',fd,fd],waitForNative:true,home});
      return await rememberOwnedChild(home,child,'codex-runtime',{configHome:codexHome,posixGroup:true},launchId);
    }finally{closeSync(fd);}
  })();
  assert.ok(wrapper);
  const wait=async(check:()=>boolean|Promise<boolean>)=>{const end=Date.now()+25000;while(!await check()){if(Date.now()>end)assert.fail('迁移验收超时：'+JSON.stringify({requests:requests.length,errors,status:broker.status(),runtimes:managedRuntimes(home)}));await delay(80);}};
  await wait(async()=>{try{const rpc=await connectAppServer(address,config,token,home,{readOnly:true});rpc.close();return true;}catch{return false;}});
                                         
  const options:any={closeTimeout:1000};
  if(token)options.headers={Authorization:'Bearer '+token};
  if(address.startsWith('unix://')){
    let stream:net.Socket|Duplex;
    if(process.platform==='win32'){
      const proxy=spawnCli('codex',['app-server','proxy','--sock',address.slice(7)],{stdio:['pipe','pipe','ignore']},config,home);await rememberOwnedHost(home,proxy,'codex','proxy',launchId);
      stream=Duplex.from({readable:proxy.stdout!,writable:proxy.stdin!});stream.on('error',()=>{});
      const socketStream=stream as any;socketStream.setNoDelay=socketStream.setKeepAlive=socketStream.setTimeout=()=>stream;
    }else stream=net.createConnection(address.slice(7));
    options.createConnection=()=>stream;
  }
  const ws=new WebSocket(address.startsWith('unix://')?'ws://localhost':address,options);
  await new Promise<void>((resolve,reject)=>{ws.once('open',resolve);ws.once('error',reject);});
  old=new RpcClient(message=>ws.send(JSON.stringify(message)),()=>ws.close());ws.on('message',raw=>old!.receive(JSON.parse(raw.toString())));ws.on('close',()=>old!.disconnected());ws.on('error',()=>old!.disconnected());
  const initialized=await old.request('initialize',{clientInfo:{name:'nxbridge',version:'0.1.1'},capabilities:{experimentalApi:true}});old.notify('initialized');assert.match(initialized.userAgent,/^nxbridge\//);
  const {thread}=await old.request('thread/start',{cwd:root,serviceTier:'default',approvalPolicy:'never',sandbox:'danger-full-access',config:{thread_unload_delay_secs:0}});
  await old.request('turn/start',{threadId:thread.id,input:[{type:'text',text:'升级前正在进行的隔离轮次'}]});await wait(()=>requests.length===1);
  const cutoff=requests.length,placeholder=store.placeholder('codex',root);
  let native:Awaited<ReturnType<typeof nativeDescendant>>|undefined;
  await wait(async()=>{
    try{native=await nativeDescendant(wrapper.identity.pid,'codex',0,address,wrapper);return true;}
    catch(error){if(error instanceof BridgeError&&error.code==='process_identity_unknown')return false;throw error;}
  });
  assert.ok(native);
  await declareOwnedNative(wrapper,native);
  saveManagedRuntime(home,{id:launchId,peerId:placeholder.id,owner:managedMarker(home),kind:'owned',state:'ready',createdAt:Date.now(),endpoint:address,pid:native.pid,procStart:native.startedAt,configHome:codexHome,executable:launch.executable.command,...(token?{authenticatedLoopback:true}:{})});
  const peer=store.adopt(placeholder.id,registration('codex',thread.id,{cwd:root,state:'busy',endpoint:{mode:'app-server',address,epoch:address,pid:native.pid,procStart:native.startedAt,configHome:codexHome}}));
  const partner=store.register(registration('claude','migration-source',{cwd:root})),pair=store.pair(partner.id,peer.id),message=store.accept(pair,partner,'迁移后只投递一次','upgrade');
  const stranded=store.accept(pair,partner,'旧任务已由其他请求处理，绝不重发','stranded');store.claim(stranded.id);store.transition(stranded.id,'runtimeQueued','升级前中断留下的排队状态');
                                
  store.deactivate(peer.id);store.updatePeer(peer.id,{state:'offline'});
  store.saveEndpoint({peerId:peer.id,mode:'embedded',address:'',epoch:'embedded:'+native.pid+':'+native.startedAt+':'+thread.id,pid:native.pid,procStart:native.startedAt,processKind:'app-server',configHome:codexHome,seenAt:Date.now(),active:true});
  await broker.start();
  assert.equal(store.endpoint(peer.id)?.mode,'app-server');assert.equal(store.bindings.current(peer.id),undefined);assert.equal(store.endpoints(peer.id).some(e=>e.active&&e.mode==='embedded'),false);
  await broker.pump();assert.equal(liveProcess(native.pid),true);assert.equal(requests.length,cutoff);assert.equal(store.message(message.id).attempts,0);assert.match((broker.status() as any).capabilities.identityIssues[0].message,/卸载/);
  assert.equal((await old.request('thread/read',{threadId:thread.id,includeTurns:false})).thread.status.type,'active');
  assert.equal(store.peer(peer.id).launchFailure,undefined);
  store.updatePeer(peer.id,{state:'paused'});
  release();await wait(async()=>{const live=(await old!.request('thread/read',{threadId:thread.id,includeTurns:true})).thread;return live.status.type==='idle'&&live.turns?.at(-1)?.status==='completed';});
  await old.request('thread/settings/update',{threadId:thread.id,serviceTier:'default'});
  await assert.rejects(codex.restart(store.peer(peer.id),store.endpoint(peer.id)!,()=>{},{identityMigration:true}),{code:'owned_subscription_waiting'});
  assert.equal(requests.length,cutoff);assert.equal(liveProcess(native.pid),true);
  const before=await old.request('thread/resume',{threadId:thread.id,excludeTurns:true});
  await wait(()=>!!before.thread.path&&existsSync(before.thread.path)&&readFileSync(before.thread.path,'utf8').includes('MIGRATION_HISTORY_1'));
  const unsubscribed=await old.request('thread/unsubscribe',{threadId:thread.id});old.close();
  const inspector=await connectAppServer(address,config,undefined,home,{readOnly:true});
  try{
    let observed:any;
    try{await wait(async()=>{
      let unloaded:any;
      try{unloaded=(await inspector.request('thread/read',{threadId:thread.id,includeTurns:true})).thread;}
      catch(error){if(error instanceof BridgeError&&error.message.includes('is not materialized yet; includeTurns is unavailable')){observed={materializing:true};return false;}throw error;}
      observed={status:unloaded.status,lastTurn:unloaded.turns?.at(-1)?.status};
      return unloaded.status.type==='notLoaded'&&unloaded.turns?.at(-1)?.status==='completed';
    });}finally{t.diagnostic(JSON.stringify({unsubscribed,unloadObservation:observed}));}
    t.diagnostic(JSON.stringify({unloadedInspection:await Promise.all(['thread/queue/list','thread/backgroundTerminals/list'].map(async method=>{try{return{method,result:await inspector.request(method,{threadId:thread.id,limit:1})};}catch(error){return{method,error:String(error)};}}))}));
  }finally{await inspector.close();}
  store.updatePeer(peer.id,{state:'offline'});await delay(1100);
  await broker.pump();await wait(async()=>{await broker.pump();return store.message(message.id).state==='contextObserved';});
  assert.equal(liveProcess(native.pid),false);assert.equal(store.peer(peer.id).sessionId,thread.id);assert.equal(store.message(message.id).attempts,1);assert.equal(store.currentPair()?.id,pair.id);
  assert.equal(store.message(stranded.id).state,'unknown');assert.equal(store.message(stranded.id).attempts,1);assert.match(store.message(stranded.id).evidence!,/不自动重发/);
  const fresh=await codex.attach(store.peer(peer.id),store.endpoint(peer.id)!),after=await fresh.request('thread/resume',{threadId:thread.id});
  for(const key of ['model','modelProvider','reasoningEffort','cwd','approvalPolicy','sandbox'])assert.deepEqual(after[key],before[key]);
  assert.equal(store.endpoint(peer.id)!.codexPermission!.sandbox,'danger-full-access');
  await broker.dispatch('hook',{session_id:thread.id,hook_event_name:'PreToolUse',permission_mode:'bypassPermissions'});
  assert.equal(codexPermission(after),'bypass');assert.equal(store.peer(peer.id).permission,'bypass');
                                          
  const observer=new CodexAdapter(config,home);let bareObservation=false;
  observer.on('register',r=>{if(r.sessionId===thread.id&&!r.endpoint.codexPermission){bareObservation=true;codex.emit('register',r,peer.id);}});
  try{
    const address=store.endpoint(peer.id)!.address,observing=await observer.connect(address);
    await observer.discoverLoaded(address,observing);assert.equal(bareObservation,false);
    await assert.rejects(observing.request('thread/read',{threadId:thread.id,includeTurns:false}),{code:'owned_subscription_waiting'});
    await broker.pump();
    await broker.dispatch('hook',{session_id:thread.id,hook_event_name:'PostToolUse',permission_mode:'bypassPermissions'});
    assert.equal(store.peer(peer.id).permission,'bypass');assert.equal(store.endpoint(peer.id)!.codexPermission!.sandbox,'danger-full-access');
  }finally{observer.close();}
  const migratedPid=store.endpoint(peer.id)!.pid;
  store.updatePeer(peer.id,{state:'paused'});
  await wait(async()=>{const current=(await fresh.request('thread/read',{threadId:thread.id,includeTurns:true})).thread;return current.status.type==='idle'&&current.turns?.at(-1)?.status==='completed';});
  await fresh.request('thread/unsubscribe',{threadId:thread.id});
  await wait(async()=>{const current=(await fresh.request('thread/read',{threadId:thread.id,includeTurns:false})).thread;return current.status.type==='notLoaded';});
  store.updatePeer(peer.id,{state:'offline'});
  const afterUnload=store.accept(pair,partner,'原线程卸载后的新消息仍只投递一次','after-unload');
  await wait(async()=>{await broker.pump();return store.message(afterUnload.id).state==='contextObserved';});
  assert.equal(store.peer(peer.id).sessionId,thread.id);assert.equal(store.endpoint(peer.id)!.pid,migratedPid);
  assert.equal(store.message(afterUnload.id).attempts,1);assert.equal(store.message(message.id).attempts,1);
  assert.equal(store.message(stranded.id).state,'unknown');assert.equal(store.message(stranded.id).attempts,1);
  assert.match(JSON.stringify(after.thread.turns),/MIGRATION_HISTORY_1/);await wait(()=>requests.length>cutoff);
  for(const request of requests.slice(cutoff)){assert.match(request.ua,/^codex-tui\/\d+\.\d+\.\d+ .* \(codex-tui; [^)]+\)$/);assert.doesNotMatch(request.ua,/nxbridge/);assert.equal(Object.hasOwn(request.body,'previous_response_id'),false);}
  assert.deepEqual(errors,[]);t.diagnostic(JSON.stringify({preUpgradeRequests:cutoff,newRequests:requests.length-cutoff,oldPid:native.pid,newPid:managedRuntimes(home).find(r=>r.value.peerId===peer.id)?.value.pid,sessionPreserved:true,newUA:requests.at(-1)!.ua}));
});
