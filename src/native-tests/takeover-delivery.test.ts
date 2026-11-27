import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {mkdirSync,readFileSync,writeFileSync} from 'node:fs';
import {stringify} from 'smol-toml';
import {Broker,type Adapters} from '../broker/coordinator.js';
import {Takeovers} from '../broker/recovery/takeover.js';
import {CodexAdapter,connectAppServer} from '../hosts/codex/adapter.js';
import {runtimeFor,saveManagedRuntime} from '../hosts/managed-runtimes.js';
import {writeHosting} from '../broker/recovery/session-hosting.js';
import {readSessionProfile,planCodexResume,verifyCodexResumeAck} from '../hosts/codex/codex-session-profile.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {Store} from '../storage/store.js';
import {ensureHome} from '../ipc/home.js';
import {processIdentity} from '../process/process.js';
import {rememberOwnedChild} from '../process/owned-processes.js';
import {delay} from '../shared/time.js';
import {FakeAdapter} from '../tests/support/fake-adapter.js';
import {registration} from '../tests/support/registration.js';
import {temporaryDirectory} from '../tests/support/temporary.js';
import {releaseNativeTestLaunches} from '../tests/support/native-launch.js';
import {lifecycle} from '../tests/support/broker-fixture.js';
import {readModelBody} from './model-http-fixture.js';

test('原生远程客户端沿用接管订阅，忙碌来信保留，冷接续只重连原线程且退出后不重复投递',{timeout:90000},async t=>{
  const temp=temporaryDirectory(),root=temp.directory,home=path.join(root,'bridge'),codexHome=path.join(root,'codex');
  mkdirSync(codexHome);await ensureHome(home);
  let requests=0,responseHold:Promise<void>|undefined,finishResponse=()=>{};
  const model=http.createServer(async(req,res)=>{
    try{
      const body=await readModelBody(req);
      if(!req.url?.includes('/responses')){res.writeHead(200,{'content-type':'application/json'});res.end('{"data":[]}');return;}
      JSON.parse(body.toString());const id='takeover-'+(++requests);
      await responseHold;
      const item={id:'answer-'+id,type:'message',role:'assistant',status:'completed',content:[{type:'output_text',text:'隔离接管轮次完成 '+id}]};
      res.writeHead(200,{'content-type':'text/event-stream'});
      for(const event of [{type:'response.created',response:{id,status:'in_progress',output:[]}},{type:'response.output_item.added',output_index:0,item},
        {type:'response.output_item.done',output_index:0,item},{type:'response.completed',response:{id,status:'completed',output:[item],usage:{input_tokens:1,output_tokens:1,total_tokens:2}}}])
        res.write('data: '+JSON.stringify(event)+'\n\n');
      res.end();
    }catch(error){res.writeHead(500);res.end(String(error));}
  });
  await new Promise<void>(resolve=>model.listen(0,'127.0.0.1',resolve));
  writeFileSync(path.join(codexHome,'config.toml'),stringify({model:'gpt-5.1-codex',model_provider:'takeover_local',model_reasoning_effort:'low',
    approval_policy:'never',sandbox_mode:'danger-full-access',web_search:'disabled',check_for_update_on_startup:false,analytics:{enabled:false},
    features:{apps:false,remote_plugin:false,code_mode:false},model_providers:{takeover_local:{name:'本地接管验收',
      base_url:'http://127.0.0.1:'+(model.address() as {port:number}).port+'/v1',wire_api:'responses',requires_openai_auth:false}}}),'utf8');
  const config={...DEFAULT_CONFIG,implicit:'never' as const,codexHome,...(process.env.NXBRIDGE_TEST_CODEX_BINARY?{codexBinary:process.env.NXBRIDGE_TEST_CODEX_BINARY}:{})};
  const database=path.join(home,'takeover-test.sqlite');let store=new Store(database),codex=new CodexAdapter(config,home),broker=new Broker(store,config,{codex,claude:new FakeAdapter()} as unknown as Adapters,home);
  const windows:Awaited<ReturnType<typeof connectAppServer>>[]=[];
  t.after(async()=>{
    finishResponse();for(const window of windows)window.close();await broker.stop();
    assert.deepEqual(await releaseNativeTestLaunches(home,()=>true),[]);
    model.closeAllConnections();await new Promise<void>(resolve=>model.close(()=>resolve()));store.close();await temp.cleanup();
  });
  const wait=async(check:()=>boolean|Promise<boolean>,description:string)=>{
    const deadline=Date.now()+15000;while(!await check()){assert.ok(Date.now()<deadline,description);await delay(40);}
  };
  const x=store.placeholder('codex',root),c=store.register(registration('claude','takeover-native-partner',{cwd:root})),pair=store.pair(c.id,x.id);
  const modelProfile=codex.modelProfile;codex.modelProfile=member=>({...modelProfile(member),serviceTier:'default'});
  const created=await codex.create(x,undefined,undefined,'bypass');store.adopt(x.id,created);
  const peer=()=>store.peer(x.id),endpoint=()=>store.endpoint(x.id)!,id=peer().sessionId!,address=endpoint().address;
  const initial=runtimeFor(home,{endpoint:address})!.value,rpc=await codex.attach(peer(),endpoint());
  const wake=()=>((broker as any).signalCodexWake(peer(),endpoint()) as Promise<unknown>);
  const idle=()=>wait(async()=>store.peer(x.id).state==='ready'&&(await (await codex.connect(address)).request('thread/read',{threadId:id,includeTurns:false})).thread.status.type==='idle','原生轮次没有回到空闲');
  await codex.wake(peer(),endpoint(),undefined,undefined,'创建隔离接管验收历史');await wait(()=>requests===1,'初始轮次没有进入本机模型');await idle();
  const remote=await connectAppServer(address,config,undefined,home);windows.push(remote);
  await remote.request('thread/resume',{threadId:id,excludeTurns:true,config:{thread_unload_delay_secs:0}});
  const events:any[]=[];remote.on('notification',event=>{if(event.params?.threadId===id||event.params?.thread?.id===id)events.push(event);});
  const stream=(turn:string)=>events.filter(event=>(event.params.turnId??event.params.turn?.id)===turn&&/^(item\/|turn\/)/.test(event.method))
    .map(event=>[event.method,event.params.item?.type??null,event.params.turn?.status??null]);
  let takeovers=new Takeovers(store);const lease=takeovers.begin(peer(),endpoint());
                                       
  const foreground=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore',windowsHide:true});
  await once(foreground,'spawn');await rememberOwnedChild(home,foreground,'host-control');
  const identity=await processIdentity(foreground.pid!);takeovers.claim(x.id,lease.id,{pid:identity.pid,procStart:identity.startedAt});
  assert.equal(codex.hasSubscription(peer(),endpoint()),true);
  const first=store.accept(pair,c,'接管空闲时收到的新任务','native-takeover-idle');
  await wake();await wait(()=>requests===2,'接管空闲时没有唤醒原线程');await idle();
  assert.equal(store.message(first.id).state,'contextObserved');assert.equal(store.message(first.id).attempts,1);
  assert.equal(await takeovers.returned(x.id,lease.id),false);
  const baseline=await remote.request('turn/start',{threadId:id,input:[{type:'text',text:'无额外订阅时的对照轮次'}]});
  await wait(()=>requests===3&&events.some(event=>event.method==='turn/completed'&&event.params.turn.id===baseline.turn.id),'对照轮次没有正常结束');await idle();
  responseHold=new Promise(resolve=>{finishResponse=resolve;});
  const busy=await remote.request('turn/start',{threadId:id,input:[{type:'text',text:'窗口正在处理人工输入'}]});
  await wait(()=>requests===4&&peer().state==='busy','远程人工轮次没有进入忙碌状态');
  const second=store.accept(pair,c,'忙碌结束后处理这条来信','native-takeover-busy');
  await wake();assert.equal(requests,4);assert.equal(store.message(second.id).state,'accepted');assert.equal(store.message(second.id).attempts,0);
                                                      
  const file=(await rpc.request('thread/read',{threadId:id,includeTurns:false})).thread.path;
  const saved=await readSessionProfile(file,id),subscriber=await connectAppServer(address,config,undefined,home);windows.push(subscriber);
  await remote.request('thread/read',{threadId:id,includeTurns:false});const eventOffset=events.length;
  const started=Date.now(),ack=await subscriber.request('thread/resume',planCodexResume(id,saved,root,{exact:true}).args,{timeout:3000});
  await verifyCodexResumeAck(saved,ack,{cwd:root});
  const after=await readSessionProfile(file,id);
  await remote.request('thread/read',{threadId:id,includeTurns:false});
  assert.deepEqual(events.slice(eventOffset).filter(event=>/^(item\/|turn\/|thread\/started)/.test(event.method)),[],'增加订阅不能向原窗口重播历史或重复轮次和条目');
  assert.deepEqual(after.settings,saved.settings);assert.equal(after.settingsOffset,saved.settingsOffset);assert.equal(requests,4);
  assert.equal(ack.thread.status.type,'active');subscriber.close();
  t.diagnostic(JSON.stringify({loadedSubscribeMs:Date.now()-started,whileModelBlocked:true,settingsUnchanged:true,request:'thread/resume',realModelCalls:0}));
  await assert.rejects(lifecycle(broker).prepare(peer()),{code:'foreground_active'});
  await assert.rejects((broker as any).restartCodex({peer_id:x.id}),{code:'foreground_active'});
  assert.deepEqual((broker as any).handoffRoutes(),[]);
  finishResponse();responseHold=undefined;
  await wait(()=>events.some(event=>event.method==='turn/completed'&&event.params.turn.id===busy.turn.id),'额外订阅后原窗口的人工轮次没有正常结束');await idle();
  assert.deepEqual(stream(busy.turn.id),stream(baseline.turn.id),'原窗口的轮次和条目通知应与无额外订阅的对照一致');
  await wake();await wait(()=>requests===5,'人工轮次结束后没有接续来信');await idle();
  assert.equal(store.message(second.id).state,'contextObserved');assert.equal(store.message(second.id).attempts,1);
  const history=readFileSync(file,'utf8');
  assert.equal(history.split('接管空闲时收到的新任务').length-1,1);assert.equal(history.split('忙碌结束后处理这条来信').length-1,1);
                                        
  await remote.request('thread/settings/update',{threadId:id,effort:'medium'});
  const chosen=await remote.request('thread/resume',{threadId:id,excludeTurns:true});assert.equal(chosen.reasoningEffort,'medium');
  const selected=(value:any)=>Object.fromEntries(['model','modelProvider','reasoningEffort','serviceTier','approvalPolicy','approvalsReviewer','sandbox','activePermissionProfile'].map(key=>[key,value[key]]));
  const pending=runtimeFor(home,{endpoint:address})!.value;
  pending.sessionVerification={sessionId:id,profile:await readSessionProfile(file,id)};saveManagedRuntime(home,pending);
  writeHosting(store,peer(),{verifying:true,verified:false,verificationRuntimeId:pending.id,verificationEndpoint:address});
                                              
  await broker.stop();codex=new CodexAdapter(config,home);broker=new Broker(store,config,{codex,claude:new FakeAdapter()} as unknown as Adapters,home);
  const restored=await lifecycle(broker).schedulePairedRecovery(x.id,true);
  assert.deepEqual(selected(await remote.request('thread/resume',{threadId:id,excludeTurns:true})),selected(chosen),'重启追加订阅不能覆盖尚未开始新一轮的人工设置');
  assert.equal(restored,true);
  assert.equal(runtimeFor(home,{endpoint:address})!.value.sessionVerification,undefined);
  assert.equal((await broker.dispatch('hosting.status',{peer_id:x.id}) as any).hosting.verifying,false);
  assert.equal(codex.hasSubscription(peer(),endpoint()),true);assert.equal(takeovers.get(x.id)?.id,lease.id);assert.equal(requests,5);
  const third=store.accept(pair,c,'升级时远程窗口保持打开的任务','native-takeover-update');
  const parked=await broker.sealForHandoff({updateId:'takeover-native-update',signal:AbortSignal.timeout(15000),commit:()=>{}});
  parked.close();store.close();store=new Store(database);takeovers=new Takeovers(store);codex=new CodexAdapter(config,home);broker=new Broker(store,config,{codex,claude:new FakeAdapter()} as unknown as Adapters,home);
  await broker.start();
  await wait(()=>requests===6&&store.message(third.id).state==='contextObserved','接班期间来信没有在远程窗口仍打开时自动送达');await idle();
  assert.deepEqual(selected(await remote.request('thread/resume',{threadId:id,excludeTurns:true})),selected(chosen),'升级接续及原来信唤醒仍保留人工设置');
  assert.equal(remote.closed,false);assert.equal(store.message(third.id).attempts,1);assert.equal(takeovers.get(x.id)?.id,lease.id);
                                             
  await broker.stop();codex=new CodexAdapter(config,home);broker=new Broker(store,config,{codex,claude:new FakeAdapter()} as unknown as Adapters,home);
  const fourth=store.accept(pair,c,'窗口在重新订阅期间退出的来信','native-takeover-close-race');
  const reconnect=await codex.connect(address),request=reconnect.request.bind(reconnect);let closedInFlight=false;
  const closeDuringAck=t.mock.method(reconnect,'request',async(method:string,params?:any,options?:Parameters<typeof reconnect.request>[2])=>{
    const ack=await request(method,params,options);
    if(method==='thread/resume'&&params.threadId===id){
      remote.close();const exited=once(foreground,'exit');foreground.kill();await exited;closedInFlight=true;
    }
    return ack;
  });
  try{assert.equal(await lifecycle(broker).schedulePairedRecovery(x.id,true),true);}
  finally{closeDuringAck.mock.restore();}
  assert.equal(closedInFlight,true);assert.equal(codex.hasSubscription(peer(),endpoint()),true);
  assert.equal(await takeovers.returned(x.id,lease.id),true);await wake();await wait(()=>requests===7,'窗口退出竞态后原来信未送达');await idle();
  assert.equal((await broker.dispatch('hosting.status',{peer_id:x.id}) as any).hosting.phase,'background');
  assert.equal(store.message(fourth.id).state,'contextObserved');assert.equal(store.message(fourth.id).attempts,1);
  const current=runtimeFor(home,{endpoint:address})!.value;
  assert.equal(current.id,initial.id);assert.equal(current.pid,initial.pid);assert.equal(current.procStart,initial.procStart);assert.equal(peer().sessionId,id);
  await wake();await wake();assert.equal(requests,7);
  assert.equal(store.message(first.id).attempts,1);assert.equal(store.message(second.id).attempts,1);
  t.diagnostic(JSON.stringify({sessionId:id,requests,realModelCalls:0,sameRuntime:true,idleDelivery:true,busyDelivery:true,crashNarrowAttach:true,updateWithRemoteOpen:true,remoteStreamUnchanged:true,closeDuringAck:true,noDuplicateAfterReturn:true}));
});
