import { overrideEnvironment } from '../tests/support/environment.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile,spawn} from 'node:child_process';
import {promisify} from 'node:util';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import {existsSync,mkdirSync,readdirSync,readFileSync,rmdirSync,unlinkSync,writeFileSync} from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { readModelBody } from './model-http-fixture.js';
import { CodexAdapter, connectAppServer, threadRegistration } from '../hosts/codex/adapter.js';
import { Broker, type Adapters } from '../broker/coordinator.js';
import { Store } from '../storage/store.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {atomicJson,readJson} from '../shared/files.js';
import {delay} from '../shared/time.js';
import {ensureHome,removeOwnedSocket} from '../ipc/home.js';
import {codexSocketAlias,codexSocketTargetMissing} from '../ipc/unix-socket.js';
import {PACKAGE_ROOT} from '../shared/package-location.js';
import {codexArguments,liveProcess,processAncestors,processIdentity} from '../process/process.js';
import {ownedProcessState} from '../process/process-stop.js';
import {releaseOwnedProcess,rememberOwnedChild} from '../process/owned-processes.js';
import {windowsHookIdentityHelper} from '../process/windows/windows-hook-identity.js';
import {BridgeError,RuntimeRecoveryHold} from '../shared/errors.js';
import { FakeAdapter } from '../tests/support/fake-adapter.js';
import { registration } from '../tests/support/registration.js';
import { temporaryDirectory } from '../tests/support/temporary.js';
import { releaseNativeTestLaunches } from '../tests/support/native-launch.js';
import {managedRuntimes,runtimeFor,saveManagedRuntime} from '../hosts/managed-runtimes.js';
import { readCodexHistory } from '../hosts/codex/codex-history.js';
import {readSessionProfile} from '../hosts/codex/codex-session-profile.js';
import { collectCodexRecycleEvidence } from '../hosts/codex/codex-recycle.js';
import { codexServerVersion } from '../hosts/codex/codex-client-identity.js';

test('Windows 原生停止助手超过绝对期限时保留同代次隔离子进程',{skip:process.platform!=='win32'},async t=>{
  const temp=temporaryDirectory();
  const child=spawn(process.execPath,['-e','setInterval(() => {}, 1000)'],{windowsHide:true,stdio:'ignore'});
  let owner:Awaited<ReturnType<typeof rememberOwnedChild>>;
  t.after(async()=>{try{if(owner)await releaseOwnedProcess(owner);else child.kill();}finally{await temp.cleanup();}});
  owner=await rememberOwnedChild(temp.directory,child,'host-control');
  assert.ok(owner);
  const helper=await windowsHookIdentityHelper(),deadline=BigInt(Date.now()-1000)*10000n+116444736000000000n;
  const {stdout}=await promisify(execFile)(helper,['stop',String(owner.identity.pid),owner.identity.startedAt,'3000',String(deadline)],{windowsHide:true,timeout:5000});
  assert.equal(JSON.parse(stdout),'pending');
  assert.equal(await ownedProcessState(owner.identity),'alive');
});

for(const sample of [false,true,'idle'] as const)test(sample==='idle'?'真实空闲 Windows 后台独立核验后自动回收原会话':
  '真实托管 Codex 安全重启原线程，保持历史/运行设置并加载新 MCP 和技能'+(sample?'（命名权限）':'（旧式权限）'),
{timeout:180000,skip:sample==='idle'&&process.platform!=='win32'},async t=>{
  const named=sample===true,clean=sample==='idle';
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge'),codexhome=path.join(temp.directory,'codex');
  mkdirSync(codexhome,{recursive:true});
  const env={NXBRIDGE_HOME:home,CODEX_HOME:codexhome,CLAUDE_CONFIG_DIR:path.join(temp.directory,'claude'),OPENAI_API_KEY:'nxbridge-local-restart-test'};
  const restoreEnvironment=overrideEnvironment(env);
  const pinned=process.env.NXBRIDGE_TEST_CODEX_BINARY;
  const triple=process.arch==='arm64'?'aarch64-pc-windows-msvc':'x86_64-pc-windows-msvc';
  const direct=clean&&pinned&&/\.js$/.test(pinned)?path.join(path.dirname(pinned),'..','..','codex-win32-'+process.arch,'vendor',triple,'bin','codex.exe'):undefined;
  const config={...DEFAULT_CONFIG,codexHome:codexhome,...(pinned?{codexBinary:direct&&existsSync(direct)?direct:pinned}:{})};
  const codex=new CodexAdapter(config,home),store=new Store(':memory:'),broker=new Broker(store,config,{codex,claude:new FakeAdapter()} as unknown as Adapters,home);
  const requests:any[]=[],pids=new Set<number>(),addresses=new Set<string>();let release!:()=>void,releaseDelivery=()=>{};
  const reasoning={type:'reasoning',id:named?'rs_00000000-0000-7000-8000-000000000001':'rs_0123456789abcdef0123456789abcdef',summary:[{type:'summary_text',text:'隔离的持久推理摘要'}],encrypted_content:'isolated-encrypted-restart-payload'};
  const held=new Promise<void>(resolve=>release=resolve);
  let releaseActive=()=>{};
  const activeReply=new Promise<void>(resolve=>releaseActive=resolve);
  const model=http.createServer(async(req,res)=>{
    try{
      const raw=await readModelBody(req);
      if(!req.url?.includes('/responses')){res.writeHead(200,{'content-type':'application/json'});res.end('{"data":[]}');return;}
      const request=JSON.parse(raw.toString());requests.push(request);if(requests.length===1)await held;
      if(JSON.stringify(request.input?.at(-1)).includes('隔离的活动回收轮次'))await activeReply;
      if(!named&&(requests.length===2||requests.length===4)){res.writeHead(429,{'content-type':'application/json'});res.end(JSON.stringify({error:{message:'隔离的重启前限流',type:'rate_limit_error'}}));return;}
      const item={type:'message',id:'restart-answer-'+requests.length,role:'assistant',status:'completed',content:[{type:'output_text',text:requests.length===1?'重启前的持久答案':'重启后的答案'}]};
      const output=requests.length===1?[reasoning,item]:[item];
      res.writeHead(200,{'content-type':'text/event-stream'});
      for(const event of [{type:'response.created',response:{id:'restart-response-'+requests.length,status:'in_progress',output:[]}},...output.flatMap((entry,index)=>[{type:'response.output_item.added',output_index:index,item:entry},{type:'response.output_item.done',output_index:index,item:entry}]),{type:'response.completed',response:{id:'restart-response-'+requests.length,status:'completed',output,usage:{input_tokens:10,output_tokens:10,total_tokens:20}}}])res.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
      res.end();
    }catch(error){res.writeHead(500);res.end(String(error));}
  });
  const wait=async(predicate:()=>boolean|Promise<boolean>)=>{const end=Date.now()+15000;while(!await predicate()){if(Date.now()>end)throw Error('重启隔离验收超时');await delay(50);}};
  let metadata:string;
  t.after(async()=>{
    release();releaseDelivery();releaseActive();await broker.stop();
    for(const record of managedRuntimes(home)){if(record.value.pid)pids.add(record.value.pid);addresses.add(record.value.endpoint);}
    assert.deepEqual(await releaseNativeTestLaunches(home),[]);await wait(()=>[...pids].every(pid=>!liveProcess(pid)));
    for(const address of addresses)await removeOwnedSocket(home,address);
    const socketRoot=[...addresses].find(address=>address.startsWith('unix://'));if(socketRoot){const directory=path.dirname(socketRoot.slice(7));if(path.resolve(directory)!==path.resolve(path.join(home,'run')))rmdirSync(directory);}
    model.closeAllConnections();await new Promise<void>(resolve=>model.close(()=>resolve()));store.close();
    restoreEnvironment();
    await temp.cleanup();
  });
  await new Promise<void>(resolve=>model.listen(0,'127.0.0.1',resolve));
  const base=`http://127.0.0.1:${(model.address() as AddressInfo).port}/v1`,marker=path.join(temp.directory,'mcp-version.txt'),fixture=path.join(temp.directory,'restart-mcp.mjs');
  writeFileSync(marker,'RESTART_MCP_V1','utf8');
  writeFileSync(fixture,`import {readFileSync} from 'node:fs';
import {McpServer} from ${JSON.stringify(pathToFileURL(path.join(PACKAGE_ROOT,'node_modules/@modelcontextprotocol/sdk/dist/esm/server/mcp.js')).href)};
import {StdioServerTransport} from ${JSON.stringify(pathToFileURL(path.join(PACKAGE_ROOT,'node_modules/@modelcontextprotocol/sdk/dist/esm/server/stdio.js')).href)};
const version=readFileSync(process.argv[2],'utf8');
const server=new McpServer({name:'restart-probe',version:'1.0.0'},{instructions:version+'_INSTRUCTIONS'});
server.registerTool('version',{description:version+'_TOOL',inputSchema:{}},async()=>({content:[{type:'text',text:version}]}));
await server.connect(new StdioServerTransport());
`,'utf8');
  const skill=path.join(codexhome,'skills','restart-marker','SKILL.md');mkdirSync(path.dirname(skill),{recursive:true});
  const skillText=(version:string)=>`---\nname: restart-marker\ndescription: RESTART_SKILL_${version}\n---\n隔离的重启技能。\n`;
  writeFileSync(skill,skillText('V1'),'utf8');
  const configText=(modelName:string,effort:string,tier:string|undefined=named?'opaque-tier-a':undefined)=>`model_provider = "restart_test"\nmodel = ${JSON.stringify(modelName)}\nmodel_reasoning_effort = ${JSON.stringify(effort)}\n${tier===undefined?'':'service_tier = '+JSON.stringify(tier)+'\n'}web_search = "disabled"\nsandbox_mode = "read-only"\napproval_policy = "never"\n[model_providers.restart_test]\nname = "隔离重启检查"\nbase_url = ${JSON.stringify(base)}\nwire_api = "responses"\nrequires_openai_auth = false\n[features]\napps = false\nremote_plugin = false\n[features.code_mode]\nenabled = false\ndirect_only_tool_namespaces = ["restart_probe"]\n[features.non_prefixed_mcp_tool_names]\nenabled = true\nserver_names = ["restart_probe"]\n[analytics]\nenabled = false\n[mcp_servers.restart_probe]\ncommand = ${JSON.stringify(process.execPath)}\nargs = ${JSON.stringify([fixture,marker])}\n`;
  const permissions=(text:string)=>{
    text=text.replace('requires_openai_auth = false','requires_openai_auth = false\nrequest_max_retries = 0\nstream_max_retries = 0')
      .replace('remote_plugin = false','remote_plugin = false\nplugins = false\nplugin_discovery_prefetch = false');
    if(clean)text=text.split('[mcp_servers.restart_probe]')[0]!;
    return named?text.replace('sandbox_mode = "read-only"','default_permissions = "restart_profile"')+'\n[permissions.restart_profile.filesystem]\n":root" = "read"\n[permissions.restart_profile.network]\nenabled = false\n':text;
  };
  writeFileSync(path.join(codexhome,'config.toml'),permissions(configText('gpt-6.1-sol','xhigh')),'utf8');
  await ensureHome(home);
                                   
  const placeholder=store.placeholder('codex',temp.directory),initial=await codex.ensureEndpoint(temp.directory,placeholder.id);
  addresses.add(initial);
  const runtimeRecord=managedRuntimes(home).find(record=>record.value.peerId===placeholder.id)!,child={pid:runtimeRecord.value.pid!};metadata=runtimeRecord.file;
  assert.equal(runtimeRecord.value.dedicated,true);pids.add(child.pid);
                                                
  const initialRpc=await codex.connect(initial),started=await initialRpc.request('thread/start',{cwd:temp.directory,serviceTier:named?'opaque-tier-a':'default'});
  const input=threadRegistration(started.thread,initial,started)!;input.endpoint={...input.endpoint,configHome:codexhome,runtimeId:runtimeRecord.value.id,pid:child.pid,procStart:runtimeRecord.value.procStart??null};
  const peer=store.adopt(placeholder.id,input);
  const partner=store.register(registration('claude','test-claude')),pair=store.pair(partner.id,peer.id),endpoint=store.endpoint(peer.id)!;
  (codex as any).ownedRoutes.subscribed({peer,endpoint},initialRpc);
  const rpc=await codex.attach(peer,endpoint);
  await rpc.request('mcpServerStatus/list',{threadId:peer.sessionId,limit:100});
  const completed=new Set<string>();
  rpc.on('notification',message=>{if(message.method==='turn/completed')completed.add(message.params.turn.id);});
  const missed=store.accept(pair,partner,'只返回隔离检查答案','missed-context-event');store.claim(missed.id);
                                  
  const observers=codex.listeners('contextObserved') as ((...args:any[])=>void)[];codex.removeAllListeners('contextObserved');
  await codex.inject(peer,endpoint,'nxbridge-'+missed.id,JSON.stringify({messages:[{message_id:missed.id,message:missed.body}]}));
  const first={turn:{id:await codex.continueInjected(peer,endpoint)}};
  await wait(()=>requests.length===1&&store.peer(peer.id).state==='busy');
  for(const observer of observers)codex.on('contextObserved',observer);store.transition(missed.id,'runtimeQueued','宿主提交 ACK');
  assert.equal(store.message(missed.id).state,'runtimeQueued');
  await rpc.request('thread/queue/add',{threadId:peer.sessionId,clientUserMessageId:'native-queued-input',input:[{type:'text',text:'排队输入不能丢失'}]},{mutation:true});
  await assert.rejects(broker.dispatch('codex.restart',{}),{code:'session_busy'});assert.equal(liveProcess(child.pid),true);
  const queued=(await rpc.request('thread/queue/list',{threadId:peer.sessionId,limit:100})).data;
  assert.equal(queued.length,1);await rpc.request('thread/queue/delete',{threadId:peer.sessionId,queuedSubmissionId:queued[0].id},{mutation:true});
  release();await wait(()=>completed.has(first.turn.id)&&store.peer(peer.id).state==='ready');
  if(!clean)assert.ok(JSON.stringify(requests[0].input).includes('RESTART_MCP_V1_INSTRUCTIONS'),'首轮应加载 MCP instructions');
  assert.ok(JSON.stringify(requests[0].input).includes(missed.id),'历史补证的消息 ID 应实际进入模型上下文');
  if(!clean)assert.ok(JSON.stringify(requests[0]).includes('RESTART_MCP_V1_TOOL'),'首轮应加载 MCP 工具');assert.ok(JSON.stringify(requests[0]).includes('RESTART_SKILL_V1'),'首轮应加载技能');
                                                     
  const originalRuntime=runtimeFor(home,{peerId:peer.id})!.value;
  saveManagedRuntime(home,{...originalRuntime,sessionId:peer.sessionId!,version:originalRuntime.version??codexServerVersion(rpc)});
  try{
    const authoritative=(await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false})).thread;
    const originalEndpoint=store.endpoint(peer.id)!;
    store.saveEndpoint({...originalEndpoint,nativeSession:{...originalEndpoint.nativeSession,transcriptPath:authoritative.path}});
    const observation=await collectCodexRecycleEvidence({home,peer:store.peer(peer.id),endpoint:store.endpoint(peer.id)!,trigger:'native_thread_wedged',
      connect:address=>connectAppServer(address,config,undefined,home,{readOnly:true}),
      preflight:async(_runtime,profile,args,inspection)=>{
        assert.equal(codexServerVersion(inspection),codexServerVersion(rpc));
        assert.equal(args.threadId,peer.sessionId);assert.equal(args.modelProvider,'restart_test');
        assert.equal(args.serviceTier,named?'opaque-tier-a':'default');
        assert.equal(args.approvalsReviewer,profile.settings.approvalsReviewer);
        if(named)assert.equal(args.permissions,'restart_profile');
      }});
    assert.equal(observation.evidence.kind,'available',JSON.stringify(observation.evidence));
    if(observation.evidence.kind==='available'){
      assert.equal(observation.evidence.old.pid,child.pid);
      assert.deepEqual(observation.evidence.work.activeTurnIds,[]);
    }
    assert.equal(requests.length,1,'独立核验不提交恢复请求或模型输入');
  }finally{if(!clean)saveManagedRuntime(home,originalRuntime);}
  if(clean){
    const lifecycle=(broker as any).memberLifecycle,wedge=t.mock.method(codex,'attach',async()=>{
      throw new RuntimeRecoveryHold('native_thread_wedged','隔离测试：原生串行队列不可用');
    });
    try{await assert.rejects(lifecycle.prepare(store.peer(peer.id)),{code:'native_thread_wedged'});}finally{wedge.mock.restore();}
    let status:any;
    try{await wait(async()=>{await broker.pump();status=lifecycle.recoveryStatus(store.peer(peer.id));return status?.recycle?.phase==='ready';});}
    catch(error){t.diagnostic(JSON.stringify({status,original:initial,rootPid:child.pid,wrapperPid:originalRuntime.wrapperPid}));throw error;}
    const current=store.endpoint(peer.id)!;pids.add(current.pid!);addresses.add(current.address);
    assert.notEqual(current.pid,child.pid);assert.equal(store.peer(peer.id).sessionId,peer.sessionId);
    assert.equal(codex.hasSubscription(store.peer(peer.id),current),true);
    assert.equal(requests.length,1,'自动恢复不能启动额外模型轮次');
    assert.equal(status.recycle.work.activeTurnIds.length,0);
    assert.deepEqual(status.recycle.work.roles,[]);
    t.diagnostic(JSON.stringify({automatic:true,noTool:true,source:status.recycle.original_source.lineage,sameSession:true}));
    return;
  }
  if(!named){
    const failure=await rpc.request('turn/start',{threadId:peer.sessionId,input:[{type:'text',text:'隔离的用户恢复前失败轮次'}]});await wait(()=>completed.has(failure.turn.id));
    const failedThread=(await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false})).thread;
    assert.equal(failedThread.status.type,'systemError');assert.equal(store.peer(peer.id).state,'ready');
    const ledger=()=>store.messages().map(({id,pairId,from,to,body,callId,attempts})=>({id,pairId,from,to,body,callId,attempts})),beforeRecovery=ledger(),turnsBeforeRecovery=new Set(completed);
    const preview=await broker.dispatch('codex.recover',{preview:true}) as any;assert.equal(preview.health.state,'failed');assert.ok(preview.recoveryCommand?.endsWith('--turn '+failure.turn.id));
    const recovery=await broker.dispatch('codex.recover',{turn_id:failure.turn.id,request_id:'native-failed-recovery'}) as any;
    assert.equal(recovery.recovered,true);assert.equal(recovery.action,'continue_failed');assert.equal(recovery.session_id,peer.sessionId);
    await wait(()=>requests.length===3&&[...completed].some(id=>!turnsBeforeRecovery.has(id))&&store.peer(peer.id).state==='ready');
    const recovered=(await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:true})).thread;
    assert.equal(recovered.id,peer.sessionId);assert.notEqual(recovered.turns.at(-1).id,failure.turn.id);assert.equal(recovered.turns.at(-1).status,'completed');
    const outputs=requests[2].input.filter((item:any)=>item.type==='function_call_output'&&item.call_id==='nxbridge-'+recovery.notice_id);
    assert.equal(outputs.length,1);assert.match(JSON.stringify(outputs[0]),/user explicitly requested recovery after failed native turn/);
    await wait(async()=>(await readCodexHistory(failedThread.path,peer.sessionId!)).receipts.some(receipt=>receipt.messageId===recovery.notice_id));
    assert.equal((await readCodexHistory(failedThread.path,peer.sessionId!)).receipts.filter(receipt=>receipt.messageId===recovery.notice_id).length,1);
    assert.deepEqual(ledger(),beforeRecovery,'用户恢复不得伪造搭档消息或重复投递旧任务');
    assert.equal((await broker.dispatch('codex.recover.result',{request_id:'native-failed-recovery'}) as any).state,'submitted');
                                           
    const restartFailure=await rpc.request('turn/start',{threadId:peer.sessionId,input:[{type:'text',text:'隔离的重启前失败轮次'}]});await wait(()=>completed.has(restartFailure.turn.id));
    assert.equal((await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false})).thread.status.type,'systemError');assert.equal(store.peer(peer.id).state,'ready');
    t.diagnostic('真实 429 后的用户恢复在原会话完成新轮次；恢复回执恰好一次，旧消息正文、路由和尝试次数保持原样。');
  }
  const before=await rpc.request('thread/resume',{threadId:peer.sessionId}),beforeRequests=requests.length;
  const deliveryGate=new Promise<void>(resolve=>releaseDelivery=resolve),originalInject=codex.inject.bind(codex);codex.inject=async(...args)=>{await deliveryGate;await originalInject(...args);};
  if(named)assert.equal(before.activePermissionProfile?.id,'restart_profile');
  const durablePath=before.thread.path;assert.ok(durablePath);await wait(()=>readFileSync(durablePath,'utf8').includes('重启前的持久答案'));
  const persistedReasoning=readFileSync(durablePath,'utf8').split('\n').filter(Boolean).map(line=>JSON.parse(line)).find(row=>row.type==='response_item'&&row.payload?.id===reasoning.id)?.payload;
  assert.deepEqual([persistedReasoning?.id,persistedReasoning?.summary,persistedReasoning?.encrypted_content],[reasoning.id,reasoning.summary,reasoning.encrypted_content],'原生落盘必须保留推理项的 ID、摘要和不透明密文');
  const extra=await rpc.request('thread/start',{cwd:temp.directory,ephemeral:true,config:{thread_unload_delay_secs:0}});
  (codex as any).ownedRoutes.subscribed({peer:{...peer,sessionId:extra.thread.id},endpoint},rpc);
  await assert.rejects(broker.dispatch('codex.restart',{}),{code:'shared_runtime'});assert.equal(liveProcess(child.pid),true);
  assert.equal(store.message(missed.id).state,'contextObserved');assert.match(store.message(missed.id).evidence!,/thread\/read/);assert.equal(store.message(missed.id).attempts,1);
  await rpc.request('thread/unsubscribe',{threadId:extra.thread.id});await wait(async()=>!(await rpc.request('thread/loaded/list',{limit:100})).data.includes(extra.thread.id));
  const runtime=readJson<any>(metadata);atomicJson(metadata,{...runtime,procStart:'错误的进程代次'});
  await assert.rejects(broker.dispatch('codex.restart',{}),{code:'unowned_runtime'});assert.equal(liveProcess(child.pid),true);atomicJson(metadata,runtime);
  if(named){
    const profile=await readSessionProfile(durablePath,peer.sessionId!);
    assert.equal(profile.verified,true);assert.equal(profile.complete,true);
    delete profile.settings.serviceTier;delete profile.appliedServiceTier;
    atomicJson(metadata,{...runtime,sessionVerification:{sessionId:peer.sessionId,profile}});
    try{
      await assert.rejects(codex.restart(peer,endpoint,()=>{},{verificationRecovery:true}),{code:'owned_profile_unverified'});
      assert.equal(liveProcess(child.pid),true,'档位证据不完整时必须保留原进程');
      assert.equal((await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false})).thread.id,peer.sessionId);
    }finally{atomicJson(metadata,runtime);}
  }
  const uncertain=store.accept(pair,partner,'未确认消息保持原样','unknown');store.claim(uncertain.id);store.transition(uncertain.id,'unknown');
  const unknownBefore=store.message(uncertain.id);
  assert.equal(store.message(uncertain.id).attempts,1);
  writeFileSync(marker,'RESTART_MCP_V2','utf8');writeFileSync(skill,skillText('V2'),'utf8');
  const changedConfig=permissions(configText('gpt-5.1-codex','low','flex'));writeFileSync(path.join(codexhome,'config.toml'),changedConfig,'utf8');
  if(!named){
                                                 
    await rpc.request('thread/settings/update',{threadId:peer.sessionId,serviceTier:'default'},{mutation:true});
    await wait(async()=>(await readSessionProfile(durablePath,peer.sessionId!)).settings.serviceTier==='default');
    await rpc.request('thread/unsubscribe',{threadId:peer.sessionId});
    await wait(async()=>!(await rpc.request('thread/loaded/list',{limit:100})).data.includes(peer.sessionId));
    assert.equal(codex.hasSubscription(store.peer(peer.id),endpoint),false);
    let lastColdHold='';
    try{await wait(async()=>{
      try{await codex.attach(store.peer(peer.id),endpoint);return true;}
      catch(error){if((error as {code?:string}).code!=='owned_cold_resume_waiting')throw error;lastColdHold=String(error);return false;}
    });}catch(error){t.diagnostic(lastColdHold);throw error;}
    assert.equal(codex.hasSubscription(store.peer(peer.id),store.endpoint(peer.id)!),true);
    const cold=await rpc.request('thread/resume',{threadId:peer.sessionId,excludeTurns:true});
    assert.deepEqual([cold.thread.id,cold.model,cold.reasoningEffort],[before.thread.id,before.model,before.reasoningEffort]);
    assert.equal((await readSessionProfile(durablePath,peer.sessionId!)).settings.serviceTier,'default');
    assert.equal(requests.length,beforeRequests,'仅恢复订阅不会启动轮次');
    assert.equal(runtimeFor(home,{peerId:peer.id})!.value.pid,child.pid);
  }
  let live!:typeof missed;
  const restart=codex.restart.bind(codex),duringRestart=t.mock.method(codex,'restart',(...args:Parameters<CodexAdapter['restart']>)=>{
                                  
    live=store.accept(pair,partner,'只返回重启后隔离检查答案','queued-before-restart');assert.equal(live.attempts,0);
    return restart(...args);
  });
  const result=await broker.dispatch('codex.restart',named?{partner:true,actor:{host:'claude',sessionId:partner.sessionId,callId:'native-partner-restart'}}:{}) as any;pids.add(result.pid);
  duringRestart.mock.restore();
  assert.equal(result.restarted,true);assert.equal(result.session_id,peer.sessionId);assert.notEqual(result.pid,child.pid);assert.equal(liveProcess(child.pid),false);
  assert.deepEqual(store.message(uncertain.id),unknownBefore,'完整历史核验只解除重启阻塞，未知消息的正文、状态及投递次数保持原样');
  assert.equal(store.currentPair()?.id,pair.id);assert.equal(store.peer(peer.id).owner,'nxbridge');assert.equal(requests.length,beforeRequests,'冷恢复本身不调用模型');
  const restored=await codex.attach(store.peer(peer.id),store.endpoint(peer.id)!);
  const after=await restored.request('thread/resume',{threadId:peer.sessionId});
  assert.equal(after.thread.id,before.thread.id);assert.equal(after.model,before.model);assert.equal(after.reasoningEffort,before.reasoningEffort);
  assert.equal((await readSessionProfile(after.thread.path,peer.sessionId!)).settings.serviceTier,named?'opaque-tier-a':'default','新配置的 flex 不能覆盖原生原有或不透明档位');
  assert.deepEqual(after.sandbox,before.sandbox);assert.deepEqual(after.approvalPolicy,before.approvalPolicy);assert.equal(after.cwd,before.cwd);assert.deepEqual(after.activePermissionProfile,before.activePermissionProfile);
  assert.match(JSON.stringify(after.thread.turns),/重启前的持久答案/);assert.equal(readFileSync(path.join(codexhome,'config.toml'),'utf8'),changedConfig);
  const refreshedMcp=await restored.request('mcpServerStatus/list',{threadId:peer.sessionId,limit:100});
  assert.ok(JSON.stringify(refreshedMcp).includes('RESTART_MCP_V2_TOOL'),'原生 MCP 初始化完成后应包含新版工具');
  restored.on('notification',message=>{if(message.method==='turn/completed')completed.add(message.params.turn.id);});
  const priorTurns=new Set(completed);releaseDelivery();await broker.pump();await wait(()=>store.message(live.id).state==='contextObserved'&&[...completed].some(id=>!priorTurns.has(id)));
  assert.match(store.message(live.id).evidence!,/item\/completed|原生持久历史/);assert.equal(store.message(live.id).attempts,1);
  assert.equal(requests.length,beforeRequests+1);const delivered=requests.at(-1);assert.equal(delivered.model,before.model);assert.equal(delivered.reasoning.effort,before.reasoningEffort);
  const replayedReasoning=delivered.input.find((item:any)=>item.type==='reasoning'&&item.id===reasoning.id);
  assert.deepEqual([replayedReasoning?.id,replayedReasoning?.summary,replayedReasoning?.encrypted_content],[reasoning.id,reasoning.summary,reasoning.encrypted_content],'冷恢复与消息注入必须原样重放推理项的 ID、摘要和不透明密文');
  assert.equal(JSON.stringify(delivered.input).includes(uncertain.id),false,'旧未知消息不能进入重启后的投递');assert.deepEqual(store.message(uncertain.id),unknownBefore);
  assert.ok(JSON.stringify(delivered.input).includes(live.id),'重启前积压的消息必须自动进入原会话模型上下文');
  assert.ok(JSON.stringify(delivered.input).includes('RESTART_MCP_V2_INSTRUCTIONS'),'重启后应加载新 MCP instructions');
  assert.ok(JSON.stringify(delivered).includes('RESTART_MCP_V2_TOOL'),'重启后应加载新 MCP 工具');assert.ok(JSON.stringify(delivered).includes('RESTART_SKILL_V2'),'重启后应加载新技能');
                                          
  assert.deepEqual((await restored.request('thread/loaded/list',{limit:100})).data,[peer.sessionId]);
  const sessions=path.join(codexhome,'sessions'),rolloutIds=new Set(readdirSync(sessions,{recursive:true}).filter((file):file is string=>typeof file==='string'&&file.endsWith('.jsonl')).map(file=>JSON.parse(readFileSync(path.join(sessions,file),'utf8').split('\n')[0]!).payload.id));
  assert.deepEqual([...rolloutIds],[peer.sessionId]);
  await wait(()=>store.peer(peer.id).state==='ready');
                                                    
  const recycleSource=store.endpoint(peer.id)!,lifecycle=(broker as any).memberLifecycle;
  if(process.platform==='linux'&&named){
    const beforeActive=requests.length;
    await restored.request('turn/start',{threadId:peer.sessionId,input:[{type:'text',text:'隔离的活动回收轮次'}]});
    await wait(()=>requests.length>beforeActive&&store.peer(peer.id).state==='busy');
  }
  const beforeRecycle=requests.length;
  const wedge=t.mock.method(codex,'attach',async()=>{throw new RuntimeRecoveryHold('native_thread_wedged','隔离测试：原生串行队列不可用');});
  try{await assert.rejects(lifecycle.prepare(store.peer(peer.id)),{code:'native_thread_wedged'});}finally{wedge.mock.restore();}
  assert.equal((store.setting<{trigger:string}>('codexRecycleCandidate:'+peer.id))?.trigger,'native_thread_wedged',
    JSON.stringify({endpointRuntime:recycleSource.runtimeId,record:runtimeFor(home,{peerId:peer.id})?.value.sessionId,session:peer.sessionId}));
  assert.ok((await restored.request('thread/loaded/list',{limit:100})).data.includes(peer.sessionId),'隔离回收触发时，旧会话必须仍在原生后台驻留');
  let recycleStatus:ReturnType<typeof lifecycle.recoveryStatus>;
  let acceptedRevision:string|undefined;
  try{
    await wait(async()=>{
      await broker.pump();recycleStatus=lifecycle.recoveryStatus(store.peer(peer.id));
      const revision=recycleStatus?.recycle?.revision;
      if(recycleStatus?.recycle?.phase==='awaiting_partner'&&revision&&revision!==acceptedRevision){
        acceptedRevision=revision;
        await broker.dispatch('partner.recover',{actor:{host:'claude',sessionId:partner.sessionId,callId:'isolated-recycle:'+revision},
          action:'recycle',incident_id:recycleStatus.incident_id,evidence_revision:revision,reason:'隔离验收同意这一代次的工作中断。'});
      }
      return recycleStatus?.recycle?.phase==='ready';
    });
  }catch(error){t.diagnostic(JSON.stringify(recycleStatus));throw error;}
  const recycled=store.endpoint(peer.id)!;pids.add(recycled.pid!);addresses.add(recycled.address);
  assert.notEqual(recycled.pid,recycleSource.pid);
  assert.equal(store.peer(peer.id).sessionId,peer.sessionId);
  assert.equal(await ownedProcessState({pid:recycleSource.pid!,startedAt:recycleSource.procStart!}),'dead');
  assert.equal(codex.hasSubscription(store.peer(peer.id),recycled),true);
  if(process.platform==='linux'&&named)assert.ok(acceptedRevision,'Linux 活动轮次必须由明确批准回收');
  assert.equal(requests.length,beforeRecycle,'同会话回收无新任务提示或模型请求');
  assert.deepEqual(store.message(uncertain.id),unknownBefore);
  const finalProfile=await readSessionProfile(recycled.nativeSession?.transcriptPath,peer.sessionId!);
  assert.equal(finalProfile.settings.serviceTier,named?'opaque-tier-a':'default');
  assert.equal(finalProfile.settings.model,before.model);
  assert.equal(finalProfile.settings.reasoningEffort,before.reasoningEffort);
  assert.equal(finalProfile.settings.modelProvider,'restart_test');
  assert.equal(finalProfile.activePermission?.id,before.activePermissionProfile?.id);
  if(process.platform==='linux'&&named){
    const freshRpc=await codex.connect(recycled.address),after=new Set<string>();
    freshRpc.on('notification',message=>{if(message.method==='turn/completed'&&message.params.turn.status==='completed')after.add(message.params.turn.id);});
    const next=await freshRpc.request('turn/start',{threadId:peer.sessionId,input:[{type:'text',text:'隔离验收回收后继续使用原会话'}]});
    await wait(()=>after.has(next.turn.id)&&store.peer(peer.id).state==='ready');
    assert.equal(requests.length,beforeRecycle+1);
    assert.equal(JSON.stringify(requests.at(-1)).includes(uncertain.id),false);
  }
  t.diagnostic(JSON.stringify({automatic:acceptedRevision===undefined,loadedOriginal:true,sameSession:true,
    scope:recycleStatus?.recycle?.work?.unattested??[],originalSource:recycleStatus?.recycle?.original_source?.lineage}));
  if(process.platform==='darwin'&&named){
                                      
    const source=recycled,survivingRpc=await codex.connect(source.address),target=codexSocketAlias(source.address.slice(7));
    assert.notEqual(survivingRpc,restored);
    unlinkSync(target);
    assert.equal(codexSocketTargetMissing(source.address.slice(7)),true);
    await assert.rejects(connectAppServer(source.address,config,undefined,home,{readOnly:true}),{code:'ENOENT'});
    assert.equal((await survivingRpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false})).thread.id,peer.sessionId);
    let stopClaims=0;
    const repair=await(async()=>{
      for(let attempt=1;;attempt++)try{
        return await codex.restart(store.peer(peer.id),source,()=>{},{lostSocketRecovery:true,beforeStop:()=>{stopClaims++;}});
      }catch(error){
        const git=error instanceof BridgeError&&error.code==='runtime_workload_active'&&/PID (\d+)）[^\n]*父进程为 (\d+)，程序 git。/.exec(error.message);
        if(!git||attempt===3)throw error;
        assert.equal(stopClaims,0,'只能等待停止预约之前被安全门禁拒绝的调用');
        assert.equal(await ownedProcessState({pid:source.pid!,startedAt:source.procStart!}),'alive');
        const pid=Number(git[1]),parentPid=Number(git[2]);
        const candidate=await processIdentity(pid).catch(error=>{if(error instanceof BridgeError&&error.code==='process_missing')return;throw error;});
        if(!candidate){t.diagnostic(JSON.stringify({stopAttempt:attempt,pid,parentPid,gitAlreadyExited:true}));continue;}
        if(candidate.parentPid!==parentPid||path.basename(candidate.image??candidate.name)!=='git')throw error;
                                    
        const argv=await codexArguments(candidate).catch(()=>[]);
        const category=argv[1]&&['status','rev-parse','config','fetch','ls-remote','show','diff','log'].includes(argv[1])?argv[1]:'unknown';
        const ancestry=await processAncestors(pid,4).catch(()=>[]);
        t.diagnostic(JSON.stringify({stopAttempt:attempt,pid,parentPid,procStart:candidate.startedAt,category,ancestry:ancestry.map(({pid,parentPid})=>({pid,parentPid}))}));
        await wait(async()=>await ownedProcessState(candidate)==='dead');
      }
    })();
    pids.add(repair.pid);addresses.add(repair.endpoint);
    assert.equal(stopClaims,1);assert.equal(repair.oldPid,source.pid);assert.equal(liveProcess(repair.oldPid),false);
    assert.equal(repair.resumed.thread.id,peer.sessionId);assert.notEqual(repair.endpoint,source.address);
    assert.equal(requests.length,beforeRequests+1,'原生空闲恢复不运行新轮次');
    const fresh=await codex.connect(repair.endpoint);
    assert.ok(JSON.stringify((await fresh.request('mcpServerStatus/list',{threadId:peer.sessionId,limit:100})).data).includes('RESTART_MCP_V2_TOOL'));
  }
  const afterRecycleRequests=requests.length;
  const blocked=store.accept(pair,partner,'恢复失败后仍要保留的新消息','restart-failure');
  const invalidConfig=changedConfig+'\n[故意损坏的隔离配置',originalRestart=codex.restart.bind(codex);
                                        
  codex.restart=(target,address,guard,options)=>originalRestart(target,address,()=>{guard();writeFileSync(path.join(codexhome,'config.toml'),invalidConfig,'utf8');},options);
  await assert.rejects(broker.dispatch('codex.restart',{}),{code:'restart_incomplete'});await broker.pump();
  assert.equal(store.peer(peer.id).state,'paused');assert.equal(store.peer(peer.id).sessionId,peer.sessionId);
  assert.equal(store.message(blocked.id).state,'accepted');assert.equal(store.message(blocked.id).attempts,0);assert.equal(requests.length,afterRecycleRequests);
  assert.match(readFileSync(durablePath,'utf8'),/重启前的持久答案/);assert.match(readFileSync(durablePath,'utf8'),/重启后的答案/);
  assert.equal(readFileSync(path.join(codexhome,'config.toml'),'utf8'),invalidConfig);
});
