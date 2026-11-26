import { overrideEnvironment } from '../tests/support/environment.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import path from 'node:path';
import {mkdirSync,writeFileSync} from 'node:fs';
import { readModelBody } from './model-http-fixture.js';
import {CodexAdapter,connectAppServer,threadRegistration} from '../hosts/codex/adapter.js';
import {Broker,type Adapters} from '../broker/coordinator.js';
import {Store} from '../storage/store.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {delay} from '../shared/time.js';
import {ensureHome} from '../ipc/home.js';
import {liveProcess,processDescendants,type ProcessIdentity} from '../process/process.js';
import {ownedProcesses} from '../process/owned-processes.js';
import {ownedProcessState} from '../process/process-stop.js';
import {managedRuntimes,runtimeFor} from '../hosts/managed-runtimes.js';
import {readSessionProfile} from '../hosts/codex/codex-session-profile.js';
import {loadedCodexThreads} from '../hosts/codex/codex-restart.js';
import type {RpcMessage} from '../ipc/rpc.js';
import { FakeAdapter } from '../tests/support/fake-adapter.js';
import { lifecycle } from '../tests/support/broker-fixture.js';
import { registration } from '../tests/support/registration.js';
import { temporaryDirectory } from '../tests/support/temporary.js';
import { releaseNativeTestLaunches } from '../tests/support/native-launch.js';

test('原生冷恢复回执丢失后，必须等待同一根线程卸载才限次重试',{timeout:170000},async t=>{
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge'),codexHome=path.join(temp.directory,'codex');mkdirSync(codexHome,{recursive:true});
  const env={NXBRIDGE_HOME:home,CODEX_HOME:codexHome,CLAUDE_CONFIG_DIR:path.join(temp.directory,'claude'),OPENAI_API_KEY:'isolated-subagent-restart'};
  const restoreEnvironment=overrideEnvironment(env);
  const config={...DEFAULT_CONFIG,codexHome,...(process.env.NXBRIDGE_TEST_CODEX_BINARY?{codexBinary:process.env.NXBRIDGE_TEST_CODEX_BINARY}:{})};
  const adapter=new CodexAdapter(config,home),store=new Store(':memory:'),broker=new Broker(store,config,{codex:adapter,claude:new FakeAdapter()} as unknown as Adapters,home);
  const stoppedTree=new Map<string,ProcessIdentity>();
  const captureTree=async()=>{for(const record of ownedProcesses(home).records.filter(value=>value.role==='codex-runtime'))if(record.windowsJob||record.posixGroup)for(const identity of [record.identity,...await processDescendants(record.identity,record)])stoppedTree.set(identity.pid+':'+identity.startedAt,identity);};
  const treeState=()=>Promise.all([...stoppedTree.values()].map(async identity=>({pid:identity.pid,procStart:identity.startedAt,name:identity.name,state:await ownedProcessState(identity)})));
  let releaseChild!:()=>void,childStarted=false,rootSpawned=false,spawnResult='',serverError='',modelRequests=0;const held=new Promise<void>(resolve=>releaseChild=resolve);
  const model=http.createServer(async(req,res)=>{
    try{
      const raw=await readModelBody(req);
      if(!req.url?.includes('/responses')){res.writeHead(200,{'content-type':'application/json'});res.end('{"data":[]}');return;}
      modelRequests++;
      const body=JSON.parse(raw.toString('utf8')),child=body.model==='gpt-6-sol';let item:any;
      if(child){childStarted=true;await held;}
      const output=(body.input??[]).find((entry:any)=>entry.type==='function_call_output'&&entry.call_id==='spawn-child');if(output)spawnResult=String(output.output);
      if(!child&&!rootSpawned){
        let spawn:any;for(const entry of body.tools??[]){if(entry.type==='namespace'){const found=entry.tools.find((value:any)=>/(?:^|\.)spawn_agent$/.test(value.name));if(found)spawn={...found,namespace:entry.name};}else if(/(?:^|\.)spawn_agent$/.test(entry.name))spawn=entry;}
        assert.ok(spawn,'原生多代理工具必须可用');const v2=(spawn.parameters??spawn.input_schema??{}).required?.includes('task_name');rootSpawned=true;
        item={type:'function_call',id:'spawn-child',call_id:'spawn-child',name:spawn.name,...(spawn.namespace?{namespace:spawn.namespace}:{}),arguments:JSON.stringify({message:'隔离子代理：完成当前测试任务。',model:'gpt-6-sol',...(v2?{task_name:'restart_child',fork_turns:'none'}:{fork_context:false})})};
      }else item={type:'message',id:child?'child-done':'root-done',role:'assistant',status:'completed',content:[{type:'output_text',text:child?'CHILD_DONE':'ROOT_DONE'}]};
      res.writeHead(200,{'content-type':'text/event-stream'});
      for(const event of [{type:'response.created',response:{id:'scope-response',status:'in_progress',output:[]}},{type:'response.output_item.added',output_index:0,item},{type:'response.output_item.done',output_index:0,item},{type:'response.completed',response:{id:'scope-response',status:'completed',output:[item],usage:{input_tokens:10,output_tokens:10,total_tokens:20}}}])res.write('data: '+JSON.stringify(event)+'\n\n');res.end();
    }catch(error){serverError=String(error);res.writeHead(500);res.end(serverError);}
  });
  await new Promise<void>(resolve=>model.listen(0,'127.0.0.1',resolve));
  t.after(async()=>{
    releaseChild();await captureTree();await broker.stop(false);adapter.close();
    const notes=await releaseNativeTestLaunches(home);assert.deepEqual(notes,[]);store.close();
    const proof=await treeState();t.diagnostic(JSON.stringify({preStopNativeTree:proof}));assert.deepEqual(proof.filter(identity=>identity.state!=='dead'),[],'停止前核验的原生后代必须全部退出');
    model.closeAllConnections();await new Promise<void>(resolve=>model.close(()=>resolve()));
    restoreEnvironment();
    await temp.cleanup();
  });
  const base=`http://127.0.0.1:${(model.address() as {port:number}).port}/v1`;
  writeFileSync(path.join(codexHome,'config.toml'),`model_provider = "scope_test"\nmodel = "gpt-5.1-codex"\nmodel_reasoning_effort = "low"\nweb_search = "disabled"\nsandbox_mode = "read-only"\napproval_policy = "never"\n[model_providers.scope_test]\nname = "隔离子代理重启验收"\nbase_url = ${JSON.stringify(base)}\nwire_api = "responses"\nrequires_openai_auth = false\nrequest_max_retries = 0\nstream_max_retries = 0\n[features]\nmulti_agent = true\nmulti_agent_v2 = false\napps = false\nremote_plugin = false\n[features.code_mode]\nenabled = false\n[analytics]\nenabled = false\n`,'utf8');
  await ensureHome(home);
  const placeholder=store.placeholder('codex',temp.directory),address=await adapter.ensureEndpoint(temp.directory,placeholder.id),runtime=managedRuntimes(home).find(record=>record.value.peerId===placeholder.id)!.value;
  let rpc=await adapter.connect(address);
  const started=await rpc.request('thread/start',{cwd:temp.directory,serviceTier:'default'}),input=threadRegistration(started.thread,address,started)!;
                                                   
  input.endpoint={...input.endpoint,configHome:codexHome,runtimeId:runtime.id,pid:runtime.pid!,procStart:runtime.procStart!};const root=store.adopt(placeholder.id,input),endpoint=store.endpoint(root.id)!;
  const partner=store.register(registration('claude','subagent-counterpart'));store.pair(partner.id,root.id);
  (adapter as any).ownedRoutes.subscribed({peer:root,endpoint},rpc);
  await adapter.attach(root,endpoint);
  const completed=new Set<string>(),recordCompletion=(message:RpcMessage)=>{if(message.method==='turn/completed')completed.add(message.params.turn.id);};
  rpc.on('notification',recordCompletion);
  const wait=async(check:()=>boolean|Promise<boolean>)=>{const deadline=Date.now()+30000;while(!await check()){if(serverError)throw Error(serverError);if(Date.now()>deadline)throw Error('原生子代理状态超时；spawn='+spawnResult);await delay(50);}};
  const turn=await rpc.request('turn/start',{threadId:root.sessionId,input:[{type:'text',text:'生成隔离子代理后结束主轮次。'}]});
  await wait(()=>childStarted&&completed.has(turn.turn.id)&&store.peer(root.id).state==='ready');
  assert.ok(spawnResult,'原生子代理的启动结果必须进入根会话');
  await assert.rejects(broker.dispatch('codex.restart',{}),{code:'subagent_active'});assert.equal(liveProcess(runtime.pid!),true);
  releaseChild();
  const childId=JSON.parse(spawnResult).agent_id;
  await wait(async()=>{const {data}=await rpc.request('thread/turns/list',{threadId:childId,limit:1,itemsView:'summary'});return data[0]?.status==='completed';});
  assert.ok((await rpc.request('thread/loaded/list',{limit:100})).data.includes(childId));
  const childTurns=()=>rpc.request('thread/turns/list',{threadId:childId,limit:1,itemsView:'summary'}).then(page=>page.data);
  const childBefore=await childTurns();
  const {thread:rootBefore}=await rpc.request('thread/read',{threadId:root.sessionId,includeTurns:false});
  await rpc.request('thread/settings/update',{threadId:root.sessionId,serviceTier:'default'},{mutation:true});
  await wait(async()=>(await readSessionProfile(rootBefore.path,root.sessionId!)).settings.serviceTier==='default');
  await rpc.request('thread/unsubscribe',{threadId:root.sessionId});
  await wait(async()=>!(await rpc.request('thread/loaded/list',{limit:100})).data.includes(root.sessionId));
  assert.ok((await rpc.request('thread/loaded/list',{limit:100})).data.includes(childId),'根会话卸载时子代理仍应加载');
  assert.equal(adapter.hasSubscription(store.peer(root.id),endpoint),false);
  const sessionId=root.sessionId!,beforeRecovery=modelRequests,first=rpc,keeper=await connectAppServer(address,config,undefined,home,{readOnly:true});
  t.after(()=>keeper.close());
                                      
  assert.equal((await keeper.request('thread/resume',{threadId:childId,excludeTurns:true})).thread.id,childId);
  const attempts:unknown[]=[];let acknowledge!:()=>void,droppingResume=false;
  const nativeAck=new Promise<void>(resolve=>acknowledge=resolve);
  const firstRequest=first.request.bind(first),firstReceive=first.receive.bind(first);
  const loseAck=t.mock.method(first,'receive',(message:RpcMessage,measurement?:Parameters<typeof first.receive>[1])=>{
    if(droppingResume&&message.id!==undefined&&message.result?.thread?.id===sessionId){droppingResume=false;acknowledge();return;}
    firstReceive(message,measurement);
  });
  const firstTrace=t.mock.method(first,'request',(method:string,params?:unknown,options?:Parameters<typeof first.request>[2])=>{
    if(method!=='thread/resume')return firstRequest(method,params,options);
    attempts.push(params);droppingResume=true;return firstRequest(method,params,{...options,timeout:4000});
  });
  try{
    const failed=assert.rejects(adapter.attach(store.peer(root.id),endpoint),{code:'timeout'});
    await Promise.race([nativeAck,failed.then(()=>assert.fail('没有收到可丢弃的真实原生恢复回执'))]);
                                
    const held=await keeper.request('thread/resume',{threadId:sessionId,excludeTurns:true});assert.equal(held.thread.id,sessionId);
    await failed;
  }finally{firstTrace.mock.restore();loseAck.mock.restore();}
  assert.equal(first.closed,true);
  const pending=()=>runtimeFor(home,{id:runtime.id})?.value.coldResumeUnconfirmed;
  assert.equal(pending()?.attempts,1);assert.equal(pending()?.transportClosed,true);
  assert.ok((await loadedCodexThreads(keeper)).has(sessionId));assert.equal(modelRequests,beforeRecovery);
  rpc=await adapter.connect(address);assert.notEqual(rpc,first);rpc.on('notification',recordCompletion);
  const calls:{method:string;id?:string}[]=[],request=rpc.request.bind(rpc);
  const traced=t.mock.method(rpc,'request',(method:string,params?:{threadId?:string},options?:Parameters<typeof rpc.request>[2])=>{
    calls.push({method,id:params?.threadId});if(method==='thread/resume')attempts.push(params);return request(method,params,options);
  });
  let wakeTurn:string|undefined;
  try{
    const member=lifecycle(broker);
    await assert.rejects(member.prepare(store.peer(root.id)),{code:'owned_subscription_waiting'});
    assert.equal(attempts.length,1);assert.equal(pending()?.attempts,1);assert.equal(modelRequests,beforeRecovery);
    assert.ok(calls.some(call=>call.method==='thread/loaded/list'));assert.ok(calls.every(call=>call.id!==sessionId));
    await keeper.request('thread/unsubscribe',{threadId:sessionId});
    await wait(async()=>!(await loadedCodexThreads(keeper)).has(sessionId));
    assert.ok((await loadedCodexThreads(keeper)).has(childId));
    await member.prepare(store.peer(root.id));assert.equal(pending(),undefined);
    assert.equal(attempts.length,2);assert.deepEqual(attempts[1],attempts[0]);assert.equal(modelRequests,beforeRecovery);
    await adapter.wake(store.peer(root.id),store.endpoint(root.id)!,undefined,undefined,'在原线程完成冷恢复后的隔离唤醒。',id=>wakeTurn=id);
  }
  finally{traced.mock.restore();}
  assert.ok(wakeTurn);await wait(()=>completed.has(wakeTurn!)&&store.peer(root.id).state==='ready');
  assert.ok(adapter.hasSubscription(store.peer(root.id),store.endpoint(root.id)!));
  assert.equal(store.endpoint(root.id)?.pid,runtime.pid);assert.equal(liveProcess(runtime.pid!),true);
  assert.ok(calls.every(call=>call.id===undefined||call.id===root.sessionId),JSON.stringify(calls));
  assert.equal(calls.filter(call=>call.method==='thread/resume').length,1);
  assert.equal(calls.filter(call=>call.method==='turn/start').length,1);
  assert.equal(modelRequests,beforeRecovery+1);
  assert.ok((await rpc.request('thread/loaded/list',{limit:100})).data.includes(childId));
  assert.deepEqual(await childTurns(),childBefore);
  const children=store.peers().filter(peer=>peer.kind==='subagent'&&peer.rootPeerId===root.id);assert.ok(children.length,'实际子代理应以父链登记');
  await captureTree();const result=await broker.dispatch('codex.restart',{}) as any;assert.equal(result.restarted,true);assert.equal(result.session_id,root.sessionId);assert.equal(result.old_pid,runtime.pid);assert.notEqual(result.pid,runtime.pid);
  const proof=await treeState();t.diagnostic(JSON.stringify({preStopNativeTree:proof}));assert.deepEqual(proof.filter(identity=>identity.state!=='dead'),[],'重启必须释放原宿主的已核验后代');stoppedTree.clear();
  for(const child of children){assert.equal(store.endpoint(child.id),undefined);assert.equal(store.peer(child.id).state,'offline');}
  assert.equal(store.endpoint(root.id)?.address===address,false);assert.equal(store.pairs().length,1);
  t.diagnostic('首次原生恢复回执在桥接端丢弃后，实际连接关闭。根线程加载期间没有重复恢复，原生完整列表确认卸载后才按相同设置重试。两次恢复期间模型调用数保持不变。已完成的子代理保留加载和轮次，随后显式重启仍核验其工作。');
});
