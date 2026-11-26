import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdirSync, readFileSync, realpathSync, statSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { stringify } from 'smol-toml';
import { Broker, type Adapters } from '../broker/coordinator.js';
import { DEFAULT_CONFIG } from '../config/defaults.js';
import { CodexAdapter } from '../hosts/codex/adapter.js';
import type { CodexSubagentReadFloor } from '../hosts/codex/codex-subagent-read.js';
import { configClient } from '../hosts/codex/stdio-codex.js';
import { managedRuntimes } from '../hosts/managed-runtimes.js';
import { preparePlugins } from '../install/setup.js';
import { hookGuardTimeout } from '../integration/hooks/hook-output.js';
import { ensureHome } from '../ipc/home.js';
import { listenBroker } from '../ipc/ipc.js';
import { runCli } from '../process/host-command.js';
import { processIdentity } from '../process/process.js';
import { DeliveryError } from '../shared/errors.js';
import { delay } from '../shared/time.js';
import { Store } from '../storage/store.js';
import { overrideEnvironment } from '../tests/support/environment.js';
import { FakeAdapter } from '../tests/support/fake-adapter.js';
import { releaseNativeTestLaunches } from '../tests/support/native-launch.js';
import { registration } from '../tests/support/registration.js';
import { temporaryDirectory } from '../tests/support/temporary.js';
import { HEALTH_WAKE_NOTICE } from '../types.js';
import { readModelBody } from './model-http-fixture.js';

function scenario(name:string,holdFinal=false){
  let release!:()=>void,releaseFinal!:()=>void,releaseChildHook!:()=>void,releaseRootHook!:()=>void;
  const held=new Promise<void>(resolve=>release=resolve);
  const finalHeld=new Promise<void>(resolve=>releaseFinal=resolve);
  const childHookHeld=new Promise<void>(resolve=>releaseChildHook=resolve),rootHookHeld=new Promise<void>(resolve=>releaseRootHook=resolve);
  return {name,held,release,holdFinal,finalHeld,releaseFinal,childHookHeld,rootHookHeld,releaseChildHook,releaseRootHook,rootStopEntered:false,rootTurnId:'',finalSampled:false,nativeResample:false,childResult:'NX_CHILD_DONE_'+name,callId:'spawn-'+name,spawned:false,childStarted:false,childReleased:false,childTurnId:'',agentId:'',parentTranscript:'',childTranscript:'',floor:undefined as CodexSubagentReadFloor|undefined,waited:false,resultRead:false,contextRead:false};
}

function tool(body:any,pattern:RegExp):{name:string;namespace?:string;parameters?:any;input_schema?:any}|undefined{
  for(const entry of body.tools??[]){
    if(entry.type==='namespace'){
      const inner=entry.tools.find((item:any)=>pattern.test(item.name));
      if(inner)return{...inner,namespace:entry.name};
    }else if(pattern.test(entry.name))return entry;
  }
}

test('官方 Codex 原生专用后台按原会话续跑，父轮次最后一次采样错过子结果也只唤醒一次',{timeout:155000},async t=>{
  const temp=temporaryDirectory(),cwd=temp.directory,home=path.join(cwd,'bridge'),codexHome=path.join(cwd,'codex'),claudeHome=path.join(cwd,'claude');
  for(const directory of [home,codexHome,claudeHome])mkdirSync(directory);
  const config={...DEFAULT_CONFIG,implicit:'never' as const,partnerProfiles:{codex:{source:'host' as const}},codexHome,claudeHome,...(process.env.NXBRIDGE_TEST_CODEX_BINARY?{codexBinary:process.env.NXBRIDGE_TEST_CODEX_BINARY}:{})};
  const restore=overrideEnvironment({NXBRIDGE_HOME:home,CODEX_HOME:codexHome,CLAUDE_CONFIG_DIR:claudeHome,OPENAI_API_KEY:'nxbridge-local-test-only'});
  const primary=scenario('primary'),fallback=scenario('queue'),race=scenario('final_race',true),phases=[primary,fallback,race];
  const nativeV2=process.env.NXBRIDGE_TEST_CODEX_V2==='1';
  let phase=primary,requests=0,providerError:unknown;
  const rootModel='gpt-5.1-codex',childModel='gpt-6-sol';
  const model=http.createServer(async(req,res)=>{
    try{
      const raw=await readModelBody(req);
      if(!req.url?.includes('/responses')){res.writeHead(200,{'content-type':'application/json'});res.end('{"data":[]}');return;}
      const body=JSON.parse(raw.toString('utf8')),step=phase,id='completion-fixture-'+ ++requests;
      const message=(text:string)=>({type:'message',id:'msg-'+id,role:'assistant',status:'completed',content:[{type:'output_text',text}]});
      const call=(spec:NonNullable<ReturnType<typeof tool>>,callId:string,args:object)=>({type:'function_call',id:'call-'+id,call_id:callId,name:spec.name,...(spec.namespace?{namespace:spec.namespace}:{}),arguments:JSON.stringify(args)});
      let item:ReturnType<typeof message>|ReturnType<typeof call>;
      if(body.model===childModel){step.childStarted=true;await step.held;item=message(step.childResult);}
      else{
        assert.equal(body.model,rootModel);
        if(!step.spawned){
          const spawn=tool(body,/(?:^|\.)spawn_agent$/);assert.ok(spawn,'原生子代理工具未加载');
          const v2=(spawn.parameters??spawn.input_schema)?.required?.includes('task_name');
          step.spawned=true;item=call(spawn,step.callId,{message:'完成隔离的子代理验收任务。',model:childModel,...(v2?{task_name:'completion_'+step.name,fork_turns:'none'}:{fork_context:false})});
        }else if(step.holdFinal&&!step.finalSampled){
          assert.ok(!JSON.stringify(body.input).includes(step.childResult),'父轮次最后一次采样必须发生在子结果之前');
          step.finalSampled=true;await step.finalHeld;
          item=message('NX_ROOT_IDLE_'+step.name);
        }else if(!step.childReleased)item=message('NX_ROOT_IDLE_'+step.name);
        else{
          const input=JSON.stringify(body.input);
          const hasContext=!!step.childTurnId&&body.input.some((entry:unknown)=>{
            const text=JSON.stringify(entry);
            return text.includes('[nxbridge own_subagents_completed]')&&text.includes(step.childTurnId);
          });
          step.contextRead||=hasContext;
          if(step.holdFinal&&!hasContext){
            assert.ok(!nativeV2&&input.includes(step.childResult),'无完成提醒的后续采样必须由原生直接送入结果');
            assert.equal(step.nativeResample,false,'同一子结果无需重复采样');
            step.nativeResample=true;
          }
          if(input.includes(step.childResult)){step.resultRead=true;item=message('NX_ROOT_READ_'+step.name);}
          else{
            assert.equal(step.waited,false,'续跑后子代理结果未进入模型请求');step.waited=true;
            const wait=tool(body,/(?:^|\.)(?:wait_agent|wait)$/);assert.ok(wait,'原生等待工具未加载');
            const spawn=body.input.find((entry:any)=>entry.type==='function_call_output'&&entry.call_id===step.callId);
            const spawned=JSON.parse(spawn?.output),idKey=Object.keys((wait.parameters??wait.input_schema)?.properties??{}).find(key=>(wait.parameters??wait.input_schema).properties[key].type==='array');
            const child=spawned.agent_id??spawned.id;assert.ok(!idKey||typeof child==='string','原生子代理启动没有返回会话编号');
            item=call(wait,'wait-'+step.name,{...(idKey?{[idKey]:[child]}:{}),timeout_ms:10000});
          }
        }
      }
      res.writeHead(200,{'content-type':'text/event-stream'});
      for(const event of [{type:'response.created',response:{id,status:'in_progress',output:[]}},{type:'response.output_item.added',output_index:0,item},{type:'response.output_item.done',output_index:0,item},{type:'response.completed',response:{id,status:'completed',output:[item],usage:{input_tokens:20,output_tokens:5,total_tokens:25}}}])res.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
      res.end();
    }catch(error){providerError=error;res.writeHead(500);res.end(String(error));}
  });
  const adapter=new CodexAdapter(config,home),store=new Store(path.join(home,'nxbridge.db'));
  const broker=new Broker(store,config,{codex:adapter,claude:new FakeAdapter()} as unknown as Adapters,home);
  let listener:Awaited<ReturnType<typeof listenBroker>>|undefined;
  t.after(async()=>{
    for(const step of phases){step.release();step.releaseFinal();step.releaseChildHook();step.releaseRootHook();}
    try{await listener?.close();await broker.stop(false);adapter.close();assert.deepEqual(await releaseNativeTestLaunches(home),[]);}
    finally{store.close();model.closeAllConnections();if(model.listening)await new Promise<void>(resolve=>model.close(()=>resolve()));restore();await temp.cleanup();}
  });
  await new Promise<void>(resolve=>model.listen(0,'127.0.0.1',resolve));
  const base=`http://127.0.0.1:${(model.address() as {port:number}).port}/v1`,configFile=path.join(codexHome,'config.toml');
  writeFileSync(path.join(home,'config.json'),JSON.stringify(config),'utf8');
  writeFileSync(configFile,stringify({model:rootModel,model_provider:'local_completion_canary',model_reasoning_effort:'low',web_search:'disabled',sandbox_mode:'read-only',approval_policy:'never',check_for_update_on_startup:false,
    analytics:{enabled:false},features:{hooks:true,multi_agent:true,multi_agent_v2:nativeV2,apps:false,remote_plugin:false,code_mode:{enabled:false}},projects:{[cwd]:{trust_level:'trusted'}},
    model_providers:{local_completion_canary:{name:'本机子代理完成验收',base_url:base,wire_api:'responses',requires_openai_auth:false,request_max_retries:0,stream_max_retries:0}}}),'utf8');
  await ensureHome(home);
  const marketplace=await preparePlugins(home);
  await runCli('codex',['plugin','marketplace','add',marketplace,'--json'],{config,home,cwd});
  await runCli('codex',['plugin','add','nxbridge@nxbridge-local','--json'],{config,home,cwd});
  const control=await configClient(config,home);
  try{
    const hooks=(await control.request('hooks/list',{cwds:[cwd]})).data.flatMap((row:any)=>row.hooks??[]).filter((hook:any)=>hook.pluginId==='nxbridge@nxbridge-local');
    assert.ok(hooks.some((hook:any)=>String(hook.eventName).toLowerCase()==='subagentstop'||String(hook.key).includes('SubagentStop')),'插件必须交给原生宿主执行子代理完成 hook');
    await control.request('config/batchWrite',{edits:[{keyPath:'hooks.state',value:Object.fromEntries(hooks.map((hook:any)=>[hook.key,{enabled:true,trusted_hash:hook.currentHash}])),mergeStrategy:'replace'}],filePath:configFile},{mutation:true});
  }finally{await control.close();}

  const syntheticTurns=new Set<string>();
  listener=await listenBroker(home,async(method,input)=>{
    if(method==='hook'){
      const hook=input as {hook_event_name?:string;prompt?:unknown;turn_id?:string;session_id?:string;agent_id?:string;transcript_path?:string;agent_transcript_path?:string};
      if(hook.prompt===HEALTH_WAKE_NOTICE&&hook.turn_id)syntheticTurns.add(hook.turn_id);
      if(hook.hook_event_name==='SubagentStop'&&hook.turn_id){
        const current=phase;
        current.childTurnId=hook.turn_id;current.agentId=hook.agent_id??'';
        current.parentTranscript=hook.transcript_path??'';current.childTranscript=hook.agent_transcript_path??'';
        const result=await broker.dispatch(method,input);
        current.floor=ledger()?.pending?.find(entry=>entry.agentId===current.agentId&&entry.childTurnId===current.childTurnId)?.floor;
        if(current.holdFinal)await current.childHookHeld;
        return result;
      }
      if(phase.holdFinal&&hook.hook_event_name==='Stop'&&hook.session_id===sessionId&&hook.turn_id===phase.rootTurnId){
        const current=phase;current.rootStopEntered=true;current.releaseChildHook();await current.rootHookHeld;
      }
    }
    return broker.dispatch(method,input);
  });
                                        
  const placeholder=store.placeholder('codex',cwd),input=await adapter.create(placeholder,undefined,undefined,'bypass');
  const root=store.adopt(placeholder.id,input),address=input.endpoint.address,sessionId=input.sessionId;
  const runtime=managedRuntimes(home).find(record=>record.value.peerId===placeholder.id)?.value;
  assert.ok(runtime?.pid&&runtime.procStart);
  const self=await processIdentity(process.pid);
  const ledger=()=>store.setting<{pending:{agentId:string;childTurnId:string;floor?:CodexSubagentReadFloor}[]}>('subagentCompletions:'+root.id);
  const partner=store.register(registration('claude','completion-counterpart',{cwd,endpoint:{mode:'claude-inbox',address:'local-counterpart',epoch:'fixture',pid:self.pid,procStart:self.startedAt}}));
  store.pair(partner.id,root.id);
  const rpc=await adapter.attach(root,store.endpoint(root.id)!);
  assert.equal(adapter.hasSubscription(root,store.endpoint(root.id)!),true);
  await broker.start();
  const completed=new Map<string,string>(),hookFailures:{event:unknown;message:unknown;entries:unknown}[]=[];
  rpc.on('notification',message=>{
    if(message.method==='turn/completed'&&message.params.threadId===sessionId)completed.set(message.params.turn.id,message.params.turn.status);
    if(message.method==='hook/completed'&&(message.params.run?.status==='failed'||hookGuardTimeout(message.params.run?.entries))){
      const run=message.params.run;
      hookFailures.push({event:run.eventName,message:run.statusMessage,entries:run.entries});
    }
  });
  const nativeRequest=rpc.request.bind(rpc);let primarySends=0,forcedFallback=0,queueAccepted=0;
  t.mock.method(rpc,'request',async(method:string,params:any,options:any)=>{
    if(method==='turn/start'&&params?.threadId===sessionId&&params.input?.[0]?.text===HEALTH_WAKE_NOTICE){
      if(phase===fallback&&forcedFallback===0){forcedFallback++;throw new DeliveryError('-32601','隔离验收：原生 turn/start 明确拒绝');}
      primarySends++;
    }
    return nativeRequest(method,params,options);
  });
  const nativeQueue=adapter.queueWake.bind(adapter);
  t.mock.method(adapter,'queueWake',async(...args:Parameters<typeof adapter.queueWake>)=>{await nativeQueue(...args);queueAccepted++;});
  const relevantEvents=()=>store.events(0,1500).filter(event=>/^subagent\.|^wake\.|^hook\./.test(String(event.type)));
  const idle=async()=>(await rpc.request('thread/read',{threadId:sessionId,includeTurns:false},{timeout:5000})).thread.status?.type==='idle';
  const rollout=(file:string)=>{
    assert.ok(path.isAbsolute(file),'原生 hook 必须携带历史的绝对路径');
    const resolved=realpathSync.native(file),relative=path.relative(realpathSync.native(codexHome),resolved);
    assert.ok(relative&&!path.isAbsolute(relative)&&relative.split(path.sep)[0]!=='..','原生历史必须在隔离 Codex 配置目录内');
    return readFileSync(resolved,'utf8').trim().split('\n').map(line=>JSON.parse(line));
  };
  const wait=async(check:()=>boolean|Promise<boolean>,why:string)=>{
    const deadline=Date.now()+25000;
    while(!await check()){
      if(providerError)throw providerError;
      if(Date.now()>=deadline){t.diagnostic(JSON.stringify({phase:phase.name,completed:[...completed],syntheticTurns:[...syntheticTurns],requests,primarySends,forcedFallback,queueAccepted,state:store.peer(root.id).state,hookFailures,events:relevantEvents()}));assert.fail(why);}
      await delay(75);
    }
  };

  for(const next of phases){
    phase=next;
    const before=new Set(completed.keys()),beforeSynthetic=new Set(syntheticTurns),beforeRead=relevantEvents().filter(event=>event.type==='subagent.results_read').length;
    const {turn}=await rpc.request('turn/start',{threadId:sessionId,input:[{type:'text',text:'启动子代理，先结束父轮次。'+phase.name}]},{mutation:true});
    phase.rootTurnId=turn.id;
    if(phase.holdFinal){
      await wait(()=>phase.childStarted&&phase.finalSampled,'父会话最后一次采样和子代理请求未同时在途');
      phase.childReleased=true;phase.release();
      await wait(()=>!!phase.floor,'父会话最后一次请求等待时没有收到原生子代理完成 hook');
                                                   
      phase.releaseFinal();
      await wait(()=>phase.rootStopEntered&&rollout(phase.childTranscript).some(row=>row.type==='event_msg'
        &&['task_complete','turn_complete'].includes(row.payload?.type)&&row.payload.turn_id===phase.childTurnId),'父会话进入 Stop 后，原生子代理未完成');
      await broker.pump();
      assert.equal(completed.has(turn.id),false);
      assert.equal([...syntheticTurns].filter(id=>!beforeSynthetic.has(id)).length,0);
      assert.equal(relevantEvents().filter(event=>event.type==='subagent.results_read').length,beforeRead);
      assert.ok(ledger()?.pending.some(entry=>entry.childTurnId===phase.childTurnId),'父轮次未采样的完成结果必须保留');
      phase.releaseRootHook();
    }else{
      await wait(async()=>phase.childStarted&&completed.get(turn.id)==='completed'&&await idle(),'父会话没有在子代理结束前空闲');
      phase.childReleased=true;phase.release();
    }
    await wait(async()=>phase.resultRead&&completed.get(turn.id)==='completed'&&(phase.nativeResample||[...syntheticTurns].some(id=>!beforeSynthetic.has(id)&&completed.get(id)==='completed'))&&await idle(),'自己的子代理完成后未续跑原父会话');
    for(let round=0;round<3;round++){await broker.pump();await delay(75);}
    assert.equal(phase.contextRead,!phase.nativeResample,'只有原生在原轮次已采样时才不需要桥接完成通知');
    const parentRows=rollout(phase.parentTranscript),childRows=rollout(phase.childTranscript);
    assert.equal(parentRows[0]?.payload.id,sessionId);
    assert.equal(childRows[0]?.payload.id,phase.agentId);
    assert.notEqual(phase.childTurnId,turn.id);
    assert.ok(childRows.some(row=>row.type==='event_msg'&&['task_started','turn_started'].includes(row.payload?.type)&&row.payload.turn_id===phase.childTurnId),'子代理 hook 必须携带子轮次编号');
    const {dev,ino,size}=statSync(phase.parentTranscript);
    assert.ok(phase.floor&&phase.floor.dev===dev&&phase.floor.ino===ino&&phase.floor.offset>0&&phase.floor.offset<size,'父会话的原生历史必须有已核验的追加位置');
    if(ledger()?.pending.length){
      t.diagnostic(JSON.stringify({phase:phase.name,floor:phase.floor,pending:ledger()?.pending,parentRows:parentRows.slice(-28).map(row=>({type:row.type,payloadType:row.payload?.type,turnId:row.payload?.turn_id??row.payload?.internal_chat_message_metadata_passthrough?.turn_id,role:row.payload?.role,kinds:row.payload?.internal_chat_message_metadata_passthrough?.content_item_kinds,hasChildResult:JSON.stringify(row.payload)?.includes(phase.childResult),historySequence:row.metadata?.history_sequence,sampledThrough:row.metadata?.sampled_through_sequence}))}));
    }
    assert.equal(ledger()?.pending.length,0,'真实父会话 Stop 必须结算已读子代理结果');
    assert.equal(relevantEvents().filter(event=>event.type==='subagent.results_read').length,beforeRead+1);
    assert.equal(store.peer(root.id).sessionId,sessionId);
    const addedWakeTurns=[...syntheticTurns].filter(id=>!beforeSynthetic.has(id));
    assert.equal(addedWakeTurns.length,phase.nativeResample?0:1);
    if(phase.holdFinal&&!phase.nativeResample){
      const ended=parentRows.findIndex(row=>row.type==='event_msg'&&['task_complete','turn_complete'].includes(row.payload?.type)&&row.payload.turn_id===turn.id);
      const resumed=parentRows.findIndex(row=>row.type==='event_msg'&&['task_started','turn_started'].includes(row.payload?.type)&&row.payload.turn_id===addedWakeTurns[0]);
      assert.ok(ended>=0&&resumed>ended,'正常首醒必须开始在原父轮次持久结束之后');
    }
    assert.equal([...completed.keys()].filter(id=>!before.has(id)).length,phase.nativeResample?1:2,'子结果只在原轮次或一次完成唤醒中采样');
  }
  assert.equal(primarySends,race.nativeResample?1:2);assert.equal(forcedFallback,1);assert.equal(queueAccepted,1);
  assert.equal(relevantEvents().filter(event=>event.type==='subagent.wake.fallback').length,1);
  assert.equal(providerError,undefined);
  t.diagnostic(JSON.stringify({nativePrimary:primarySends,officialQueue:queueAccepted,finalRaceNativeResample:race.nativeResample,finalRaceSyntheticWake:!race.nativeResample,rootWakeTurns:syntheticTurns.size,loopbackModelRequests:requests,realModelCalls:0}));
});
