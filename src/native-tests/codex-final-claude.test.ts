import { overrideEnvironment } from '../tests/support/environment.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { temporaryDirectory } from '../tests/support/temporary.js';
import { FakeAdapter } from '../tests/support/fake-adapter.js';
import { setupPublish } from './publish-fixture.js';
import { NativeTerminal } from './terminal-fixture.js';
import { holdModelStream } from './model-stream-fixture.js';
import { listenBroker } from '../ipc/ipc.js';
import { ensureHome } from '../ipc/home.js';
import { DEFAULT_CONFIG } from '../config/defaults.js';
import { resolveCli } from '../process/host-command.js';
import { processIdentity } from '../process/process.js';
import { Store } from '../storage/store.js';
import { Broker } from '../broker/coordinator.js';
import { ClaudeAdapter } from '../hosts/claude/adapter.js';
import { saveManagedRuntime } from '../hosts/managed-runtimes.js';
import { managedMarker } from '../install/installation.js';
import { delay } from '../shared/time.js';

                                                         
test('实际 Claude：忙时保留后台完成事实，停下时同轮继续，空闲时只叫醒一次且不转发最终正文',{skip:process.platform!=='win32',timeout:170000},async t=>{
  const temp=temporaryDirectory(),root=temp.directory,home=path.join(root,'bridge'),claudehome=path.join(root,'claude'),codexhome=path.join(root,'codex');
  for(const directory of [home,claudehome,codexhome])mkdirSync(directory,{recursive:true});
  const PRIVATE='NATIVE_CODEX_PRIVATE_FINAL 不应披露的隔离正文',HOLD='NATIVE_COMPLETION_HOLD',CHECK='auto check-ins left:';
  type Call={method:string;at:number;order:number;readyBoundary:boolean;turnId?:string;state?:string;blocked?:boolean;recorded?:boolean;completionNotice?:boolean;remainingAfterBlock?:number};
  type ModelRequest={text:string;at:number;order:number;ended?:number};
  const requests:ModelRequest[]=[],calls:Call[]=[],errors:string[]=[];
  let order=0;
  let terminal:NativeTerminal|undefined,holder:ChildProcess|undefined,listener:Awaited<ReturnType<typeof listenBroker>>|undefined,passed=false,phase='启动';
  let held=false,released=false,holding=false;
  const started=Date.now(),stage=(name:string)=>{phase=name;process.stderr.write(JSON.stringify({completionGuardStage:name,elapsedMs:Date.now()-started})+'\n');};
  const model=http.createServer(async(req,res)=>{try{
    let raw='';for await(const chunk of req)raw+=chunk;
    if(!req.url?.includes('/v1/messages')||req.url.includes('count_tokens')){res.writeHead(200,{'content-type':'application/json'});res.end('{"input_tokens":1,"data":[]}');return;}
    const body=JSON.parse(raw),main=body.tools?.some((tool:any)=>tool.name==='ListAgents'),text=JSON.stringify(body.messages??[]);
    const request:ModelRequest|undefined=main?{text,at:Date.now(),order:++order}:undefined;
    if(request)requests.push(request);
                                                  
    const hold=main&&!held&&text.includes(HOLD);if(hold)held=true;
    const reply=main?'NATIVE_COMPLETION_ACK':JSON.stringify(body.system).includes('security monitor')?'<severity>0</severity>':'{"state":"done"}';
    const message={id:'final-'+Date.now(),type:'message',role:'assistant',model:body.model,content:[{type:'text',text:reply}],stop_reason:'end_turn',stop_sequence:null,usage:{input_tokens:10,output_tokens:5}};
    if(!body.stream){if(request)request.ended=++order;res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify(message));return;}
    res.writeHead(200,{'content-type':'text/event-stream'});
    const emit=(event:any)=>res.write('event: '+event.type+'\ndata: '+JSON.stringify(event)+'\n\n');
    emit({type:'message_start',message:{...message,content:[],stop_reason:null}});
    emit({type:'content_block_start',index:0,content_block:{type:'text',text:''}});
    if(hold){holding=true;const until=Date.now()+90000;while(!released&&Date.now()<until)if(!await holdModelStream(res,500))return;holding=false;}
    for(const event of [{type:'content_block_delta',index:0,delta:{type:'text_delta',text:reply}},{type:'content_block_stop',index:0},{type:'message_delta',delta:{stop_reason:'end_turn',stop_sequence:null},usage:{output_tokens:5}},{type:'message_stop'}])emit(event);
    if(request)request.ended=++order;
    res.end();
  }catch(error){errors.push(String(error));if(!res.headersSent)res.writeHead(500);res.end(String(error));}});
  await new Promise<void>(resolve=>model.listen(0,'127.0.0.1',resolve));
  const env={NXBRIDGE_HOME:home,CLAUDE_CONFIG_DIR:claudehome,CODEX_HOME:codexhome,ANTHROPIC_API_KEY:'native-final-local-key',ANTHROPIC_BASE_URL:`http://127.0.0.1:${(model.address() as any).port}`,DISABLE_AUTOUPDATER:'1',DISABLE_TELEMETRY:'1',CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1',CLAUDE_CODE_DISABLE_BACKGROUND_TASKS:'1'};
  const restore=overrideEnvironment(env);
  const config={...DEFAULT_CONFIG,implicit:'never' as const,compatibilityWake:'strict' as const,claudeHome:claudehome,codexHome:codexhome};
  writeFileSync(path.join(home,'config.json'),JSON.stringify(config),'utf8');
  writeFileSync(path.join(codexhome,'config.toml'),'web_search="disabled"\n[features]\napps=false\nremote_plugin=false\n','utf8');
  writeFileSync(path.join(claudehome,'.claude.json'),JSON.stringify({hasCompletedOnboarding:true,customApiKeyResponses:{approved:[env.ANTHROPIC_API_KEY.slice(-20)],rejected:[]},projects:{[root.replaceAll('\\','/')]:{hasTrustDialogAccepted:true}}}),'utf8');
  writeFileSync(path.join(claudehome,'settings.json'),JSON.stringify({crossSessionInbound:'accept',env:{ANTHROPIC_API_KEY:env.ANTHROPIC_API_KEY,ANTHROPIC_BASE_URL:env.ANTHROPIC_BASE_URL,CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1'}}),'utf8');
  await ensureHome(home);
  const store=new Store(path.join(home,'nxbridge.db')),claudeAdapter=new ClaudeAdapter(config,home),codexAdapter=new FakeAdapter();
  const broker=new Broker(store,config,{codex:codexAdapter,claude:claudeAdapter} as any,home);
  t.after(async()=>{
    try{
      if(!passed)t.diagnostic(JSON.stringify({phase,errors,holding,requests:requests.map(r=>({at:r.at-started,tip:r.text.includes(CHECK),notice:r.text.includes('source=background_completed'),private:r.text.includes(PRIVATE)})),calls:calls.slice(-40),pairs:store.pairs(),peers:store.peers().map(p=>({id:p.id,host:p.host,state:p.state,owner:p.owner})),guard:(()=>{try{const codex=store.peers().find(p=>p.host==='codex');return codex?broker.turnEnds.status(codex):undefined;}catch(error){return String(error);}})(),terminal:terminal?.output.slice(-2500)}));
      await terminal?.close();
      if(holder&&holder.exitCode===null&&holder.signalCode===null){const exit=once(holder,'exit');holder.kill();await exit;}
      await listener?.close();await broker.stop(false);claudeAdapter.close();
    }finally{store.close();model.closeAllConnections();if(model.listening)await new Promise<void>(resolve=>model.close(()=>resolve()));restore();await temp.cleanup();}
  });
                                                         
  const dispatch=broker.dispatch.bind(broker);
  t.mock.method(broker,'dispatch',async(method:string,input:unknown)=>{
    const fields=input as {turn_id?:string;state?:string;ready_boundary?:boolean;actor?:{sessionId?:string}}|undefined;
    const record:Call={method,at:Date.now(),order:++order,readyBoundary:fields?.ready_boundary===true,turnId:fields?.turn_id,state:fields?.state};
    calls.push(record);if(calls.length>400)calls.shift();
    const result=await dispatch(method,input);
    if(method==='peer.turn.stop'){
      record.blocked=!!(result as {block?:string})?.block;
      if(record.blocked){
        const member=store.peers().find(p=>p.host==='claude'&&p.sessionId===fields?.actor?.sessionId);
        if(member)record.remainingAfterBlock=broker.turnEnds.budget.status(member.id).remaining;
      }
    }
    if(method==='peer.turn.completed')record.recorded=(result as {recorded?:boolean})?.recorded;
    if(method==='health.monitor')record.completionNotice=(result as {notices?:{source?:string;componentIds?:string[]}[]})?.notices
      ?.some(notice=>notice.source==='background_completed'||notice.componentIds?.some(id=>id.startsWith('background-completed:')));
    return result;
  });
  listener=await listenBroker(home,(method,input)=>broker.dispatch(method,input));
  stage('安装隔离发布包');await setupPublish();
  stage('启动实际终端');
  const cli=resolveCli('claude');terminal=await NativeTerminal.start(root,home,cli.command,[...cli.prefix,'--model','claude-sonnet-4-6'],root,{...process.env,TERM:'xterm-256color',COLORTERM:'truecolor'});
  const claude=()=>store.peers().find(p=>p.host==='claude'&&p.kind==='root'&&!!p.sessionId);
                                         
  stage('热身一轮');await terminal.prompt('NATIVE_FINAL_WARMUP');
  await terminal.wait(()=>requests.some(r=>r.text.includes('NATIVE_FINAL_WARMUP'))&&claude()?.state==='ready','实际 Claude 没有完成热身一轮',60000);
  const claudePeer=claude()!;
  stage('登记并配对托管 Codex 夹具');
  holder=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{windowsHide:true,stdio:'ignore'});await once(holder,'spawn');
  const identity=await processIdentity(holder.pid!),codexSession=randomUUID(),address='ws://127.0.0.1:49197',runtimeId=randomUUID();
  const codex=store.adopt(store.placeholder('codex',root).id,{host:'codex',sessionId:codexSession,cwd:root,name:'隔离后台 Codex',kind:'root',permission:'prompting',state:'ready',
    endpoint:{mode:'app-server',address,epoch:'fixture:'+identity.startedAt,pid:holder.pid!,procStart:identity.startedAt,configHome:codexhome,runtimeId}});
  saveManagedRuntime(home,{id:runtimeId,owner:managedMarker(home),kind:'owned',state:'ready',dedicated:true,authenticatedLoopback:true,endpoint:address,createdAt:Date.now(),
    peerId:codex.id,sessionId:codexSession,pid:holder.pid!,procStart:identity.startedAt,configHome:codexhome});
  const pair=store.pair(claudePeer.id,codex.id);
  assert.equal(codex.owner,'nxbridge');assert.equal(claudePeer.owner,'user');assert.equal(config.completionWakeLimit,4);
  const history=path.join(codexhome,'sessions','2026','11','23','rollout-fixture-'+codexSession+'.jsonl');mkdirSync(path.dirname(history),{recursive:true});
  writeFileSync(history,JSON.stringify({type:'session_meta',payload:{id:codexSession}})+'\n','utf8');
  store.setSetting('codexTranscript:'+codex.id,{sessionId:codexSession,file:history});
                                           
  for(const child of ['first-child','second-child'])broker.subagentCompletions.record(store.peer(codex.id),child,child+'-turn',undefined,history);
  const childWake=broker.subagentCompletions.reserve(store.peer(codex.id),'native');assert.ok(childWake);
  broker.subagentCompletions.outcome(store.peer(codex.id),childWake,'submitted','completed-child-wake');
  assert.ok(broker.subagentCompletions.consume(store.peer(codex.id),broker.subagentCompletions.context(store.peer(codex.id))));
  broker.subagentCompletions.settle(store.peer(codex.id),history,'completed-child-wake');
  assert.equal(broker.subagentCompletions.status(store.peer(codex.id))?.unconfirmed,2);
                                                       
  const last=store.accept(pair,store.peer(codex.id),'隔离进度：控制台候选已构建，接着做公网检查','fixture-last-send');
  store.transition(last.id,'contextObserved','原生上下文已核验');
  const messagesBefore=store.messages().length;
  const codexPeer=()=>store.peer(codex.id),status=()=>broker.turnEnds.status(codexPeer());
  const native=(method:string,params:object)=>codexAdapter.emit('native',{address,message:{jsonrpc:'2.0',method,params:{threadId:codexSession,...params}}});
  const backgroundTurn=(withWork:boolean)=>{
    const turnId=randomUUID(),base=Date.now();
    const row=(offset:number,payload:unknown,type='response_item')=>({timestamp:new Date(base+offset).toISOString(),type,payload});
    const event=(offset:number,type:string,extra:object={})=>row(offset,{type,turn_id:turnId,...extra},'event_msg');
    const append=(...rows:unknown[])=>appendFileSync(history,rows.map(value=>JSON.stringify(value)+'\n').join(''),'utf8');
    append(event(0,'task_started'));
    codexAdapter.emit('state',codexSession,'busy',address);
    native('turn/started',{turn:{id:turnId,status:'inProgress'}});
    return{turnId,async finish(){
      if(withWork){
        native('item/completed',{turnId,item:{id:'native-command',type:'commandExecution'}});
        append(row(1,{type:'custom_tool_call',name:'exec',call_id:'fixture-exec'}),row(2,{type:'custom_tool_call_output',call_id:'fixture-exec'}),
          event(3,'item_completed',{thread_id:codexSession,item:{type:'AgentMessage',phase:'final_answer',id:'fixture-answer',content:[{type:'Text',text:PRIVATE}]}}));
      }
      append(event(4,'task_complete'));
      native('turn/completed',{turn:{id:turnId,status:'completed'}});
      codexAdapter.emit('state',codexSession,'ready',address);
      await broker.turnEnds.scan(codexPeer());broker.turnEnds.refresh(pair);
    }};
  };
  stage('Claude 进入实际忙碌');
  await terminal.prompt(HOLD);
  await terminal.wait(()=>holding&&claude()?.state==='busy','实际 Claude 没有进入忙碌',40000);
  const heldTurn=store.endpoint(claudePeer.id)?.nativeHealth?.turnId;
  assert.ok(heldTurn,'原生 Stop 前已有正在运行的根轮次');
  stage('后台原生完成，既无干活工具也无最终正文');
  const empty=backgroundTurn(false);
  assert.equal(status().state,'running');await empty.finish();
  assert.equal(status().state,'pending');assert.equal(status().turn_id,empty.turnId);assert.equal(status().reason,'no_tools');
  assert.equal(status().hold,'foreground_busy');
  stage('忙碌期间经过至少一次 Mod 监测');
  const busyFrom=Date.now();await delay(17000);
  assert.equal(claude()?.state,'busy');
  assert.equal(requests.some(r=>r.at>=busyFrom&&r.text.includes(CHECK)),false,'Claude 忙时不得附入或另开一轮');
  assert.equal(status().hold,'foreground_busy');
  stage('Claude 的第一个 Stop 原生拦截，继续片段保持同一轮');
  const beforeStop=order;released=true;
  const blocks=()=>calls.filter(c=>c.order>beforeStop&&c.method==='peer.turn.stop'&&c.blocked);
  const completions=()=>calls.filter(c=>c.order>beforeStop&&c.method==='peer.turn.completed'&&c.recorded);
  const continuation=()=>requests.filter(r=>r.order>beforeStop);
  await terminal.wait(()=>blocks().some(c=>c.turnId===heldTurn)&&completions().some(c=>c.turnId===heldTurn)
    &&continuation().some(r=>r.text.includes(CHECK))&&claude()?.state==='ready','Claude 没有在原轮次继续并正常停下',35000);
  assert.deepEqual(blocks().map(c=>c.turnId),[heldTurn]);
  assert.deepEqual(completions().map(c=>c.turnId),[heldTurn],'同一原生根轮次只确认一次终态');
  assert.equal(store.endpoint(claudePeer.id)?.nativeHealth?.turnId,heldTurn);
  assert.equal(continuation().length,1,'同轮提示只触发一个继续片段');
  const blocked=blocks()[0]!,completed=completions()[0]!,continued=continuation()[0]!;
  assert.ok(continued.ended&&blocked.order<continued.order&&continued.ended<completed.order,'原生 Stop 拦截后才继续，继续结束后才确认完成');
  assert.equal(blocked.remainingAfterBlock,3,'原生 Stop 拦截实际消耗一次额度');
  assert.match(continued.text,/last turn: no work-tool calls/);
  assert.equal(calls.some(c=>c.method==='peer.state'&&c.state==='ready'&&c.order>beforeStop&&c.order<continued.ended!),false,'继续片段结束前前台未提前变为空闲');
  assert.equal(status().state,'completed');
  assert.deepEqual(status().budget,{limit:4,used:0,remaining:4,awaitingProof:0},'无操作的成功继续片段全额退回');
  const idleFrom=Date.now();
  stage('前台空闲时，后台下一轮只产生一次通知唤醒');
  const worked=backgroundTurn(true);await worked.finish();
  assert.equal(status().turn_id,worked.turnId);assert.equal(status().reason,'work_after_message');
  await terminal.wait(()=>requests.some(r=>r.at>=idleFrom&&r.text.includes('source=background_completed')),'前台空闲后没有收到完成提醒',50000);
  const woken=requests.filter(r=>r.at>=idleFrom&&r.text.includes('source=background_completed'));
  assert.equal(woken.length,1);assert.match(woken[0]!.text,/last turn: work not reported/);
  assert.match(woken[0]!.text,/auto check-ins left: 3\/4/);
  const noticeReceipts=calls.filter(c=>c.at>=idleFrom&&c.method==='health.monitor'&&c.completionNotice&&c.readyBoundary);
  assert.equal(noticeReceipts.length,1,'Mod 只在原生空闲边界领取一次完成提醒');
  stage('叫醒这一轮结束，再经过一次监测');
  await terminal.wait(()=>claude()?.state==='ready','叫醒轮次没有结束',40000);
                                          
  const after=Date.now();await delay(17000);
  assert.equal(requests.filter(r=>r.at>=after).length,0,'同一回合只叫醒一次');
  assert.equal(status().state,'completed');
  assert.equal(broker.subagentCompletions.status(codexPeer())?.unconfirmed,2,'自动转达不伪造子结果的读取证明');
  assert.equal(store.messages().length,messagesBefore,'完成提示不能伪造成搭档消息');
  assert.equal(requests.some(r=>r.text.includes(PRIVATE)||r.text.includes('[nxbridge codex_final]')||r.text.includes(last.id)),false,'请求只能收到简短状态事实');
  assert.deepEqual(errors,[]);
  passed=true;t.diagnostic(JSON.stringify({actualClaudePid:terminal.identity.pid,busyHeld:true,sameTurnStop:true,idleWokenOnce:true,realModelCalls:0}));
});
