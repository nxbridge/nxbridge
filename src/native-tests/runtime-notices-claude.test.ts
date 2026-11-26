import { overrideEnvironment } from '../tests/support/environment.js';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import assert from 'node:assert/strict';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { mkdirSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { spawn, execFile } from 'node:child_process';
import { once } from 'node:events';
import { promisify } from 'node:util';
import ts from 'typescript';
import { temporaryDirectory } from '../tests/support/temporary.js';
import { setupPublish } from './publish-fixture.js';
import { brokerCall } from '../ipc/ipc.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import { ClaudeAdapter } from '../hosts/claude/adapter.js';
import { processIdentity } from '../process/process.js';
import { stopOwnedProcess } from '../process/process-stop.js';
import { reserveBrokerStartup } from '../broker/broker-startup.js';
import { installedCacheDirectories } from '../install/installation.js';
import {delay} from '../shared/time.js';
import {resolveCli} from '../process/host-command.js';
import {windowsPowerShell} from '../shared/windows-tools.js';
import { pairLegacyClaude } from './legacy-claude-fixture.js';

const execute=promisify(execFile);

                                             
function probeNativeNotices(original:string,eventsFile:string):string{
  const source=ts.createSourceFile('register.js',original,ts.ScriptTarget.Latest,true,ts.ScriptKind.JS);
  const edits:{at:number;text:string}[]=[],calls:ts.CallExpression[]=[];
  const visit=(node:ts.Node)=>{if(ts.isCallExpression(node))calls.push(node);ts.forEachChild(node,visit);};
  visit(source);
  const one=<T>(matches:T[],label:string):T=>{
    assert.equal(matches.length,1,'发布插件应有一个 '+label+' 观察点');
    return matches[0]!;
  };
  const literal=(node:ts.Node|undefined,text:string)=>!!node&&ts.isStringLiteralLike(node)&&node.text===text;
  const name=(node:ts.Node|undefined)=>{assert.ok(node&&ts.isIdentifier(node),'观察点缺少绑定标识');return node.text;};
  const parentWhere=<T extends ts.Node>(node:ts.Node,match:(candidate:ts.Node)=>candidate is T):T=>{
    for(let parent=node.parent;parent;parent=parent.parent)if(match(parent))return parent;
    return assert.fail('发布插件的观察点不在预期的语法结构内');
  };
  const record=(api:string,fields:string)=>`__nativeEvents.push({at:Date.now(),${fields}});await ${api}.fs.write(${JSON.stringify(eventsFile)},JSON.stringify(__nativeEvents));`;
  const insert=(at:number,text:string)=>edits.push({at,text});
  const observeCallback=(node:ts.Node|undefined)=>{
    assert.ok(node&&(ts.isArrowFunction(node)||ts.isFunctionExpression(node))&&ts.isBlock(node.body),'观察点缺少原生回调');
    return {node,at:node.body.getStart(source)+1};
  };

  for(const event of ['prompt.submit','turn.complete']){
    const call=one(calls.filter(node=>literal(node.arguments[0],event)),event);
    const callback=observeCallback(call.arguments.at(-1));
    const api=name(callback.node.parameters[0]?.name),input=name(callback.node.parameters[1]?.name);
    insert(callback.at,record(api,`event:${JSON.stringify(event)},origin:${input}.origin,isMeta:${input}.isMeta,agentId:${input}.agentId,text:${input}.text?.slice(0,250),fields:Object.keys(${input})`));
  }
  const clocks=calls.filter(node=>{
    const method=node.expression,interval=node.arguments[0];
    return ts.isPropertyAccessExpression(method)&&method.name.text==='every'&&ts.isPropertyAccessExpression(method.expression)&&method.expression.name.text==='clock'
      &&!!interval&&ts.isNumericLiteral(interval)&&Number(interval.text)===15000;
  });
  assert.equal(clocks.length,2,'发布插件应保留原生通知和状态时钟');
  clocks.forEach((clock,index)=>{
    const callback=observeCallback(clock.arguments[1]),api=name((clock.expression as ts.PropertyAccessExpression).expression.getChildAt(0,source));
    insert(callback.at,record(api,`event:'clock-tick',clock:${index}`));
  });

  const monitor=one(calls.filter(node=>literal(node.arguments[1],'monitor')&&ts.isAwaitExpression(node.parent)&&ts.isVariableDeclaration(node.parent.parent)),'monitor 读取');
  const health=parentWhere(monitor,ts.isVariableDeclaration),healthStatement=parentWhere(health,ts.isVariableStatement);
  const statements=healthStatement.parent;assert.ok(ts.isBlock(statements));
  const next=statements.statements[statements.statements.indexOf(healthStatement)+1],index=healthStatement.declarationList.declarations.indexOf(health);
  const sameStatement=healthStatement.declarationList.declarations[index+1];
  const context=sameStatement??(next&&ts.isVariableStatement(next)?next.declarationList.declarations[0]:undefined),api=name(monitor.arguments[0]);
  const capture=context?.initializer;
  assert.ok(context&&capture&&ts.isAwaitExpression(capture)&&ts.isCallExpression(capture.expression)
    &&capture.expression.arguments.length===1&&name(capture.expression.arguments[0])===api,'monitor 读取后应捕获同一个原生窗口上下文');
  const contextName=name(context.name),contextStatement=parentWhere(context,ts.isVariableStatement);
  insert(contextStatement.end,';'+record(api,`event:'monitor',health:${name(health.name)},permission:${contextName}.nativePermission,binding:${contextName}.binding`));

  const control=one(calls.filter(node=>{
    const method=node.expression,argv=node.arguments[0];
    return ts.isPropertyAccessExpression(method)&&method.name.text==='run'&&ts.isPropertyAccessExpression(method.expression)&&method.expression.name.text==='process'
      &&!!argv&&ts.isArrayLiteralExpression(argv)&&literal(argv.elements[0],'node')&&literal(argv.elements[3],'mod');
  }),'mod 控制子进程');
  const handler=parentWhere(control,ts.isFunctionDeclaration);assert.ok(handler.body);
  const controlApi=name(handler.parameters[0]?.name),operation=name(handler.parameters[1]?.name),controlContext=name(handler.parameters[2]?.name);
  const result=parentWhere(control,ts.isVariableDeclaration);
                                             
  insert(handler.body.getStart(source)+1,`if(${operation}==='monitor-enqueue')delete ${controlContext}.nativePermission;const __permissionAtCall=${controlContext}.nativePermission;`);
  const resultName=name(result.name);
  insert(parentWhere(result,ts.isVariableStatement).end,`;if(['monitor','monitor-enqueue','monitor-prepare','monitor-ack'].includes(${operation})){${record(controlApi,`event:'control-result',operation:${operation},permission:__permissionAtCall,exitCode:${resultName}.exitCode,error:${resultName}.stderr,result:${resultName}.stdout`)}}`);
  return 'const __nativeEvents=[];'+edits.sort((a,b)=>b.at-a.at).reduce((code,{at,text})=>code.slice(0,at)+text+code.slice(at),original);
}

test('官方 Claude：被动事实不启动空闲轮次、正常下一轮可见；搭档这一轮失败经原生队列真实叫醒一次；broker 离线及重启保持被动', {skip:!['win32','linux'].includes(process.platform),timeout:170000},async t=>{
  const temp=temporaryDirectory(),directory=temp.directory,home=path.join(directory,'bridge'),claudehome=path.join(directory,'claude'),codexhome=path.join(directory,'codex');
  for(const d of [home,claudehome,codexhome])mkdirSync(d,{recursive:true});
  const requests:{body:any;at:number}[]=[],nativeEvents=path.join(directory,'native-events.json');let targetId='',sourceId='',pairId='';
  const targets:ReturnType<typeof spawn>[]=[];let completed=false,phase='启动原生 Claude';
                                                   
  const startedAt=Date.now(),mark=(name:string)=>{phase=name;process.stderr.write(`[runtime-notices +${Date.now()-startedAt}ms] ${name}
`);};
  const model=http.createServer(async(req,res)=>{
    let raw='';for await(const piece of req)raw+=piece;
    if(!req.url?.includes('/v1/messages')||req.url.includes('count_tokens')){res.writeHead(200,{'content-type':'application/json'});res.end('{"input_tokens":1,"data":[]}');return;}
    const body=JSON.parse(raw);requests.push({body,at:Date.now()});
    const main=body.tools?.some((tool:any)=>tool.name==='ListAgents'),results=(body.messages??[]).flatMap((m:any)=>Array.isArray(m.content)?m.content:[]);
    const tool=main&&!results.some((c:any)=>c.type==='tool_result'&&c.tool_use_id==='delivery-list')?'ListAgents':main&&!results.some((c:any)=>c.type==='tool_result'&&c.tool_use_id==='delivery-send')?'SendMessage':undefined;
    const text=main?'运行时通知已接收':JSON.stringify(body.system).includes('security monitor')?'<severity>0</severity>':'{"state":"done"}';
    const block=tool?{type:'tool_use',id:tool==='ListAgents'?'delivery-list':'delivery-send',name:tool,input:{}}:{type:'text',text:''},delta=tool?{type:'input_json_delta',partial_json:JSON.stringify(tool==='ListAgents'?{}:{to:'nxbridge:codex',message:'持久队列探针 '+ '通知提示验证 '.repeat(400)})}:{type:'text_delta',text};
    if(!body.stream){res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify({id:'probe',type:'message',role:'assistant',model:body.model,content:[{type:'text',text}],stop_reason:'end_turn',usage:{input_tokens:10,output_tokens:10}}));return;}
    res.writeHead(200,{'content-type':'text/event-stream'});
    for(const event of [{type:'message_start',message:{id:'probe',type:'message',role:'assistant',model:body.model,content:[],usage:{input_tokens:10,output_tokens:1}}},{type:'content_block_start',index:0,content_block:block},{type:'content_block_delta',index:0,delta},{type:'content_block_stop',index:0},{type:'message_delta',delta:{stop_reason:tool?'tool_use':'end_turn'},usage:{output_tokens:10}},{type:'message_stop'}])res.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
    res.end();
  });
  await new Promise<void>(resolve=>model.listen(0,'127.0.0.1',resolve));
  const env:NodeJS.ProcessEnv={NXBRIDGE_HOME:home,CLAUDE_CONFIG_DIR:claudehome,CODEX_HOME:codexhome,ANTHROPIC_API_KEY:'nxbridge-runtime-test-key',ANTHROPIC_BASE_URL:`http://127.0.0.1:${(model.address() as AddressInfo).port}`,DISABLE_AUTOUPDATER:'1',DISABLE_TELEMETRY:'1',CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1'},restoreEnvironment=overrideEnvironment(env);
  t.after(async()=>{
    if(!completed){let status:any;try{status=await brokerCall('status',{}, {home,start:false,timeout:2000});}catch(error){status=String(error);}let notices:unknown;try{const db=new DatabaseSync(path.join(home,'nxbridge.db'),{readOnly:true});try{notices=db.prepare('SELECT key,value FROM settings WHERE key IN (?,?,?)').all('healthNoticeAdmission:'+sourceId,'turnHealthDelivery:'+status.pair?.id+':'+sourceId,'pairIssues:'+targetId).map(row=>({key:row.key,value:JSON.parse(row.value as string)}));}finally{db.close();}}catch(error){notices=String(error);}t.diagnostic(JSON.stringify({phase,nativeEvents:observedEvents(),requests:mainRequestsSummary(),notices,status:typeof status==='string'?status:{pair:status.pair,peers:status.peers?.map((p:any)=>({id:p.id,state:p.state,runtime:p.runtime})),endpoints:status.endpoints,messages:status.messages?.map((m:any)=>({id:m.id,from:m.from,state:m.state,attempts:m.attempts}))},log:existsSync(path.join(home,'run','broker.log'))?readFileSync(path.join(home,'run','broker.log'),'utf8').slice(-2500):''}));}
    for(const child of targets)if(child.exitCode===null&&child.signalCode===null){const exit=once(child,'exit');child.kill();await exit;}
    await temp.cleanup();
    model.closeAllConnections();await new Promise<void>(resolve=>model.close(()=>resolve()));restoreEnvironment();
  });
  writeFileSync(path.join(home,'config.json'),JSON.stringify({...DEFAULT_CONFIG,compatibilityWake:'strict'}),'utf8');
  const paths=[directory];
  if(process.platform==='win32')for(const script of ['(Get-Location).Path','$fs=New-Object -ComObject Scripting.FileSystemObject; $fs.GetFolder((Get-Location).Path).ShortPath'])paths.push((await execute(windowsPowerShell(),['-NoProfile','-NonInteractive','-Command',script],{cwd:directory,windowsHide:true})).stdout.trim());
  writeFileSync(path.join(claudehome,'.claude.json'),JSON.stringify({hasCompletedOnboarding:true,customApiKeyResponses:{approved:[env.ANTHROPIC_API_KEY!.slice(-20)],rejected:[]},projects:Object.fromEntries(paths.map(p=>[p.replaceAll('\\','/'),{hasTrustDialogAccepted:true}]))}),'utf8');
  writeFileSync(path.join(claudehome,'settings.json'),JSON.stringify({crossSessionInbound:'accept',autoUpdatesChannel:'stable',env:{ANTHROPIC_API_KEY:env.ANTHROPIC_API_KEY,ANTHROPIC_BASE_URL:env.ANTHROPIC_BASE_URL,CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1'}}),'utf8');
  await setupPublish();
                                 
  for(const plugin of [path.join(home,'marketplace','plugins','claude'),...installedCacheDirectories(claudehome,'claude')]){
    const file=path.join(plugin,'hooks','register.js');
    writeFileSync(file,probeNativeNotices(readFileSync(file,'utf8'),nativeEvents),'utf8');
  }
  const nativeCli=resolveCli('claude');await execute(nativeCli.command,[...nativeCli.prefix,'plugin','validate',path.join(home,'marketplace','plugins','claude')],{windowsHide:true,timeout:15000});
  const newTarget=async()=>{
    const child=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{windowsHide:true,stdio:'ignore'});await once(child,'spawn');targets.push(child);const native=await processIdentity(child.pid!);
    const peer=await brokerCall('register',{host:'codex',sessionId:'fixture',cwd:directory,name:'隔离接收进程',kind:'root',permission:'prompting',endpoint:{mode:'embedded',address:'',epoch:'fixture:'+native.startedAt,pid:child.pid,procStart:native.startedAt}});targetId=peer.id;return child;
  };
  await newTarget();
  await pairLegacyClaude(home,directory,{...DEFAULT_CONFIG,compatibilityWake:'strict',claudeHome:claudehome},targetId);
  await brokerCall('send',{actor:{host:'codex',sessionId:'fixture',callId:'boot'},message:'请向搭档发一条持久队列探针，然后结束这一轮。'});
  const main=()=>requests.filter(r=>r.body.tools?.some((tool:any)=>tool.name==='ListAgents'));
  function observedEvents():any[]{try{return JSON.parse(readFileSync(nativeEvents,'utf8'));}catch{return[];}}
  function mainRequestsSummary(){return requests.map(r=>{const messages=JSON.stringify(r.body.messages),noticeAt=messages.lastIndexOf('system_notice source=runtime_monitor');return{at:r.at,main:r.body.tools?.some((tool:any)=>tool.name==='ListAgents'),notices:['nxbridge notice pickup ','system_notice source=runtime_monitor','system_notice source=pair_anomaly','NATIVE_CODEX_TERMINAL_FAILED','native host exited','reachable again','broker endpoint unavailable','bridge restarted'].filter(text=>messages.includes(text)),...(noticeAt>=0?{latestNotice:messages.slice(noticeAt,noticeAt+650)}:{})};});}
  const waitUntil=async(predicate:()=>Promise<boolean>|boolean,timeout=30000)=>{const end=Date.now()+timeout;while(!await predicate()){assert.ok(Date.now()<end,phase+'：原生运行时通知等待超时');await delay(150);}};
                                                
                                             
  const advanceNoticeCadence=()=>{
    const db=new DatabaseSync(path.join(home,'nxbridge.db'));try{
      db.exec('PRAGMA busy_timeout=300');db.exec('BEGIN IMMEDIATE');
      const key='healthNoticeAdmission:'+sourceId,row=db.prepare('SELECT value FROM settings WHERE key=?').get(key);assert.ok(row,'原生通知尚未实际准入');
      const value=JSON.parse(row.value as string),at=Date.now()-600000;value.lastAt=at;
      for(const kind of Object.values(value.kinds) as {at:number}[])kind.at=at;
      db.prepare('UPDATE settings SET value=?,updated_at=? WHERE key=?').run(JSON.stringify(value),Date.now(),key);db.exec('COMMIT');
    }finally{if(db.isTransaction)db.exec('ROLLBACK');db.close();}
  };
  await waitUntil(async()=>{const status=await brokerCall('status');const c=status.peers.find((p:any)=>p.id===status.pair?.claude);sourceId=c?.id??'';pairId=status.pair?.id??'';return c?.state==='ready'&&status.messages.some((m:any)=>m.from===sourceId&&m.state==='accepted');},60000);
  mark('等待原生初始化和首轮完全结束');
  await waitUntil(async()=>new Set(observedEvents().filter(e=>e.event==='clock-tick').map(e=>e.clock)).size===2&&observedEvents().some(e=>e.event==='turn.complete'&&!e.agentId)&&(await brokerCall('status')).peers.find((p:any)=>p.id===sourceId)?.state==='ready');
  assert.notEqual((await brokerCall('status')).peers.find((p:any)=>p.id===sourceId)?.permission,'unknown','原生 classic 事件必须提供已观察的权限模式，不从测试配置推断');
  assert.ok(main().some(r=>JSON.stringify(r.body.messages).includes('nxbridge tip (auto length check, not judgment')),'真实 Claude 请求未收到长报告提示');
  const preparedSince=(text:string,at:number)=>observedEvents().some(e=>e.event==='control-result'&&e.operation==='monitor-prepare'&&e.at>=at&&e.exitCode===0&&e.result?.includes(text));
  const naturalTurn=async(text:string)=>{
    const before=main().length,turns=observedEvents().filter(e=>e.event==='turn.complete'&&!e.agentId).length;
    const status=await brokerCall('status'),source=status.peers.find((p:any)=>p.id===targetId),receiver=status.peers.find((p:any)=>p.id===sourceId),endpoint=status.endpoints.find((e:any)=>e.peerId===sourceId&&e.active),adapter=new ClaudeAdapter(DEFAULT_CONFIG),id='natural-'+Date.now();
    try{await adapter.deliver(receiver,endpoint,{...status.messages[0],id,from:source.id,to:receiver.id,body:text,notice:false},source);}finally{adapter.close();}
    await waitUntil(async()=>main().length>before&&observedEvents().filter(e=>e.event==='turn.complete'&&!e.agentId).length>turns&&(await brokerCall('status')).peers.find((p:any)=>p.id===sourceId)?.state==='ready');
    return main().slice(before);
  };
                                                                   
  const database=(write:boolean)=>{const db=new DatabaseSync(path.join(home,'nxbridge.db'),write?{}:{readOnly:true});db.exec('PRAGMA busy_timeout=300');return db;};
  const setting=(key:string)=>{const db=database(false);try{const row=db.prepare('SELECT value FROM settings WHERE key=?').get(key);return row?JSON.parse(row.value as string):undefined;}finally{db.close();}};
  const rewind=(work:(db:DatabaseSync,update:(key:string,change:(value:any)=>any)=>void)=>void)=>{
    const db=database(true);try{
      db.exec('BEGIN IMMEDIATE');
      work(db,(key,change)=>{const row=db.prepare('SELECT value FROM settings WHERE key=?').get(key);assert.ok(row,key+' 尚未出现');db.prepare('UPDATE settings SET value=?,updated_at=? WHERE key=?').run(JSON.stringify(change(JSON.parse(row.value as string))),Date.now(),key);});
      db.exec('COMMIT');
    }finally{if(db.isTransaction)db.exec('ROLLBACK');db.close();}
  };
  const monitorsSince=(at:number)=>observedEvents().filter(e=>e.event==='monitor'&&e.at>=at);
  const controlsSince=(operations:string[],at:number)=>observedEvents().filter(e=>e.event==='control-result'&&operations.includes(e.operation)&&e.at>=at);
  mark('搭档这一轮失败：真实 Claude 经原生队列被叫醒一次');
  const failedTurn='NATIVE_CODEX_TERMINAL_FAILED',failedAt=Date.now(),wakeBaseline=main().length,messagesBefore=(await brokerCall('status')).messages.length;
  const fixture=targets.at(-1)!,fixtureIdentity=await processIdentity(fixture.pid!);
                                                          
  rewind(db=>{const at=Date.now()-121000;assert.ok(db.prepare("UPDATE messages SET created_at=?,updated_at=? WHERE sender=? AND recipient=? AND state='accepted'").run(at,at,sourceId,targetId).changes>=1,'缺少仍在等待的消息');});
  await brokerCall('register',{host:'codex',sessionId:'fixture',cwd:directory,name:'隔离接收进程',kind:'root',permission:'prompting',state:'ready',endpoint:{mode:'embedded',address:'',epoch:'fixture:'+fixtureIdentity.startedAt,pid:fixture.pid,procStart:fixtureIdentity.startedAt,nativeHealth:{turnId:failedTurn,state:'failed',completion:'failed',lastProgressAt:failedAt,lastEventAt:failedAt,tools:[],error:'exceeded retry limit, last status: 429 Too Many Requests',source:'native-hook'}}});
  await waitUntil(()=>{const issues=setting('pairIssues:'+targetId)?.issues??[];return issues.some((issue:any)=>issue.kind==='turn_failed'&&String(issue.factKey).includes(failedTurn))&&issues.some((issue:any)=>issue.kind==='delivery_accepted');});
  assert.equal(main().length,wakeBaseline,'刚失败先等 30 秒：排队的消息或新一轮可能马上接上');
  mark('告知推进到第 3 档，等插件留下');
  const heldAt=Date.now();
  rewind((_db,update)=>update('pairIssues:'+targetId,value=>({...value,issues:value.issues.map((issue:any)=>issue.kind==='delivery_accepted'?{...issue,firstSeenAt:Date.now()-181000}:issue)})));
  await waitUntil(()=>monitorsSince(heldAt).some(e=>(e.health?.notices??[]).some((n:any)=>n.tier===3&&n.wake===false)));await delay(300);
  assert.equal(controlsSince(['monitor-enqueue','monitor-prepare'],heldAt).length,0,'告知留在插件里：不领取、不唤醒');assert.equal(main().length,wakeBaseline);
  mark('失败已观测，推进 30 秒起点');
  rewind((_db,update)=>update('pairIssues:'+targetId,value=>({...value,issues:value.issues.map((issue:any)=>issue.kind==='turn_failed'?{...issue,firstSeenAt:Date.now()-31000}:issue)})));
  await waitUntil(()=>main().slice(wakeBaseline).some(r=>JSON.stringify(r.body.messages).includes(failedTurn)),60000);
  mark('叫醒轮次已发出，等待结束');
  await waitUntil(async()=>observedEvents().some(e=>e.event==='turn.complete'&&!e.agentId&&e.at>=failedAt)&&(await brokerCall('status')).peers.find((p:any)=>p.id===sourceId)?.state==='ready');
  const woken=JSON.stringify(main().slice(wakeBaseline).map(r=>r.body.messages));
  assert.match(woken,/system_notice source=pair_anomaly/);assert.match(woken,/will not resume this work by itself/);assert.match(woken,/native error: exceeded retry limit, last status: 429/);
  assert.equal(controlsSince(['monitor-enqueue'],failedAt).filter(e=>e.exitCode===0).length,1,'终态失败经原生唤醒队列叫醒一次');
                                                                
  const first=main().slice(wakeBaseline).find(r=>JSON.stringify(r.body.messages).includes(failedTurn));
  assert.match(JSON.stringify(first?.body.messages),/remain accepted for 2\+ min/,'告知随同一次唤醒附上');
  const ledger=(setting('turnHealthDelivery:'+pairId+':'+sourceId)?.notices??[]).filter((n:any)=>/:(?:turn_failed|delivery_accepted)$/.test(n.kind??''));
  assert.deepEqual(ledger.map((n:any)=>[n.kind.split(':').at(-1),n.state]).sort(),[['delivery_accepted','submitted'],['turn_failed','submitted']]);
  assert.equal((await brokerCall('status')).messages.length,messagesBefore,'失败事实不能伪造搭档消息');
  mark('等待被动故障通知');
                                          
  advanceNoticeCadence();
  const baseline=main().length,killedAt=Date.now(),targetExit=once(fixture,'exit');fixture.kill();await targetExit;
  const incidentKey='turnHealthDelivery:'+pairId+':'+sourceId,alert=()=>(setting(incidentKey)?.notices??[]).find((n:any)=>n.kind==='runtime'&&n.phase==='alert'&&n.state==='pending');
  await waitUntil(()=>setting(incidentKey)?.active===true&&!!alert());
  assert.equal(alert().tier,1,'刚出现、原因未知的不可达只进状态栏（第 1 档）');
  mark('第 1 档已出现，推进五分钟依赖起点');
                                                      
  const rewoundAt=Date.now();
  rewind((db,update)=>{const at=Date.now()-300001;update(incidentKey,value=>({...value,since:at}));assert.ok(db.prepare("UPDATE messages SET created_at=? WHERE sender=? AND recipient=? AND state='accepted'").run(at,sourceId,targetId).changes>=1,'缺少仍在等待的消息');});
  assert.equal(monitorsSince(killedAt).filter(e=>e.at<rewoundAt).some(e=>(e.health?.notices??[]).length>0),false,'第 1 档不进入模型通知');
  await waitUntil(()=>monitorsSince(rewoundAt).some(e=>(e.health?.notices??[]).some((n:any)=>n.tier===3&&n.wake===false)));await delay(500);
  mark('插件已收到第 3 档被动事实，开始正常下一轮');
  assert.equal(main().length,baseline,'被动事实不能启动模型轮次');
  assert.equal(controlsSince(['monitor-enqueue'],killedAt).length,0,'被动事实不得使用原生唤醒队列');
  assert.equal(preparedSince('native host exited',killedAt),false,'被动事实留到下一个安全边界才取正文');
  assert.ok((await naturalTurn('继续原来的隔离验收任务')).some(r=>JSON.stringify(r.body.messages).includes('native host exited')),'正常下一轮必须看到被动附入的故障事实');
  assert.ok(preparedSince('native host exited',killedAt),'正文在下一轮的提交边界取出');
  mark('停止原生 MCP 并冻结 broker 重启');
  const beforeCrash=await brokerCall('status'),bridge=beforeCrash.endpoints.find((e:any)=>e.peerId===sourceId&&e.active)?.bridge;
  assert.ok(bridge?.pid,'原生 Claude MCP 未登记自身 PID');assert.equal((await processIdentity(bridge.pid)).startedAt,bridge.procStart);
  await stopOwnedProcess({pid:bridge.pid,startedAt:bridge.procStart});await delay(300);
                                                  
                                                                        
  const holder=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{windowsHide:true,stdio:'ignore'});await once(holder,'spawn');targets.push(holder);
  const releaseStartup=await reserveBrokerStartup(home,await processIdentity(holder.pid!));
  const broker=await brokerCall('ping',{}, {start:false}),brokerKilledAt=Date.now(),offlineBaseline=main().length;
  try{
    mark('broker 离线期间保持被动');await stopOwnedProcess({pid:broker.pid,startedAt:broker.runtime.generation});
                                      
    await waitUntil(()=>monitorsSince(brokerKilledAt).some(e=>e.health!==undefined));await delay(500);
    assert.equal(main().length,offlineBaseline,'broker 离线监测不得启动模型');
    assert.equal(controlsSince(['monitor-enqueue','monitor-prepare'],brokerKilledAt).length,0,'离线不足两分钟不领取通知');
  }finally{await releaseStartup();}
  const restartReleasedAt=Date.now();
                                                  
  mark('等待 broker 重启后保持被动');
  await waitUntil(async()=>{try{await brokerCall('ping',{}, {start:false,timeout:1000});return true;}catch{return false;}},45000);
  mark('broker 已恢复，等待一次插件监测');const restartedAt=Date.now();await waitUntil(()=>monitorsSince(restartedAt).some(e=>e.health!==undefined),30000);await delay(500);
  assert.equal(main().length,offlineBaseline,'重启事实不得启动模型');
  assert.equal(controlsSince(['monitor-enqueue','monitor-prepare'],brokerKilledAt).length,0,'重启事实只在查询时可见（第 2 档），不推送');
  assert.ok((setting(incidentKey)?.notices??[]).some((n:any)=>n.kind==='bridge_restart'&&n.state==='pending'&&n.message.includes('bridge restarted')),'重启事实保留为查询时可见的第 2 档');
  const launchFile=path.join(home,'run','launch-broker.json'),launch=existsSync(launchFile)?JSON.parse(readFileSync(launchFile,'utf8')):undefined;
  t.diagnostic(JSON.stringify({brokerRestart:{noticeAfterReleaseMs:Date.now()-restartReleasedAt,launchAfterReleaseMs:launch?.spawnedAt?launch.spawnedAt-restartReleasedAt:null,clockTicks:observedEvents().filter(e=>e.event==='clock-tick'&&e.at>=restartReleasedAt).map(e=>({clock:e.clock,afterReleaseMs:e.at-restartReleasedAt}))}}));
  const status=await brokerCall('status'),outgoing=status.messages.filter((m:any)=>m.from===sourceId);assert.ok(outgoing.length);assert.ok(outgoing.every((m:any)=>m.state==='accepted'&&m.attempts===0));
  assert.equal(status.messages.some((m:any)=>/runtime monitor/.test(m.body)),false);assert.ok(main().every(r=>!(r.body.messages??[]).some((m:any)=>JSON.stringify(m).includes('claude.status()'))));
  assert.equal(main().some(r=>JSON.stringify(r.body.messages).includes('nxbridge notice pickup ')),false,'取件协议不能泄露到模型请求');
  assert.equal(controlsSince(['monitor-enqueue'],failedAt).length,1,'整个会话只有终态失败使用原生唤醒队列，且只用一次');
  assert.equal(['reachable again','bridge restarted','broker endpoint unavailable'].some(text=>preparedSince(text,0)),false,'恢复、重启和短暂离线事实不推送给模型');
  t.diagnostic('实际 Claude Mod：终态失败经原生队列叫醒一次；被动事实不启动轮次、下一轮附入可见；broker 离线与重启保持被动；信息通知自动模型轮次为 0，真实付费模型调用为 0。');
  completed=true;
});
