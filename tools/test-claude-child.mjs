                                           
import assert from 'node:assert/strict';
import http from 'node:http';
import {spawn} from 'node:child_process';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdtempSync,mkdirSync,readFileSync,writeFileSync,readdirSync,existsSync,lstatSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {randomUUID} from 'node:crypto';
const profileOnly=process.argv[4]==='--profile-only';
const stopProofOnly=process.argv[4]==='--stop-proof';
const migrationProof=process.argv[4]==='--migration-proof',legacyPublish=process.argv[5];
if(migrationProof)assert.ok(process.argv[3]&&legacyPublish,'迁移验收需要新旧两个发布入口');
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),base=path.join(root,'dist/');
const {resolveCli}=await import(base+'process/host-command.js');
const claude=resolveCli('claude'),codex=resolveCli('codex'),node=process.execPath;
const ownRegistry=process.env.NXBRIDGE_PROCESS_REGISTRY?undefined:mkdtempSync(path.join(tmpdir(),'nxbridge-test-processes-'));
process.env.NXBRIDGE_PROCESS_REGISTRY??=ownRegistry;process.env.NXBRIDGE_TEST_SCOPE??=randomUUID();
const fixture={directory:process.argv[2]},directory=mkdtempSync(fixture.directory+'/child-lifecycle-'),home=directory+'/home',bridge=home+'/.nxbridge',claudehome=home+'/.claude';
for(const dir of ['','.claude','.codex','.local/bin','.run'])mkdirSync(home+'/'+dir,{recursive:true,mode:0o700});
                                                                     
const env={NXBRIDGE_PROCESS_REGISTRY:process.env.NXBRIDGE_PROCESS_REGISTRY,NXBRIDGE_TEST_SCOPE:process.env.NXBRIDGE_TEST_SCOPE,HOME:home,PATH:[home+'/.local/bin',path.dirname(node),'/usr/local/bin','/usr/bin','/bin'].join(path.delimiter),LANG:'C.UTF-8',NXBRIDGE_HOME:bridge,CLAUDE_CONFIG_DIR:claudehome,CODEX_HOME:home+'/.codex',XDG_RUNTIME_DIR:home+'/.run',DISABLE_AUTOUPDATER:'1',DISABLE_TELEMETRY:'1',CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1'},requests=[],errors=[];let phase=1,hold=false,release,toolMode='reply';
Object.assign(process.env,env);
if(migrationProof){env.ENABLE_TOOL_SEARCH='false';process.env.ENABLE_TOOL_SEARCH='false';}
let scheduledId;
function toolInput(tool,body){
 if(tool==='SendMessage')return{to:'nxbridge:codex',message:'托管回复 '+phase};
 if(tool==='Bash')return{command:'printf approval > '+directory+'/must-not-exist',description:'隔离审批验收'};
 if(tool==='CronCreate'||tool==='CronDelete'){
  assert.ok(body.tools.some(entry=>entry.name===tool),'原生托管 Claude 未暴露 '+tool+'；请求工具：'+body.tools.map(entry=>entry.name).join(', '));
  return tool==='CronCreate'?{cron:'0 0 1 * *',prompt:'隔离验收不会触发的任务',recurring:false,durable:false}:{id:scheduledId};
 }
 return{questions:[{header:'接管验证',question:'选择一个验收项',options:[{label:'甲',description:'第一项'},{label:'乙',description:'第二项'}],multiSelect:false}]};
}
const model=http.createServer(async(req,res)=>{try{
 let raw='';for await(const c of req)raw+=c;if(!req.url?.includes('/v1/messages')||req.url.includes('count_tokens')){res.writeHead(200,{'content-type':'application/json'});res.end('{"input_tokens":1,"data":[]}');return;}
 const body=JSON.parse(raw);requests.push(body);
 const main=body.tools?.some(t=>t.name==='ListAgents'),results=(body.messages??[]).flatMap(m=>Array.isArray(m.content)?m.content:[]).filter(v=>v.type==='tool_result'),id='child-'+toolMode+'-'+phase;
 const result=results.find(r=>r.tool_use_id===id),tool=main&&!result?(toolMode==='reply'?'SendMessage':toolMode):undefined,input=toolInput(tool,body);
 if(toolMode==='CronCreate'&&result){
  scheduledId=/Scheduled (?:recurring job|one-shot task) ([\w-]+)/.exec(JSON.stringify(result.content))?.[1];
  assert.ok(scheduledId,'无法从原生 CronCreate 回执核验任务 ID：'+JSON.stringify(result.content));
 }
 if(hold&&main)await new Promise(r=>release=r);
 res.writeHead(200,{'content-type':'text/event-stream'});
 for(const e of [{type:'message_start',message:{id:'owned-'+requests.length,type:'message',role:'assistant',model:body.model,content:[],stop_reason:null,stop_sequence:null,usage:{input_tokens:1,output_tokens:1}}},{type:'content_block_start',index:0,content_block:tool?{type:'tool_use',id,name:tool,input:{}}:{type:'text',text:''}},{type:'content_block_delta',index:0,delta:tool?{type:'input_json_delta',partial_json:JSON.stringify(input)}:{type:'text_delta',text:'done'}},{type:'content_block_stop',index:0},{type:'message_delta',delta:{stop_reason:tool?'tool_use':'end_turn',stop_sequence:null},usage:{output_tokens:1}},{type:'message_stop'}])res.write('event: '+e.type+'\ndata: '+JSON.stringify(e)+'\n\n');res.end();
}catch(e){errors.push(String(e));res.end();}});await new Promise(r=>model.listen(0,'127.0.0.1',r));
const key='child-lifecycle-fake',url='http://127.0.0.1:'+model.address().port;env.ANTHROPIC_API_KEY=key;env.ANTHROPIC_BASE_URL=url;Object.assign(process.env,env);writeFileSync(claudehome+'/settings.json',JSON.stringify({crossSessionInbound:'accept',...(profileOnly?{skipDangerousModePermissionPrompt:true}:{}),env:{ANTHROPIC_API_KEY:key,ANTHROPIC_BASE_URL:url,CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1'}}));writeFileSync(claudehome+'/.claude.json',JSON.stringify({hasCompletedOnboarding:true,customApiKeyResponses:{approved:[key.slice(-20)],rejected:[]},projects:{[directory]:{hasTrustDialogAccepted:true}}}));
const {preparePlugins}=await import(base+'install/setup.js'),{brokerCall}=await import(base+'ipc/ipc.js'),{processIdentity,liveProcess}=await import(base+'process/process.js'),{saveClaudeBackground}=await import(base+'hosts/claude/claude-capability.js'),{claudeChild}=await import(base+'hosts/claude/claude-child.js');
const {persistentSocketDirectory}=await import(base+'process/posix/persistent-socket-directory.js');
function inboxPath(record){
 const address=record.registration.endpoint.address,name=path.basename(address);
 assert.match(name,/^ci-[a-f0-9]{16}\.sock$/);
 assert.equal(path.dirname(address),persistentSocketDirectory(bridge,name));
 assert.ok(lstatSync(address).isSocket());return address;
}
const {registerTestDirectory,cleanupTestProcesses}=await import(base+'process/test-processes.js'),{stopOwnedProcess}=await import(base+'process/process-stop.js');registerTestDirectory(directory);
const plugin=(await preparePlugins(bridge))+'/plugins/claude',wrapper=directory+'/wrapper.mjs',nativeErrors=directory+'/native-stderr.log',nativeOutput=directory+'/native-stdout.log',nativeDebug=directory+'/native-debug.log';writeFileSync(wrapper,`import{spawn}from'node:child_process';import{appendFileSync}from'node:fs';const args=process.argv.slice(2);if(args[0]==='agents'||args.includes('--bg'))process.exit(2);const launch=args[0]==='plugin'||args.includes('--version')?args:[...args,'--plugin-dir',${JSON.stringify(plugin)},'--debug-file',${JSON.stringify(nativeDebug)}];const c=spawn(${JSON.stringify(claude.command)},[...${JSON.stringify(claude.prefix)},...launch],{stdio:args.includes('-p')?['inherit','pipe','pipe']:'inherit',env:process.env});c.stdout?.on('data',data=>{appendFileSync(${JSON.stringify(nativeOutput)},data);process.stdout.write(data);});c.stderr?.on('data',data=>appendFileSync(${JSON.stringify(nativeErrors)},data));for(const s of ['SIGTERM','SIGINT'])process.on(s,()=>c.kill(s));c.once('exit',(code)=>process.exit(code??1));`);
const stopProofFile=directory+'/native-stop-proof.json',legacyInbox=home+'/.run/legacy.sock',legacyControl=home+'/.run/worker.sock',legacyLaunch=directory+'/legacy-launch';
if(stopProofOnly||migrationProof){
 const proofPlugin=directory+'/stop-proof',mcpConfig=directory+(migrationProof?'/.mcp.json':'/stop-proof-mcp.json');
 mkdirSync(proofPlugin+'/.claude-plugin',{recursive:true});mkdirSync(proofPlugin+'/hooks');
 writeFileSync(proofPlugin+'/.claude-plugin/plugin.json',JSON.stringify({name:'stop-proof',version:'1.0.0'}));
 writeFileSync(proofPlugin+'/hooks/hooks.json',JSON.stringify({modules:['./proof.js']}));
                                                    
 writeFileSync(proofPlugin+'/hooks/proof.js',`export function register(on){
  on('classic.Stop',async($,e,next)=>{
   if(!e.agent_id&&!e.agent_transcript_path){
    const count=value=>Array.isArray(value)?value.length:null;
    const evidence={sessionId:e.session_id,tasks:count(e.background_tasks),crons:count(e.session_crons)};
    await $.process.run([${JSON.stringify(node)},'-e','require("node:fs").writeFileSync(process.argv[1],process.argv[2],"utf8")',${JSON.stringify(stopProofFile)},JSON.stringify(evidence)]);
   }
   return next(e);
  });
 }`);
 writeFileSync(mcpConfig,JSON.stringify({mcpServers:{'stop-proof':{command:node,args:[base+'mcp-entry.js','mcp','--host','claude']}}}));
 let source=readFileSync(wrapper,'utf8').replace("'--debug-file',","'--plugin-dir',"+JSON.stringify(proofPlugin)+",'--mcp-config',"+JSON.stringify(mcpConfig)+",'--debug-file',");
 if(migrationProof)source=source.replace("{appendFileSync}","{appendFileSync,existsSync,writeFileSync}")
  .replace("const launch=args[0]",`const i=args.indexOf('--messaging-socket-path');if(args.includes('-p')&&!existsSync(${JSON.stringify(legacyLaunch)})){if(i<0)process.exit(72);writeFileSync(${JSON.stringify(legacyLaunch)},args[i+1]);args[i+1]=${JSON.stringify(legacyInbox)};}const launch=args[0]`);
 writeFileSync(wrapper,source);
}
if(profileOnly){
 const proofPlugin=directory+'/profile-proof';mkdirSync(proofPlugin+'/.claude-plugin',{recursive:true});mkdirSync(proofPlugin+'/hooks');
 writeFileSync(proofPlugin+'/.claude-plugin/plugin.json',JSON.stringify({name:'profile-proof',version:'1.0.0'}));writeFileSync(proofPlugin+'/hooks/hooks.json',JSON.stringify({modules:['./proof.js']}));
                                      
 writeFileSync(proofPlugin+'/hooks/proof.js',`export function register(on){
 on('session.append',async($,e,next)=>{const result=await next(e);if(e.door==='command'&&JSON.stringify(result.message).includes('<local-command-stdout>'))try{
  const evidence={result,user:await $.settings.read({source:'user'}),merged:await $.settings.read({key:'effortLevel'})};
  await $.process.run([${JSON.stringify(node)},'-e','require("node:fs").writeFileSync(process.argv[1],process.argv[2],"utf8")',${JSON.stringify(directory+'/effort-command-proof.json')},JSON.stringify(evidence)]);
 }catch(error){await $.ui.status('NXBRIDGE_PROFILE_READ_PROOF_ERROR '+String(error));}return result;});}
 `);
 const source=readFileSync(wrapper,'utf8').replace("args[0]==='agents'","args.includes('agents')").replace("args[0]==='plugin'","args.includes('plugin')").replace("const args=process.argv.slice(2);","const args=process.argv.slice(2);if(!args.includes('--version')&&!args.includes('plugin')&&!args.includes('agents')){const i=args.indexOf('--settings');if(args.filter(v=>v==='--settings').length!==1||JSON.parse(args[i+1]??'{}').env?.NXBRIDGE_PROFILE_SENTINEL!=='preserved')process.exit(71);}").replace("'--debug-file',","'--plugin-dir',"+JSON.stringify(proofPlugin)+",'--debug-file',");writeFileSync(wrapper,source);
}
const {writeConfiguration}=await import(base+'config/configuration.js'),{loadConfig}=await import(base+'config/load.js');writeConfiguration(bridge,[{key:'hosts.claude.command',value:[node,wrapper,...(profileOnly?['--settings',JSON.stringify({env:{NXBRIDGE_PROFILE_SENTINEL:'preserved'}})]:[])]},{key:'hosts.claude.home',value:claudehome},{key:'hosts.codex.home',value:home+'/.codex'},{key:'hosts.codex.binary',value:codex.prefix[0]??codex.command},...(profileOnly?[{key:'partners.claude.model',value:'sonnet'},{key:'partners.claude.effort',value:'low'}]:[])],'setup');const config=loadConfig(bridge);saveClaudeBackground(bridge,config,{available:false,checkedAt:Date.now()});
const actor={host:'codex',sessionId:'owned-sender',callId:'phase-1'},report={directory,paidCalls:0,steps:[]};let foreground,foregroundTty=false,peerId,sessionId,foregroundOutput='',foregroundErrors='';
const status=()=>brokerCall('status');const wait=async f=>{const end=Date.now()+40000;while(!await f()){if(Date.now()>end){const snapshot=await status();throw Error('托管生命周期等待超时 '+JSON.stringify({phase,toolMode,errors,count:requests.length,peers:snapshot.peers.map(p=>({id:p.id,state:p.state,owner:p.owner,sessionId:p.sessionId})),endpoints:snapshot.endpoints.map(e=>({pid:e.pid,active:e.active,questionSupport:e.questionSupport})),record:peerId?claudeChild(bridge,peerId):undefined,foregroundOutput:foregroundOutput.slice(-2000),foregroundErrors:foregroundErrors.slice(-1000),log:existsSync(bridge+'/run/claude-child.log')?readFileSync(bridge+'/run/claude-child.log','utf8').slice(-1000):''}));}await new Promise(r=>setTimeout(r,100));}};
async function send(){actor.callId='phase-'+phase;await brokerCall('send',{actor,message:'托管来信 '+phase});}
                                                 
async function ready(){await wait(async()=>{const s=await status(),p=s.peers.find(p=>p.id===s.pair?.claude);peerId=p?.id;sessionId=p?.sessionId;const reply=s.messages.find(m=>m.body==='托管回复 '+phase),health=peerId?claudeChild(bridge,peerId)?.health:undefined;return p?.state==='ready'&&reply&&health?.state==='ready'&&health.lastEventAt>=reply.createdAt&&s.endpoints.some(e=>e.peerId===peerId&&e.active&&e.questionSupport);});}
async function nativeStopProof(record){
 const snapshot=()=>status().then(s=>({endpoint:s.endpoints.find(e=>e.peerId===peerId&&e.active&&e.pid===record.native.pid),reply:s.messages.find(m=>m.body==='托管回复 '+phase)}));
 let endpoint,reply;
 for(let i=0;i<50;i++){
  ({endpoint,reply}=await snapshot());
  if(existsSync(stopProofFile)&&endpoint?.nativeBackground?.tasks!==undefined&&endpoint.nativeBackground.crons!==undefined)break;
  await new Promise(r=>setTimeout(r,100));
 }
 const native=existsSync(stopProofFile)?JSON.parse(readFileSync(stopProofFile,'utf8')):null;
 const init=readFileSync(nativeOutput,'utf8').split('\n').flatMap(line=>{try{const value=JSON.parse(line);return value.type==='system'&&value.subtype==='init'?value.mcp_servers??[]:[];}catch{return[];}});
 report.stopProof={native,endpoint:endpoint?.nativeBackground,nativeHealth:endpoint?.nativeHealth,mcp:init.find(item=>item.name==='stop-proof')};
 assert.equal(report.stopProof.mcp?.status,'connected','真实后台没有连接验收 MCP');
 assert.deepEqual(native,{sessionId,tasks:0,crons:0},'真实托管 Claude Stop 未提供完整空闲计数');
 assert.equal(endpoint.nativeBackground.tasks,0);
 assert.equal(endpoint.nativeBackground.crons,0);
 assert.ok(endpoint.nativeBackground.observedAt>=reply.createdAt,'计数来自首轮完成前的旧状态');
 report.steps.push('真实托管 Claude 首轮结束后，原生 Stop 与协调器均观察到零后台任务、零定时任务');
}
async function foregroundResume(){foregroundTty=true;foregroundOutput='';foregroundErrors='';const quote=v=>"'"+v.replaceAll("'","'\\''")+"'",entry=process.argv[3]?bridge+'/bin/nxbridge.cjs':base+'cli.js';foreground=spawn('/usr/bin/script',['-q','-e','-c','stty rows 35 cols 120; exec '+quote(process.execPath)+' '+quote(entry)+' open claude','/dev/null'],{cwd:directory,env:{...env,TERM:'xterm-256color'},stdio:['pipe','pipe','pipe']});foreground.stdout.on('data',chunk=>{const value=chunk.toString();foregroundOutput=(foregroundOutput+value).slice(-5000);if(value.includes('\x1b[6n'))foreground.stdin.write('\x1b[1;1R');if(value.includes('\x1b[c'))foreground.stdin.write('\x1b[?1;2c');});foreground.stderr.on('data',chunk=>foregroundErrors=(foregroundErrors+chunk.toString()).slice(-2000));await wait(async()=>claudeChild(bridge,peerId)?.phase==='user'&&(await status()).endpoints.some(e=>e.peerId===peerId&&e.active&&e.pid===claudeChild(bridge,peerId)?.foreground?.pid&&e.questionSupport===true));}
async function endForeground(){const p=foreground,closed=new Promise(r=>p.once('close',r));if(foregroundTty)p.stdin.write('/exit\r');else p.stdin.end();let timeout;try{await Promise.race([closed,new Promise((_,reject)=>timeout=setTimeout(()=>reject(Error('前台退出超时')),12000))]);}finally{clearTimeout(timeout);}foreground=undefined;await wait(async()=>{const s=await status(),p=s.peers.find(p=>p.id===peerId);return p.owner==='nxbridge'&&p.state==='offline'&&!s.pair.paused;});assert.equal(existsSync(directory+'/must-not-exist'),false,'接管和退出不能替用户批准或重做原工具');if(foregroundTty)report.steps.push('实际 PTY 中运行 nxbridge open claude，经配置包装器进入原生 TUI，并由 /exit 归还所有权');}
async function failureFacts(){
 if(existsSync(directory+'/effort-command-proof.json'))report.nativeEffortEvidence=JSON.parse(readFileSync(directory+'/effort-command-proof.json','utf8'));
 report.brokerLog=existsSync(bridge+'/run/broker.log')?readFileSync(bridge+'/run/broker.log','utf8').slice(-6000):'';
 const {Store}=await import(base+'storage/store.js'),store=new Store(bridge+'/nxbridge.db');
 try{report.diagnostics=store.events(0,100000).filter(event=>/mod\.|question\.|context\.|claude\.|message\./.test(event.type)).slice(-80);report.modDiagnostic=peerId?store.setting('modReceiptDiagnostic:'+peerId):undefined;if(migrationProof&&peerId)report.migrationState=store.setting('transportMigration:'+peerId);}finally{store.close();}
 report.nativeErrors=existsSync(nativeErrors)?readFileSync(nativeErrors,'utf8').slice(-4000):'';
 report.nativeInit=existsSync(nativeOutput)?readFileSync(nativeOutput,'utf8').split('\n').flatMap(line=>{try{const value=JSON.parse(line);return value.type==='system'&&value.subtype==='init'?[{plugins:value.plugins,errors:value.errors,mcpServers:value.mcp_servers}]:[];}catch{return[];}}):[];
 report.toolResults=requests.flatMap(request=>(request.messages??[]).flatMap(message=>Array.isArray(message.content)?message.content:[]).filter(value=>value.type==='tool_result'));
 report.nativeDebug=existsSync(nativeDebug)?readFileSync(nativeDebug,'utf8').split('\n').filter(line=>/module|\bmod\b|error|hooks/i.test(line)).slice(-50).join('\n'):'';
}
try{
 if(process.argv[3]){const {stdout}=await promisify(execFile)(node,[migrationProof?legacyPublish:process.argv[3],'setup','--claude-command',node,'--claude-command-arg',wrapper,...(profileOnly?['--claude-command-arg','--settings','--claude-command-arg',JSON.stringify({env:{NXBRIDGE_PROFILE_SENTINEL:'preserved'}})]:[]),'--claude-home',claudehome,'--codex-home',home+'/.codex'],{env,cwd:directory,timeout:80000,maxBuffer:8*1024*1024});const installed=JSON.parse(stdout);assert.equal(installed.installed,true);report.buildId=installed.runtime.buildId;report.steps.push('共享 ZIP 独立安装并通过实际发布入口启动托管后端');}
 const native=await processIdentity(process.pid),source=await brokerCall('register',{host:'codex',sessionId:actor.sessionId,cwd:directory,name:'验收发送方',kind:'root',permission:'prompting',endpoint:{mode:'embedded',address:'',epoch:'child-lifecycle',pid:native.pid,procStart:native.startedAt}});
 if(profileOnly){
  const selection=await brokerCall('pair.code',{actor});await brokerCall('pair.selection.commit',{actor,selection_id:selection.selection_id,action:'create'});
  report.steps.push('经真实 broker create 逐字段采用默认模型与推理强度，并在首份任务前核验');
 }else{
                                   
  const {Store}=await import(base+'storage/store.js'),{ensureClaudeChild,claudeChildFile}=await import(base+'hosts/claude/claude-child.js'),store=new Store(bridge+'/nxbridge.db');
  try{
   const legacy=store.placeholder('claude',directory);
   const legacyControlIntent=migrationProof?()=>{
                                               
    const file=claudeChildFile(bridge,legacy.id),intent=JSON.parse(readFileSync(file,'utf8'));
    assert.equal(intent.phase,'starting');assert.equal(intent.worker,undefined);
    writeFileSync(file,JSON.stringify({...intent,address:legacyControl}));
   }:undefined;
   const child=await ensureClaudeChild(bridge,config,legacy,undefined,undefined,legacyControlIntent,migrationProof?{model:'sonnet',effort:'low'}:undefined);
   store.updatePeer(legacy.id,{sessionId:child.sessionId});store.adopt(legacy.id,child.registration);store.pair(legacy.id,source.id);
  }finally{store.close();}
 }
 if(profileOnly){await wait(async()=>(await brokerCall('partner.profile',{actor,action:'get'})).status==='applied');const initial=await brokerCall('partner.profile',{actor,action:'get'});assert.equal(initial.requested.model,'sonnet');assert.equal(initial.effective.effort,'low');report.initialProfile=initial.effective;const changed=await brokerCall('partner.profile',{actor,action:'set',effort:'medium'});assert.equal(changed.status,'applied',JSON.stringify(changed));assert.equal(changed.effective.effort,'medium');assert.equal(requests.length,0);report.steps.push('首份任务前回读启动别名与推理强度，并在无 turn 快照时切换设置，未发模型请求');}
 await send();await ready();let record=claudeChild(bridge,peerId);assert.equal(record.phase,'active');const initialInbox=migrationProof?record.registration.endpoint.address:inboxPath(record);assert.notEqual(record.worker.pid,record.native.pid);assert.equal(record.native.parentPid,record.wrapper.pid);const reference=readdirSync(bridge+'/run').find(n=>n.startsWith('runtime-'+record.worker.pid+'-'));assert.ok(reference);if(report.buildId)assert.equal(JSON.parse(readFileSync(bridge+'/run/'+reference,'utf8')).buildId,report.buildId);report.sessionId=sessionId;report.pairId=(await status()).pair.id;report.steps.push('包装器拒绝 native bg，托管 worker 发布 inbox 并完成真实模型→Mod 回信；工作进程登记运行时引用');
 if(migrationProof){
  assert.equal(initialInbox,legacyInbox);assert.ok(lstatSync(initialInbox).isSocket());assert.equal(record.idleStop,undefined,'迁移必须覆盖旧版 worker 的结束接口');
  assert.equal(record.address,legacyControl);assert.ok(lstatSync(legacyControl).isSocket());
  await nativeStopProof(record);
  const old=record,initial=await status(),initialPeer=initial.peers.find(p=>p.id===peerId);
  const originalRequest=requests.find(r=>r.tools?.some(t=>t.name==='ListAgents'));
  assert.ok(originalRequest?.model);assert.equal(originalRequest.output_config?.effort,'low');
  const initialEndpoint=initial.endpoints.find(e=>e.peerId===peerId&&e.active);
  const projectRoot=path.join(claudehome,'projects');
  const transcript=initialEndpoint.nativeSession?.transcriptPath??readdirSync(projectRoot,{withFileTypes:true})
    .filter(project=>project.isDirectory()).map(project=>path.join(projectRoot,project.name,sessionId+'.jsonl')).find(file=>existsSync(file));
  assert.ok(transcript&&existsSync(transcript),'真实历史路径必须可读取');
  const history=readFileSync(transcript,'utf8'),recoveryKey='managedRecovery:'+peerId;
  const {Store}=await import(base+'storage/store.js'),fact=key=>{const store=new Store(bridge+'/nxbridge.db');try{return store.setting(key);}finally{store.close();}};
  const crashBefore=fact(recoveryKey)?.attempts??[],migration=()=>fact('transportMigration:'+peerId);
  const oldEndpoint=async()=>(await status()).endpoints.find(e=>e.peerId===peerId&&e.active&&e.pid===old.native.pid);
  phase++;toolMode='CronCreate';await send();await wait(async()=>{assert.deepEqual(errors,[]);return !!scheduledId&&(await oldEndpoint())?.nativeBackground?.crons===1;});
  report.steps.push('真实托管 Claude 调用 CronCreate，原生 Stop 如实发布一个会话定时任务');
  phase++;toolMode='reply';hold=true;await send();await wait(async()=>typeof release==='function'&&(await status()).peers.find(p=>p.id===peerId)?.state==='busy');
  const originalBusy=await status(),newEntry=process.argv[3],ping=await brokerCall('ping');
  const {stdout}=await promisify(execFile)(node,[newEntry,'update','--from',path.dirname(newEntry)],{env,cwd:directory,timeout:80000,maxBuffer:8*1024*1024});
  const upgraded=JSON.parse(stdout);assert.equal(upgraded.installed,true);assert.notEqual(upgraded.runtime.buildId,report.buildId);assert.equal(upgraded.runtime.previous?.buildId,report.buildId);assert.notEqual((await brokerCall('ping')).pid,ping.pid);
  assert.equal((await status()).pair.id,originalBusy.pair.id);assert.ok(liveProcess(old.worker.pid)&&liveProcess(old.native.pid));
  await wait(()=>migration()?.code==='session_busy');assert.equal(migration().attempts,0);report.steps.push('从真实旧版升级，忙碌轮次和原 worker/native 保持运行，未消费迁移额度');
  hold=false;release();release=undefined;await ready();
  await wait(()=>migration()?.code==='claude_inbox_scheduled_work');
  assert.equal(migration().attempts,0);assert.ok(liveProcess(old.worker.pid)&&liveProcess(old.native.pid));
  report.steps.push('原会话虽空闲仍有真实定时任务，升级保留进程及任务');
  phase++;toolMode='CronDelete';await send();
  await wait(async()=>{assert.deepEqual(errors,[]);const ep=await oldEndpoint();return ep?.nativeBackground?.crons===0&&ep.nativeBackground.tasks===0;});
  report.steps.push('原生 CronDelete 完成后发布零后台任务和零定时任务');
  const lastOld=await status(),messageFacts=s=>s.messages.filter(m=>m.to===peerId||m.from===peerId).map(m=>[m.id,m.body,m.attempts,m.state]);
  const priorFacts=messageFacts(lastOld),callsBefore=requests.length;
  await wait(()=>migration()?.state==='ready');
  const settled=migration(),current=claudeChild(bridge,peerId),after=await status(),afterPeer=after.peers.find(p=>p.id===peerId);
  assert.equal(settled.attempts,1);assert.deepEqual(settled.paths,[legacyInbox,legacyControl].sort());
  assert.equal(settled.source.address,initialInbox);assert.equal(settled.replacement.address,inboxPath(current));
  assert.notEqual(current.worker.pid,old.worker.pid);assert.notEqual(current.native.pid,old.native.pid);assert.equal(liveProcess(old.worker.pid),false);assert.equal(liveProcess(old.native.pid),false);
  assert.equal(path.dirname(current.address),persistentSocketDirectory(bridge,path.basename(current.address)));assert.ok(lstatSync(current.address).isSocket());
  assert.equal(current.sessionId,report.sessionId);assert.equal(after.pair.id,report.pairId);assert.equal(afterPeer.owner,'nxbridge');assert.equal(afterPeer.permission,initialPeer.permission);
  assert.deepEqual(current.modelProfile,{model:originalRequest.model,effort:originalRequest.output_config.effort});
  assert.deepEqual(fact(recoveryKey)?.attempts??[],crashBefore);assert.deepEqual(messageFacts(after),priorFacts);assert.equal(requests.length,callsBefore);
  assert.ok(readFileSync(transcript,'utf8').startsWith(history),'原会话历史前缀必须在');
  phase++;toolMode='reply';await send();await ready();assert.equal(sessionId,report.sessionId);
  assert.equal(requests.findLast(r=>r.tools?.some(t=>t.name==='ListAgents')).model,originalRequest.model);
  assert.equal(requests.findLast(r=>r.tools?.some(t=>t.name==='ListAgents')).output_config?.effort,'low');
  assert.equal(migration().attempts,1);
  report.migration={before:old.native.pid,after:current.native.pid,attempts:settled.attempts,crashAttempts:crashBefore.length};report.steps.push('原生空闲后只迁移一次，旧进程退出，原会话、配对、历史、权限和消息账本保持，下一条来信正常送达');
 }else if(stopProofOnly){
  await nativeStopProof(record);
 }else if(profileOnly){
  const settingsBefore=readFileSync(claudehome+'/settings.json','utf8'),main=()=>requests.filter(request=>request.tools?.some(tool=>tool.name==='ListAgents'));
  assert.equal(main()[0].model,report.initialProfile.model);assert.equal(main()[0].output_config?.effort,'medium');
  const choice={model:'opus',effort:'high'},changed=await brokerCall('partner.profile',{actor,action:'set',...choice}),chosen=changed.effective;
  assert.equal(changed.status,'applied',JSON.stringify(changed));assert.deepEqual(changed.requested,choice);assert.match(chosen.model,/^claude-opus-/);assert.equal(chosen.effort,choice.effort);assert.equal(changed.session_id,sessionId);
  phase++;await send();await ready();assert.equal(main().at(-1).model,chosen.model);assert.equal(main().at(-1).output_config?.effort,chosen.effort);assert.equal(readFileSync(claudehome+'/settings.json','utf8'),settingsBefore);report.steps.push('真实控制接口切换双字段并回读，下一轮请求采用新配置且不改用户设置');
  const beforeTakeover=requests.length;writeFileSync(claudehome+'/settings.json',JSON.stringify({...JSON.parse(settingsBefore),model:'sonnet',effortLevel:'low'}));
  try{await foregroundResume();await wait(async()=>{const endpoint=(await status()).endpoints.find(endpoint=>endpoint.peerId===peerId&&endpoint.active);return endpoint?.nativeProfile?.model===chosen.model&&endpoint.nativeProfile.effective?.effort===chosen.effort;});assert.equal(requests.length,beforeTakeover);
   foreground.stdin.write('/effort medium\r');await wait(()=>existsSync(directory+'/effort-command-proof.json'));report.nativeEffortEvidence=JSON.parse(readFileSync(directory+'/effort-command-proof.json','utf8'));await wait(async()=>{const endpoint=(await status()).endpoints.find(endpoint=>endpoint.peerId===peerId&&endpoint.active);return endpoint?.nativeProfile?.runtimeEffort==='medium';});assert.equal(requests.length,beforeTakeover,'用户 /effort 与只读观察不应请求模型');await endForeground();chosen.effort='medium';assert.equal(claudeChild(bridge,peerId).modelProfile.effort,'medium');assert.deepEqual((await brokerCall('partner.profile',{actor,action:'get'})).requested??{},{});}finally{writeFileSync(claudehome+'/settings.json',settingsBefore);}
  phase++;await send();await ready();assert.equal(main().at(-1).model,chosen.model);assert.equal(main().at(-1).output_config?.effort,chosen.effort);report.steps.push('自定义 --settings 保留；默认值漂移时前台继承原配置，/effort 修改后不发模型请求直接退出，归还后的下一轮保留实际强度');
  const oldNative=claudeChild(bridge,peerId).native;await stopOwnedProcess(oldNative,{force:true});await wait(()=>claudeChild(bridge,peerId)?.native?.pid!==oldNative.pid);phase++;await send();await ready();assert.equal(claudeChild(bridge,peerId).sessionId,sessionId);assert.equal(main().at(-1).model,chosen.model);assert.equal(main().at(-1).output_config?.effort,chosen.effort);report.steps.push('冷恢复通过启动参数保留会话基线，原会话和配对均不变');
  phase++;hold=true;await send();await wait(()=>typeof release==='function');const next={model:'claude-sonnet-4-6',effort:'medium'},queued=await brokerCall('partner.profile',{actor,action:'set',...next});assert.equal(queued.status,'pending');assert.equal(main().at(-1).model,chosen.model);hold=false;release();release=undefined;await ready();await wait(async()=>(await brokerCall('partner.profile',{actor,action:'get'})).status==='applied');phase++;await send();await ready();assert.equal(main().at(-1).model,next.model);assert.equal(main().at(-1).output_config?.effort,next.effort);report.steps.push('忙碌轮次保留旧设置，空闲后应用新选择');
  await assert.rejects(brokerCall('partner.profile',{actor,action:'set',effort:'max'}),{code:'profile_change_unsupported'});const reset=await brokerCall('partner.profile',{actor,action:'reset'});assert.deepEqual(reset.requested,{});assert.equal(reset.effective.model,next.model);assert.equal(readFileSync(claudehome+'/settings.json','utf8'),settingsBefore);assert.equal((await status()).pair.id,report.pairId);report.steps.push('不支持的强度明确拒绝，清除选择保持当前设置且用户默认值不变');
 }else{
 phase++;hold=true;await send();await wait(()=>typeof release==='function');await assert.rejects(brokerCall('open',{host:'claude'}),{code:'claude_turn_busy'});const ping=await brokerCall('ping');await stopOwnedProcess({pid:ping.pid,startedAt:ping.runtime.generation},{force:true});await wait(async()=>{try{return(await brokerCall('ping')).pid!==ping.pid;}catch{return false;}});assert.ok(liveProcess(record.worker.pid)&&liveProcess(record.native.pid));assert.equal((await status()).pair.id,report.pairId);report.steps.push('忙碌时 broker 崩溃重建，worker/native 存活且配对不变');hold=false;release();release=undefined;await ready();
 phase++;const old=record.native.pid;await stopOwnedProcess(record.native,{force:true});await wait(()=>claudeChild(bridge,peerId)?.native?.pid!==old);await send();await ready();record=claudeChild(bridge,peerId);assert.notEqual(record.native.pid,old);assert.equal(record.sessionId,sessionId);assert.notEqual(inboxPath(record),initialInbox);report.steps.push('原生进程退出后同 ID 冷恢复并公布新的持久 inbox，旧消息不重放');
 for(const tool of ['Bash','AskUserQuestion']){phase++;toolMode=tool;await send();if(tool==='AskUserQuestion'){await wait(async()=>(await status()).questions.some(q=>q.status==='pending'));const q=(await status()).questions.find(q=>q.status==='pending');await brokerCall('question.answer',{actor:{...actor,callId:'escalate-'+phase},input:{request_id:q.id,action:'needs_user',rationale:'必须由用户作出原生选择'}});}await wait(()=>claudeChild(bridge,peerId)?.human?.tool===tool);await wait(async()=>(await status()).peers.find(p=>p.id===peerId).state==='needsHuman');const before=requests.length;await foregroundResume();assert.equal(requests.length,before);await endForeground();assert.equal(requests.length,before);report.steps.push(tool+' 挂起→needsHuman→接管同 ID→SessionEnd 归还所有权，无新模型请求');toolMode='reply';phase++;await send();await ready();assert.equal(claudeChild(bridge,peerId).sessionId,sessionId);report.steps.push('前台退出后下一条来信通过托管 worker 恢复同一会话');}
 phase++;hold=true;await send();await wait(()=>typeof release==='function');writeConfiguration(bridge,[{key:'hosts.claude.command',value:['/usr/bin/false']}],'setup');try{const entry=process.argv[3]?bridge+'/bin/nxbridge.cjs':base+'cli.js';await assert.rejects(promisify(execFile)(node,[entry,'open','claude','--interrupt'],{env,cwd:directory,timeout:20000}));assert.equal(claudeChild(bridge,peerId).phase,'stopped');const returned=(await status()).peers.find(p=>p.id===peerId);assert.equal(returned.owner,'nxbridge');assert.equal(returned.sessionId,sessionId);}finally{hold=false;release?.();release=undefined;writeConfiguration(bridge,[{key:'hosts.claude.command',value:[node,wrapper]}],'setup');}report.steps.push('busy --interrupt 后，真实前台启动器失败，CLI 自动归还原会话所有权');
 phase++;await send();await ready();const beforeWorker=claudeChild(bridge,peerId),countBefore=requests.length;await brokerCall('pause');await stopOwnedProcess(beforeWorker.worker,{force:true});await wait(()=>!liveProcess(beforeWorker.native.pid));assert.equal(requests.length,countBefore,JSON.stringify({phase,extra:requests.slice(countBefore).map(r=>({model:r.model,main:r.tools?.some(t=>t.name==='ListAgents'),messages:r.messages?.slice(-3).map(m=>({role:m.role,content:Array.isArray(m.content)?m.content.map(v=>({type:v.type,id:v.id,tool_use_id:v.tool_use_id,text:v.text?.slice(-300)})):String(m.content).slice(-300)}))})),record:claudeChild(bridge,peerId)}));assert.equal((await status()).pair.paused,true);await brokerCall('resume');await wait(()=>claudeChild(bridge,peerId)?.worker?.pid!==beforeWorker.worker.pid);phase++;await send();await ready();assert.equal(claudeChild(bridge,peerId).sessionId,sessionId);report.steps.push('暂停期间 worker 退出不重放、不建副本；恢复后同 ID 接续');
 }
 assert.deepEqual(errors,[]);report.requests=requests.length;report.passed=true;
}catch(e){report.passed=false;report.error=e.stack;try{await failureFacts();}catch(error){report.diagnosticError=String(error);}process.exitCode=1;}finally{hold=false;release?.();foreground?.kill();try{report.shutdown=await brokerCall('shutdown',{releaseOwned:true},{start:false});}catch(e){report.shutdownError=String(e);}try{report.processCleanup=await cleanupTestProcesses(directory);assert.deepEqual(report.processCleanup.notes,[]);if(ownRegistry&&path.dirname(ownRegistry)===path.resolve(tmpdir()))rmSync(ownRegistry,{recursive:true,force:true});}catch(e){report.passed=false;report.cleanupError=String(e);process.exitCode=1;}if(report.buildId)try{await new Promise(r=>setTimeout(r,200));const {stdout}=await promisify(execFile)(process.execPath,[bridge+'/bin/nxbridge.cjs','uninstall','--purge'],{env,cwd:directory,timeout:80000,maxBuffer:4*1024*1024});report.uninstall=JSON.parse(stdout);assert.equal(report.uninstall.removed,true);}catch(e){report.passed=false;report.cleanupError=String(e);process.exitCode=1;}model.closeAllConnections();await new Promise(r=>model.close(r));writeFileSync(fixture.directory+'/child-lifecycle-report.json',JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));}
