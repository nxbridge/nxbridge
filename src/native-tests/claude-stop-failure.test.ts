import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import path from 'node:path';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { resolveCli } from '../process/host-command.js';
import { ClaudeStreamHealth } from '../hosts/claude/claude-stream.js';
import { temporaryDirectory } from '../tests/support/temporary.js';

test('实际 Claude：Stop 继续片段遭遇不可重试 400 时仍报告根轮次终态',{timeout:35000},async t=>{
  const temp=temporaryDirectory(),cwd=temp.directory,config=path.join(cwd,'claude'),plugin=path.join(cwd,'plugin'),hooks=path.join(plugin,'hooks');
  for(const dir of [config,hooks,path.join(plugin,'.claude-plugin')])mkdirSync(dir,{recursive:true});
  const observed=path.join(cwd,'events.json'),marker='CONTINUE_FIXTURE_ONLY';
  const modelRequests:string[]=[];
  let child:ChildProcess|undefined,closed:Promise<unknown>|undefined;
  const model=http.createServer(async(req,res)=>{
    let raw='';for await(const data of req)raw+=data;
    if(req.url?.includes('count_tokens')){res.writeHead(200,{'content-type':'application/json'});res.end('{"input_tokens":1}');return;}
    if(!req.url?.startsWith('/v1/messages')){res.writeHead(404);res.end();return;}
    const body=JSON.parse(raw),continuing=JSON.stringify(body.messages).includes(marker);
    modelRequests.push(continuing?'continued':'initial');
    if(continuing){
      res.writeHead(400,{'content-type':'application/json'});res.end('{"type":"error","error":{"type":"invalid_request_error","message":"fixture non-retryable"}}');return;
    }
    const message={id:'fixture-message',type:'message',role:'assistant',model:body.model,content:[],stop_reason:null,usage:{input_tokens:10,output_tokens:2}};
    res.writeHead(200,{'content-type':'text/event-stream'});
    for(const event of [{type:'message_start',message},{type:'content_block_start',index:0,content_block:{type:'text',text:''}},{type:'content_block_delta',index:0,delta:{type:'text_delta',text:'Fixture draft'}},{type:'content_block_stop',index:0},{type:'message_delta',delta:{stop_reason:'end_turn',stop_sequence:null},usage:{output_tokens:2}},{type:'message_stop'}])
      res.write('event: '+event.type+'\ndata: '+JSON.stringify(event)+'\n\n');
    res.end();
  });
  t.after(async()=>{
    try{if(child&&child.exitCode===null&&child.signalCode===null)child.kill();await closed;}
    finally{model.closeAllConnections();if(model.listening)await new Promise<void>(resolve=>model.close(()=>resolve()));await temp.cleanup();}
  });
  await new Promise<void>(resolve=>model.listen(0,'127.0.0.1',resolve));
  const key='nxbridge-stop-failure-fixture-key',url='http://127.0.0.1:'+(model.address() as {port:number}).port;
  const modelEnv={ANTHROPIC_API_KEY:key,ANTHROPIC_BASE_URL:url,CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC:'1',DISABLE_AUTOUPDATER:'1',DISABLE_TELEMETRY:'1'};
  writeFileSync(path.join(config,'.claude.json'),JSON.stringify({hasCompletedOnboarding:true,customApiKeyResponses:{approved:[key.slice(-20)],rejected:[]},projects:{[cwd.replaceAll('\\','/')]:{hasTrustDialogAccepted:true}}}));
  writeFileSync(path.join(config,'settings.json'),JSON.stringify({env:modelEnv}));
  writeFileSync(path.join(plugin,'.claude-plugin','plugin.json'),JSON.stringify({name:'stop-failure-fixture',version:'1.0.0'}));
  writeFileSync(path.join(hooks,'hooks.json'),JSON.stringify({modules:['./observe.js']}));
  writeFileSync(path.join(hooks,'observe.js'),`
const events=[];
function record($,what,e,more){events.push({what,turnId:e.turnId,promptId:e.prompt_id,active:e.stop_hook_active,reason:e.reason,apiError:e.error,aborted:e.isAborted,...more});return $.fs.write(${JSON.stringify(observed)},JSON.stringify(events));}
export function register(on){
  on('turn.start',async($,e,next)=>{if(!e.agentId)await record($,'start',e);return next(e);});
  on('classic.Stop',async($,e,next)=>{
    const result=await next(e);await record($,'stop',e);
    return !e.stop_hook_active&&!result?.block&&!result?.preventContinuation?{...result,block:${JSON.stringify(marker)}}:result;
  });
  on('classic.StopFailure',async($,e,next)=>{await record($,'failure',e);return next(e);});
  on('turn.complete',async($,e,next)=>{if(!e.agentId)await record($,'complete',e,{answerLength:e.answer?.length});return next(e);});
}`);
  const cli=resolveCli('claude'),debug=path.join(cwd,'native-debug.log');
  const isolatedEnv:NodeJS.ProcessEnv={...process.env,...modelEnv,CLAUDE_CONFIG_DIR:config};
  for(const variable of ['ANTHROPIC_AUTH_TOKEN','CLAUDE_CODE_USE_BEDROCK','CLAUDE_CODE_USE_VERTEX','CLAUDE_CODE_USE_FOUNDRY'])delete isolatedEnv[variable];
  const runtime=child=spawn(cli.command,[...cli.prefix,'-p','--input-format','stream-json','--output-format','stream-json','--verbose','--include-partial-messages','--plugin-dir',plugin,'--permission-mode','default','--model','sonnet','--debug-file',debug],{
    cwd,env:isolatedEnv,stdio:['pipe','pipe','pipe'],windowsHide:true,
  });
  closed=once(runtime,'close');let stdout='',stderr='';
  runtime.stdout.on('data',value=>stdout+=String(value));runtime.stderr.on('data',value=>stderr+=String(value));
  runtime.stdin.end(JSON.stringify({type:'user',message:{role:'user',content:[{type:'text',text:'Stop failure fixture request'}]}})+'\n');
  await closed;
  let events:any[]=[];
  try{events=JSON.parse(readFileSync(observed,'utf8'));}catch{}
  const terminal=stdout.split('\n').filter(line=>line.includes('"type":"result"')).map(line=>JSON.parse(line) as {terminal_reason?:string;api_error_status?:number;is_error?:boolean});
  t.diagnostic(JSON.stringify({modelRequests,events,terminal:terminal.map(row=>[row.terminal_reason,row.api_error_status,row.is_error]),stderr}));
  assert.equal(modelRequests[0],'initial');assert.ok(modelRequests.length>=2);assert.ok(modelRequests.slice(1).every(value=>value==='continued'));
  assert.deepEqual(terminal.map(row=>[row.terminal_reason,row.api_error_status,row.is_error]),[['api_error',400,true]]);
  const health=new ClaudeStreamHealth();
  for(const line of stdout.trim().split('\n'))health.observe(JSON.parse(line));
  assert.equal(health.get()?.state,'failed');
  assert.equal(health.get()?.completion,'failed');
  assert.equal(health.get()?.error,'Claude API 请求失败（HTTP 400）');
  const root=events.filter(row=>['start','stop','failure','complete'].includes(row.what));
  assert.deepEqual(root.map(row=>row.what),['start','stop','failure','complete']);
  const [began,stop,failure,complete]=root;
  assert.ok(began.turnId);assert.equal(complete.turnId,began.turnId);
  assert.equal(stop.active,false);assert.ok(stop.promptId);assert.equal(failure.promptId,stop.promptId);
  assert.equal(complete.aborted,false,'API 失败不能只靠中断字段识别');
});
