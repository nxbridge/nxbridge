import test from 'node:test';
import assert from 'node:assert/strict';
import { ClaudeStreamHealth } from '../hosts/claude/claude-stream.js';
import { SILENCE_MS } from '../broker/health/turn-health.js';
import { claudeBackgroundMode } from '../hosts/claude/claude-capability.js';
import { claudeChildFile, claudeHistoryExists, type ClaudeChildRecord } from '../hosts/claude/claude-child.js';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { temporary } from './support/temporary.js';
import { fixture } from './support/broker-fixture.js';
import { atomicJson, readJson } from '../shared/files.js';
import { formatStatus } from '../integration/mcp/presentation.js';

test('托管 Claude 的审批、原生进展、重试及失败分开记录，不保留正文',()=>{
  let now=1000;const health=new ClaudeStreamHealth(()=>now);
  health.observe({type:'system',subtype:'status',status:'requesting',uuid:'turn'});now+=SILENCE_MS;
  health.observe({type:'user',message:{content:[{type:'text',text:'后来信'}]}});assert.equal(health.get()?.state,'silent');
  health.observe({type:'system',subtype:'api_retry',attempt:2,max_retries:10});assert.equal(health.get()?.state,'networkRetrying');assert.equal(health.get()?.lastProgressAt,1000);
  health.observe({type:'stream_event',event:{type:'content_block_delta',delta:{text:'不保存正文'}}});assert.equal(health.get()?.state,'running');assert.equal(health.get()?.retry,undefined);
  health.observe({type:'assistant',message:{content:[{type:'tool_use',id:'tool',name:'Bash',input:{secret:'不保存'}}]}});now+=SILENCE_MS;assert.equal(health.get()?.state,'waitingTool');
  health.observe({type:'control_request',request:{subtype:'can_use_tool',tool_name:'AskUserQuestion',input:{secret:'不保存'}}});assert.equal(health.get()?.state,'needsHuman');assert.deepEqual(health.human,{kind:'question',tool:'AskUserQuestion'});
  health.observe({type:'result',subtype:'error_during_execution',is_error:true,result:'不保存'});assert.equal(health.get()?.state,'failed');assert.equal(health.human,undefined);assert.ok(!JSON.stringify(health).includes('不保存'));
});

test('Claude 回执按失败证据判定，诊断只保留已知原因和 HTTP 状态',()=>{
  const health=new ClaudeStreamHealth(()=>1000),cases=[
    {message:{subtype:'success',is_error:true},error:'Claude 未提供具体失败原因'},
    {message:{subtype:'success',is_error:true,terminal_reason:'api_error',api_error_status:401},error:'Claude API 请求失败（HTTP 401）'},
    {message:{subtype:'success',is_error:false,terminal_reason:'turn_setup_failed'},error:'Claude 无法启动这一轮'},
    {message:{subtype:'success',is_error:true,api_error_type:'私有信息',api_error_status:'500 私有信息'},error:'Claude API 请求失败'},
    {message:{subtype:'error_max_turns',is_error:false},error:'Claude 达到执行轮次上限'},
    {message:{subtype:'error_私有信息'},error:'Claude 未提供具体失败原因'},
    {message:{subtype:'success',is_error:true,terminal_reason:'私有信息'},error:'Claude 未提供具体失败原因'},
  ];
  for(const [index,{message,error}] of cases.entries()){
    const uuid='result-'+index;
    health.observe({type:'result',uuid,...message,result:'私有信息',errors:['私有信息']});
    assert.deepEqual(health.get(),{turnId:uuid,state:'failed',completion:'failed',error,lastProgressAt:1000,lastEventAt:1000,observedProgressAt:1000,tools:[]});
  }
  health.observe({type:'result',subtype:'success',is_error:false});
  assert.equal(health.get()?.state,'ready');assert.equal(health.get()?.completion,'success');assert.equal(health.get()?.error,undefined);
  health.observe({type:'result',subtype:'success',is_error:false,terminal_reason:'aborted_tools'});
  assert.equal(health.get()?.completion,'interrupted');
});

test('缺少 is_error 的结构化中断仍结束轮次，失败证据继续优先',()=>{
  for(const outcome of [{subtype:'interrupted'},{terminal_reason:'aborted_streaming'},{terminal_reason:'aborted_tools'}]){
    const health=new ClaudeStreamHealth(()=>1000);
    health.observe({type:'system',subtype:'status',status:'requesting',uuid:'interrupted-turn'});
    health.observe({type:'assistant',message:{content:[{type:'tool_use',id:'pending-tool'}]}});
    health.observe({type:'result',...outcome});
    assert.equal(health.get()?.state,'ready');assert.equal(health.get()?.completion,'interrupted');assert.deepEqual(health.get()?.tools,[]);
    health.observe({type:'result',...outcome,is_error:true});assert.equal(health.get()?.state,'failed');
  }
});

test('旧 Claude worker 的矛盾结果在状态里仍显示失败，原记录和轮次时间保留',t=>{
  const {broker,store,c,x}=fixture(t);store.pair(c.id,x.id);store.updatePeer(c.id,{owner:'nxbridge'});
  const file=claudeChildFile(broker.home,c.id);
  mkdirSync(path.dirname(file),{recursive:true});
  const health={turnId:'old-turn',state:'failed' as const,completion:'success',error:'Claude 原生 turn 失败（success）',lastProgressAt:1000,lastEventAt:1100,tools:[]};
  atomicJson(file,{version:1,peerId:c.id,sessionId:c.sessionId!,cwd:c.cwd,address:'fixture',phase:'active',hasSession:true,updatedAt:1100,health} satisfies ClaudeChildRecord);
  const status=broker.status({host:'codex',sessionId:'x',callId:'status'}) as {partner:{health:ClaudeChildRecord['health']}};
  assert.deepEqual(status.partner.health,{...health,completion:'failed',error:'Claude 未提供具体失败原因'});
  assert.match(formatStatus(status),/这一轮执行失败：Claude 未提供具体失败原因/);
  assert.deepEqual(readJson<ClaudeChildRecord>(file).health,health);
});

test('原生 PID 登记不等于已有会话历史，空会话和已落盘会话分别识别',t=>{
  const home=temporary(t),id=randomUUID();assert.equal(claudeHistoryExists(home,id),false);
  const project=path.join(home,'projects','project');mkdirSync(project,{recursive:true});writeFileSync(path.join(project,id+'.jsonl'),'{}\n','utf8');
  assert.equal(claudeHistoryExists(home,id),true);assert.equal(claudeHistoryExists(home,randomUUID()),false);
});

test('后台能力的路由按平台选择，Windows 保留原生后台且不启用子进程替代',()=>{
  assert.equal(claudeBackgroundMode(undefined,'win32'),'native');assert.equal(claudeBackgroundMode({available:true},'linux'),'native');
  assert.equal(claudeBackgroundMode({available:false},'linux'),'child');assert.equal(claudeBackgroundMode({available:false},'win32'),'unsupported');
});
