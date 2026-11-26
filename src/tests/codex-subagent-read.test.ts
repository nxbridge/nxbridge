import test, { type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { appendFileSync, renameSync, statSync, writeFileSync } from 'node:fs';
import { open } from 'node:fs/promises';
import path from 'node:path';
import { captureCodexSubagentReadFloor, readCodexSampledSubagentCompletions, readCodexSubagentCompletions, readCodexSubagentContinuation, type CodexSubagentReadCandidate } from '../hosts/codex/codex-subagent-read.js';
import { temporary } from './support/temporary.js';

const sessionId='parent-session',parentTurn='parent-turn',agentId='child-session',agentPath='/root/child';
const meta={type:'session_meta',payload:{id:sessionId}};
const event=(type:string,turn_id=parentTurn)=>({type:'event_msg',payload:{type,turn_id}});
const v1=(reference=agentId)=>({type:'response_item',payload:{type:'message',role:'user',content:[{type:'input_text',text:`<subagent_notification>\n${JSON.stringify({agent_path:reference,status:{completed:'子代理结果'}})}\n</subagent_notification>`}],internal_chat_message_metadata_passthrough:{content_item_kinds:['multi_agent.subagent_notification']}}});
const v2Meta={type:'inter_agent_communication_metadata',payload:{trigger_turn:false}};
const v2=(history_sequence:number)=>({type:'response_item',metadata:{history_sequence},payload:{type:'agent_message',author:agentPath,recipient:'/root',content:[{type:'input_text',text:`Message Type: FINAL_ANSWER\nTask name: /root\nSender: ${agentPath}\nPayload:\n子代理结果`}]}});
const output=(sampled_through_sequence?:number)=>({type:'response_item',metadata:{sampled_through_sequence},payload:{type:'message',role:'assistant',content:[{type:'output_text',text:'父会话输出'}]}});
const nativeCompleted=(childTurnId:string,thread_id=sessionId,agent_thread_id=agentId,agent_path=agentPath)=>({type:'event_msg',payload:{type:'item_completed',thread_id,turn_id:'initiating-parent-turn',item:{type:'SubAgentActivity',kind:'completed',id:`subagent-completed-${childTurnId}`,agent_thread_id,agent_path}}});
const nativeResult=(historySequence:number,turn_id=parentTurn,text='子代理结果',author=agentPath)=>{
  const row=v2(historySequence);
  return{...row,payload:{...row.payload,author,content:[{type:'input_text',text:`Message Type: FINAL_ANSWER\nTask name: /root\nSender: ${author}\nPayload:\n${text}`}],internal_chat_message_metadata_passthrough:{turn_id,content_item_metadata:[{provenance:{type:'tool',namespace:'collaboration'}}]}}};
};
const sampled=(sampled_through_sequence:number,turnId=parentTurn,type='reasoning')=>({type:'response_item',metadata:{sampled_through_sequence,retained_source:{id:{turn_id:turnId}}},payload:{type}});

function fixture(t:TestContext){
  const directory=temporary(t),file=path.join(directory,'parent.jsonl');
  writeFileSync(file,'','utf8');
  const append=(...rows:unknown[])=>appendFileSync(file,rows.map(row=>JSON.stringify(row)+'\n').join(''),'utf8');
  append(meta,event('task_started'));
  const completion=(childTurnId='child-turn',withPath=false):CodexSubagentReadCandidate=>{
    const floor=captureCodexSubagentReadFloor(file,sessionId);
    assert.ok(floor);
    return{key:childTurnId,agentId,childTurnId,floor,...(withPath?{agentPath}:{})};
  };
  const read=(...pending:CodexSubagentReadCandidate[])=>[...readCodexSubagentCompletions(file,sessionId,parentTurn,pending)];
  const readSampled=async(...pending:CodexSubagentReadCandidate[])=>[...await readCodexSampledSubagentCompletions(file,sessionId,parentTurn,pending)];
  const continuation=(...pending:CodexSubagentReadCandidate[])=>readCodexSubagentContinuation(file,sessionId,pending);
  return{file,append,completion,read,readSampled,continuation};
}

test('V1 只确认本轮原生匹配结果之后的父会话输出',t=>{
  const f=fixture(t),pending=f.completion();
  f.append(output(),v1('other-child'),output(),v1());
  assert.deepEqual(f.read(pending),[]);
  f.append(output());
  assert.deepEqual(f.read(pending),[pending.key]);
  assert.deepEqual([...readCodexSubagentCompletions(f.file,sessionId,'other-turn',[pending])],[]);
  f.append(event('task_complete'));
  assert.deepEqual(f.read(pending),[]);
});

test('父线程空闲期间的结果在下一轮模型输出后得到确认，回滚则撤销证明',t=>{
  const f=fixture(t),pending=f.completion();
  f.append(event('task_complete'),v1());
  assert.deepEqual(f.read(pending),[]);
  f.append(event('task_started'),output());
  assert.deepEqual(f.read(pending),[pending.key]);
  f.append(event('thread_rolled_back'),event('task_started'),output());
  assert.deepEqual(f.read(pending),[]);
});

test('V2 使用采样边界区分旧请求输出和结果进入上下文后的输出',t=>{
  const f=fixture(t),pending=f.completion('child-turn',true);
  f.append(v2(91),output(91));
  assert.deepEqual(f.read(pending),[]);
  f.append(v2Meta,v2(94),output(93),event('token_count'));
  assert.deepEqual(f.read(pending),[]);
  f.append(output(94));
  assert.deepEqual(f.read(pending),[pending.key]);
});

test('父会话最后一步之后的子代理结果，以及同一子代理先前轮次的结果均不确认',t=>{
  const f=fixture(t),first=f.completion();
  f.append(v1(),output());
  assert.deepEqual(f.read(first),[first.key]);
  const later=f.completion('child-followup');
  assert.deepEqual(f.read(later),[]);
  f.append(v1(),event('token_count'));
  assert.deepEqual(f.read(later),[]);
  f.append(output());
  assert.deepEqual(f.read(later),[later.key]);
  assert.deepEqual(f.read(first,later),[]);
});

test('同名非原生通知、重复原生通知和压缩输出不能证明模型读取',t=>{
  const f=fixture(t),pending=f.completion();
  const plain=v1();
  f.append({...plain,payload:{...plain.payload,internal_chat_message_metadata_passthrough:undefined}},output());
  assert.deepEqual(f.read(pending),[]);
  f.append(v1(),{...output(),metadata:{compaction_output:true}});
  assert.deepEqual(f.read(pending),[]);
  f.append(v1(),output());
  assert.deepEqual(f.read(pending),[]);
});

test('损坏、轮换或超过读取上限的父会话历史保留待确认项',t=>{
  const f=fixture(t),pending=f.completion();
  assert.equal(captureCodexSubagentReadFloor(f.file,'unverified-session'),undefined);
  f.append(v1(),output(),{type:'response_item',payload:{text:'a'.repeat(1024*1024)}});
  assert.deepEqual(f.read(pending),[]);
  renameSync(f.file,f.file+'.old');
  f.append(meta,event('task_started'));
  const rotated=f.completion();
  f.append(v1(),output());
  assert.deepEqual(f.read(pending),[]);
  assert.deepEqual(f.read(rotated),[rotated.key]);
  appendFileSync(f.file,'{无效记录}\n','utf8');
  assert.deepEqual(f.read(rotated),[]);
});

test('长轮次可分别确认同一子代理的两次完成',async t=>{
  const f=fixture(t),expired=f.completion('expired');
  f.append(nativeCompleted(expired.childTurnId),v2Meta,nativeResult(1),sampled(1));
  const padding=JSON.stringify({type:'response_item',payload:{type:'reasoning',text:'a'.repeat(1024*1024)}})+'\n';
  for(let i=0;i<33;i++)appendFileSync(f.file,padding,'utf8');
  const first=f.completion('first',true);
  f.append(nativeCompleted(first.childTurnId),v2Meta,nativeResult(91,parentTurn,'一'.repeat(100_000)),sampled(91));
  const second=f.completion('second',true);
  f.append(nativeCompleted(second.childTurnId),v2Meta,nativeResult(94),sampled(93,parentTurn,'function_call'));
  assert.deepEqual(await f.readSampled(expired,first,second),[first.key]);
  f.append(sampled(94,parentTurn,'function_call'));
  assert.deepEqual(await f.readSampled(expired,first,second),[first.key,second.key]);
  const inferred=await f.continuation(expired,first,second);
  assert.deepEqual(inferred,{verified:true,read:new Set([first.key,second.key]),continued:new Set([first.key,second.key]),openTurnId:parentTurn});
  assert.deepEqual(await f.continuation(),{verified:true,read:new Set(),continued:new Set(),openTurnId:parentTurn});
  f.append(event('task_complete'),sampled(94));
  assert.deepEqual(await f.continuation(first,second),{verified:true,read:inferred.read,continued:new Set(),closedTurnId:parentTurn});
  assert.equal((await f.continuation()).openTurnId,undefined);
});

test('只采信父轮次内有原生来源的结果和模型采样',async t=>{
  const f=fixture(t),wrongTurn=f.completion('wrong-turn',true);
  f.append(nativeCompleted(wrongTurn.childTurnId),v2Meta,nativeResult(91,'other-turn'),sampled(91));
  assert.deepEqual(await f.readSampled(wrongTurn),[]);
  const pending=f.completion('verified');
  f.append(nativeCompleted(pending.childTurnId),nativeResult(92),v2Meta,v2(93),v2Meta,nativeResult(94,parentTurn,'Agent errored: other turn'),v2Meta,nativeResult(95),sampled(95,'other-turn'),sampled(95,parentTurn,'function_call_output'),sampled(94));
  assert.deepEqual(await f.readSampled(wrongTurn,pending),[]);
  const model=output(95);
  f.append({...model,payload:{...model.payload,internal_chat_message_metadata_passthrough:{turn_id:parentTurn}}});
  assert.deepEqual(await f.readSampled(wrongTurn,pending),[pending.key]);
});

test('原生完成事件必须与保存的文件位置、子轮次和身份一致',async t=>{
  const f=fixture(t),pending=f.completion('exact',true);
  f.append(nativeCompleted(pending.childTurnId),v2Meta,nativeResult(91),sampled(91));
  const wrongPosition={...pending,floor:{...pending.floor,offset:pending.floor.offset+1}};
  const wrongIdentity={...pending,agentId:'other-agent'};
  const wrongPath={...pending,agentPath:'/root/other'};
  const wrongTurn={...pending,childTurnId:'other-turn'};
  for(const candidate of [wrongPosition,wrongIdentity,wrongPath,wrongTurn])assert.deepEqual(await f.readSampled(candidate),[]);
  assert.deepEqual(await f.readSampled(pending,wrongTurn),[pending.key]);
  assert.deepEqual(await f.readSampled(pending,{...pending,key:'conflicting-key'}),[]);
  const legacy={...pending,floor:{...pending.floor,exactIno:undefined}};
  const legacyProof=await f.continuation(legacy);
  assert.deepEqual(legacyProof.read,new Set(Number.isSafeInteger(legacy.floor.ino)?[legacy.key]:[]));
  assert.deepEqual(legacyProof.continued,new Set());
  const later=f.completion('later');
  f.append(nativeCompleted(later.childTurnId),v2Meta,nativeResult(92),sampled(92));
  assert.deepEqual(await f.readSampled(wrongPosition,later),[later.key]);
  const missing=f.completion('missing');
  f.append(event('token_count'),nativeCompleted(missing.childTurnId),v2Meta,nativeResult(94),sampled(94));
  assert.deepEqual(await f.readSampled(missing),[]);
  const wrongSession=f.completion('wrong-session');
  f.append(nativeCompleted(wrongSession.childTurnId,'other-session'),v2Meta,nativeResult(95),sampled(95));
  assert.deepEqual(await f.readSampled(wrongSession),[]);
});

test('迟到、重复或乱序的结果不能证明下一次同名完成已采样',async t=>{
  const f=fixture(t),first=f.completion('first');
  f.append(nativeCompleted(first.childTurnId),v2Meta,nativeResult(91),sampled(91));
  assert.deepEqual(await f.readSampled(first),[first.key]);
  const second=f.completion('second');
  f.append(nativeCompleted(second.childTurnId),v2Meta,nativeResult(91),sampled(91));
  assert.deepEqual(await f.readSampled(first,second),[first.key]);
  const third=f.completion('third');
  f.append(nativeCompleted(third.childTurnId),v2Meta,nativeResult(94),sampled(94));
  assert.deepEqual(await f.readSampled(first,second,third),[first.key]);
});

test('相邻两次完成之间缺少结果时不会把迟到消息归给后一轮',async t=>{
  const f=fixture(t),first=f.completion('first');
  f.append(nativeCompleted(first.childTurnId));
  const second=f.completion('second');
  f.append(nativeCompleted(second.childTurnId),v2Meta,nativeResult(94),sampled(94));
  assert.deepEqual(await f.readSampled(first,second),[]);
  assert.deepEqual(await f.readSampled(first),[]);
});

test('只处理完整记录，重复结果撤销当前完成的采样证明',async t=>{
  const f=fixture(t),pending=f.completion();
  f.append(nativeCompleted(pending.childTurnId),v2Meta,nativeResult(91));
  appendFileSync(f.file,JSON.stringify(sampled(91)),'utf8');
  assert.deepEqual(await f.readSampled(pending),[]);
  appendFileSync(f.file,'\n','utf8');
  assert.deepEqual(await f.readSampled(pending),[pending.key]);
  f.append(v2Meta,nativeResult(92),sampled(92));
  assert.deepEqual(await f.readSampled(pending),[]);
});

test('压缩前已完成的采样保留，回滚和轮换使原文件证明失效',async t=>{
  const f=fixture(t),first=f.completion('first',true);
  f.append(nativeCompleted(first.childTurnId),v2Meta,nativeResult(91),sampled(91));
  const second=f.completion('second');
  f.append(nativeCompleted(second.childTurnId),v2Meta,nativeResult(94),{type:'compacted'},sampled(94));
  assert.deepEqual(await f.readSampled(first,second),[first.key]);
  f.append(event('thread_rolled_back'),sampled(95));
  assert.deepEqual(await f.readSampled(first,second),[]);
  renameSync(f.file,f.file+'.old');
  f.append(meta,event('task_started'));
  const rotated=f.completion('first',true);
  f.append(nativeCompleted(rotated.childTurnId),v2Meta,nativeResult(91),sampled(91),output(91));
  const sameRoundedInode={...first,floor:{...first.floor,ino:rotated.floor.ino}};
  assert.deepEqual(await f.readSampled(sameRoundedInode),[]);
  assert.deepEqual((await f.continuation(sameRoundedInode)).continued,new Set());
  assert.deepEqual(f.read(sameRoundedInode),[]);
  assert.deepEqual(await f.readSampled(rotated),[rotated.key]);
  assert.deepEqual(f.read(rotated),[rotated.key]);
});

test('只有唯一原生结果之后的同轮 MCP 调用能独立确认继续工作',async t=>{
  const f=fixture(t),pending=f.completion('continued');
  f.append(nativeCompleted(pending.childTurnId));
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set(),continued:new Set(),openTurnId:parentTurn});
  f.append(event('mcp_tool_call_begin'));
  assert.deepEqual((await f.continuation(pending)).continued,new Set());
  f.append(v2Meta,nativeResult(91));
  f.append(event('mcp_tool_call_begin','unrelated-turn'));
  assert.deepEqual((await f.continuation(pending)).continued,new Set());
  f.append({type:'event_msg',payload:{type:'mcp_tool_call_begin',thread_id:'other-session',turn_id:parentTurn}});
  assert.deepEqual((await f.continuation(pending)).continued,new Set());
  const at='2026-11-26T10:31:55.000Z';
  f.append({...event('mcp_tool_call_begin'),timestamp:at});
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set(),continued:new Set([pending.key]),openTurnId:parentTurn,lastProgressAt:Date.parse(at)});
  appendFileSync(f.file,JSON.stringify(event('thread_rolled_back')),'utf8');
  assert.deepEqual(await f.continuation(pending),{verified:false,read:new Set(),continued:new Set()});
  appendFileSync(f.file,'\n','utf8');
  f.append(event('mcp_tool_call_begin'),sampled(95));
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set(),continued:new Set()});
  f.append(event('turn_started','next-turn'));
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set(),continued:new Set(),openTurnId:'next-turn'});
});

test('只有当前父轮次采样才能确认结果，终态之后的迟到采样保持未确认',async t=>{
  const f=fixture(t),pending=f.completion('late');
  f.append(nativeCompleted(pending.childTurnId),v2Meta,nativeResult(91),event('turn_aborted'),sampled(91));
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set(),continued:new Set(),closedTurnId:parentTurn});
  f.append(event('turn_started','next-turn'),sampled(92,'next-turn'));
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set(),continued:new Set(),openTurnId:'next-turn'});
});

test('最后一轮的推理和最终答复可证明采样但不能证明继续工作',async t=>{
  const f=fixture(t),pending=f.completion(),at='2026-11-26T10:31:55.000Z';
  const final=output(91);
  f.append(nativeCompleted(pending.childTurnId),v2Meta,nativeResult(91),sampled(90),
    {...final,timestamp:at,payload:{...final.payload,phase:'final_answer',internal_chat_message_metadata_passthrough:{turn_id:parentTurn}}});
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set([pending.key]),continued:new Set(),openTurnId:parentTurn,lastProgressAt:Date.parse(at)});
  f.append(event('task_complete'));
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set([pending.key]),continued:new Set(),closedTurnId:parentTurn});
});

async function readPositions(t:TestContext,filename:string):Promise<number[]>{
  const file=await open(filename,'r');
  const type=Object.getPrototypeOf(file) as Pick<typeof file,'read'>;
  const read=type.read as (this:typeof file,buffer:Buffer,offset:number,length:number,position:number)=>Promise<unknown>;await file.close();
  const positions:number[]=[];
  t.mock.method(type,'read',function(this:typeof file,buffer:Buffer,offset:number,length:number,position:number){
    positions.push(position);return read.call(this,buffer,offset,length,position);
  });
  return positions;
}

test('新增完整记录增量读取，重复结果撤销已取得的继续工作证明',async t=>{
  const f=fixture(t),pending=f.completion(),positions=await readPositions(t,f.file);
  f.append(nativeCompleted(pending.childTurnId),v2Meta,nativeResult(91),event('mcp_tool_call_begin'));
  assert.deepEqual((await f.continuation(pending)).continued,new Set([pending.key]));
  assert.ok(positions.includes(0));
  positions.length=0;
  assert.deepEqual((await f.continuation(pending)).continued,new Set([pending.key]));
  assert.deepEqual(positions,[]);
  f.append(v2Meta,nativeResult(92),event('mcp_tool_call_begin'));
  assert.deepEqual((await f.continuation(pending)).continued,new Set());
  assert.ok(positions.length>0&&positions.every(position=>position>pending.floor.offset));
});

test('同一文件截断后增长不会沿用旧的结果和轮次证明',async t=>{
  const f=fixture(t),pending=f.completion();
  f.append(nativeCompleted(pending.childTurnId),v2Meta,nativeResult(91),sampled(91,parentTurn,'function_call'));
  assert.deepEqual((await f.continuation(pending)).read,new Set([pending.key]));
  writeFileSync(f.file,'','utf8');
  f.append(meta,event('task_started','replacement'),{type:'response_item',payload:{text:'a'.repeat(2000)}},event('task_complete','replacement'));
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set(),continued:new Set(),closedTurnId:'replacement'});
});

test('超过行上限的图像结果不妨碍核验之后的父轮次活动和子结果',async t=>{
  const f=fixture(t),before=f.completion('before-image');
  f.append(nativeCompleted(before.childTurnId),v2Meta,nativeResult(91),sampled(91,parentTurn,'function_call'));
  f.append({type:'response_item',payload:{type:'function_call_output',output:[{type:'input_image',image_url:'data:image/png;base64,'+'A'.repeat(5*1024*1024)}]}});
  const after={...f.completion('after-image'),agentId:'other-child',agentPath:'/root/other'},at='2026-11-26T10:40:07.000Z';
  f.append(nativeCompleted(after.childTurnId,sessionId,after.agentId,after.agentPath),v2Meta,nativeResult(94,parentTurn,'另一子代理结果',after.agentPath),{...sampled(94,parentTurn,'function_call'),timestamp:at});
  assert.deepEqual(await f.continuation(before,after),{verified:true,read:new Set([after.key]),continued:new Set([after.key]),openTurnId:parentTurn,lastProgressAt:Date.parse(at)});
});

test('超大行未写完时缓存跳过位置，增量读到换行后才能输出新证据',async t=>{
  const f=fixture(t),pending=f.completion(),positions=await readPositions(t,f.file);
  f.append(nativeCompleted(pending.childTurnId),v2Meta,nativeResult(91));
  appendFileSync(f.file,'{"type":"response_item","payload":{"output":"'+'A'.repeat(5*1024*1024),'utf8');
  const unavailable={verified:false,read:new Set(),continued:new Set()};
  assert.deepEqual(await f.continuation(pending),unavailable);
  assert.ok(positions.includes(0));positions.length=0;
  assert.deepEqual(await f.continuation(pending),unavailable);
  assert.deepEqual(positions,[]);
  for(let part=0;part<2;part++){
    const previousSize=statSync(f.file).size;
    appendFileSync(f.file,'A'.repeat(512*1024),'utf8');
    assert.deepEqual(await f.continuation(pending),unavailable);
    assert.ok(positions.length>0&&positions.every(position=>position>=previousSize-128));positions.length=0;
  }
  appendFileSync(f.file,'"}}\n','utf8');
  f.append(sampled(91,parentTurn,'function_call'));
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set(),continued:new Set(),openTurnId:parentTurn});
  assert.ok(positions.length>0&&positions.every(position=>position>5*1024*1024));
});

test('损坏的完整记录断开前后结果关联，后续轮次证据仍可核验',async t=>{
  const f=fixture(t),pending=f.completion();
  f.append(nativeCompleted(pending.childTurnId),v2Meta,nativeResult(91),sampled(91,parentTurn,'function_call'));
  assert.deepEqual((await f.continuation(pending)).read,new Set([pending.key]));
  appendFileSync(f.file,'{"type":"event_msg","payload":{"type":"thread_rolled_back"\n','utf8');
  f.append(sampled(92,parentTurn,'function_call'));
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set(),continued:new Set(),openTurnId:parentTurn});
  f.append(event('task_complete'));
  assert.deepEqual(await f.continuation(pending),{verified:true,read:new Set(),continued:new Set(),closedTurnId:parentTurn});
});

test('存在损坏记录时，不能把同一作者迟到的旧结果归给新完成项',async t=>{
  const f=fixture(t),before=f.completion('before-gap');
  f.append(nativeCompleted(before.childTurnId),v2Meta,nativeResult(91),sampled(91,parentTurn,'function_call'));
  appendFileSync(f.file,'{"type":"event_msg"\n','utf8');
  const after=f.completion('after-gap');
  f.append(nativeCompleted(after.childTurnId),v2Meta,nativeResult(92),sampled(92,parentTurn,'function_call'));
  assert.deepEqual(await f.continuation(before,after),{verified:true,read:new Set(),continued:new Set(),openTurnId:parentTurn});
});

test('无作者路径的旧完成记录损坏时，按已保存的子代理身份拒绝迟到结果',async t=>{
  const f=fixture(t),before=f.completion('corrupt-completion');
  appendFileSync(f.file,JSON.stringify(nativeCompleted(before.childTurnId)).slice(0,-2)+'\n','utf8');
  const after=f.completion('new-completion');
  assert.equal(before.agentPath,undefined);
  assert.deepEqual(await f.continuation(before,after),{verified:true,read:new Set(),continued:new Set()});
  f.append(nativeCompleted(after.childTurnId),v2Meta,nativeResult(107,parentTurn,'迟到的旧结果'),sampled(107,parentTurn,'function_call'));
  assert.deepEqual(await f.continuation(before,after),{verified:true,read:new Set(),continued:new Set(),openTurnId:parentTurn});
});
