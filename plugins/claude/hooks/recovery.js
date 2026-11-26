import { call, captureContext } from './mod-control.js';
import { modelMessage } from './errors.js';

const SPEC={name:'checkpoint',isDeferred:true,description:"path of this window's CURRENT task-state file (your own notes; absolute, outside repo): set/get/clear. nxbridge shows the path in recovery facts after compaction; stores path only, never reads the file",inputSchema:{type:'object',properties:{action:{type:'string',enum:['set','get','clear']},path:{type:'string'}},required:['action'],additionalProperties:false}};
                                                                                                      
const READ_SPEC={name:'read_messages',isDeferred:true,description:"re-read pair msgs by id (read-only; both directions; this window's pairs incl archived): after compact/resume re-read before acting; ask partner to resend only if still missing. no state change",inputSchema:{type:'object',properties:{message_ids:{type:'array',items:{type:'string'},minItems:1,maxItems:20}},required:['message_ids'],additionalProperties:false}};
const GUIDANCE='nxbridge: keep codex partner tasks/decisions as faithfully as user prompts: goal, done/pending, next step, approvals/revocations, msg ids, CURRENT file path. partner relays user intent, never user approval';
const RECOVER_SPEC={name:'recover_partner',isDeferred:true,description:'inspect or act on partner recovery. preview/result: read-only. retry: the single extra same-session managed launch after automatic attempts exhaust. recycle: only for a stuck-session notice — ends the stuck process + restores the same session (may cut whatever it is running); needs that notice\'s incident_id + evidence_revision; stale revision -> returns current facts, nothing done. retry/recycle need a recorded reason; never touch user-opened windows, config or approvals; repeating the same incident returns its existing result.',inputSchema:{type:'object',properties:{action:{type:'string',enum:['preview','retry','result','recycle']},incident_id:{type:'string'},evidence_revision:{type:'string'},reason:{type:'string'}},additionalProperties:false}};
let tool,readTool,recoverTool;
                                                                  
export const compactions=new WeakMap();
export async function recoveryStart($){tool=(await $.tool.register(SPEC)).tool;readTool=(await $.tool.register(READ_SPEC)).tool;recoverTool=(await $.tool.register(RECOVER_SPEC)).tool;}
export async function recoveryCall($,e,next){
  if(recoverTool&&e.tool===recoverTool){
    if(e.agentId)return{deny:'nxbridge recover_partner: root session only'};
    const input=Object.fromEntries(['action','incident_id','evidence_revision','reason'].filter(key=>e[key]!==undefined).map(key=>[key,e[key]]));
    try{return{result:JSON.stringify(await call($,'partner-recover',{...input,callId:e.tool_use_id}))};}
    catch(error){return{deny:modelMessage(error)};}
  }
  if(readTool&&e.tool===readTool){
    if(e.agentId)return{deny:'nxbridge read_messages: root session only'};
    try{return{result:JSON.stringify(await call($,'read-messages',{message_ids:e.message_ids,callId:e.tool_use_id}))};}
    catch(error){return{deny:modelMessage(error)};}
  }
  if(!tool||e.tool!==tool)return next(e);
  if(e.agentId)return{deny:'nxbridge checkpoint: root session only'};
  const input=Object.fromEntries(['action','path'].filter(key=>e[key]!==undefined).map(key=>[key,e[key]]));
  try{return{result:JSON.stringify(await call($,'checkpoint',{input,callId:e.tool_use_id}))};}
  catch(error){return{deny:modelMessage(error)};}
}
                                               
const RECOVERY_LEAD='〔nxbridge 压缩后自动附上的恢复信息，供 AI 接续工作，你无需处理〕',RECOVERY_HEAD='nxbridge recovery facts rev ';
function isRecoveryFacts(text){return typeof text==='string'&&(text.startsWith(RECOVERY_HEAD)||text.startsWith(RECOVERY_LEAD+'\n'+RECOVERY_HEAD));}
export async function recoveryCompact($,e,next){
  if(e.agentId)return next(e);
  const context=await captureContext($),lease=JSON.stringify(context.binding);
  const result=await next({...e,instructions:[e.instructions,GUIDANCE].filter(Boolean).join('\n\n')});
                                 
  if(e.trigger==='precompute'||next.signal?.aborted||result?.skip||!Array.isArray(result?.messages))return result;
  if(await captureContext($)!==context||JSON.stringify(context.binding)!==lease)return result;
  compactions.set(context,{id:globalThis.crypto?.randomUUID?.()??`${Date.now()}-${Math.random().toString(16).slice(2)}`,at:Date.now()});
  try{
    const facts=await call($,'recovery',{context});
    if(!facts?.text||await captureContext($)!==context)return result;
    return{...result,messages:[...result.messages.filter(message=>!isRecoveryFacts(message.text)),{role:'user',text:RECOVERY_LEAD+'\n'+facts.text,toolUses:[]}]};
  }catch{return result;}
}
