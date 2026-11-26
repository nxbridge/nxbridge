import { randomUUID } from 'node:crypto';
import { SILENCE_MS, type TurnHealth } from '../../broker/health/turn-health.js';

const unknownFailure='Claude 未提供具体失败原因';
const terminalFailures=new Map<unknown,string>([
  ['blocking_limit','Claude 达到使用限制'],
  ['rapid_refill_breaker','Claude 因使用额度保护停止'],
  ['prompt_too_long','提示过长'],
  ['image_error','Claude 无法处理图片'],
  ['model_error','Claude 模型执行失败'],
  ['malformed_tool_use_exhausted','Claude 多次生成无效的工具调用'],
  ['budget_exhausted','Claude 达到费用上限'],
  ['structured_output_retry_exhausted','Claude 多次生成无效的结构化输出'],
  ['tool_deferred_unavailable','Claude 无法继续已延后的工具调用'],
  ['turn_setup_failed','Claude 无法启动这一轮'],
]);
const resultFailures=new Map<unknown,string>([
  ['error_during_execution','Claude 在执行过程中出错'],
  ['error_max_turns','Claude 达到执行轮次上限'],
  ['error_max_budget_usd','Claude 达到费用上限'],
  ['error_max_structured_output_retries','Claude 多次生成无效的结构化输出'],
]);

function resultOutcome(message:{subtype?:unknown;is_error?:unknown;terminal_reason?:unknown;api_error_status?:unknown;api_error_type?:unknown;api_error_code?:unknown}):Pick<TurnHealth,'state'|'completion'|'error'>|undefined{
  const {subtype,is_error,terminal_reason,api_error_status,api_error_type,api_error_code}=message;
  const status=typeof api_error_status==='number'&&Number.isInteger(api_error_status)&&api_error_status>=400&&api_error_status<=599?api_error_status:undefined;
                                                      
  const apiFailure=terminal_reason==='api_error'||status!==undefined||typeof api_error_type==='string'&&api_error_type.length>0||typeof api_error_code==='string'&&api_error_code.length>0;
  const reason=terminalFailures.get(terminal_reason)??(apiFailure?`Claude API 请求失败${status===undefined?'':`（HTTP ${status}）`}`:resultFailures.get(subtype));
  if(is_error===true||reason||typeof subtype==='string'&&subtype.startsWith('error_'))return{state:'failed',completion:'failed',error:reason??unknownFailure};
  const interrupted=subtype==='interrupted'||terminal_reason==='aborted_streaming'||terminal_reason==='aborted_tools';
  if(is_error===false||is_error===undefined&&(subtype==='success'||interrupted)){
    return{state:'ready',completion:interrupted?'interrupted':'success'};
  }
}

                                             
export function normalizeClaudeTurnHealth(value:TurnHealth):TurnHealth{
  if(value.state!=='failed')return value;
  const legacyError=value.error==='Claude 原生 turn 失败（success）'||value.error==='Claude 原生 turn 失败（unknown）';
  if(!legacyError&&value.completion!=='success')return value;
  return{...value,completion:'failed',error:legacyError?unknownFailure:value.error??unknownFailure};
}

                                  
export class ClaudeStreamHealth {
  value?:TurnHealth;
  human?:{kind:'approval'|'question';tool:string};
  constructor(private now=Date.now){}
  get():TurnHealth|undefined{
    if(!this.value)return;
    const v=this.value;
    return{...v,tools:[...v.tools],state:['ready','failed','needsHuman'].includes(v.state)?v.state:v.retry?'networkRetrying':v.tools.length?'waitingTool':this.now()-v.lastProgressAt>=SILENCE_MS?'silent':'running'};
  }
  observe(message:any):void{
    const now=this.now(),type=message?.type;
    if(type==='system'&&message.subtype==='status'&&message.status==='requesting'){
      if(!this.value||['ready','failed'].includes(this.value.state))this.value={turnId:message.uuid??randomUUID(),lastProgressAt:now,lastEventAt:now,state:'running',tools:[]};return;
    }
    if(type==='control_request'&&message.request?.subtype==='can_use_tool'){
      const tool=String(message.request.tool_name??'unknown');this.human={kind:tool==='AskUserQuestion'?'question':'approval',tool};
      this.value??={turnId:randomUUID(),lastProgressAt:now,lastEventAt:now,state:'running',tools:[]};this.value.state='needsHuman';this.value.lastEventAt=now;return;
    }
    if(type==='system'&&message.subtype==='api_retry'){
      this.value??={turnId:randomUUID(),lastProgressAt:now,lastEventAt:now,state:'running',tools:[]};
      this.value.lastEventAt=now;this.value.retry={message:'Claude 原生 API 重试',since:this.value.retry?.since??now,...(typeof message.attempt==='number'?{attempt:message.attempt}:{}),...(typeof message.max_retries==='number'?{max:message.max_retries}:{})};return;
    }
    const event=type==='stream_event'?message.event:undefined;
    if(event?.type==='message_start'||type==='assistant'){
      if(!this.value||['ready','failed'].includes(this.value.state))this.value={turnId:message.uuid??randomUUID(),lastProgressAt:now,lastEventAt:now,state:'running',tools:[]};
    }
    if(type==='result'){
      const outcome=resultOutcome(message);if(!outcome)return;
                                           
      const previous=this.value;
      const active=previous&&!['ready','failed'].includes(previous.state)?previous:undefined;
      const turnId=active?.turnId??(typeof message.uuid==='string'?message.uuid:randomUUID());
      this.value={...outcome,turnId,lastProgressAt:active?.lastProgressAt??now,lastEventAt:now,observedProgressAt:now,tools:[]};this.human=undefined;return;
    }
    const v=this.value;if(!v)return;
    const blocks=type==='assistant'?message.message?.content:type==='user'?message.message?.content:[];
    if(Array.isArray(blocks))for(const block of blocks){
      if(block.type==='tool_use')v.tools=[...new Set([...v.tools,block.id])];
      if(block.type==='tool_result')v.tools=v.tools.filter(id=>id!==block.tool_use_id);
    }
    if(event?.type==='content_block_delta'||event?.type==='content_block_start'||type==='assistant'||type==='user'&&Array.isArray(blocks)&&blocks.some(b=>b.type==='tool_result')){
      v.lastProgressAt=now;v.lastEventAt=now;v.observedProgressAt=now;
      if(!this.human)v.state='running';delete v.retry;
    }
  }
}
