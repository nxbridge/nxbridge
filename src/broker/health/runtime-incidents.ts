import { randomUUID } from 'node:crypto';
import type { Endpoint, Peer } from '../../types.js';
import {describeError} from '../../shared/errors.js';
import type {HealthStorage} from './contracts.js';

export type IncidentLayer='native'|'transport'|'protocol'|'delivery'|'receipt'|'task'|'integration';
export type CauseClass='transient'|'payload'|'protocol'|'auth'|'configuration'|'human'|'exhausted'|'unknown';
export interface IncidentAction {owner:'runtime'|'model'|'user';text:string}
export interface IncidentStatus {
  id:string;peerId:string;layer:IncidentLayer;causeClass:CauseClass;code:string;
  state:'observing'|'blocked'|'recovering';since:number;lastFailureAt:number;attempts:number;retryAt?:number;
  message:string;userText:string;affectedMessageIds?:string[];action?:IncidentAction;
}
interface Incident extends IncidentStatus {scope:string;successAt?:number;closed?:boolean}

                                  
export function missingClaudeInboxModelText(peer:Peer,self=false):string{
  const subject=self?"Your Claude inbox socket path is missing; you cannot receive partner messages.":'Claude partner inbox socket path missing (possibly OS temporary cleanup); its process is alive.';
  const action=peer.owner==='nxbridge'?'nxbridge attempts a bounded same-session restart at safe idle; if blocked, inspect recovery in nxbridge status.':`Tell the user: at idle exit ${self?'this Claude window':'that Claude window'}, then use its original launch in its cwd with --resume ${peer.sessionId}.`;
  return`${subject} ${action} Safely unsent messages flush after re-registration; messages kept, do not resend. Uncertain sends are never replayed.`;
}
export function missingCodexSocketModelText(peer:Peer,self=false):string{
  const subject=self?'Your Codex protected socket pathname is missing; new native connections cannot reach this process.':'Codex partner protected socket pathname is missing (possibly OS temporary cleanup); its process is alive.';
  const action=peer.owner==='nxbridge'?'nxbridge attempts a bounded same-session restart only with verified idle work and its original attested live connection; inspect recovery if held.':`Tell user: at idle exit that Codex window, then in its original cwd and launch environment run codex --no-daemon resume ${peer.sessionId}.`;
  return`${subject} ${action} Messages are kept; do not resend. Uncertain sends are never replayed.`;
}
const delays=[1000,2000,4000,8000,15000,30000,60000];
const key=(id:string)=>'runtimeIncident:'+id;
const scope=(peer:Peer,endpoint?:Endpoint)=>JSON.stringify([peer.sessionId,endpoint?.runtimeId??endpoint?.native?.procStart??endpoint?.procStart??endpoint?.epoch]);

export function faultCause(error:unknown):{code:string;layer:IncidentLayer;causeClass:CauseClass;userText:string;action:IncidentAction}{
  const code=String((error as {code?:unknown})?.code??'unknown'),message=describeError(error);
  if(['native_thread_wedged','native_thread_unconfirmed','owned_subscription_waiting'].includes(code))return{code,layer:'native',causeClass:'unknown',userText:message,action:{owner:code==='native_thread_wedged'?'user':'runtime',text:message}};
  if(code==='reply_socket_conflict')return{code,layer:'transport',causeClass:'configuration',userText:'nxbridge 接收回执的连接文件位置被别的文件占用了，消息暂时发不出去',action:{owner:'user',text:'运行 nxbridge status 查看占用的路径；不要重发任务'}};
  if(code==='payload_too_large')return{code,layer:'protocol',causeClass:'unknown',userText:'原生消息超过大小保护，连接已关闭，正在降频核验。',action:{owner:'runtime',text:'使用有界请求重新核验连接，不重复拉取全量历史。'}};
  if(/payload|response_too_large|WS_ERR_UNSUPPORTED_MESSAGE_LENGTH|1009/.test(code)||/max(?:imum)?.*(?:payload|message).*size|unsupported.*message.*length/i.test(message))return{code,layer:'protocol',causeClass:'payload',userText:'原生响应超过大小保护，已停止重复读取。',action:{owner:'model',text:'Report the protocol size fault once; continue independent work.'}};
  if(/auth|token_missing|unauthori[sz]ed|401|403/.test(code))return{code,layer:'protocol',causeClass:'auth',userText:'原生连接认证失败，需要检查登录或认证配置。',action:{owner:'user',text:'检查原生宿主的登录和认证配置。'}};
  if(['trust_required','workspace_untrusted','claude_bypass_consent_required','permission_denied','consent_required'].includes(code))return{code,layer:'integration',causeClass:'human',userText:'原生宿主需要用户确认信任或权限。',action:{owner:'user',text:'在原生窗口完成当前信任或权限提示。'}};
  if(/config|invalid_endpoint|bootstrap|profile_missing|command_missing/.test(code))return{code,layer:'integration',causeClass:'configuration',userText:'运行配置无法使用，已停止重复尝试。',action:{owner:'user',text:'检查 nxbridge doctor 给出的配置原因。'}};
  if(/invalid_json|protocol|invalid_response|-3260[012]|schema/.test(code))return{code,layer:'protocol',causeClass:'protocol',userText:'原生协议无法兼容，已停止相同请求。',action:{owner:'model',text:'Report the protocol compatibility fault once; do not repeat the same request.'}};
  if(/timeout|disconnect|ECONN|EPIPE|ETIMEDOUT|unreachable|offline|attach_failed/.test(code))return{code,layer:'transport',causeClass:'transient',userText:'搭档连接暂时中断，消息已保留，后台正在退避重连。',action:{owner:'runtime',text:'后台自动重连；结果不明的发送不重放。'}};
  return{code,layer:'delivery',causeClass:'unknown',userText:'搭档投递尚未确认，消息已保留，正在核验。',action:{owner:'runtime',text:'核验原编号的投递结果。'}};
}

                                          
export class RuntimeIncidents {
  constructor(private storage:HealthStorage,private now=Date.now){}
  current(peer:Peer,endpoint?:Endpoint):IncidentStatus|undefined{
    const value=this.storage.setting<Incident>(key(peer.id));if(!value||value.closed||value.scope!==scope(peer,endpoint))return;
    if(value.successAt&&this.now()-value.successAt>=30000&&value.successAt>=value.lastFailureAt){value.closed=true;this.storage.setSetting(key(peer.id),value);return;}
    const {scope:_scope,successAt:_successAt,closed:_closed,...status}=value;return status;
  }
  canAttempt(peer:Peer,endpoint?:Endpoint):boolean{
    const value=this.current(peer,endpoint);if(!value)return true;
                                        
    if(['claude_inbox_missing','codex_socket_target_missing'].includes(value.code))return true;
    return value.state==='recovering'||(value.retryAt??Infinity)<=this.now();
  }
  failed(peer:Peer,endpoint:Endpoint|undefined,error:unknown,messageIds:string[]=[]):IncidentStatus{
    const before=this.storage.setting<Incident>(key(peer.id)),identity=scope(peer,endpoint),cause=faultCause(error),same=before&&!before.closed&&before.scope===identity;
    if(cause.code==='claude_inbox_missing'){
      Object.assign(cause,{layer:'transport',causeClass:'configuration',userText:peer.owner==='nxbridge'?'后台 Claude 接收消息的连接文件不见了（可能被系统清理删掉），正在空闲时自动重启恢复':'本窗口接收消息的连接文件不见了（可能被系统清理删掉），收不到 Codex 的消息，会话本身正常',action:peer.owner==='nxbridge'?{owner:'runtime',text:'在安全空闲点自动恢复同一会话；待发消息保留，结果不明的发送不重放。'}:{owner:'user',text:`空闲时退出本窗口，在原目录加 --resume ${peer.sessionId} 重新打开，积压的消息会自动补发`}});
      if(same&&before.code===cause.code&&!before.successAt)return this.current(peer,endpoint)!;
    }
    if(cause.code==='codex_socket_target_missing'){
      Object.assign(cause,{layer:'transport',causeClass:'configuration',userText:peer.owner==='nxbridge'?'后台 Codex 接收消息的连接文件不见了（可能被系统清理删掉），正在空闲时自动重启恢复':'这个 Codex 窗口接收消息的连接文件不见了（可能被系统清理删掉），消息发不进去，会话本身正常',action:peer.owner==='nxbridge'?{owner:'runtime',text:'只在核验原连接和全部工作空闲后恢复同一会话；待发消息保留，结果不明的发送不重放。'}:{owner:'user',text:`空闲时退出这个 Codex 窗口，在原目录用原来的方式重新打开原会话（codex --no-daemon resume ${peer.sessionId}），积压的消息会自动补发`}});
      if(same&&before.code===cause.code&&!before.successAt)return this.current(peer,endpoint)!;
    }
    const attempts=(same?before.attempts:0)+1,since=same?before.since:this.now();
    const value:Incident={id:same?before.id:randomUUID(),peerId:peer.id,scope:identity,...cause,state:['transient','unknown'].includes(cause.causeClass)?'observing':'blocked',since,lastFailureAt:this.now(),attempts,message:cause.code==='reply_socket_conflict'?describeError(error):'Partner operation has not succeeded ('+cause.code+').',affectedMessageIds:messageIds.slice(0,3)};
    value.retryAt=this.now()+(value.state==='observing'?delays[Math.min(attempts-1,delays.length-1)]!:900000);
    this.storage.setSetting(key(peer.id),value);this.storage.event('runtime.incident.failed',{peerId:peer.id,incidentId:value.id,layer:value.layer,causeClass:value.causeClass,code:value.code,attempts,retryAt:value.retryAt});return value;
  }
  succeeded(peer:Peer,endpoint?:Endpoint):void{
    const value=this.storage.setting<Incident>(key(peer.id));if(!value||value.closed||value.scope!==scope(peer,endpoint))return;
    value.successAt??=this.now();value.state='recovering';value.userText='搭档投递已恢复，正在观察稳定性。';delete value.retryAt;this.storage.setSetting(key(peer.id),value);
  }
  list(peers:{peer:Peer;endpoint?:Endpoint}[]):IncidentStatus[]{return peers.flatMap(({peer,endpoint})=>{const value=this.current(peer,endpoint);return value?[value]:[];});}
}
