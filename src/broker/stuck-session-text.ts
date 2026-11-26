                                                                      
                                                       
                                                                               
                                                                
                                   
                                                                                        
                                        
  
                                                                          
                                                                                                           
                                                                                 
                                                                          
                                                                                  
                                                                                                
                                                                                                        
                                                                               
                                                                                         
                                                                                    
                                                                           
                                                                                                

export type StuckTrigger='native_thread_wedged'|'lease_lost'|'cold_resume_exhausted'|'unload_overdue';
export type StuckProgress={state:'progressing'|'stalled'|'unknown';lastAt?:number|string};
                                                                                                 
export type StuckWork='active_turn'|'unknown_work'|'idle';
export type RecycleStage='preflight'|'stop'|'launch'|'verify';
export type RecycleBy='auto'|'partner'|'user';
export type StaleChange='progress_resumed'|'turn_changed'|'process_changed'|'authority_changed'|'preflight_changed'|'target_changed';
type Host='claude'|'codex';

                          
function clock(at:number|string|undefined):string|undefined{
  if(at===undefined)return undefined;
  const date=new Date(at);if(Number.isNaN(date.getTime()))return undefined;
  return`${String(date.getHours()).padStart(2,'0')}:${String(date.getMinutes()).padStart(2,'0')}`;
}
                           
function age(at:number|string|undefined,now=Date.now()):string|undefined{
  if(at===undefined)return undefined;
  const ms=now-new Date(at).getTime();if(!Number.isFinite(ms)||ms<0)return undefined;
  const s=Math.round(ms/1000);if(s<90)return`${s}s`;
  const m=Math.round(s/60);if(m<90)return`${m}m`;
  return`${Math.floor(m/60)}h${m%60?`${m%60}m`:''}`;
}
const hostName=(host:Host)=>host==='codex'?'Codex':'Claude';
                     
const peerLabel=(host:Host,name:string)=>name.toLowerCase()===host?hostName(host):`${hostName(host)} "${name}"`;
                                      
const userPeer=(host:Host,name:string)=>name.toLowerCase()===host?`后台 ${hostName(host)} `:`后台 ${hostName(host)}「${name}」`;
                                                           
const tool=(recipient:Host)=>recipient==='claude'?'recover_partner (deferred: ToolSearch select:mcp__nxbridge__recover_partner)':'claude.recover_partner';

                
const triggerAi:Record<StuckTrigger,string>={
  native_thread_wedged:'its native thread queue wedged; msgs can\'t enter',
  lease_lost:'channel lost on nxbridge restart and couldn\'t be reattached',
  cold_resume_exhausted:'same-session reload failed',
  unload_overdue:'session didn\'t unload when expected, so the channel couldn\'t reattach',
};
const triggerUser:Record<StuckTrigger,string>={
  native_thread_wedged:'会话卡死，消息进不去',
  lease_lost:'nxbridge 重启后没能重新接上它',
  cold_resume_exhausted:'重新接上原会话失败',
  unload_overdue:'会话没按时释放，接不上',
};
const stageAi:Record<RecycleStage,string>={preflight:'pre-stop restore check',stop:'stopping the old process',launch:'relaunch',verify:'verifying the restored session'};
const stageUser:Record<RecycleStage,string>={preflight:'恢复前检查',stop:'结束旧进程',launch:'重新启动',verify:'核对恢复结果'};
const staleAi:Record<StaleChange,string>={progress_resumed:'work progressed again',turn_changed:'a different turn is running',process_changed:'its process set changed',authority_changed:'ownership facts changed',preflight_changed:'the restore check result changed',target_changed:'the target runtime changed'};

               
function workAi(work:StuckWork,subagents:number):string{
  if(work==='active_turn')return`a turn is active${subagents?` (incl ${subagents} subagent${subagents>1?'s':''})`:''}`;
  if(work==='unknown_work')return'no active turn seen, but can\'t confirm it stopped';
  return'idle';
}
function waitAi(work:StuckWork):string{
  if(work==='active_turn')return'nxbridge recovers it automatically once that turn ends and it is confirmed idle';
  if(work==='unknown_work')return'nxbridge recovers it automatically only once it can confirm nothing is running (may never happen)';
  return'nxbridge recovers it automatically';
}
function lossAi(work:StuckWork):string{
  if(work==='active_turn')return'cuts the running turn (unsaved work lost)';
  if(work==='unknown_work')return'may cut work it can\'t see';
  return'nothing running is lost';
}
function progressAi(p:StuckProgress,now?:number):string{
  if(p.state==='progressing'){const a=age(p.lastAt,now);return`still progressing${a?` (last progress ${a} ago)`:''}`;}
  if(p.state==='stalled'){const at=clock(p.lastAt);return`no progress${at?` since ${at}`:''}`;}
  return'progress unknown';
}
function tipAi(p:StuckProgress,now?:number):string{
  if(p.state==='progressing')return'Still progressing -> wait unless the user needs it now';
  if(p.state==='stalled'){const a=age(p.lastAt,now);return`No progress${a?` for ${a}`:''}: waiting may never end -> recycle if you need it`;}
  return'Progress unknown -> read preview before deciding';
}

                                                                  
function workUser(work:StuckWork|undefined,tense:'now'|'then'):string{
  const then=tense==='then'?'当时':'';
  if(work==='active_turn')return`${then}它这一轮还在进行`;
  if(work==='unknown_work')return`${then}没看到进行中的一轮，但没法确认它已经停下`;
  if(work==='idle')return tense==='then'?'当时没在干活':'现在没在干活';
  return`${then}可能还在干活`;
}
              
function waitUser(work:StuckWork|undefined):string{
  if(work==='active_turn')return'这一轮做完、确认空闲后会自动恢复';
  if(work==='unknown_work')return'能确认它停下后才会自动恢复，也可能一直确认不了';
  if(work==='idle')return'会自动恢复';
  return'确认它停下后会自动恢复';
}
                        
const cutUser=(work:StuckWork|undefined)=>work==='active_turn'?'它正在做的那一轮':work==='unknown_work'?'可能还在跑的工作':work==='idle'?'':'它正在做的事';

export interface StuckNoticeFields {
  peerName:string;peerHost:Host;recipientHost:Host;trigger:StuckTrigger;since:number|string;progress:StuckProgress;
                           
  held:number;
                                                             
  work:StuckWork;subagents:number;
  incidentId:string;revision:string;
                         
  updated?:boolean;
  now?:number;
}
                                         
export function stuckNotice(f:StuckNoticeFields):string{
  const at=clock(f.since),who=peerLabel(f.peerHost,f.peerName);
  return[
    `nxbridge stuck-session ${f.updated?'update (replaces earlier notice; use this revision)':'notice'} (facts; not partner msg/user approval): ${who} stuck${at?` since ${at}`:''}: ${triggerAi[f.trigger]}.`,
    `Work: ${workAi(f.work,f.subagents)}; ${progressAi(f.progress,f.now)}.${f.held?` ${f.held} of your msgs held.`:''}`,
    `Wait -> ${waitAi(f.work)}. Recycle now -> ends its process + restores the same session; ${lossAi(f.work)}: ${tool(f.recipientHost)} action=recycle incident_id=${f.incidentId} evidence_revision=${f.revision} reason=<why>.`,
    `${tipAi(f.progress,f.now)}. Details: action=preview.`,
  ].join(' ');
}

                                                          
export function staleRevisionResult(f:{oldRevision:string;newRevision:string;changes:StaleChange[];work:StuckWork;subagents:number;progress:StuckProgress;held:number;now?:number}):string{
  const what=f.changes.map(c=>staleAi[c]).join('; ')||'facts changed';
  return`not applied: facts changed since revision ${f.oldRevision} (${what}). Now: ${workAi(f.work,f.subagents)}; ${progressAi(f.progress,f.now)}${f.held?`; ${f.held} msg${f.held>1?'s':''} held`:''}. Still want it -> recycle again w/ evidence_revision=${f.newRevision}; else wait.`;
}
                                                                                
                                                       
export function recycleAcceptanceQueuedResult(f:{revision:string}):string{
  return`accepted for evidence_revision=${f.revision}; nothing stopped yet: it runs on the next stable look with this same revision; if facts change you get a new notice instead and this acceptance lapses.`;
}
                                                                                                    
                                                       
export function recycleAcceptanceLapsedNotice(f:{peerName:string;peerHost:Host;revision:string;held:number}):string{
  return`nxbridge: your recycle acceptance for ${peerLabel(f.peerHost,f.peerName)} (evidence_revision=${f.revision}) lapsed: its state couldn't be verified in time; nothing was stopped; ${f.held} msg${f.held===1?'':'s'} held. Still want it -> accept the revision in the next notice.`;
}
                                                 
export type EvidenceSource='catalog'|'history'|'process'|'profile'|'rollout';
const evidenceSourceUser:Record<EvidenceSource,string>={catalog:'会话列表',history:'会话历史',process:'进程情况',profile:'原来的设置',rollout:'会话记录'};
export function evidenceUnverifiedReason(source:EvidenceSource|string,detail?:string):string{
  const name=(evidenceSourceUser as Record<string,string>)[source]??source,clean=detail?.replace(/[。．.；;，,\s]+$/,'');
  return`没法核验${name}${clean?`：${clean}`:''}`;
}
                                                                              
export function recycleRefusedResult(reason:string):string{
  return`not applied: ${reason}. Session kept as is; msgs stay held; nothing replayed. Don't retry this incident; tell the user only if it blocks their task.`;
}

export interface RecycleDoneFields {
  peerName:string;peerHost:Host;sessionId:string;by:RecycleBy;
                                    
  work:StuckWork;
                           
  delivered:number;
                               
  uncertainIds:string[];
}
                                                                                            
export function recycleDoneNotice(f:RecycleDoneFields):string{
  const who=peerLabel(f.peerHost,f.peerName),by=f.by==='auto'?'auto-recycled while idle':f.by==='partner'?'recycled on your request':'recycled on the user\'s request';
  const was=f.work==='active_turn'?'Its running turn was cut; unsaved work lost.':f.work==='unknown_work'?'No active turn seen, but unconfirmed work may have been cut.':'It was idle; nothing was cut.';
  const parts=[`nxbridge: ${who} recovered (same session ${f.sessionId.slice(0,8)}, ${by}). ${was}${f.delivered?` ${f.delivered} held msg${f.delivered>1?'s':''} delivered.`:''} Nothing replayed.`];
  if(f.uncertainIds.length)parts.push(`Delivery of these msgs of yours couldn't be confirmed (not resent): ${f.uncertainIds.join(', ')}. Before repeating one, check whether it already acted on it (ask it, or check its history); resend only what's missing (nxref:msg:<id> avoids retyping).`);
  if(f.work==='active_turn')parts.push('Cut work still needed -> send a new instruction; it won\'t resume on its own.');
  else if(f.work==='unknown_work')parts.push('If such work mattered -> check its history/output; it won\'t resume on its own.');
  else if(!f.uncertainIds.length)parts.push('No action needed.');
  return parts.join(' ');
}
                                                                               
export function recycleHeldNotice(f:{peerName:string;peerHost:Host;stage:RecycleStage;reason:string;held:number;userStep?:string}):string{
  return`nxbridge: recovering ${peerLabel(f.peerHost,f.peerName)} stopped at ${stageAi[f.stage]} before ending anything: ${f.reason}. Its process was not touched; ${f.held} msg${f.held===1?'':'s'} held; nothing replayed.${f.userStep?` User step: ${f.userStep}.`:''} Don't retry yourself; tell the user only if it blocks their task.`;
}

                                
                                                                                                                                                                               
                                                                                         
                                                                                                     

                                                                                                        
                                                                  
export function recycleStopFailedPendingNotice(f:{peerName:string;peerHost:Host;held:number}):string{
  return`nxbridge: ending ${peerLabel(f.peerHost,f.peerName)} didn't finish: its old process was still alive at the deadline; nothing relaunched; ${f.held} msg${f.held===1?'':'s'} held; nothing replayed. Its facts are still settling; a retry command follows once they're stable. Nothing to do yet.`;
}
                                                    
export function recycleStopFailedNotice(f:{peerName:string;peerHost:Host;recipientHost:Host;held:number;incidentId:string;revision:string}):string{
  return`nxbridge: ending ${peerLabel(f.peerHost,f.peerName)} didn't finish: its old process was still alive at the deadline; nothing relaunched; ${f.held} msg${f.held===1?'':'s'} held; nothing replayed. Try once more (may cut whatever it is running) -> ${tool(f.recipientHost)} action=recycle incident_id=${f.incidentId} evidence_revision=${f.revision} reason=<why>; else keep waiting.`;
}
                                                                        
export type AuthorityLoss='released_to_user'|'adopted'|'unpaired'|'member_disabled'|'foreign_holder';
const lossReasonAi:Record<AuthorityLoss,string>={released_to_user:'the session was released to the user\'s own window',adopted:'the session was taken over in a user window',unpaired:'the pair was unpaired',member_disabled:'nxbridge was turned off for it',foreign_holder:'another program now holds the session'};
const lossReasonUser:Record<AuthorityLoss,string>={released_to_user:'会话已转到你自己的窗口',adopted:'会话已在你的窗口里接手',unpaired:'配对已解除',member_disabled:'已对它关闭 nxbridge',foreign_holder:'会话被别的程序占用了'};
                                                                    
export type OldOutcome='ended'|'untouched'|'unknown';
const oldAi:Record<OldOutcome,string>={ended:'Its old process had already been ended.',untouched:'Its process was not touched.',unknown:'A stop signal may have been sent; whether its old process ended is unknown.'};
const oldUser:Record<OldOutcome,string>={ended:'旧进程已经结束',untouched:'旧进程没动',unknown:'可能已经发过结束信号，确认不了旧进程结束没有'};
export function recycleAuthorityLostNotice(f:{peerName:string;peerHost:Host;loss:AuthorityLoss;old:OldOutcome}):string{
  return`nxbridge: no longer recovering ${peerLabel(f.peerHost,f.peerName)}: ${lossReasonAi[f.loss]}. ${oldAi[f.old]} Held msgs follow the usual rules for that; nothing replayed.`;
}

                                                                                                                                                   
                                                                                                                               
                                                        
                                                                       
export function recycleCleanupBlockedNotice(f:{peerName:string;peerHost:Host;leftover?:number;held:number;userStep:string}):string{
  const what=f.leftover!==undefined&&f.leftover>0?`${f.leftover} process${f.leftover===1?'':'es'} it started ${f.leftover===1?'is':'are'} still running and unattested`:'processes it started may still be running (not confirmed gone)';
  return`nxbridge: ${peerLabel(f.peerHost,f.peerName)}'s old process ended, but ${what}, so nxbridge won't relaunch yet; it relaunches the same session automatically once they're gone. ${f.held} msg${f.held===1?'':'s'} held; nothing replayed. User action if it matters: ${f.userStep}. Don't retry yourself; tell the user only if it blocks their task.`;
}
                                             
export function recycleOldUnknownNotice(f:{peerName:string;peerHost:Host;reason:string;held:number;userStep:string}):string{
  return`nxbridge: a stop signal may have reached ${peerLabel(f.peerHost,f.peerName)}, but whether its old process ended can't be confirmed (${f.reason}); nxbridge won't relaunch until it can. ${f.held} msg${f.held===1?'':'s'} held; nothing replayed. User action if it matters: ${f.userStep}. Don't retry yourself; tell the user only if it blocks their task.`;
}
                        
export const RECYCLE_CLEANUP_USER_STEP='运行 nxbridge doctor 查看这些还在跑的程序；确认可以结束后自己结束它们，nxbridge 会接着按原会话重新启动';
export const RECYCLE_OLD_UNKNOWN_USER_STEP='运行 nxbridge doctor 查看旧进程的情况；能确认它已经结束后，nxbridge 会接着按原会话重新启动';
                                                                                          
export function recycleLaunchUnknownNotice(f:{peerName:string;peerHost:Host;reason:string;userStep:string}):string{
  return`nxbridge: recovering ${peerLabel(f.peerHost,f.peerName)} can't confirm whether its relaunch is running (${f.reason}); nxbridge won't launch it again. User action needed: ${f.userStep}. Don't retry yourself; tell the user in the originating window.`;
}
                             
export const RECYCLE_AMBIGUOUS_USER_STEP='运行 nxbridge doctor 查看；或者在你自己的 Codex 窗口里打开这个会话（会由你接手，这次恢复随之结束）';

export type StuckUserKind='auto_recovered'|'partner_recovered'|'user_recovered'|'held'|'no_partner'|'stop_failed'|'authority_lost'|'launch_unknown'|'cleanup_blocked'|'old_unknown';
                         
export type SuspendReason='broker_restarting'|'pair_paused'|'question_open'|'partner_changed';
const suspendUser:Record<SuspendReason,string>={broker_restarting:'nxbridge 正在重启或更新',pair_paused:'配对暂停了',question_open:'有问题在等回答',partner_changed:'搭档的会话变了'};
export interface StuckUserFields {
  peerName:string;peerHost:Host;trigger:StuckTrigger;since?:number|string;
                                                                                  
  work?:StuckWork;
  stage?:RecycleStage;reasonText?:string;userStep?:string;
                                                   
  loss?:AuthorityLoss;old?:OldOutcome;
                                                                                                
  leftover?:number;reclaiming?:boolean;
                       
  suspend?:SuspendReason;
                                         
  retryPending?:boolean;
}
                                                                          
export function stuckUserLine(kind:StuckUserKind,f0:StuckUserFields):string{
                                  
  const f={...f0,...(f0.reasonText?{reasonText:f0.reasonText.replace(/[。．.；;，,\s]+$/,'')}:{})};
  const who=userPeer(f.peerHost,f.peerName),at=clock(f.since),when=at?`${at} `:'',why=`（${triggerUser[f.trigger]}）`,cut=cutUser(f.work);
  if(kind==='auto_recovered')return`${who}${when}卡住了${why}，当时没在干活，nxbridge 已自动结束它并按原会话恢复`;
  if(kind==='partner_recovered'){
    if(f.work==='idle')return`${who}${when}卡住了${why}，搭档确认后已结束它并按原会话恢复（当时没在干活）`;
    return`${who}${when}卡住了${why}，${workUser(f.work,'then')}；搭档确认后已结束它并按原会话恢复，${f.work==='active_turn'?'这一轮中断了，没保存的进度丢失':'如果当时还有工作在跑，可能被一起中断了'}`;
  }
  if(kind==='user_recovered'){
    if(f.work==='idle')return`已按你的要求结束卡住的${who}并按原会话恢复（当时没在干活）`;
    return`已按你的要求结束卡住的${who}并按原会话恢复，${f.work==='active_turn'?'它正在做的那一轮中断了':'如果当时还有工作在跑，可能被一起中断了'}`;
  }
  if(kind==='held')return`${who}卡住了${why}，恢复停在「${stageUser[f.stage??'preflight']}」${f.reasonText?`：${f.reasonText}`:''}；还没动它，消息也保留着${f.userStep?`。${f.userStep}`:''}`;
  if(kind==='stop_failed')return`结束卡住的${who}没成功：到时限旧进程还在；没有重新启动，消息保留着${f.userStep?`。要再试一次（可能中断它正在做的事）：${f.userStep}`:f.retryPending?'；情况还在变化，稳定后会给出再试一次的命令':''}`;
  if(kind==='authority_lost')return`${who}的恢复已停止：${lossReasonUser[f.loss??'unpaired']}；${oldUser[f.old??'unknown']}，nxbridge 不再接着恢复`;
  if(kind==='cleanup_blocked'){
    if(f.reclaiming)return`卡住的${who}旧进程已经结束，正在清理它启动的程序，清理完按原会话重新启动`;
    const what=f.leftover!==undefined&&f.leftover>0?`它启动的 ${f.leftover} 个程序还在跑、认不出归属`:'它启动的程序可能还在跑，还没确认清理干净';
    return`卡住的${who}旧进程已经结束，但${what}，先不重新启动；它们结束后会自动按原会话重新启动。${f.userStep??RECYCLE_CLEANUP_USER_STEP}`;
  }
  if(kind==='old_unknown')return`结束信号可能已经发给卡住的${who.trimEnd()}，但确认不了旧进程结束没有${f.reasonText?`（${f.reasonText}）`:''}，确认前不会重新启动；${f.userStep??RECYCLE_OLD_UNKNOWN_USER_STEP}`;
  if(kind==='launch_unknown')return`恢复${who}时确认不了重新启动的进程在不在${f.reasonText?`（${f.reasonText}）`:''}，nxbridge 不会再启动一次；${f.userStep??RECYCLE_AMBIGUOUS_USER_STEP}`;
  return`${who}卡住了${why}，${workUser(f.work,'now')}；现在没有搭档能决定，先不动它，消息保留着，${waitUser(f.work)}${f.userStep?`。要现在结束并按原会话恢复${cut?`（会中断${cut}）`:''}：${f.userStep}`:''}`;
}

export type StuckPhase='observing'|'awaiting_partner'|'preflight'|'stop_requested'|'stopped'|'launch_requested'|'verifying'|'ready'|'held'
  |'suspended'|'reconnecting'|'handed_to_recovery'|'stop_failed'|'authority_lost'|'launch_unknown'|'cleanup_blocked'|'old_unknown';
                                                                        
                                                                            
                                                                                               
                                                                          
export function stuckStatusText(phase:StuckPhase,f:StuckUserFields):string|undefined{
  const who=userPeer(f.peerHost,f.peerName),why=`（${triggerUser[f.trigger]}）`;
  if(phase==='observing')return`${who}卡住了${why}，正在核对它是不是还在干活`;
  if(phase==='awaiting_partner')return`${who}卡住了${why}，${workUser(f.work,'now')}；已告诉搭档，等它决定：继续等（${waitUser(f.work)}），或现在结束并恢复`;
  if(phase==='held'||phase==='stop_failed'||phase==='authority_lost'||phase==='launch_unknown'||phase==='cleanup_blocked'||phase==='old_unknown')return stuckUserLine(phase,f);
  if(phase==='suspended')return`恢复卡住的${who}暂停在「${stageUser[f.stage??'stop']}」：${suspendUser[f.suspend??'broker_restarting']}；过去后自动接着做`;
  if(phase==='reconnecting')return`nxbridge 刚重启，正在按原会话重新接上${who.trimEnd()}，核对完原来的设置后再投递`;
  if(phase==='handed_to_recovery')return`卡住的${who}旧进程已经结束，正在按原会话重新启动`;
  if(phase==='ready')return undefined;
  const stage:RecycleStage=phase==='preflight'?'preflight':phase==='verifying'?'verify':phase==='launch_requested'?'launch':'stop';
  return`正在结束卡住的${who}并按原会话恢复（${stageUser[stage]}）`;
}

                                                         
export const RECOVER_PARTNER_DESCRIPTION='inspect or act on partner recovery. preview/result: read-only. retry: the single extra same-session managed launch after automatic attempts exhaust. recycle: only for a stuck-session notice — ends the stuck process + restores the same session (may cut whatever it is running); needs that notice\'s incident_id + evidence_revision; stale revision -> returns current facts, nothing done. retry/recycle need a recorded reason; never touch user-opened windows, config or approvals; repeating the same incident returns its existing result.';
                                                    
export const RECOVER_RECYCLE_HELP='recover codex [目标] --recycle --incident <事件ID> --revision <证据版本> --reason <原因>：结束卡住的后台 Codex 并按原会话恢复，可能中断它正在做的事（先用 recover codex --recycle --dry-run 查看事件ID和证据版本）';
