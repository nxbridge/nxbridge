                                           
                                                                               
                                                                 
                                                                                                                         
                                                                                                            
                                                                                                                                                          

export type TurnEndTipKind='background_zero'|'background_unreported'|'foreground_completed';
export type CompletionReason='no_tools'|'work_after_message'|'unverified'|'reported';
export type CompletionHold='foreground_busy'|'background_busy'|'pending_user'|'paused'|'disabled'|'runtime_unavailable'|'native_state_unverified'|'wake_budget_exhausted'|'no_foreground'|'notice_cooldown'|'budget_unavailable'|'notice_retry_held'|'foreground_tip_pending';
type Host='claude'|'codex';
export interface TurnEndTipFields {peerName:string;peerHost:Host;recipientHost:Host;completedAt?:number|string;reason?:CompletionReason;remaining:number;limit:number}
export interface CompletionGuardView {
  background_id?:string;foreground_id?:string;state:'not_applicable'|'running'|'completed'|'pending'|'limited'|'unavailable';
  turn_id?:string;completed_at?:number|string;reason?:CompletionReason;hold?:CompletionHold;
  budget?:{used:number;limit:number;remaining:number;awaitingProof:number};
                                  
  background_host?:Host;
}

                          
function clock(at:number|string|undefined):string|undefined{
  if(at===undefined)return undefined;
  const date=new Date(at);if(Number.isNaN(date.getTime()))return undefined;
  return`${String(date.getHours()).padStart(2,'0')}:${String(date.getMinutes()).padStart(2,'0')}`;
}

const lastTurn:Record<CompletionReason,string>={no_tools:'no work-tool calls',work_after_message:'work not reported',unverified:'unverified',reported:'reported, then stopped'};

                              
export function turnEndTip(kind:TurnEndTipKind,fields:TurnEndTipFields):string{
  const peer=fields.peerName,codex=fields.recipientHost==='codex';
  const ask=codex?`message it via claude.send_message reply_expected=true w/ next step`:`message it w/ [reply] + next step`;
  if(kind==='background_zero')return`nxbridge: partner ${peer} is idle; this turn made no work-tool calls. You own open work -> do it now; ${peer} must act -> ${ask}; otherwise end without messaging`;
  if(kind==='background_unreported')return`nxbridge: partner ${peer} is idle and hasn't seen work you did after your last msg to it -> report ${codex?'via claude.send_message':'by msg'} before ending (${codex?'reply_expected=true':'[reply]'} only if it must act); nothing worth reporting -> end without messaging`;
  const at=clock(fields.completedAt);
  return`nxbridge: partner ${peer} ended its turn${at?` at ${at}`:''} and is idle; last turn: ${lastTurn[fields.reason??'unverified']}. It owns open work -> ${ask}; done or needs the user -> end without messaging (auto check-ins left: ${fields.remaining}/${fields.limit})`;
}

const reasonText:Record<CompletionReason,string>={no_tools:'这一轮没调用干活的工具',work_after_message:'做了事但还没汇报',unverified:'没能确认这一轮做了什么',reported:'已汇报'};
const holdText:Record<Exclude<CompletionHold,'wake_budget_exhausted'|'native_state_unverified'|'background_busy'|'budget_unavailable'>,string>={
  foreground_busy:'等前台这一轮结束后再提醒',pending_user:'等你处理完待回答的问题或待批准的操作后再提醒',paused:'搭档已暂停，暂不提醒',
  disabled:'nxbridge 已关闭，暂不提醒',runtime_unavailable:'前台暂时连不上，恢复后再提醒',no_foreground:'但没有可以提醒的前台窗口',notice_cooldown:'刚提醒过，暂不重复提醒',notice_retry_held:'提醒没能送进前台，等前台下次忙完再送',foreground_tip_pending:'正在确认前台是否收到了结束时的提醒，等前台这一轮收尾',
};

                                        
export function completionGuardText(view:CompletionGuardView):string|undefined{
  const who=view.background_host?`后台 ${view.background_host==='codex'?'Codex':'Claude'}`:'后台搭档';
  const at=clock(view.completed_at),stopped=`${who}${at?' '+at+' ':''}做完一轮停下了${view.reason?`（${reasonText[view.reason]}）`:''}`;
                                           
  if(view.hold==='budget_unavailable')return`${stopped}；自动提醒的次数记录读不懂（可能装过别的版本），已暂停自动提醒（运行 nxbridge doctor 查看）`;
  if(view.state==='limited'||view.hold==='wake_budget_exhausted'){
    if(view.budget!==undefined&&view.budget.limit<=0)return`${stopped}；自动提醒已在配置里关闭（collaboration.completion_wake_limit = 0）`;
    return`${stopped}；自动提醒已用完${view.budget?`（${view.budget.limit} 次）`:''}，在前台窗口发条消息就会恢复`;
  }
  if(view.state==='completed')return stopped;
  if(view.state==='pending'){
    if(view.hold==='native_state_unverified')return`还没能确认${who}的实际状态，暂不提醒`;
    if(view.hold===undefined||view.hold==='background_busy')return undefined;
    return`${stopped}，${holdText[view.hold]}`;
  }
  if(view.state==='unavailable')return`暂时无法跟踪${who}是否做完${view.hold==='runtime_unavailable'?'（连接暂时中断）':view.hold==='native_state_unverified'?'（没能确认它的实际状态）':''}`;
  return undefined;
}
