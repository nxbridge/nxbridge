                                                                        
                                  
                                                                                  

export type CodexDeliveryHoldCode='disabled'|'pair_paused'|'startup_route_pending'|'context_transition'|'hosting_transition'|'session_conflict'|'restart_in_progress'|'peer_retiring'|'profile_unverified'|'owned_subscription_waiting'|'session_busy'|'subagent_active'|'pending_user'|'runtime_unavailable'|'peer_paused'|'takeover_unverified'|'hosting_window_closed'|'hosting_awaiting_window'|'hosting_blocked'|'profile_pending_takeover'|'restart_incomplete';

const texts:Record<CodexDeliveryHoldCode,{status:string;model:string}>={
                                      
  disabled:{status:'nxbridge 在这一侧已关闭，消息先保留，重新打开后再送',model:'held: nxbridge off for this member; delivers after `nxbridge on`; no resend'},
                           
  pair_paused:{status:'配对已暂停，消息先保留，恢复后再送（nxbridge resume）',model:'held: pair paused; delivers after resume; no resend'},
                              
  startup_route_pending:{status:'协调器启动后正在核对这个会话的原生记录，核对完再送',model:'held: startup check of this session pending (bounded); delivers when it clears'},
                                
  context_transition:{status:'配对窗口正在切换上下文，切换完再送',model:'held: paired window switching context; delivers on next verified context'},
                                                          
  hosting_transition:{status:'这个会话正在转交给别的窗口或转回后台，转交完再送',model:'held: hosting transfer in progress; delivers after it completes'},
                                   
  session_conflict:{status:'发现另一个窗口也在用这个 Codex 会话，先不投递，请运行 nxbridge pair 选择',model:'held: session conflict on this runtime; user must resolve via `nxbridge pair`; no resend'},
                                          
  restart_in_progress:{status:'这个会话正在重启，重启完再送',model:'held: restart in progress; delivers after it finishes'},
                       
  peer_retiring:{status:'这个后台会话正在停用，暂不投递',model:'held: partner session retiring; delivery paused; re-pair if still needed'},
                             
  profile_unverified:{status:'还没确认这个后台用的模型、推理强度和服务档位，确认后再送',model:'held: partner model/effort/service tier not verified yet; delivers once verified'},
                                                
  owned_subscription_waiting:{status:'和这个后台会话的连接还没接上，nxbridge 会自动重新接上，接上后再送',model:'held: own subscription not attached; auto-reattach pending; its hook pickup may still deliver; no resend'},
                            
  session_busy:{status:'对方正在工作，下次调用工具或这一轮结束时会取到',model:'queued: partner busy; picked up at its next tool call or turn end'},
                           
  subagent_active:{status:'对方的子代理还在运行，主线程接着处理时再送',model:'queued: partner subagent running; delivers when its main thread resumes'},
                     
  pending_user:{status:'对方在等你批准操作或回答问题，处理完再送',model:'held: partner awaits user approval/answer; delivers after user acts'},
                  
  runtime_unavailable:{status:'暂时连不上对方的运行环境，恢复后再送',model:'held: partner runtime unavailable; delivers after recovery; no resend'},
                                
  peer_paused:{status:'对方会话已暂停（例如这一轮被中断），在那个窗口里继续或运行 nxbridge resume 后再送',model:'held: partner session paused (e.g. turn interrupted); delivers after the user continues in that window or runs `nxbridge resume`; no resend'},
                                                
  takeover_unverified:{status:'这个会话正开在一个窗口里，但没法确认是 nxbridge 打开的，先不送；那个窗口关掉后会自动接回再送',model:'held: session open in a window not provably ours; delivers after that window closes; no resend'},
                        
  hosting_window_closed:{status:'对方窗口已关闭：重新打开这个会话后送达，或下次发信时选择在后台继续',model:'held: partner window closed; delivers when reopened; next send asks the user (bg / wait / new / unpair); no resend'},
                       
  hosting_awaiting_window:{status:'后台已停，等你在自己的窗口打开这个会话后再送',model:'held: waiting for the user to open the session in their own window'},
                        
  hosting_blocked:{status:'会话暂时转不过去，消息先保留，原因和下一步见会话状态',model:'held: hosting blocked; see hosting reason + user step; no resend'},
                                        
  profile_pending_takeover:{status:'有模型设置的变更要等你关掉打开查看的窗口后才能生效，之前先不送',model:'held: pending profile change cannot apply while the session is open in a window; delivers after it closes'},
                                                       
  restart_incomplete:{status:'上次重启没能按原设置完成（例如服务档位），为安全先暂停投递；运行 nxbridge status 查看原因',model:'held: last restart did not complete with the original settings (e.g. service tier); safety pause; do NOT run `nxbridge resume`; see `nxbridge status`'},
};

export function codexDeliveryHoldText(code:CodexDeliveryHoldCode):{status:string;model:string}{return texts[code];}

                                                      
                                                              
                                                                    
export const TAKEOVER_VERIFY_STEPS={
  policy_verification_failed:'为安全不会采用窗口里改过的权限：请在那个窗口里把权限改回原来的设置',
  restart_profile_changed:'为安全只沿用窗口里改过的模型、推理强度和服务档位，其他会话设置（协作模式、指令、模型服务、目录、插件等）要和原来一致：请在那个窗口里改回原来的设置',
  takeover_profile_unverified:'等那个窗口里的设置改动记进会话记录后会自动接回，或在那个窗口里改回原来的设置',
} as const;
