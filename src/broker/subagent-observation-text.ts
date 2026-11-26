                                                    
                                                                          
                                                                           

export type SubagentObservationKind='active'|'checking'|'stale'|'ended';

const reasons:Record<SubagentObservationKind,string>={
                          
  active:'主线程这一轮还在进行，最近有进展',
                               
  checking:'nxbridge 刚重启，正在从会话记录核对主线程的进展',
                                             
  stale:'主线程这一轮很久没有观察到新进展',
                                      
  ended:'主线程这一轮已经结束，还没能确认它处理了子代理的结果',
};

export function subagentObservationReason(kind:SubagentObservationKind):string{return reasons[kind];}
