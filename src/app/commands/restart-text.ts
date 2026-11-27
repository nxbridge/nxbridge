                                                             
export const restartResultCommand=(requestId:string)=>'nxbridge restart codex --result '+requestId;
                                         
export const restartRequestText=(requestId:string)=>'重启请求 '+requestId+' 已发出；这里没等到结果时，运行 '+restartResultCommand(requestId)+' 查询（只查结果，不会再重启）。';
                   
export const restartRequestIdText=(requestId:string)=>'（重启请求 '+requestId+'）';
               
export const restartPendingText=(requestId:string)=>'重启请求 '+requestId+' 还在进行，会话记录大的可能要几分钟；这里先不等了，没有重复重启。稍后运行 '+restartResultCommand(requestId)+' 查询结果。';
                                   
export const restartUnknownText=(requestId:string)=>'重启请求 '+requestId+' 的结果确认不了：协调器没有它的完成记录，这里没有重复重启。可以稍后运行 '+restartResultCommand(requestId)+' 再查一次；仍查不到时，用 nxbridge status 核对原会话，只看到新进程不能说明这次重启已完整完成。';
