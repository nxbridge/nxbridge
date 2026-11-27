                                
export function stopDeadline(operation,input){
  const at=operation==='turn-stop-check'&&input.stop_hook_active===false?input.deadline_at:undefined;
  if(at===undefined)return;
  let cancelled=false;
  return{
    cancel(){cancelled=true;},
    check(maximum=Infinity){
      const remaining=at-Date.now();
      if(cancelled||remaining<=0){cancelled=true;throw Error('Stop check deadline elapsed');}
      return Math.min(maximum,remaining);
    }
  };
}
