export const STARTUP_CATCHUP_WAIT_MS=8000,STARTUP_ROUTE_CEILING_MS=120000;
export const STARTUP_SERVING_METHODS=new Set(['ping','status','receive','messages.read','send']);
type Recovery={phase:'pending'|'degraded';since:number;nextCheck:number;timer:NodeJS.Timeout};

                                              
export class StartupRoutes {
  private routes=new Map<string,Recovery>();
  constructor(private degraded:(memberId:string)=>void,private ceiling=STARTUP_ROUTE_CEILING_MS){}
  begin(ids:Iterable<string>):void{
    for(const id of ids){
      if(this.routes.has(id))continue;
      const state:Recovery={phase:'pending',since:Date.now(),nextCheck:0,timer:setTimeout(()=>this.fail(id),this.ceiling)};
      state.timer.unref();this.routes.set(id,state);
    }
  }
  get pending():boolean{for(const state of this.routes.values())if(state.phase==='pending')return true;return false;}
  blocked(id:string):boolean{return this.routes.get(id)?.phase==='pending';}
  incomplete(id:string):boolean{return this.routes.has(id);}
  claimCheck(id:string):boolean{
    const state=this.routes.get(id);if(!state)return true;
    if(Date.now()<state.nextCheck)return false;
    state.nextCheck=Date.now()+5000;return true;
  }
  finish(id:string):void{const state=this.routes.get(id);if(state)clearTimeout(state.timer);this.routes.delete(id);}
  fail(id:string):void{
    const state=this.routes.get(id);if(!state||state.phase==='degraded')return;
    clearTimeout(state.timer);state.phase='degraded';this.degraded(id);
  }
  status(ids?:readonly string[]){
    return [...this.routes].filter(([id])=>!ids||ids.includes(id)).map(([memberId,state])=>({memberId,phase:state.phase,since:state.since,
      reason:state.phase==='pending'?'协调器启动后正在核对这个会话的原生记录；消息先排队，核对完再送达。':'核对会话记录超时，仍在后台重试；新消息照常投递，结果不明的旧投递不重发。'}));
  }
  close():void{for(const state of this.routes.values())clearTimeout(state.timer);this.routes.clear();}
}

                                 
export async function waitForStartupCatchup(work:Promise<unknown>,budget=STARTUP_CATCHUP_WAIT_MS):Promise<void>{
  let timer:NodeJS.Timeout|undefined;
  try{await Promise.race([work,new Promise<void>(resolve=>{timer=setTimeout(resolve,budget);})]);}
  finally{clearTimeout(timer);}
}
