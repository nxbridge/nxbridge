import type { Store } from '../../storage/store.js';
import {describeError} from '../../shared/errors.js';

export const CODEX_WAKE_RETRY_MS=30000,CODEX_WAKE_MAX_ATTEMPTS=3;
interface WakeReservation {scope:string;anchor:string;attempts:number;at:number;error?:string;exhausted?:boolean;turnId?:string}

                                    
export class CodexWakeSignals {
  private active=new Set<string>();
  constructor(private store:Store,private now=Date.now){}
  status(peerId:string):WakeReservation|undefined{return this.store.setting<WakeReservation>('codexWake:'+peerId);}
  private disposition(previous:WakeReservation|undefined,scope:string,anchor:string):'fresh'|'retry'|'waiting'|'exhaust'|'exhausted'{
    if(previous?.scope!==scope||previous.anchor!==anchor)return'fresh';
    if(previous.exhausted)return'exhausted';
    if(this.now()-previous.at<CODEX_WAKE_RETRY_MS)return'waiting';
    return previous.attempts>=CODEX_WAKE_MAX_ATTEMPTS?'exhaust':'retry';
  }
  ready(peerId:string,scope:string,anchor:string):boolean{
    if(this.active.has(peerId))return false;
    const state=this.disposition(this.status(peerId),scope,anchor);
    return state!=='waiting'&&state!=='exhausted';
  }
  clear(peerId:string):void{if(this.status(peerId))this.store.setSetting('codexWake:'+peerId,null);}
  restartCandidate(peerId:string,scope:string):WakeReservation|undefined{
    const value=this.status(peerId);
    if(!this.active.has(peerId)&&value?.scope===scope&&value.attempts===1&&value.error===undefined&&!value.exhausted)return value;
  }
  settleQuiesced(peerId:string,expected:WakeReservation):boolean{
    const current=this.restartCandidate(peerId,expected.scope);
    if(!current||current.anchor!==expected.anchor||current.at!==expected.at||current.turnId!==expected.turnId)return false;
    this.clear(peerId);this.store.event('wake.quiesced',{peerId,attempts:current.attempts,turnId:current.turnId});return true;
  }
  async signal(peerId:string,scope:string,anchor:string,submit:(accepted:(turnId:string)=>void)=>Promise<void|false>):Promise<void>{
    if(this.active.has(peerId))return;
    this.active.add(peerId);
    try{
      const previous=this.status(peerId),state=this.disposition(previous,scope,anchor);
      if(state==='waiting'||state==='exhausted')return;
      if(state==='exhaust'){
        this.store.setSetting('codexWake:'+peerId,{...previous!,exhausted:true});this.store.event('wake.exhausted',{peerId,attempts:previous!.attempts});
        return;
      }
      const reservation:WakeReservation={scope,anchor,attempts:state==='retry'?previous!.attempts+1:1,at:this.now()};
      this.store.setSetting('codexWake:'+peerId,reservation);
      try{
        const accepted=(turnId:string)=>{if(this.status(peerId)?.at===reservation.at){reservation.turnId=turnId;this.store.setSetting('codexWake:'+peerId,reservation);}};
        if(await submit(accepted)===false&&this.status(peerId)?.at===reservation.at)this.store.setSetting('codexWake:'+peerId,previous??null);
      }
      catch(error){if(this.status(peerId)?.at===reservation.at)this.store.setSetting('codexWake:'+peerId,{...reservation,error:describeError(error).slice(0,1200)});this.store.event('wake.unknown',{peerId,error:describeError(error).slice(0,1200)});}
    }finally{this.active.delete(peerId);}
  }
}
