import type {HealthStorage} from './contracts.js';

export const COMPLETION_WAKE_LIMIT=4;
interface ReservationIdentity {id:string;scope:string;at:number}
type Reservation=ReservationIdentity&(
  | {state:'reserved'}
  | {state:'retryable'}
  | {state:'started';turnId:string}
  | {state:'completed';turnId:string}
  | {state:'refunded';turnId:string}
  | {state:'released'}
);
interface Budget {
  version:2;epoch:string;limit:number;used:number;human?:{id:string;at:number};reservations:Reservation[];
}
interface LegacyReservation extends ReservationIdentity {turnId?:string;retryable?:true;settled?:true;refunded?:true;released?:true}
interface LegacyBudget extends Omit<Budget,'version'|'reservations'> {version:1;reservations:LegacyReservation[]}
export interface CompletionWakeStatus {used:number;limit:number;remaining:number;awaitingProof:number;available?:false}
const key=(peerId:string)=>'completionWakeBudget:'+peerId;
const pending=(entry:Reservation)=>entry.state==='reserved'||entry.state==='retryable'||entry.state==='started';
const returned=(entry:Reservation)=>entry.state==='refunded'||entry.state==='released';
const charge=(entry:Reservation|undefined)=>entry&&!returned(entry)?1:0;
function normalize(value:LegacyBudget):Budget {
  return{...value,version:2,reservations:value.reservations.map((entry):Reservation=>{
    const identity={id:entry.id,scope:entry.scope,at:entry.at};
                                                      
    if(entry.released)return{...identity,state:'released'};
    if(entry.settled)return{...identity,state:entry.refunded?'refunded':'completed',turnId:entry.turnId!};
    if(entry.turnId)return{...identity,state:'started',turnId:entry.turnId};
    return{...identity,state:entry.retryable?'retryable':'reserved'};
  })};
}

                                         
export class CompletionWakeBudget {
  constructor(private storage:HealthStorage,private configuredLimit:number|(()=>number)=COMPLETION_WAKE_LIMIT){}
  private limit():number{return typeof this.configuredLimit==='function'?this.configuredLimit():this.configuredLimit;}
  private read(peerId:string):Budget|undefined {
    const value=this.storage.setting<Budget|LegacyBudget>(key(peerId));
                                                   
    if(value&&value.version!==1&&value.version!==2)return;
    return value?.version===1?normalize(value):value??{version:2,epoch:'initial:'+peerId,limit:this.limit(),used:0,reservations:[]};
  }
  private ceiling(value:Budget):number{return Math.min(value.limit,this.limit());}
  private transition(value:Budget,previous:Reservation|undefined,next:Reservation):void {
    value.used+=charge(next)-charge(previous);
    if(previous)value.reservations[value.reservations.indexOf(previous)]=next;
    else value.reservations.push(next);
  }
  private save(peerId:string,value:Budget):void {
                           
    const refunded=value.reservations.filter(returned).slice(-64);
    value.reservations=value.reservations.filter(entry=>!returned(entry)||refunded.includes(entry));
    this.storage.setSetting(key(peerId),value);
  }
  human(peerId:string,input:{id:string;at:number}):boolean {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return false;
                              
      if(value.human&&(value.human.id===input.id||value.human.at>=input.at))return false;
                                                
      const reservations=value.reservations.filter(entry=>entry.at>=input.at);
      this.save(peerId,{version:2,epoch:input.id,limit:this.limit(),used:reservations.reduce((used,entry)=>used+charge(entry),0),human:input,reservations});
      return true;
    });
  }
  admit(peerId:string,noticeId:string,scope:string):{epoch:string}|undefined {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return;
      const ceiling=this.ceiling(value),previous=value.reservations.find(entry=>entry.id===noticeId);
      if(previous){
        if(previous.state==='released'&&previous.scope===scope&&value.used<ceiling){
          this.transition(value,previous,{id:previous.id,scope,at:Date.now(),state:'reserved'});
          this.save(peerId,value);return{epoch:value.epoch};
        }
        if(ceiling===0||value.used>ceiling||previous.scope!==scope||previous.state!=='retryable')return;
        this.transition(value,previous,{...previous,state:'reserved'});this.save(peerId,value);return{epoch:value.epoch};
      }
      if(value.used>=ceiling)return;
      this.transition(value,undefined,{id:noticeId,scope,at:Date.now(),state:'reserved'});
      this.save(peerId,value);return{epoch:value.epoch};
    });
  }
  canAdmit(peerId:string,noticeId:string|undefined,scope:string):boolean {
    const value=this.read(peerId);if(!value)return false;
    const ceiling=this.ceiling(value),entry=value.reservations.find(entry=>entry.id===noticeId);
    return entry?entry.scope===scope&&(entry.state==='released'?value.used<ceiling:ceiling>0&&value.used<=ceiling&&entry.state==='retryable'):value.used<ceiling;
  }
  reservedAllowed(peerId:string,noticeId:string,scope:string):boolean {
    const value=this.read(peerId);if(!value)return false;
    const entry=value.reservations.find(entry=>entry.id===noticeId);
    const ceiling=this.ceiling(value);
    return ceiling>0&&value.used<=ceiling&&entry?.scope===scope&&pending(entry);
  }
  beforeSubmitFailed(peerId:string,noticeId:string,scope:string,unsubmittedTurnId?:string):boolean {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return false;
      const entry=value.reservations.find(entry=>entry.id===noticeId);
                                     
      if(!entry||entry.scope!==scope||!pending(entry)||entry.state==='started'&&entry.turnId!==unsubmittedTurnId)return false;
      if(entry.state==='retryable')return true;
      this.transition(value,entry,{id:entry.id,scope,at:entry.at,state:'retryable'});this.save(peerId,value);return true;
    });
  }
  releaseUnsubmitted(peerId:string,noticeId:string):boolean {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return false;
      const entry=value.reservations.find(entry=>entry.id===noticeId);
      if(entry?.state!=='retryable')return false;
                                             
      this.transition(value,entry,{...entry,state:'released'});this.save(peerId,value);return true;
    });
  }
  started(peerId:string,noticeId:string,scope:string,turnId:string):boolean {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return false;
      const entry=value.reservations.find(entry=>entry.id===noticeId);
      if(!entry||entry.scope!==scope||!pending(entry))return false;
      if(entry.state==='started')return entry.turnId===turnId;
      this.transition(value,entry,{...entry,state:'started',turnId});this.save(peerId,value);return true;
    });
  }
  completed(peerId:string,noticeId:string,scope:string,turnId:string,inert:boolean):boolean {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return false;
      const entry=value.reservations.find(entry=>entry.id===noticeId);
      if(entry?.state!=='started'||entry.scope!==scope||entry.turnId!==turnId)return false;
      this.transition(value,entry,{...entry,state:inert?'refunded':'completed'});
      this.save(peerId,value);return inert;
    });
  }
  status(peerId:string):CompletionWakeStatus {
    const value=this.read(peerId);
    if(!value)return{used:0,limit:this.limit(),remaining:0,awaitingProof:0,available:false};
    const limit=this.ceiling(value);
    return{used:value.used,limit,remaining:Math.max(0,limit-value.used),
      awaitingProof:value.reservations.filter(pending).length};
  }
  humanInputAt(peerId:string):number|undefined{return this.read(peerId)?.human?.at;}
}
