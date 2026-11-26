import type {HealthStorage} from './contracts.js';

export const COMPLETION_WAKE_LIMIT=4;
interface Reservation {
  id:string;scope:string;at:number;turnId?:string;
  retryable?:true;settled?:true;refunded?:true;
}
interface Budget {
  version:1;epoch:string;limit:number;used:number;human?:{id:string;at:number};reservations:Reservation[];
}
export interface CompletionWakeStatus {used:number;limit:number;remaining:number;awaitingProof:number;available?:false}
const key=(peerId:string)=>'completionWakeBudget:'+peerId;

                                         
export class CompletionWakeBudget {
  constructor(private storage:HealthStorage,private configuredLimit:number|(()=>number)=COMPLETION_WAKE_LIMIT){}
  private limit():number{return typeof this.configuredLimit==='function'?this.configuredLimit():this.configuredLimit;}
  private read(peerId:string):Budget|undefined {
    const value=this.storage.setting<Budget>(key(peerId));
                                                   
    if(value&&value.version!==1)return;
    return value??{version:1,epoch:'initial:'+peerId,limit:this.limit(),used:0,reservations:[]};
  }
  private ceiling(value:Budget):number{return Math.min(value.limit,this.limit());}
  private save(peerId:string,value:Budget):void {
                           
    const refunded=value.reservations.filter(entry=>entry.refunded).slice(-64);
    value.reservations=value.reservations.filter(entry=>!entry.refunded||refunded.includes(entry));
    this.storage.setSetting(key(peerId),value);
  }
  human(peerId:string,input:{id:string;at:number}):boolean {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return false;
                              
      if(value.human&&(value.human.id===input.id||value.human.at>=input.at))return false;
                                                
      const reservations=value.reservations.filter(entry=>entry.at>=input.at);
      this.save(peerId,{version:1,epoch:input.id,limit:this.limit(),used:reservations.filter(entry=>!entry.refunded).length,human:input,reservations});
      return true;
    });
  }
  admit(peerId:string,noticeId:string,scope:string):{epoch:string}|undefined {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return;
      const ceiling=this.ceiling(value),previous=value.reservations.find(entry=>entry.id===noticeId);
      if(previous){
        if(ceiling===0||value.used>ceiling||previous.scope!==scope||previous.settled||!previous.retryable)return;
        delete previous.retryable;this.save(peerId,value);return{epoch:value.epoch};
      }
      if(value.used>=ceiling)return;
      value.used++;value.reservations.push({id:noticeId,scope,at:Date.now()});
      this.save(peerId,value);return{epoch:value.epoch};
    });
  }
  canAdmit(peerId:string,noticeId:string|undefined,scope:string):boolean {
    const value=this.read(peerId);if(!value)return false;
    const ceiling=this.ceiling(value),entry=value.reservations.find(entry=>entry.id===noticeId);
    return entry?ceiling>0&&value.used<=ceiling&&entry.scope===scope&&!entry.settled&&entry.retryable===true:value.used<ceiling;
  }
  reservedAllowed(peerId:string,noticeId:string,scope:string):boolean {
    const value=this.read(peerId);if(!value)return false;
    const entry=value.reservations.find(entry=>entry.id===noticeId);
    return this.ceiling(value)>0&&value.used<=this.ceiling(value)&&entry?.scope===scope&&!entry.settled;
  }
  beforeSubmitFailed(peerId:string,noticeId:string,scope:string,unsubmittedTurnId?:string):boolean {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return false;
      const entry=value.reservations.find(entry=>entry.id===noticeId);
                                     
      if(!entry||entry.scope!==scope||entry.settled||entry.turnId&&entry.turnId!==unsubmittedTurnId)return false;
      if(entry.retryable)return true;
      delete entry.turnId;entry.retryable=true;this.save(peerId,value);return true;
    });
  }
  releaseUnsubmitted(peerId:string,noticeId:string):boolean {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return false;
      const entry=value.reservations.find(entry=>entry.id===noticeId);
      if(!entry?.retryable||entry.settled)return false;
      entry.settled=true;entry.refunded=true;value.used--;this.save(peerId,value);return true;
    });
  }
  started(peerId:string,noticeId:string,scope:string,turnId:string):boolean {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return false;
      const entry=value.reservations.find(entry=>entry.id===noticeId);
      if(!entry||entry.scope!==scope||entry.settled||entry.turnId&&entry.turnId!==turnId)return false;
      if(entry.turnId===turnId)return true;
      entry.turnId=turnId;delete entry.retryable;this.save(peerId,value);return true;
    });
  }
  completed(peerId:string,noticeId:string,scope:string,turnId:string,inert:boolean):boolean {
    return this.storage.transaction(()=>{
      const value=this.read(peerId);if(!value)return false;
      const entry=value.reservations.find(entry=>entry.id===noticeId);
      if(!entry||entry.scope!==scope||entry.turnId!==turnId||entry.settled)return false;
      entry.settled=true;delete entry.retryable;
      if(inert){entry.refunded=true;value.used--;}
      this.save(peerId,value);return inert;
    });
  }
  status(peerId:string):CompletionWakeStatus {
    const value=this.read(peerId);
    if(!value)return{used:0,limit:this.limit(),remaining:0,awaitingProof:0,available:false};
    const limit=this.ceiling(value);
    return{used:value.used,limit,remaining:Math.max(0,limit-value.used),
      awaitingProof:value.reservations.filter(entry=>!entry.settled).length};
  }
  humanInputAt(peerId:string):number|undefined{return this.read(peerId)?.human?.at;}
}
