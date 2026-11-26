import {randomUUID} from 'node:crypto';
import type {Endpoint,Host,Pair,Peer} from '../types.js';
import type {Store} from '../storage/store.js';
import {readCodexTurnEvidence} from '../hosts/codex/codex-turn-evidence.js';
import type {CodexTurnCause,CodexTurnOrigin} from '../hosts/codex/codex-turn-origin.js';
import {CompletionWakeBudget,type CompletionWakeStatus} from './health/completion-wake-budget.js';
import type {DeliveryHealth} from './health/delivery.js';
import {noticeScope,type RuntimeNotice} from './health/contracts.js';
import {turnNativeScope,type TurnEndStats} from './turn-end-evidence.js';
import {completionGuardText,turnEndTip,type CompletionGuardView,type CompletionHold} from './turn-end-text.js';

export type CompletionReason='no_tools'|'work_after_message'|'unverified'|'reported';
interface Fact {
  version:1;pairId:string;sessionId:string;scope:string;state:'running'|'completed'|'unavailable';
  turnId?:string;at?:number;reason?:CompletionReason;stats?:TurnEndStats;observed?:string;noticeId?:string;terminal?:'failed'|'unverified';
}
interface Intervention {
  version:1;id:string;authority:string;scope:string;sessionId:string;turnId?:string;
  actions:number;sent:boolean;entered?:true;confirmed?:true;released?:true;kind:'stop'|'notice';backgroundId?:string;backgroundTurnId?:string;
}
interface ForegroundState {scope:string;turnId:string;busy:boolean;edge:string;at:number}
type Members=Pick<Role,'pair'|'background'|'foreground'>;
interface Role {pair:Pair;background:Peer;foreground:Peer;source:Endpoint;target:Endpoint}
export interface TurnEndView {
  background_id?:string;foreground_id?:string;background_host?:Host;
  state:'not_applicable'|'running'|'completed'|'pending'|'limited'|'unavailable';
  turn_id?:string;completed_at?:number;reason?:CompletionReason;hold?:CompletionHold;budget?:CompletionWakeStatus;user_text?:string;
}
interface Options {
  owned(peer:Peer):boolean;
  gate(background:Peer,foreground:Peer,boundaryPeerId?:string):CompletionHold|undefined;
  source(peer:Peer):string|undefined;
  stats(peer:Peer,turnId?:string,completed?:boolean):TurnEndStats|undefined;
}
const factKey=(peerId:string)=>'turnEndFact:'+peerId;
const segmentKey=(peerId:string)=>'turnEndIntervention:'+peerId;
const claimKey=(peerId:string)=>'turnEndStop:'+peerId;

                                          
export class TurnEndCoordinator {
  readonly budget:CompletionWakeBudget;
  private scans=new Map<string,Promise<void>>();
  constructor(private store:Store,private notices:DeliveryHealth,private options:Options,limit:()=>number){
    this.budget=new CompletionWakeBudget(store,limit);
    if(!store.setting('turnEndUpgrade:1'))store.transaction(()=>{
      for(const pair of store.pairs())for(const id of [pair.claude,pair.codex])notices.cancelSource(pair.id,id,'codex_final');
      store.retireFinalForwarding();
      store.setSetting('turnEndUpgrade:1',true);
    });
  }
  private members(peer:Peer):Members|undefined {
    const pair=this.store.pairFor(peer.id);if(!pair)return;
    const members=[this.store.peer(pair.claude),this.store.peer(pair.codex)];
    const background=members.find(member=>member.owner==='nxbridge'),foreground=members.find(member=>member.owner==='user');
    if(!background||!foreground||!members.every(member=>['root','fork'].includes(member.kind)))return;
    return{pair,background,foreground};
  }
  private role(peer:Peer):Role|undefined {
    const members=this.members(peer);if(!members)return;
    const {background,foreground}=members;
    if(!background.sessionId||!foreground.sessionId||!this.options.owned(background))return;
    const source=this.store.endpoint(background.id),target=this.store.endpoint(foreground.id);
    if(!source?.active||!target?.active)return;
    return{...members,source,target};
  }
  private foregroundState(peer:Peer,turnId:string,busy:boolean,at=Date.now()):void {
    if(this.members(peer)?.foreground.id!==peer.id)return;
    const endpoint=this.store.endpoint(peer.id);if(!endpoint)return;
    const key='turnEndForeground:'+peer.id,scope=turnNativeScope(peer,endpoint),old=this.store.setting<ForegroundState>(key);
    if(old?.scope===scope&&at<old.at)return;
    if(old?.scope===scope&&old.turnId===turnId&&old.busy===busy)return;
    const edge=!busy&&old?.scope===scope&&old.busy&&old.turnId===turnId?turnId:old?.scope===scope?old.edge:'initial';
    this.store.transaction(()=>{
      this.store.setSetting(key,{scope,turnId,busy,edge,at} satisfies ForegroundState);
      if(!busy&&old?.scope===scope&&old.busy&&old.turnId===turnId){
        const value=this.store.setting<Intervention>(segmentKey(peer.id));
                                                        
        if(value?.kind==='stop'&&!value.confirmed&&value.scope===scope&&value.turnId===turnId){value.released=true;this.store.setSetting(segmentKey(peer.id),value);}
      }
    });
  }
  stopped(peer:Peer,turnId:string):void {this.foregroundState(peer,turnId,false);}
  private foregroundEdge(peer:Peer):string {
    const endpoint=this.store.endpoint(peer.id),value=this.store.setting<ForegroundState>('turnEndForeground:'+peer.id);
    return JSON.stringify([endpoint?turnNativeScope(peer,endpoint):peer.sessionId,value?.edge??'initial']);
  }
  private fact(peer:Peer):Fact|undefined {
    const value=this.store.setting<Fact>(factKey(peer.id));return value?.version===1&&value.sessionId===peer.sessionId&&value.pairId===this.store.pairFor(peer.id)?.id?value:undefined;
  }
  private reason(role:Role,stats:TurnEndStats|undefined):CompletionReason {
    if(!stats?.covered)return'unverified';
    if(!stats.workTools)return'no_tools';
    const sent=this.store.lastMessageFacts(role.pair.id,role.background.id);
    return stats.lastWorkAt!==undefined&&stats.lastWorkAt>(sent?.createdAt??0)?'work_after_message':'reported';
  }
  private needsNotice(fact:Fact):boolean{return fact.state==='completed'&&fact.reason!=='reported'&&fact.observed!==fact.turnId;}
  private pendingStop(role:Role,fact:Fact):boolean {
    const value=this.store.setting<Intervention>(segmentKey(role.foreground.id));
    return value?.kind==='stop'&&!value.released&&value.sessionId===role.foreground.sessionId&&value.scope===turnNativeScope(role.foreground,role.target)&&
      value.backgroundId===role.background.id&&value.backgroundTurnId===fact.turnId;
  }
  private cancel(role:Members,fact:Fact):void {
    if(!fact.noticeId)return;
    this.notices.cancel(role.pair.id,role.foreground.id,fact.noticeId);
    this.budget.releaseUnsubmitted(role.foreground.id,fact.noticeId);
                                  
  }
  private retryHeld(role:Role,fact:Fact):boolean {
    const notice=fact.noticeId?this.notices.get(role.pair.id,role.foreground.id,fact.noticeId):undefined;
    return notice?.retryBeforeSubmit===true&&notice.completionRetry?.edge===this.foregroundEdge(role.foreground)&&notice.completionRetry.offers>=2;
  }
  private meta(other:Peer,recipient:Peer,remaining:number,limit:number,fact?:Fact){
    return{peerName:other.name,peerHost:other.host,recipientHost:recipient.host,remaining,limit,
      ...(fact?.at!==undefined?{completedAt:fact.at}:{}),...(fact?.reason?{reason:fact.reason}:{})};
  }
  begin(peer:Peer,turnId:string):void {
    this.foregroundState(peer,turnId,true);
    if(this.members(peer)?.foreground.id===peer.id){
      const previous=this.store.setting<Intervention>(segmentKey(peer.id));
                                                    
      if(previous?.kind==='stop'&&previous.turnId&&previous.turnId!==turnId)this.settle(peer,previous.turnId,undefined);
    }
    const role=this.role(peer);if(!role||role.background.id!==peer.id)return;
    const previous=this.fact(peer);
    if(previous?.turnId===turnId&&previous.state!=='running')return;
    if(previous?.state==='running'&&previous.turnId===turnId&&previous.scope===turnNativeScope(peer,role.source))return;
    if(previous)this.cancel(role,previous);
    this.store.setSetting(factKey(peer.id),{version:1,pairId:role.pair.id,sessionId:peer.sessionId!,scope:turnNativeScope(peer,role.source),state:'running',turnId} satisfies Fact);
  }
  native(peer:Peer,health:{turnId:string;state:string;completion?:string;lastEventAt?:number}|undefined):void {
    if(!health)return;
    this.foregroundState(peer,health.turnId,!['ready','failed'].includes(health.state),health.lastEventAt);
    if(health.state==='failed'||health.completion==='interrupted'){
      if(this.members(peer)?.foreground.id===peer.id)this.settle(peer,health.turnId,undefined);
      const role=this.role(peer);if(!role||role.background.id!==peer.id)return;
      const previous=this.fact(peer);if(previous?.state==='running'&&previous.turnId!==health.turnId)return;
      if(previous)this.cancel(role,previous);
      this.store.setSetting(factKey(peer.id),{version:1,pairId:role.pair.id,sessionId:peer.sessionId!,scope:turnNativeScope(peer,role.source),state:'unavailable',terminal:'failed',turnId:health.turnId} satisfies Fact);
    }else if(health.state!=='ready')this.begin(peer,health.turnId);
    else{
      const old=this.fact(peer);
      if(old?.state==='running'&&old.turnId===health.turnId)this.store.setSetting(factKey(peer.id),{...old,state:'unavailable',terminal:'unverified'} satisfies Fact);
    }
  }
  human(peer:Peer,id:string,at=Date.now()):void {
    if(this.members(peer)?.foreground.id===peer.id)this.budget.human(peer.id,{id:peer.sessionId+':'+id,at});
  }
  sent(peer:Peer):void {
    const endpoint=this.store.endpoint(peer.id),segment=this.store.setting<Intervention>(segmentKey(peer.id));
    if(endpoint&&segment?.sessionId===peer.sessionId&&segment.scope===turnNativeScope(peer,endpoint)&&!segment.sent){segment.sent=true;this.store.setSetting(segmentKey(peer.id),segment);}
  }
  private segment(peer:Peer,role:Role,id:string,kind:Intervention['kind'],turnId?:string,stats?:TurnEndStats):Intervention|undefined {
    const endpoint=peer.id===role.background.id?role.source:role.target;
    const scope=turnNativeScope(peer,endpoint),reserved=this.budget.admit(role.foreground.id,id,scope);if(!reserved)return;
    const value:Intervention={version:1,id,authority:role.foreground.id,scope,sessionId:peer.sessionId!,actions:stats?.actions??0,sent:false,kind,
      backgroundId:role.background.id,backgroundTurnId:this.fact(role.background)?.turnId,...(turnId?{turnId}:{})};
    if(turnId)this.budget.started(value.authority,id,scope,turnId);
    this.store.setSetting(segmentKey(peer.id),value);return value;
  }
  private settle(peer:Peer,turnId:string,stats:TurnEndStats|undefined,receipts?:{id:string;inert:boolean}[]):void {
    this.store.transaction(()=>{
      const endpoint=this.store.endpoint(peer.id),value=this.store.setting<Intervention>(segmentKey(peer.id));
      if(!endpoint||!value||value.sessionId!==peer.sessionId||value.scope!==turnNativeScope(peer,endpoint)||value.turnId&&value.turnId!==turnId)return;
      const receipt=receipts?.find(entry=>entry.id===value.id);
                                            
      if(!value.turnId&&!receipt)return;
      if(!this.budget.started(value.authority,value.id,value.scope,turnId))return;
      if(value.kind==='stop'&&value.backgroundId!==peer.id&&(receipt||value.confirmed))this.observeFact(peer,value);
      const inert=!value.sent&&stats?.covered===true&&stats.actions===value.actions&&(!receipt||receipt.inert);
      this.budget.completed(value.authority,value.id,value.scope,turnId,inert);
      this.store.setSetting(segmentKey(peer.id),null);
    });
  }
  complete(peer:Peer,input:{turnId:string;completedAt:number;stats?:TurnEndStats;interventions?:{id:string;inert:boolean}[]}):void {
    this.store.transaction(()=>{
      this.foregroundState(peer,input.turnId,false,input.completedAt);
      this.settle(peer,input.turnId,input.stats,input.interventions);
      const role=this.role(peer);if(!role||role.background.id!==peer.id)return;
      const old=this.fact(peer),scope=turnNativeScope(peer,role.source);
      if(old?.state==='running'&&old.turnId!==input.turnId)return;
      if(old?.turnId===input.turnId&&old.state==='unavailable'&&old.terminal!=='unverified')return;
      if(old?.state==='completed'&&old.turnId===input.turnId){
        if(old.scope===scope&&old.reason==='unverified'&&input.stats?.covered){old.reason=this.reason(role,input.stats);old.stats=input.stats;if(old.reason==='reported')this.cancel(role,old);}
        old.scope=scope;this.store.setSetting(factKey(peer.id),old);return;
      }
      if(old)this.cancel(role,old);
      const value:Fact={version:1,pairId:role.pair.id,sessionId:peer.sessionId!,scope,state:'completed',turnId:input.turnId,at:input.completedAt,
        reason:this.reason(role,input.stats),...(input.stats?{stats:input.stats}:{}),...(old?.observed===input.turnId?{observed:input.turnId}:{})};
      this.store.setSetting(factKey(peer.id),value);
      this.store.event('turn.end.completed',{peerId:peer.id,turnId:input.turnId,reason:value.reason});
    });
  }
  private observeFact(peer:Peer,value:Intervention):void {
    if(!value.backgroundId)return;
    const fact=this.store.setting<Fact>(factKey(value.backgroundId)),pair=this.store.pairFor(peer.id);
    if(fact?.state!=='completed'||fact.turnId!==value.backgroundTurnId||fact.pairId!==pair?.id)return;
    fact.observed=fact.turnId;
    this.cancel({pair,background:this.store.peer(value.backgroundId),foreground:peer},fact);
    this.store.setSetting(factKey(value.backgroundId),fact);
  }
  confirmStop(peer:Peer,turnId:string):boolean {
    return this.store.transaction(()=>{
      const endpoint=this.store.endpoint(peer.id),value=this.store.setting<Intervention>(segmentKey(peer.id));
      if(endpoint&&value?.kind==='stop'&&value.turnId===turnId&&value.scope===turnNativeScope(peer,endpoint)){
        value.confirmed=true;this.store.setSetting(segmentKey(peer.id),value);
        if(value.backgroundId!==peer.id)this.observeFact(peer,value);
        return true;
      }
      return false;
    });
  }
  checkStop(peer:Peer,input:{turn_id:string;stop_hook_active:boolean;stats:TurnEndStats;background?:{tasks?:number;crons?:number};deadline_at?:number}):{block?:string;intervention_id?:string} {
    if(input.stop_hook_active)return{};
    const role=this.role(peer);if(!role)return{};
    if(peer.host==='claude'&&(input.background?.tasks!==0||input.background?.crons!==0))return{};
    if(this.options.gate(role.background,role.foreground,peer.id))return{};
    const endpoint=peer.id===role.background.id?role.source:role.target;
    const scope=turnNativeScope(peer,endpoint),claim=this.store.setting<{scope:string;turnId:string}>(claimKey(peer.id));
    if(claim?.scope===scope&&claim.turnId===input.turn_id)return{};
    let kind:'background_zero'|'background_unreported'|'foreground_completed';
    let fact:Fact|undefined;
    if(peer.id===role.background.id){
      const ids=input.stats.partnerMessageIds;
      const driven=input.stats.partnerDriven===true&&(!ids?.length||ids.some(id=>{try{const m=this.store.message(id);return m.pairId===role.pair.id&&m.from===role.foreground.id&&m.to===peer.id;}catch{return false;}}));
      const sent=this.store.lastMessageFacts(role.pair.id,peer.id);
      if(input.stats.covered&&input.stats.workTools===0&&driven)kind='background_zero';
      else if(input.stats.lastWorkAt!==undefined&&input.stats.lastWorkAt>(sent?.createdAt??0))kind='background_unreported';
      else return{};
    }else{
      fact=this.fact(role.background);
      if(!fact||!this.needsNotice(fact)||fact.scope!==turnNativeScope(role.background,role.source))return{};
      kind='foreground_completed';
    }
    return this.store.transaction(()=>{
                                                    
      if(input.deadline_at!==undefined&&Date.now()>input.deadline_at)return{};
      const id='turn-stop:'+randomUUID(),value=this.segment(peer,role,id,'stop',input.turn_id,input.stats);if(!value)return{};
      if(peer.id===role.foreground.id)this.foregroundState(peer,input.turn_id,true);
      this.store.setSetting(claimKey(peer.id),{scope,turnId:input.turn_id,id});
                                                 
      const budget=this.budget.status(role.foreground.id),other=peer.id===role.background.id?role.foreground:role.background;
      this.store.event('turn.end.tip',{peerId:peer.id,turnId:input.turn_id,kind,interventionId:id});
      return{block:turnEndTip(kind,this.meta(other,peer,budget.remaining,budget.limit,fact)),intervention_id:id};
    });
  }
  refresh(pair:Pair):void {
    const role=this.role(this.store.peer(pair.codex));
    if(!role){
      for(const id of [pair.claude,pair.codex]){
        const background=this.store.peer(id),fact=this.fact(background);
        const foreground=this.store.peer(id===pair.claude?pair.codex:pair.claude);
        if(fact?.noticeId){
          const retry=this.notices.get(pair.id,foreground.id,fact.noticeId);if(retry?.retryBeforeSubmit)this.notSubmitted(foreground,[retry]);
          this.notices.cancel(pair.id,foreground.id,fact.noticeId);
        }
      }
      return;
    }
    let fact=this.fact(role.background);if(!fact)return;
    const retry=fact.noticeId?this.notices.get(pair.id,role.foreground.id,fact.noticeId):undefined;
    if(retry?.retryBeforeSubmit){this.notSubmitted(role.foreground,[retry]);fact=this.fact(role.background)!;}
    const budget=this.budget.status(role.foreground.id);
    if(!this.needsNotice(fact)){this.cancel(role,fact);return;}
    if(fact.scope!==turnNativeScope(role.background,role.source)||this.pendingStop(role,fact)||this.options.gate(role.background,role.foreground)||
      !this.budget.canAdmit(role.foreground.id,fact.noticeId,turnNativeScope(role.foreground,role.target))){
      if(fact.noticeId)this.notices.cancel(pair.id,role.foreground.id,fact.noticeId);return;
    }
    const created=!fact.noticeId;
    fact.noticeId??='background-completed:'+randomUUID();
    const message=turnEndTip('foreground_completed',this.meta(role.background,role.foreground,Math.max(0,budget.remaining-1),budget.limit,fact));
    const scope=noticeScope(pair,role.foreground,role.background,this.store.bindings.current(role.foreground.id)?.epoch,this.store.bindings.current(role.background.id)?.epoch);
    const notice:RuntimeNotice={id:fact.noticeId,message,state:'pending',source:'background_completed',kind:'background_completed:'+role.background.id,
      factKey:fact.turnId,tier:4,wake:true,readyOnly:true,scope,interventionId:fact.noticeId};
    this.notices.enqueue(pair.id,role.foreground.id,notice);
    this.notices.updatePending(pair.id,role.foreground.id,notice.id,message,scope,fact.turnId);
    if(created)this.store.setSetting(factKey(role.background.id),fact);
  }
  ready(pair:Pair,boundary=false):boolean {
    const role=this.role(this.store.peer(pair.codex)),fact=role?this.fact(role.background):undefined;
    return !!role&&!!fact&&this.needsNotice(fact)&&fact.scope===turnNativeScope(role.background,role.source)&&
      this.budget.canAdmit(role.foreground.id,fact.noticeId,turnNativeScope(role.foreground,role.target))&&!this.pendingStop(role,fact)&&!this.retryHeld(role,fact)&&!this.options.gate(role.background,role.foreground,boundary?role.foreground.id:undefined);
  }
  admitted(pair:Pair,notice:RuntimeNotice):void {
    const role=this.role(this.store.peer(pair.codex)),fact=role?this.fact(role.background):undefined;
    if(!role||!fact?.noticeId||notice.id!==fact.noticeId&&!notice.componentIds?.includes(fact.noticeId))return;
                                
    this.segment(role.foreground,role,fact.noticeId,'notice');
    this.notices.completionOffer(pair.id,role.foreground.id,fact.noticeId,this.foregroundEdge(role.foreground));
    fact.observed=fact.turnId;this.store.setSetting(factKey(role.background.id),fact);
    this.store.event('turn.end.notified',{peerId:role.foreground.id,source:role.background.id,turnId:fact.turnId,noticeId:fact.noticeId});
  }
  reservedReady(pair:Pair,noticeId:string):boolean {
    const role=this.role(this.store.peer(pair.codex)),fact=role?this.fact(role.background):undefined;
    return !!role&&fact?.noticeId===noticeId&&fact.state==='completed'&&fact.scope===turnNativeScope(role.background,role.source)&&
      this.budget.reservedAllowed(role.foreground.id,noticeId,turnNativeScope(role.foreground,role.target))&&!this.options.gate(role.background,role.foreground);
  }
  notSubmitted(peer:Peer,notices:RuntimeNotice[],unsubmittedTurnId?:string):void {
    const value=this.store.setting<Intervention>(segmentKey(peer.id));
    if(value?.kind!=='notice'||value.sessionId!==peer.sessionId||value.entered&&value.turnId!==unsubmittedTurnId||
      !notices.some(notice=>notice.id===value.id||notice.interventionId===value.id||notice.interventionIds?.includes(value.id)))return;
    this.store.transaction(()=>{
      if(!this.budget.beforeSubmitFailed(value.authority,value.id,value.scope,unsubmittedTurnId))return;
      if(value.entered||value.turnId){delete value.entered;delete value.turnId;this.store.setSetting(segmentKey(peer.id),value);}
      const fact=value.backgroundId?this.store.setting<Fact>(factKey(value.backgroundId)):undefined;
      if(fact?.noticeId===value.id&&fact.observed!==undefined){delete fact.observed;this.store.setSetting(factKey(value.backgroundId!),fact);}
    });
  }
  started(peer:Peer,turnId:string,noticeId?:string):void {
    const endpoint=this.store.endpoint(peer.id),value=this.store.setting<Intervention>(segmentKey(peer.id));
    if(!endpoint||!value||value.scope!==turnNativeScope(peer,endpoint)||value.kind!=='notice'||!value.entered||noticeId&&noticeId!==value.id)return;
    if(this.budget.started(value.authority,value.id,value.scope,turnId)){value.turnId=turnId;this.store.setSetting(segmentKey(peer.id),value);}
  }
  entered(peer:Peer,noticeId:string):void {
    const endpoint=this.store.endpoint(peer.id),value=this.store.setting<Intervention>(segmentKey(peer.id));
    if(endpoint&&value?.id===noticeId&&value.scope===turnNativeScope(peer,endpoint)&&!value.entered){value.entered=true;this.store.setSetting(segmentKey(peer.id),value);}
  }
  consumeQueued(peer:Peer,noticeId:string,turnId?:string):boolean {
    const role=this.role(peer),value=this.store.setting<Intervention>(segmentKey(peer.id)),fact=role?this.fact(role.background):undefined;
                                                   
    if(value?.kind==='notice'&&value.id===noticeId&&value.entered)return !!role&&peer.id===role.foreground.id&&
      value.scope===turnNativeScope(peer,role.target)&&!!turnId&&value.turnId===turnId&&fact?.noticeId===noticeId&&
      fact.state==='completed'&&fact.scope===turnNativeScope(role.background,role.source);
    if(!role||peer.id!==role.foreground.id||value?.id!==noticeId||value.kind!=='notice'||
      value.scope!==turnNativeScope(peer,role.target)||fact?.noticeId!==noticeId||fact.state!=='completed'||
      fact.scope!==turnNativeScope(role.background,role.source)||this.options.gate(role.background,role.foreground,peer.id)||
      !this.budget.reservedAllowed(value.authority,value.id,value.scope)){
      const pair=this.store.pairFor(peer.id);
      if(pair&&value?.kind==='notice'&&value.id===noticeId)this.store.transaction(()=>this.notSubmitted(peer,this.notices.outcome(pair.id,peer.id,noticeId,'pending'),turnId));
      return false;
    }
    this.entered(peer,noticeId);if(turnId)this.started(peer,turnId,noticeId);return true;
  }
  scan(peer:Peer,current?:()=>boolean):Promise<void> {
    if(peer.host!=='codex'||!peer.sessionId)return Promise.resolve();
    const existing=this.scans.get(peer.id);if(existing)return existing;
    const work=this.read(peer,current).finally(()=>{if(this.scans.get(peer.id)===work)this.scans.delete(peer.id);});this.scans.set(peer.id,work);return work;
  }
  private async read(peer:Peer,currentObservation?:()=>boolean):Promise<void> {
    const endpoint=this.store.endpoint(peer.id);if(!endpoint)return;
    const scope=turnNativeScope(peer,endpoint),before=JSON.stringify(this.fact(peer));
    const file=this.options.source(peer),evidence=await readCodexTurnEvidence(file,peer.sessionId!);
    const current=this.store.peer(peer.id),active=this.store.endpoint(peer.id);
                                        
    if(currentObservation?.()===false||!active||turnNativeScope(current,active)!==scope||JSON.stringify(this.fact(current))!==before||evidence.status!=='supported'||!evidence.caughtUp)return;
                                            
                                             
    const own=this.store.setting<{sessionId:string;ids:string[]}>('turnEndOrigins:'+peer.id);
    for(const turn of evidence.turns)if(turn.trigger==='user'&&turn.userInputs.length&&
      !(own?.sessionId===peer.sessionId&&turn.userInputs.some(id=>id!==null&&own.ids.includes(id))))this.human(peer,turn.turnId,turn.startedAt);
    const humanAt=this.budget.humanInputAt(peer.id);
    if(own?.sessionId===peer.sessionId&&humanAt!==undefined){
                                                     
                                            
      const past=new Set(evidence.turns.filter(turn=>turn.startedAt<=humanAt).flatMap(turn=>turn.userInputs)),ids=own.ids.filter(id=>!past.has(id));
      if(ids.length!==own.ids.length)this.store.setSetting('turnEndOrigins:'+peer.id,{...own,ids});
    }
    if(evidence.openTurnId){if(this.members(peer)?.background.id===peer.id)this.begin(peer,evidence.openTurnId);return;}
    const latest=evidence.turns.at(-1);
    if(latest&&latest.turnId===evidence.latestTurnId){
      if(this.fact(peer)?.state==='running'&&this.fact(peer)?.turnId!==latest.turnId)this.begin(peer,latest.turnId);
      this.complete(peer,{turnId:latest.turnId,completedAt:latest.completedAt,stats:this.options.stats(peer,latest.turnId,true)});
    }
  }
  async origin(peer:Peer,cause:CodexTurnCause):Promise<CodexTurnOrigin> {
    const id=randomUUID(),previous=this.store.setting<{sessionId:string;ids:string[]}>('turnEndOrigins:'+peer.id);
                                                             
    const ids=previous?.sessionId===peer.sessionId?previous.ids:[];
    this.store.setSetting('turnEndOrigins:'+peer.id,{sessionId:peer.sessionId,ids:[...ids,id]});
    return{turnTrigger:'nxbridge:'+cause+':'+id,clientUserMessageId:id};
  }
  status(peer:Peer):TurnEndView {
    const view=this.view(peer),user_text=completionGuardText(view as CompletionGuardView);
    return{...view,...(user_text?{user_text}:{})};
  }
  private view(peer:Peer):TurnEndView {
    const role=this.role(peer);if(!role)return{state:'not_applicable',hold:'no_foreground'};
    const fact=this.fact(role.background),budget=this.budget.status(role.foreground.id);
    const base={background_id:role.background.id,foreground_id:role.foreground.id,background_host:role.background.host,budget};
    if(budget.available===false)return{...base,state:'unavailable',hold:'budget_unavailable'};
    if(!fact)return{...base,state:'unavailable',hold:'native_state_unverified'};
    const value={...base,...(fact.turnId?{turn_id:fact.turnId}:{}),...(fact.at!==undefined?{completed_at:fact.at}:{}),...(fact.reason?{reason:fact.reason}:{})};
    if(fact.state!=='completed')return{...value,state:fact.state};
    const hold=this.options.gate(role.background,role.foreground);
    if(hold==='background_busy')return{...value,state:'running',hold};
    if(fact.scope!==turnNativeScope(role.background,role.source))return{...value,state:'unavailable',hold:'native_state_unverified'};
    if(!this.needsNotice(fact))return{...value,state:'completed'};
    if(this.pendingStop(role,fact))return{...value,state:'pending',hold:'foreground_tip_pending'};
    if(this.retryHeld(role,fact))return{...value,state:'pending',hold:'notice_retry_held'};
    if(!this.budget.canAdmit(role.foreground.id,fact.noticeId,turnNativeScope(role.foreground,role.target)))return{...value,state:'limited',hold:'wake_budget_exhausted'};
    const wait=hold??(fact.noticeId&&!this.notices.eligible(role.pair.id,role.foreground.id,fact.noticeId)?'notice_cooldown':undefined);
    return{...value,state:'pending',...(wait?{hold:wait}:{})};
  }
}
