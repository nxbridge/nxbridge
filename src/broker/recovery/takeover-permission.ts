import type {Store} from '../../storage/store.js';
import type {Endpoint,Pair,Peer} from '../../types.js';
import {sameTakeoverPermission,sameTakeoverHistory,sameTakeoverScope,sameTakeoverModelSettings,takeoverActualPolicy,takeoverPermissionFields,type TakeoverPermissionEvidence,type TakeoverPermissionState,type TakeoverPermissionView,type AcceptedTakeoverPermission,type TakeoverRestoreIntent,type TakeoverRestoreUnavailable} from '../../hosts/takeover-permission.js';
import {BridgeError,DeliveryError,RuntimeRecoveryHold} from '../../shared/errors.js';
import {sameDirectory} from '../../shared/files.js';
import type {DeliveryHealth} from '../health/delivery.js';
import type {Takeover} from './takeover.js';
import {takeoverPermissionNotice,takeoverPermissionResult} from './takeover-permission-copy.js';

interface Recipient {pairId:string;id:string;sessionId:string}
interface NoticeRef extends Recipient {noticeId:string}
type LegacyRestore =
  | {kind:'queued';requester:Recipient}
  | {kind:'checking';requester?:Recipient;intent:TakeoverRestoreIntent;reported?:NoticeRef};
interface Choice {
  id:string;original:TakeoverPermissionEvidence;final:TakeoverPermissionEvidence;state:TakeoverPermissionState;
  unavailable?:TakeoverRestoreUnavailable;offers?:NoticeRef[];requestPair?:string;attempt?:number;
  error?:string;errorPhase?:TakeoverPermissionView['error_phase'];actual?:TakeoverPermissionEvidence;correction?:true;
                          
  restore?:LegacyRestore;submitAt?:number;submitOffset?:number;requestedPairId?:string;noticed?:true;
}
interface InFlight {
  choice:Choice;
  intent:TakeoverRestoreIntent;
  reported?:NoticeRef|NoticeRef[];
  openedDuringCheck?:true;
  probe?:{generation:string;attempts:number;preflightFailures?:number;nextAt:number};
}
interface PermissionState {
  sessionId:string;
  closing?:{id:string;opened:TakeoverPermissionEvidence};
  choice?:Choice;
  inFlight?:InFlight;
  carry?:Choice;
  carryOriginal?:TakeoverPermissionEvidence;
  accepted?:AcceptedTakeoverPermission;
}
interface PermissionHost {
  observe:(peer:Peer,opened:TakeoverPermissionEvidence)=>Promise<TakeoverPermissionEvidence>;
  probe:(peer:Peer,endpoint:Endpoint,before:TakeoverPermissionEvidence,guard:()=>void,submitting:()=>void)=>Promise<TakeoverPermissionEvidence>;
  probeGeneration?:(peer:Peer,endpoint:Endpoint)=>string|undefined;
  unavailable:(original:TakeoverPermissionEvidence,closed:TakeoverPermissionEvidence)=>TakeoverRestoreUnavailable|undefined;
  references?:(peer:Peer)=>TakeoverPermissionEvidence[];
  restore:(peer:Peer,endpoint:Endpoint,original:TakeoverPermissionEvidence,closed:TakeoverPermissionEvidence,guard:()=>void,submitting:(intent:TakeoverRestoreIntent)=>void)=>Promise<TakeoverPermissionEvidence>;
}
interface PermissionLifecycle {
  active:(id:string)=>boolean;
  available:(peer:Peer,opening?:true)=>boolean;
  authorized:(peer:Peer,endpoint:Endpoint,pair:Pair)=>boolean;
  occupy:(id:string)=>()=>void;
  stopped:()=>boolean;
}
const key=(id:string)=>'takeoverPermission:'+id;
const UNCONFIRMED_AFTER_MS=10000;
const PROBE_BACKOFF_MS=[2000,8000,30000];
const PROBE_PREFLIGHT_BACKOFF_MS=[2000,8000,60000];
const exhaustedProbe=(choice:Choice,flight?:InFlight)=>flight?.choice===choice&&flight.probe
  &&(flight.probe.attempts>=PROBE_BACKOFF_MS.length||(flight.probe.preflightFailures??0)>=PROBE_PREFLIGHT_BACKOFF_MS.length);
const reportRefs=(flight?:InFlight)=>Array.isArray(flight?.reported)?flight.reported:flight?.reported?[flight.reported]:[];
function resultNoticeId(choice:Choice,flight?:InFlight):string{
  const exhausted=exhaustedProbe(choice,flight);
  const id='takeover-permission:'+choice.id+':'+choice.state+':'+(choice.attempt??1);
  return exhausted?id+':probe-exhausted':id;
}
const pending=(choice?:Choice)=>choice?.state==='pending'||choice?.state==='restore_pending';
const sameRecipient=(left?:Recipient,right?:Recipient)=>!!left&&!!right&&left.pairId===right.pairId&&left.id===right.id&&left.sessionId===right.sessionId;
const sameSnapshot=(left:TakeoverPermissionEvidence,right:TakeoverPermissionEvidence)=>sameTakeoverScope(left,right)&&sameTakeoverPermission(left.policy,right.policy);
function migrate(state:PermissionState):PermissionState{
  for(const slot of ['choice','carry'] as const){
    const choice=state[slot];if(!choice)continue;
    const legacy=choice.restore,requestPair=legacy?.requester?.pairId??choice.requestedPairId;
    const intent=legacy?.kind==='checking'?legacy.intent:choice.submitOffset===undefined?undefined:
      {at:choice.submitAt??0,before:{...choice.final,offset:choice.submitOffset}};
    if(requestPair)choice.requestPair=requestPair;
    delete choice.restore;delete choice.requestedPairId;delete choice.submitAt;delete choice.submitOffset;
    if(intent&&!state.inFlight){
      state.inFlight={choice,intent,...(legacy?.kind==='checking'&&legacy.reported?{reported:legacy.reported}:{})};
      delete state[slot];
    }
  }
  return state;
}
const changeView=(choice:Choice,flight?:InFlight):TakeoverPermissionView=>({
  change_id:choice.id,state:choice.state,changed_fields:takeoverPermissionFields(choice.original.policy,choice.final.policy),
  restore_available:!choice.unavailable,...(choice.unavailable?{restore_unavailable_reason:choice.unavailable}:{}),
  ...(choice.error?{error_code:choice.error}:{}),...(choice.errorPhase?{error_phase:choice.errorPhase}:{}),
  ...(choice.actual?{actual_policy:takeoverActualPolicy(choice.actual.policy)}:{}),...(choice.correction?{correction:true}:{}),
  ...(exhaustedProbe(choice,flight)?{recheck_via_open:true}:{}),
});

function submissionError(error:unknown,hasIntent:boolean):{kind:'retry'|'failed'|'superseded'|'unknown';code:string}{
  const code=String((error as BridgeError|null)?.code??'policy_unverified');
  if(hasIntent)return{kind:error instanceof DeliveryError&&!error.uncertain?'failed':'unknown',code};
  if(code==='session_changed'||code==='policy_changed')return{kind:'superseded',code};
  if(error instanceof RuntimeRecoveryHold||['timeout','disconnected','rpc_backpressure','handoff_sealed','session_busy','response_too_large','route_unavailable','ambiguous_endpoint','context_transition'].includes(code))
    return{kind:'retry',code};
  return{kind:'failed',code};
}

                                     
export class TakeoverPermissions {
  private readonly running=new Map<string,Promise<void>>();
  constructor(private store:Store,private delivery:DeliveryHealth,private hosts:Partial<Record<Peer['host'],PermissionHost>>,private lifecycle:PermissionLifecycle){}
  private state(peer:Peer):PermissionState|undefined{
    const state=this.store.setting<PermissionState>(key(peer.id));
    return peer.owner==='nxbridge'&&state?.sessionId===peer.sessionId?migrate(state):undefined;
  }
  private current(id:string):PermissionState|undefined{const peer=this.store.getPeer(id);return peer&&this.state(peer);}
  private save(peer:Peer,state:PermissionState):void{this.store.setSetting(key(peer.id),state);}
  view(peer:Peer):TakeoverPermissionView|undefined{
    const state=this.state(peer),choice=state?.choice??state?.inFlight?.choice;
    if(choice)return changeView(choice,state?.inFlight);
  }
  holdsDelivery(id:string):boolean{return !!this.current(id)?.inFlight;}
  blocksControl(id:string):boolean{
    const flight=this.current(id)?.inFlight;
    return !!flight&&Date.now()-flight.intent.at<UNCONFIRMED_AFTER_MS;
  }
  accepted(id:string):AcceptedTakeoverPermission|undefined{return this.current(id)?.accepted;}
  tracked(lease:Takeover,peer:Peer):boolean{
    return !!this.hosts[peer.host]&&peer.owner==='nxbridge'&&lease.sessionId===peer.sessionId&&lease.kind==='remote'&&!lease.adopted&&!!lease.permissionBaseline;
  }
  opened(lease:Takeover,peer:Peer):void{
    if(this.hosts[peer.host]&&lease.kind==='remote'&&!lease.adopted&&this.blocksControl(peer.id))
      throw new BridgeError('restore_unconfirmed','正在核验上次权限恢复，请稍后打开。');
  }
  foregroundMayHaveOpened(lease:Takeover,peer:Peer):void{
    const state=this.state(peer);
    if(!state||!this.hosts[peer.host]||lease.kind!=='remote'||lease.adopted)return;
    if(state.inFlight)state.inFlight.openedDuringCheck=true;
    if(!this.tracked(lease,peer)){if(state.inFlight)this.save(peer,state);return;}
    if(state.closing){
      state.carryOriginal??=state.carry?.original??state.closing.opened;
      delete state.closing;
    }
    if(pending(state.choice)){
      const choice=state.choice!;
      state.carry=choice;
      state.choice={...choice,state:'superseded'};
      if(choice.state==='restore_pending')this.notifyResult(peer,state.choice);
    }
    this.save(peer,state);
  }
  returned(lease:Takeover,peer:Peer):void{
    if(!this.tracked(lease,peer))return;
    const state=this.state(peer)??{sessionId:lease.sessionId};
    this.save(peer,{...state,closing:{id:lease.id,opened:lease.permissionBaseline!}});
  }
  private recipient(peer:Peer):Recipient|undefined{
    const pair=this.store.pairFor(peer.id);if(!pair)return;
    const id=pair[peer.host==='codex'?'claude':'codex'],sessionId=this.store.getPeer(id)?.sessionId;
    if(sessionId)return{pairId:pair.id,id,sessionId};
  }
  private notify(recipient:Recipient|undefined,peer:Peer,choice:Choice,result=false,flight?:InFlight):NoticeRef|undefined{
    if(!recipient)return;
    const source=result?'takeover_permission_result':'takeover_permission_change';
    const noticeId=result?resultNoticeId(choice,flight):'takeover-permission:'+choice.id+':'+recipient.sessionId;
    const view=changeView(choice,flight),partner={host:peer.host,name:peer.name,member_id:peer.id};
    const message=result?takeoverPermissionResult(view,partner):takeoverPermissionNotice(view,partner);
    if(!message)return;
    this.delivery.enqueue(recipient.pairId,recipient.id,{id:noticeId,factKey:noticeId,source,kind:source,tier:3,wake:false,consumesNoticeCredit:false,
      ...(!result?{recipientSessionId:recipient.sessionId}:{}),subjectSession:{id:peer.id,sessionId:choice.final.sessionId},state:'pending',message,...(flight?{retainUntilReconciled:true}:{})});
    return{...recipient,noticeId};
  }
  private resultRecipient(peer:Peer,requestPair?:string):Recipient|undefined{
    const pair=this.store.pairFor(peer.id);
    if(!pair||requestPair!==undefined&&pair.id!==requestPair)return;
    const id=pair[peer.host==='codex'?'claude':'codex'];
    return {pairId:pair.id,id,sessionId:this.store.getPeer(id)?.sessionId??''};
  }
  private notifyResult(peer:Peer,choice:Choice,submitted=false):void{
    if(submitted||choice.requestPair)this.notify(this.resultRecipient(peer,submitted?undefined:choice.requestPair),peer,choice,true);
  }
  private cancelOffers(peer:Peer,choice:Choice):void{
    for(const ref of choice.offers??[])this.delivery.cancel(ref.pairId,ref.id,ref.noticeId);
                         
    const recipient=this.recipient(peer);
    if(choice.noticed&&recipient)this.delivery.cancel(recipient.pairId,recipient.id,'takeover-permission:'+choice.id);
  }
  private offer(peer:Peer,state:PermissionState,choice:Choice):void{
    const recipient=this.recipient(peer);
    if(!recipient||choice.offers?.some(ref=>sameRecipient(ref,recipient)))return;
    this.store.transaction(()=>{
      if(choice.noticed){
        const legacyId='takeover-permission:'+choice.id;
        const sent=this.delivery.reconcileNotice(recipient.pairId,recipient.id,legacyId);
        delete choice.noticed;
        if(sent){(choice.offers??=[]).push({...recipient,noticeId:legacyId});this.save(peer,state);return;}
      }
      const ref=this.notify(recipient,peer,choice);
      if(ref)(choice.offers??=[]).push(ref);
      this.save(peer,state);
    });
  }
  decide(peer:Peer,id:string,action:'keep'|'restore'):TakeoverPermissionView{
    const state=this.state(peer),choice=state?.choice,requester=this.recipient(peer);
    if(state?.inFlight?.choice.id===id&&action==='restore')return changeView(state.inFlight.choice,state.inFlight);
    if(!state||!choice||choice.id!==id||!requester||this.lifecycle.active(peer.id)||state.closing)
      throw new BridgeError('permission_choice_changed','权限选择已改变，请重新查看搭档设置。');
    if(action==='restore'&&choice.unavailable)throw new BridgeError(choice.unavailable,'无法精确恢复原权限。');
    const retry=action==='restore'&&choice.state==='failed'&&(choice.error==='policy_unverified'||choice.error==='restore_not_applied');
    if(choice.state==='pending'||retry||action==='keep'&&choice.state==='restore_pending')this.store.transaction(()=>{
      choice.state=action==='keep'?'kept':'restore_pending';
      if(retry)choice.attempt=(choice.attempt??1)+1;
      choice.requestPair=action==='restore'?requester.pairId:undefined;
      delete choice.error;delete choice.errorPhase;delete choice.actual;
      this.cancelOffers(peer,choice);
      this.save(peer,state);
    });
    else if(!(action==='keep'&&choice.state==='kept'||action==='restore'&&['restore_pending','restoring','restored','failed'].includes(choice.state)))
      throw new BridgeError('permission_choice_changed','该权限选择已结算。');
    return changeView(choice);
  }
  synchronize(id:string,compareOnly=false):Promise<void>{
    const current=this.running.get(id);if(current)return current;
    const peer=this.store.getPeer(id),state=peer&&this.state(peer);
    if(!peer||!state||!state.closing&&(compareOnly||!pending(state.choice)&&!state.inFlight))return Promise.resolve();
    return this.run(id,()=>this.advance(peer,state,compareOnly));
  }
  private run(id:string,start:()=>Promise<void>):Promise<void>{
    const work=start().finally(()=>{if(this.running.get(id)===work)this.running.delete(id);});
    this.running.set(id,work);
    return work;
  }
  async prepareOpen(id:string):Promise<void>{
    for(let running=this.running.get(id);running;running=this.running.get(id))await running;
    const peer=this.store.getPeer(id),state=peer&&this.state(peer);
    const host=peer&&this.hosts[peer.host];
    if(!peer||!state||!host||this.lifecycle.active(id)||this.lifecycle.stopped())return;
    await this.run(id,async()=>{
      if(state.inFlight)await this.reconcile(peer,state,true);
      const latest=this.current(id);
      if(latest?.closing)await this.close(peer,latest,host);
    });
  }
  private returnRequestToChoice(peer:Peer,id:string):boolean{
    const state=this.current(peer.id),choice=state?.choice;
    if(!state||choice?.id!==id||choice.state!=='restore_pending'||choice.requestPair&&choice.requestPair===this.store.pairFor(peer.id)?.id)return false;
    this.store.transaction(()=>{
      choice.state='pending';delete choice.requestPair;
      this.offer(peer,state,choice);this.save(peer,state);
    });
    return true;
  }
  private recordAccepted(peer:Peer,state:PermissionState,opened:TakeoverPermissionEvidence,final:TakeoverPermissionEvidence):void{
    const accepted=state.accepted,continuous=accepted&&opened.offset>=accepted.final.offset&&sameSnapshot(opened,accepted.final);
    let previous=continuous?accepted.previous:[];
    const host=this.hosts[peer.host];
    try{
      const references=host?.references?.(peer);
      if(references)previous=previous.filter(value=>references.some(reference=>sameSnapshot(value,reference)));
    }catch{                            }
    if(sameTakeoverScope(opened,final)&&!sameTakeoverPermission(opened.policy,final.policy))
      previous=[...previous.filter(value=>!sameSnapshot(value,opened)),opened];
    state.accepted=previous.length&&sameTakeoverScope(opened,final)?{previous,final}:undefined;
  }
  private async close(peer:Peer,state:PermissionState,host:PermissionHost):Promise<PermissionState|undefined>{
    const {id,opened}=state.closing!;
    let final:TakeoverPermissionEvidence;
    try{final=await host.observe(peer,opened);}catch{return;}
    const current=this.current(peer.id);
    if(this.lifecycle.stopped()||this.lifecycle.active(peer.id)||current?.closing?.id!==id||!sameTakeoverHistory(opened,final)||final.offset<opened.offset)return;
    this.store.transaction(()=>{
      const previous=current.carry,original=current.carryOriginal??previous?.original??opened;
      this.recordAccepted(peer,current,opened,final);
      const reusable=previous?.state==='pending'&&sameSnapshot(previous.final,final)&&!sameTakeoverPermission(original.policy,final.policy);
      if(previous&&!reusable)this.cancelOffers(peer,previous);
      if(sameTakeoverPermission(original.policy,final.policy)){
        if(previous)current.choice=undefined;
      }else{
        current.choice={id:reusable?previous.id:id,original,final,state:'pending',unavailable:host.unavailable(original,final),
          ...(reusable?{offers:previous.offers,noticed:previous.noticed}:{})};
      }
      delete current.carry;delete current.carryOriginal;delete current.closing;
      if(current.choice?.state==='pending')this.offer(peer,current,current.choice);
      this.save(peer,current);
    });
    return current;
  }
  private reportUnconfirmed(peer:Peer,id:string):void{
    const state=this.current(peer.id),flight=state?.inFlight,choice=flight?.choice;
    if(this.lifecycle.stopped()||!state||!flight||choice?.id!==id||Date.now()-flight.intent.at<UNCONFIRMED_AFTER_MS)return;
    const reports=reportRefs(flight),previous=reports.at(-1);
    const firstReport=choice.state!=='failed'||choice.error!=='restore_unconfirmed';
    choice.state='failed';choice.error='restore_unconfirmed';choice.errorPhase='after_write';
    const recipient=this.resultRecipient(peer);
    if(recipient){
      const id=resultNoticeId(choice,flight);
      const exhausted=exhaustedProbe(choice,flight);
                                      
      if(reports.some(ref=>ref.pairId===recipient.pairId&&ref.id===recipient.id
        &&(!exhausted||ref.noticeId===id||ref.noticeId.startsWith(id+':'))))return;
    }
    this.store.transaction(()=>{
      const ref=this.notify(recipient,peer,choice,true,flight);
      if(ref){
                                          
        if(previous)this.delivery.cancel(previous.pairId,previous.id,previous.noticeId);
        flight.reported=[...reports,ref];
      }
      if(firstReport||ref)this.save(peer,state);
    });
  }
  private finish(peer:Peer,id:string,status:'restored'|'failed'|'superseded',phase:TakeoverPermissionView['error_phase'],code?:string,evidence?:TakeoverPermissionEvidence):void{
    const state=this.current(peer.id),flight=state?.inFlight?.choice.id===id?state.inFlight:undefined;
    const choice=flight?.choice??state?.choice;
    if(this.lifecycle.stopped()||!state||choice?.id!==id||!flight&&choice.state!=='restore_pending')return;
    this.store.transaction(()=>{
      choice.state=status;choice.error=code;choice.errorPhase=code?phase:undefined;
      if(evidence){
        if(!flight?.openedDuringCheck)this.recordAccepted(peer,state,flight?.intent.before??choice.final,evidence);
        if(status!=='restored')choice.actual=evidence;
      }
      const reports=reportRefs(flight);
      for(const ref of reports){
        const reported=this.delivery.reconcileNotice(ref.pairId,ref.id,ref.noticeId);
        if(reported&&status==='restored')choice.correction=true;
      }
      if(reports.length)choice.attempt=(choice.attempt??1)+1;
      if(flight){
        delete state.inFlight;
        state.choice??=choice;
      }
      this.notifyResult(peer,choice,!!flight);
      if(flight&&code==='restore_not_applied'&&state.choice===choice&&this.store.pairFor(peer.id)?.id!==choice.requestPair){
                                         
        choice.state='pending';choice.attempt=(choice.attempt??1)+1;
        delete choice.requestPair;delete choice.error;delete choice.errorPhase;delete choice.actual;
        this.offer(peer,state,choice);
      }
      this.save(peer,state);
    });
  }
  private settle(peer:Peer,id:string,evidence:TakeoverPermissionEvidence,activeProof=false):boolean{
    const flight=this.current(peer.id)?.inFlight;
    if(flight?.choice.id!==id)return false;
    const {choice,intent}=flight;
    if(this.lifecycle.active(peer.id)||evidence.sessionId!==intent.before.sessionId)return false;
    const sameHistory=sameTakeoverHistory(intent.before,evidence);
    const continuous=sameHistory&&evidence.offset>intent.before.offset;
    if(!continuous&&!activeProof)return false;
    const attributed=continuous&&!flight.openedDuringCheck;
    const sameModel=sameTakeoverModelSettings(intent.before,evidence),sameOriginal=sameTakeoverPermission(choice.original.policy,evidence.policy);
    const restored=attributed&&sameOriginal&&sameTakeoverScope(choice.original,evidence)&&sameModel;
    const unchanged=attributed&&sameSnapshot(intent.before,evidence)&&sameModel;
    const code=restored?undefined:flight.openedDuringCheck?'restore_attribution_unconfirmed':unchanged?'restore_not_applied':
      sameOriginal?'restore_verification_mismatch':'restore_mismatch';
    this.finish(peer,choice.id,restored?'restored':'failed','after_write',code,evidence);
    return true;
  }
  private routeGuard(peer:Peer,endpoint:Endpoint,pairId:string,validate:()=>boolean):()=>void{
    return ()=>{
      const current=this.store.getPeer(peer.id),route=this.store.endpoint(peer.id),pair=this.store.pairFor(peer.id);
      if(!current||current.owner!=='nxbridge'||current.sessionId!==peer.sessionId||pair?.id!==pairId||!validate())
        throw new BridgeError('session_changed','恢复期间搭档或会话已改变。');
      if(this.lifecycle.stopped()||this.lifecycle.active(peer.id)||route?.epoch!==endpoint.epoch||route.address!==endpoint.address
        ||route.runtimeId!==endpoint.runtimeId||pair.paused||!this.lifecycle.authorized(current,route,pair))
        throw new RuntimeRecoveryHold('owned_subscription_waiting','等待后台重新接通后核对权限。');
    };
  }
  private async reconcile(peer:Peer,state:PermissionState,opening?:true):Promise<void>{
    const host=this.hosts[peer.host]!,flight=state.inFlight!,id=flight.choice.id;
    const current=()=>{
      const latest=this.current(peer.id);
      return latest?.inFlight?.choice.id===id?latest:undefined;
    };
    let evidence:TakeoverPermissionEvidence|undefined;
    try{evidence=await host.observe(peer,flight.intent.before);}catch{                     }
    const latest=current();
    if(this.lifecycle.stopped()||this.lifecycle.active(peer.id)||!latest?.inFlight)return;
    const endpoint=this.store.endpoint(peer.id),pair=this.store.pairFor(peer.id);
    if(evidence&&(!endpoint?.nativeSession?.transcriptPath||sameDirectory(evidence.file,endpoint.nativeSession.transcriptPath))&&this.settle(peer,id,evidence))return;
    evidence=undefined;
    const liveGeneration=endpoint&&(host.probeGeneration?host.probeGeneration(peer,endpoint):endpoint.epoch);
    const generation=liveGeneration??(endpoint&&'route:'+endpoint.epoch);
    const probe=flight.probe?.generation===generation?flight.probe:undefined;
    if(endpoint&&pair&&!pair.paused&&this.lifecycle.authorized(peer,endpoint,pair)&&generation
      &&(opening||(probe?.attempts??0)<PROBE_BACKOFF_MS.length&&(probe?.nextAt??0)<=Date.now())){
      const guard=this.routeGuard(peer,endpoint,pair.id,()=>!!current());
      let release:(()=>void)|undefined,submitted=false;
      const attempts=probe?.attempts??0;
      const retry=(count:number,preflightFailures:number,after:number)=>{
        const state=current();
        if(!state?.inFlight)return;
        state.inFlight.probe={generation,attempts:count,preflightFailures,nextAt:Date.now()+after};
        this.save(peer,state);
      };
      const deferPreflight=()=>{
        if(this.lifecycle.stopped())return;
        const failures=Math.min((probe?.preflightFailures??0)+1,PROBE_PREFLIGHT_BACKOFF_MS.length);
        retry(attempts,failures,PROBE_PREFLIGHT_BACKOFF_MS[failures-1]!);
      };
      if(!liveGeneration||!this.lifecycle.available(peer,opening))deferPreflight();
      else{
        try{
          if(!opening)release=this.lifecycle.occupy(peer.id);
          guard();
          evidence=await host.probe(peer,endpoint,flight.intent.before,guard,()=>{
            const count=Math.min(attempts+1,PROBE_BACKOFF_MS.length);
            retry(count,0,PROBE_BACKOFF_MS[count-1]!);submitted=true;
          });guard();
        }catch{
          evidence=undefined;
          if(!submitted)deferPreflight();
        }finally{release?.();}
      }
      if(evidence&&this.settle(peer,id,evidence,true))return;
    }
    this.reportUnconfirmed(peer,id);
  }
  private async advance(peer:Peer,state:PermissionState,compareOnly:boolean):Promise<void>{
    const host=this.hosts[peer.host];if(!host||this.lifecycle.stopped()||this.lifecycle.active(peer.id))return;
    if(state.closing){
      const closed=await this.close(peer,state,host);
      if(closed)state=closed;
    }
    if(compareOnly)return;
    if(state.inFlight){
      await this.reconcile(peer,state);
      return;
    }
    const choice=state.choice;if(!choice||state.closing)return;
    if(choice.state==='pending'){this.offer(peer,state,choice);return;}
    if(choice.state!=='restore_pending')return;
    const pair=this.store.pairFor(peer.id),endpoint=this.store.endpoint(peer.id);
    if(this.returnRequestToChoice(peer,choice.id)||!pair)return;
    if(!endpoint||pair.paused||!this.lifecycle.available(peer))return;
    let flight:InFlight|undefined,release:(()=>void)|undefined;
    const guard=this.routeGuard(peer,endpoint,pair.id,()=>{
      const current=this.current(peer.id),latest=current?.choice;
      return flight?current?.inFlight?.choice.id===choice.id:
        latest?.id===choice.id&&latest.state==='restore_pending'&&latest.requestPair===pair.id;
    });
    try{
      release=this.lifecycle.occupy(peer.id);
      const final=await host.restore(peer,endpoint,choice.original,choice.final,guard,intent=>{
        const inFlight:InFlight={choice:{...choice,state:'restoring'},intent};
        this.save(peer,{...state,choice:undefined,inFlight});
        flight=inFlight;
      });
      if(flight)this.settle(peer,choice.id,final);
      else if(sameSnapshot(choice.original,final))this.finish(peer,choice.id,'restored',undefined,undefined,final);
    }catch(error){
      if(!flight&&this.returnRequestToChoice(peer,choice.id))return;
      const outcome=submissionError(error,!!flight);
      if(outcome.kind==='failed'||outcome.kind==='superseded')this.finish(peer,choice.id,outcome.kind,'before_write',outcome.code);
    }finally{release?.();}
  }
}
