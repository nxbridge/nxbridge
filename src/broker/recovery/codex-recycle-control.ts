import { createHash } from 'node:crypto';
import type { Store } from '../../storage/store.js';
import type { Endpoint, Pair, Peer, RegisterInput } from '../../types.js';
import type { CodexAdapter, CodexContextReceipt } from '../../hosts/codex/adapter.js';
import { catchUpCodexHistory } from '../../hosts/codex/codex-history.js';
import { runtimeFor } from '../../hosts/managed-runtimes.js';
import { BridgeError } from '../../shared/errors.js';
import { sameDirectory } from '../../shared/files.js';
import type { DeliveryHealth } from '../health/delivery.js';
import { memberEnabled } from '../member-control.js';
import { hostingRecord, writeHosting } from './session-hosting.js';
import { CodexRecycle, codexRecycleFenced, codexRecycleNoticeKey, codexRecycleWorkKind, type CodexRecycleAcceptance, type CodexRecycleOperation, type CodexRecycleTrigger, type CodexRecycleWork } from './codex-recycle.js';
import { recoveryUserStep, type ManagedIncident, type ManagedRecovery } from './managed-recovery.js';
import { RECYCLE_AMBIGUOUS_USER_STEP, RECYCLE_CLEANUP_USER_STEP, RECYCLE_OLD_UNKNOWN_USER_STEP, recycleAcceptanceLapsedNotice, recycleAcceptanceQueuedResult, recycleAuthorityLostNotice, recycleCleanupBlockedNotice, recycleDoneNotice, recycleHeldNotice, recycleLaunchUnknownNotice, recycleOldUnknownNotice, recycleStopFailedNotice, recycleStopFailedPendingNotice, recycleRefusedResult, staleRevisionResult, stuckNotice, stuckStatusText, stuckUserLine, type AuthorityLoss, type OldOutcome, type RecycleStage, type StaleChange, type StuckProgress, type StuckTrigger, type SuspendReason } from '../stuck-session-text.js';

interface Candidate { pairId:string; sessionId:string; runtimeId:string; pid:number; procStart:string; trigger:CodexRecycleTrigger; at:number; idleSince?:number }
interface DeliveryProgress {sessionId:string;pairId:string;turns:Record<string,{received?:true;completedAt?:number}>;lastCompleted?:number}
const marker=(id:string)=>'codexRecycleCandidate:'+id;
const progress=(id:string)=>'codexRecycleDeliveredTurn:'+id;
const copyTrigger=(trigger:CodexRecycleTrigger):StuckTrigger=>({
  native_thread_wedged:'native_thread_wedged',cold_exhausted:'cold_resume_exhausted',
  loaded_unleased:'lease_lost',loaded_unleased_expired:'unload_overdue',
})[trigger] as StuckTrigger;
const copyWork=(work:CodexRecycleWork)=>({work:codexRecycleWorkKind(work),subagents:Math.max(0,work.activeTurnIds.length-1),
  progress:{state:work.progress==='idle'?'unknown':work.progress,lastAt:work.lastProgressAt} satisfies StuckProgress});
const copyStage=(stage:Extract<CodexRecycleOperation,{phase:'held'}>):RecycleStage=>
  stage.paused?.phase==='retiring'||stage.paused?.phase==='verifying'?'verify':
    stage.paused?.phase==='launch_requested'||stage.paused?.phase==='stopped'?'launch':stage.paused||terminalStop(stage)?'stop':'preflight';
const terminalStop=(stage:CodexRecycleOperation|undefined)=>stage?.phase==='held'&&['codex_recycle_stop_cancelled','codex_recycle_stop_deadline'].includes(stage.code);
const basisOf=(stage:CodexRecycleOperation|undefined)=>stage&&('basis' in stage?stage.basis:'attempt' in stage?stage.attempt:
  stage.phase==='held'?(stage.paused?.phase==='retiring'?stage.paused.verification.attempt:stage.paused?.attempt):undefined);
interface Services {
  store:Store;home:string;codex:CodexAdapter;recovery:ManagedRecovery;delivery:DeliveryHealth;
  stopped:()=>boolean;scope:(pair:Pair,peer:Peer)=>string;
  incident:(peer:Peer)=>ManagedIncident;
  noExternalHolder:(peer:Peer)=>Promise<void>;
  fold:(peer:Peer,receipt:CodexContextReceipt)=>void;
}

export class CodexRecycleControl {
  private engine:CodexRecycle;
  private due=new Map<string,number>();
  private observationBackoff=new Map<string,number>();
  constructor(private services:Services){this.engine=new CodexRecycle(services.recovery);}

  fenced(id:string):boolean {return codexRecycleFenced(this.services.recovery.get(id));}
  pending():boolean {return this.engine.pending;}
  requireUnfenced(id:string):void {
    if(this.fenced(id))throw new BridgeError('restart_in_progress','专用 Codex 正在核对同会话恢复，请稍后查看恢复结果。');
  }

  reconcileAuthority(peer:Peer):void{
    const {store,recovery}=this.services,incident=recovery.get(peer.id);
    if(!incident?.recycle)return;
    if(incident.state==='closed'){this.finalizeProfile(incident);return;}
    const loss=this.authorityLoss(peer,incident);
    if(loss){
      this.finalizeProfile(incident);
      recovery.closeRecycle(incident,()=>this.notifyAuthorityLost(peer,incident,loss));store.setSetting(marker(peer.id),null);
      this.observationBackoff.delete(peer.id);
    }
  }

  private finalizeProfile(incident:ManagedIncident):void{
    const stage=incident.recycle;
    if(stage?.phase!=='ready'||!stage.finalized)return;
    try{this.services.codex.finalizeRecycle?.(incident.peerId,incident.sessionId,stage.fresh,stage.attempt.profile.sourceId);}
    catch(error){this.services.store.event('codex.recycle.profile_cleanup_pending',{peerId:incident.peerId,error:String(error)});}
  }

  private hasRegistrationConflict(peerId:string):boolean{
    const {store}=this.services;
    return !!(store.setting('managedSessionConflict:'+peerId)||store.setting('managedRuntimeConflict:'+peerId));
  }

  reconcileFinishedProfile(peerId:string):void{
    const incident=this.services.recovery.get(peerId);
    if(incident)this.finalizeProfile(incident);
  }

  private authorityLoss(peer:Peer,incident:ManagedIncident):AuthorityLoss|undefined{
    const {store}=this.services;
    if(hostingRecord(store,peer)?.released)return 'released_to_user';
    if(!memberEnabled(store,peer))return 'member_disabled';
    if(store.pairFor(peer.id)?.id!==incident.pairId)return 'unpaired';
    if(peer.owner!=='nxbridge'||peer.sessionId!==incident.sessionId||store.bindings.current(peer.id)||store.setting('takeover:'+peer.id))return 'adopted';
    if(this.hasRegistrationConflict(peer.id))return 'foreign_holder';
  }

  private notifyAuthorityLost(peer:Peer,incident:ManagedIncident,loss:AuthorityLoss):void{
    const {store,delivery}=this.services,pair=store.pairFor(peer.id),stage=incident.recycle;
    if(!pair||pair.id!==incident.pairId||!stage||stage.phase==='ready'&&stage.finalized)return;
    delivery.cancelKind(pair.id,pair.claude,'codex_recycle_decision:'+peer.id);
    const phase=stage.phase==='held'?stage.paused??stage:stage;
    const old:OldOutcome=phase.phase==='stop_requested'&&phase.signal!=='unsubmitted'||terminalStop(stage)?'unknown':
      ['stopped','launch_requested','verifying','retiring','handback','ready'].includes(phase.phase)?'ended':'untouched';
    const owner=store.peer(pair.claude),fields={peerName:peer.name,peerHost:'codex' as const,loss,old,trigger:copyTrigger(stage.trigger)};
    delivery.enqueue(pair.id,owner.id,{id:incident.id+':authority-lost',source:'managed_recovery',kind:'codex_recycle_authority_lost:'+peer.id,
      incidentId:incident.id,scope:this.services.scope(pair,owner),state:'pending',wake:false,tier:3,consumesNoticeCredit:false,
      factKey:incident.id,message:recycleAuthorityLostNotice(fields)});
    store.setSetting('codexRecycleUserLine:'+owner.id,{incidentId:incident.id,pairId:pair.id,line:stuckUserLine('authority_lost',fields)});
  }

  noteHold(peer:Peer,error:unknown):boolean {
    const {store,home}=this.services,pair=store.pairFor(peer.id),endpoint=store.endpoint(peer.id);
    if(this.services.recovery.get(peer.id)?.recycle?.phase==='handback')return false;
    if(peer.host!=='codex'||peer.owner!=='nxbridge'||!peer.sessionId||!pair||!endpoint?.runtimeId||endpoint.daemon)return false;
    if(this.hasRegistrationConflict(peer.id))return false;
    const runtime=runtimeFor(home,{id:endpoint.runtimeId})?.value;
    if(!runtime?.dedicated||runtime.kind!=='owned'||runtime.peerId!==peer.id||runtime.sessionId!==peer.sessionId||!runtime.pid||!runtime.procStart)return false;
    const wait=error as {code?:string;recycleEvidence?:string},cold=runtime.coldResumeUnconfirmed;
    const trigger=wait.code==='native_thread_wedged'?'native_thread_wedged':
      wait.recycleEvidence==='cold_exhausted'&&cold?.attempts===2&&cold.transportClosed?'cold_exhausted':
      wait.recycleEvidence==='loaded_unleased'?'loaded_unleased':undefined;
    if(!trigger)return false;
    const prior=store.setting<Candidate>(marker(peer.id));
    if(prior?.runtimeId===runtime.id&&prior.procStart===runtime.procStart&&prior.pairId===pair.id&&prior.sessionId===peer.sessionId
      &&(prior.trigger===trigger||prior.trigger!=='loaded_unleased'&&trigger==='loaded_unleased'))return true;
    store.setSetting(marker(peer.id),{pairId:pair.id,sessionId:peer.sessionId,runtimeId:runtime.id,pid:runtime.pid,procStart:runtime.procStart,trigger,at:Date.now()} satisfies Candidate);
    this.due.delete(peer.id);this.observationBackoff.delete(peer.id);return true;
  }

  observeDeliveredTurn(peer:Peer,turnId:string,phase:'received'|'completed',at=Date.now()):void {
    const {store}=this.services,pair=store.pairFor(peer.id);
    if(peer.host!=='codex'||peer.owner!=='nxbridge'||!peer.sessionId||!pair||!turnId)return;
    const key=progress(peer.id),saved=store.setting<DeliveryProgress>(key);
    const value:DeliveryProgress=saved?.sessionId===peer.sessionId&&saved.pairId===pair.id?saved:{sessionId:peer.sessionId,pairId:pair.id,turns:{}};
    const turn=value.turns[turnId]??{};
    if(phase==='received')turn.received=true;else turn.completedAt??=at;
    if(turn.received&&turn.completedAt)value.lastCompleted=Math.max(value.lastCompleted??0,turn.completedAt);
    value.turns[turnId]=turn;
    const ids=Object.keys(value.turns);
    if(ids.length>32)delete value.turns[ids[0]!];
    store.setSetting(key,value);
  }

  private endpoint(peer:Peer,incident:ManagedIncident|undefined,candidate:Candidate|undefined):Endpoint|undefined {
    const {store}=this.services,live=store.endpoint(peer.id),operation=incident?.recycle;
    const runtimeId=operation?.phase==='ready'?operation.fresh.runtimeId:operation?.phase==='handback'?undefined:basisOf(operation)?.old.runtimeId??candidate?.runtimeId;
    if(!runtimeId||live?.runtimeId===runtimeId)return live;
    return store.endpoints(peer.id).filter(value=>value.runtimeId===runtimeId).at(-1);
  }

  async advance(peer:Peer,acceptance?:CodexRecycleAcceptance):Promise<boolean> {
    const {store,home,codex,recovery,delivery}=this.services,pair=store.pairFor(peer.id);
    this.reconcileAuthority(peer);
                                                
    if(this.hasRegistrationConflict(peer.id)){
      if(store.setting(marker(peer.id)))store.setSetting(marker(peer.id),null);
      this.due.delete(peer.id);this.observationBackoff.delete(peer.id);return false;
    }
    if(!pair||peer.host!=='codex'||!peer.sessionId||!codex.recycleHost)return false;
    let incident=recovery.get(peer.id);
    const candidate=store.setting<Candidate>(marker(peer.id));
    const operation=incident?.state==='closed'?undefined:incident?.recycle;
    if(!operation&&(!candidate||candidate.pairId!==pair.id||candidate.sessionId!==peer.sessionId))return false;
    const cancelDecisions=(keep?:string)=>delivery.cancelKind(pair.id,pair.claude,'codex_recycle_decision:'+peer.id,keep);
    if(!acceptance&&(this.due.get(peer.id)??0)>Date.now())return terminalStop(operation)||this.fenced(peer.id);
    const endpoint=this.endpoint(peer,incident,candidate);
    if(!endpoint?.runtimeId)return this.fenced(peer.id);
    const selected=store.endpoint(peer.id);
    if(!this.fenced(peer.id)&&operation?.phase!=='ready'&&operation?.phase!=='handback'&&selected&&selected.runtimeId!==endpoint.runtimeId){
      if(incident?.recycle)recovery.updateRecycle(incident,incident.recycle,undefined);
      cancelDecisions();
      store.setSetting(marker(peer.id),null);return false;
    }
    if(!this.fenced(peer.id)&&operation?.phase!=='handback'&&codex.hasSubscription(peer,endpoint)&&(candidate?.trigger==='loaded_unleased'||candidate?.trigger==='loaded_unleased_expired')){
      if(incident?.recycle&&incident.recycle.phase!=='ready')recovery.updateRecycle(incident,incident.recycle,undefined);
      cancelDecisions();
      store.setSetting(marker(peer.id),null);return false;
    }
    const running=runtimeFor(home,{id:endpoint.runtimeId})?.value;
    let trigger=candidate?.trigger??operation?.trigger;
    if(!trigger)return false;
    const native=endpoint.native??endpoint;
    const sameCandidate=candidate?.runtimeId===endpoint.runtimeId&&candidate.pid===native.pid&&candidate.procStart===native.procStart
      &&candidate.pairId===pair.id&&candidate.sessionId===peer.sessionId;
    if(trigger==='loaded_unleased_expired'&&candidate&&!sameCandidate&&!this.fenced(peer.id)){
      if(operation&&incident)recovery.updateRecycle(incident,operation,undefined);
      cancelDecisions();store.setSetting(marker(peer.id),null);return false;
    }
    if(trigger==='loaded_unleased'&&sameCandidate&&running?.id===candidate.runtimeId&&running.procStart===candidate.procStart){
                                               
      if(candidate.idleSince!==undefined&&Date.now()-candidate.idleSince>=(running.threadUnloadDelaySecs??60)*1000+5000){
        trigger='loaded_unleased_expired';
        store.setSetting(marker(peer.id),{...candidate,trigger} satisfies Candidate);
      }
    }
    incident??=this.services.incident(peer);
    if(incident.state==='closed')incident=this.services.incident(peer);
    const owner=store.peer(pair.claude),host=codex.recycleHost(peer,endpoint,owner.sessionId??undefined);
    const guard=(current:ManagedIncident)=>{
      const live=store.getPeer(peer.id),paired=store.pairFor(peer.id),source=store.getPeer(owner.id),active=store.endpoint(peer.id);
      const stage=current.recycle,ready=stage?.phase==='ready'||stage?.phase==='handback'?stage.fresh:undefined;
      const original=active?.epoch===endpoint.epoch&&active.runtimeId===endpoint.runtimeId&&(active.native??active).pid===(endpoint.native??endpoint).pid
        &&(active.native??active).procStart===(endpoint.native??endpoint).procStart&&active.configHome===endpoint.configHome;
      const replacement=ready&&active?.runtimeId===ready.runtimeId&&active.address===ready.endpoint&&(active.native??active).pid===ready.pid&&(active.native??active).procStart===ready.procStart;
      const loss=live?this.authorityLoss(live,current):'unpaired';
      const foreign=active&&!original&&!replacement||store.peersForSession('codex',peer.sessionId!).some(other=>other.id!==peer.id&&other.owner==='user'
        &&store.endpoints(other.id).some(ep=>ep.active&&(!ep.configHome||!endpoint.configHome||sameDirectory(ep.configHome,endpoint.configHome))));
      if(loss||!live||!paired||!sameDirectory(live.cwd,peer.cwd)||foreign)
        throw Object.assign(new BridgeError('codex_recycle_authority_lost','原后台归属已改变，回收已结束。'),{loss:loss??'foreign_holder'});
      const suspend:SuspendReason|undefined=this.services.stopped()?'broker_restarting':
        paired.paused||live.state==='paused'?'pair_paused':live.state==='needsHuman'||store.questions(true).some(q=>q.pairId===pair.id)?'question_open':
          !source||source.sessionId!==owner.sessionId||!memberEnabled(store,source)?'partner_changed':undefined;
      if(suspend)throw Object.assign(new BridgeError('incident_changed','当前无法继续恢复，回收已暂停。'),{suspend});
    };
    const readLedger=()=>{
      const pending=store.recycleDeliveryFacts(pair.id,peer.id);
      const uncertain=pending.filter(row=>['sending','runtimeQueued','unknown'].includes(row.state));
      return {held:pending.filter(row=>['accepted','held'].includes(row.state)).map(row=>row.id),
        uncertain:uncertain.map(row=>({id:row.id,senderId:row.from})),...(uncertain.length?{uncertainSince:uncertain[0]!.createdAt}:{})};
    };
    this.due.set(peer.id,Date.now()+5000);
    let unsubscribed:RegisterInput|undefined;
    const result=await this.engine.advance(incident,trigger,{
      ...host,guard,
      authorityLost:(current,loss)=>this.notifyAuthorityLost(peer,current,loss),
      observe:async kind=>{
        const evidence=await host.observe(kind,()=>readLedger().held.length>0);
        if(evidence.kind==='held'&&sameCandidate&&running?.id===candidate.runtimeId&&running.pid===candidate.pid&&running.procStart===candidate.procStart
          &&await host.inspectOld({runtimeId:running.id,endpoint:running.endpoint,pid:candidate.pid,procStart:candidate.procStart,bootId:running.bootId}).catch(()=>'unknown')==='dead')
          return {kind:'natural'};
        if(kind==='loaded_unleased'&&candidate?.trigger==='loaded_unleased'){
          const current=store.setting<Candidate>(marker(peer.id));
          if(current?.trigger==='loaded_unleased'&&current.runtimeId===candidate.runtimeId&&current.pairId===pair.id){
            const idle=evidence.kind==='natural'&&evidence.idleLoaded;
            const idleSince=idle?current.idleSince??Date.now():undefined;
            if(current.idleSince!==idleSince)store.setSetting(marker(peer.id),{...current,idleSince});
          }
        }
        return evidence;
      },
      stop:async(old,deadline,authorize,markSignal,force)=>{authorize();await this.services.noExternalHolder(peer);authorize();await host.stop(old,deadline,authorize,markSignal,force);},
      launch:async(intent,authorize,profile)=>{authorize();await this.services.noExternalHolder(peer);authorize();await host.launch(intent,authorize,profile);},
      prepareUnsubscribed:async(fresh,profile)=>{unsubscribed=await host.prepareUnsubscribed(fresh,profile);},
      recoveredSubscription:()=>{
        const route=store.endpoint(peer.id),native=route?.native??route;
        if(!route?.runtimeId||!native?.pid||!native.procStart||!codex.hasSubscription(store.peer(peer.id),route))return;
        const expected=operation?.phase==='handback'?operation.fresh:undefined;
        const bootId=route.runtimeId===expected?.runtimeId?expected?.bootId:runtimeFor(home,{id:route.runtimeId})?.value.bootId;
        return {runtimeId:route.runtimeId,endpoint:route.address,pid:native.pid,procStart:native.procStart,bootId};
      },
      foldHandbackReceipts:async(profile,ledger)=>{
        const pending=store.recycleDeliveryFacts(pair.id,peer.id);
        const ids=new Set([...ledger.uncertain.map(row=>row.id),...ledger.held]);
        const tracked=pending.filter(row=>ids.has(row.id));
        if(!tracked.length)return ()=>{};
        const since=tracked.reduce((earliest,row)=>Math.min(earliest,row.createdAt),Infinity);
        const history=await catchUpCodexHistory(profile.file,peer.sessionId!,since);
        const receipts=history.receipts.filter(receipt=>ids.has(receipt.messageId));
        return ()=>{const target=store.peer(peer.id);for(const receipt of receipts)this.services.fold(target,receipt);};
      },
      admitUnsubscribed:()=>{
        const input=unsubscribed!;
        for(const previous of store.endpoints(peer.id))if(previous.active&&previous.address!==input.endpoint.address)store.deactivate(peer.id,previous.epoch);
        const target=store.adopt(peer.id,input);
        writeHosting(store,target,{verified:false,verifying:false,verificationError:undefined});
        cancelDecisions();store.setSetting(marker(peer.id),null);
      },
      partnerReachable:()=>!!store.endpoint(owner.id)&&!['offline','unknown','paused'].includes(store.peer(owner.id).state),
      deliveredTurnCompletedSince:at=>{
        const evidence=store.setting<DeliveryProgress>(progress(peer.id));
        return evidence?.sessionId===peer.sessionId&&evidence.pairId===pair.id&&(evidence.lastCompleted??0)>at;
      },
      ledger:readLedger,
      notify:(current,basis,ledger)=>{
        const {decision}=basis;
        cancelDecisions(decision.noticeId);
        delivery.enqueue(pair.id,owner.id,{id:decision.noticeId,source:'managed_recovery',kind:'codex_recycle_decision:'+peer.id,incidentId:current.id,
          scope:this.services.scope(pair,owner),state:'pending',wake:true,tier:4,factKey:codexRecycleNoticeKey(current.id,basis),
          message:stuckNotice({peerName:peer.name,peerHost:'codex',recipientHost:owner.host,trigger:copyTrigger(trigger),since:decision.since,
            ...copyWork(decision.work),held:ledger.held.length,
            incidentId:current.id,revision:decision.revision,updated:operation?.phase==='awaiting_partner'})});
      },
      acceptanceLapsed:(current,revision)=>{
        cancelDecisions();delivery.cancelKind(pair.id,owner.id,'codex_recycle_held:'+peer.id);
        const fact=JSON.stringify([current.id,revision]);
        const wake=!!store.endpoint(owner.id)&&store.peer(owner.id).state==='ready';
        delivery.enqueue(pair.id,owner.id,{id:'recycle-lapsed:'+createHash('sha256').update(fact).digest('hex').slice(0,32),
          source:'managed_recovery',kind:'codex_recycle_acceptance_lapsed:'+peer.id,incidentId:current.id,scope:this.services.scope(pair,owner),
          state:'pending',wake,tier:wake?4:3,factKey:fact,...(!wake?{wakeWhenIdle:true as const}:{}),
          message:recycleAcceptanceLapsedNotice({peerName:peer.name,peerHost:'codex',revision,held:readLedger().held.length})});
      },
      admit:(_current,fresh)=>{
        const admitted=host.admission(fresh);
        for(const previous of store.endpoints(peer.id))if(previous.active&&previous.address!==fresh.endpoint)store.deactivate(peer.id,previous.epoch);
        const target=store.adopt(peer.id,admitted.input),route=store.endpoint(peer.id);
        if(!route||route.runtimeId!==fresh.runtimeId)throw new BridgeError('runtime_changed','结果事务未登记同一替代后台。');
        writeHosting(store,target,{policy:admitted.policy,verified:true,verifying:false,verificationError:undefined});
        for(const receipt of admitted.receipts)this.services.fold(target,receipt);
        host.subscribe({peer:target,endpoint:route},admitted);
      },
      finish:(current,ready)=>{
        const pending=new Set(readLedger().uncertain.map(row=>row.id)),unknown=ready.attempt.ledger.uncertain.filter(row=>pending.has(row.id)).map(row=>row.id);
        const by=ready.attempt.acceptance?.by??(ready.attempt.acceptance?'partner':'auto'),work=codexRecycleWorkKind(ready.attempt.decision.work);
        const actionable=unknown.length>0||work!=='idle'&&by!=='auto',wake=actionable&&store.peer(owner.id).state==='ready';
        const delivered=ready.attempt.ledger.held.filter(id=>['contextObserved','responded'].includes(store.message(id).state)).length;
        const userLine=stuckUserLine(by==='auto'?'auto_recovered':by==='partner'?'partner_recovered':'user_recovered',
          {peerName:peer.name,peerHost:'codex',trigger:copyTrigger(trigger),since:ready.attempt.decision.since,work});
        cancelDecisions();
        delivery.cancelKind(pair.id,owner.id,'codex_recycle_held:'+peer.id);
        delivery.enqueue(pair.id,owner.id,{id:ready.attempt.id+':result',source:'managed_recovery',kind:'codex_recycle_result:'+peer.id,
          incidentId:current.id,scope:this.services.scope(pair,owner),state:'pending',wake,tier:wake?4:3,factKey:ready.attempt.id,affectedMessageIds:unknown,
          ...(actionable&&!wake?{wakeWhenIdle:true as const}:{}),
          ...(!actionable?{consumesNoticeCredit:false as const}:{}),
          message:recycleDoneNotice({peerName:peer.name,peerHost:'codex',sessionId:peer.sessionId!,by,work,delivered,uncertainIds:unknown})});
        store.setSetting(marker(peer.id),null);
        store.setSetting('codexRecycleUserLine:'+owner.id,{incidentId:current.id,pairId:pair.id,line:userLine});
        store.event('codex.recycle.ready',{incidentId:current.id,peerId:peer.id,sessionId:peer.sessionId,uncertain:unknown,userLine});
      },
    },acceptance);
    this.finalizeProfile(result.incident);
    const stage=result.incident.state==='closed'?undefined:result.incident.recycle;
    if(result.retryObservation||!acceptance&&operation?.phase==='observing'&&stage?.phase==='observing'){
      const delay=Math.min(30000,2*(this.observationBackoff.get(peer.id)??5000));
      this.observationBackoff.set(peer.id,delay);this.due.set(peer.id,Date.now()+delay);
    }else this.observationBackoff.delete(peer.id);
    if(stage?.phase==='observing'&&operation?.phase==='awaiting_partner')cancelDecisions();
    if(!stage){
      cancelDecisions();
      delivery.cancelKind(pair.id,owner.id,'codex_recycle_held:'+peer.id);
      if(trigger!=='loaded_unleased')store.setSetting(marker(peer.id),null);
    }
    const exhausted=stage?.phase==='held'&&stage.observationGap?.exhausted;
    const lapsed=exhausted?stage?.observationGap?.lapsedRevision:undefined;
                                                          
    const publication=exhausted&&stage.observationGap?.publication;
    if(exhausted&&!lapsed&&!publication)cancelDecisions();
    if(publication)delivery.cancelKind(pair.id,owner.id,'codex_recycle_held:'+peer.id);
    if(operation?.observationGap?.exhausted&&!exhausted&&stage&&stage.phase!=='held')
      delivery.cancelKind(pair.id,owner.id,'codex_recycle_held:'+peer.id);
    if(!lapsed&&!publication&&stage?.phase==='held'&&stage.code!=='codex_recycle_partner_unreachable'&&stage.code!=='incident_changed'&&(result.kind!=='stale'||exhausted)
      &&store.endpoint(owner.id)&&!['offline','unknown','paused'].includes(store.peer(owner.id).state)){
      const stopFailed=stage.code==='codex_recycle_stop_deadline',revision=stage.basis?.decision.revision;
      const fact=JSON.stringify([result.incident.id,copyStage(stage),stage.code,stage.reason,stage.leftover,stopFailed?revision:undefined,...(stage.retryPending?['awaiting_stable_evidence']:[])]);
      const wake=store.peer(owner.id).state==='ready';
      const id='recycle-held:'+createHash('sha256').update(fact).digest('hex').slice(0,32);
      const common={peerName:peer.name,peerHost:'codex' as const,held:readLedger().held.length};
      const message=stopFailed?stage.retryPending||!revision?recycleStopFailedPendingNotice(common):recycleStopFailedNotice({...common,recipientHost:owner.host,
        incidentId:result.incident.id,revision}):stage.paused&&['launch_requested','verifying','retiring'].includes(stage.paused.phase)?
          recycleLaunchUnknownNotice({...common,reason:stage.reason,userStep:RECYCLE_AMBIGUOUS_USER_STEP}):stage.paused?.phase==='stopped'?
          recycleCleanupBlockedNotice({...common,leftover:stage.leftover||undefined,userStep:RECYCLE_CLEANUP_USER_STEP}):stage.paused?.phase==='stop_requested'?
          recycleOldUnknownNotice({...common,reason:stage.reason,userStep:RECYCLE_OLD_UNKNOWN_USER_STEP}):
          recycleHeldNotice({...common,stage:copyStage(stage),reason:stage.reason,userStep:recoveryUserStep(peer,result.incident)});
      if(!exhausted)cancelDecisions();
      if(stopFailed||exhausted)delivery.cancelKind(pair.id,owner.id,'codex_recycle_held:'+peer.id,id);
      delivery.enqueue(pair.id,owner.id,{id,source:'managed_recovery',kind:'codex_recycle_held:'+peer.id,incidentId:result.incident.id,scope:this.services.scope(pair,owner),
        state:'pending',wake,tier:wake?4:3,factKey:fact,...(!wake?{wakeWhenIdle:true as const}:{}),
        message});
    }
    if(terminalStop(stage))return true;
    if(trigger==='loaded_unleased'&&!this.fenced(peer.id))return false;
    return result.kind!=='natural'&&!!result.incident.recycle&&result.incident.recycle.phase!=='ready'&&result.incident.recycle.phase!=='handback';
  }

  acceptanceText(peer:Peer,previous:ManagedIncident|undefined,acceptance:CodexRecycleAcceptance):string|undefined {
    const current=this.services.recovery.get(peer.id),stage=current?.recycle,before=basisOf(previous?.recycle),after=basisOf(stage);
    if(!stage)return 'No recycle was performed. There is no current recycle decision. See recovery status.';
    if(current?.id===acceptance.incidentId&&after?.acceptance?.evidenceRevision===acceptance.evidenceRevision&&(stage.phase==='awaiting_partner'||stage.phase==='observing'||stage.phase==='held'&&stage.code==='codex_recycle_stop_deadline'))
      return recycleAcceptanceQueuedResult({revision:acceptance.evidenceRevision});
    if(current?.id===acceptance.incidentId&&stage.observationGap?.lapsedRevision===acceptance.evidenceRevision)
      return recycleAcceptanceLapsedNotice({peerName:peer.name,peerHost:'codex',revision:acceptance.evidenceRevision,
        held:this.services.store.recycleDeliveryFacts(current.pairId,peer.id).filter(message=>['held','accepted'].includes(message.state)).length});
    if(stage?.phase==='held')return recycleRefusedResult(stage.code);
    if(stage?.phase!=='awaiting_partner'||after?.decision.revision===acceptance.evidenceRevision)return;
    const changes:StaleChange[]=[];
    if(!before||before.old.runtimeId!==after?.old.runtimeId||before.old.procStart!==after?.old.procStart)changes.push('target_changed');
    else if(before.profile.settingsKey!==after?.profile.settingsKey)changes.push('preflight_changed');
    else if(before.decision.work.progress!=='progressing'&&after?.decision.work.progress==='progressing')changes.push('progress_resumed');
    else if(after?.decision.work.activeTurnIds.length&&JSON.stringify(before.decision.work.activeTurnIds)!==JSON.stringify(after.decision.work.activeTurnIds))changes.push('turn_changed');
    else if(JSON.stringify(before.decision.work.roles)!==JSON.stringify(after?.decision.work.roles))changes.push('process_changed');
    return after?staleRevisionResult({oldRevision:acceptance.evidenceRevision,newRevision:after.decision.revision,changes,...copyWork(after.decision.work),
      held:this.services.store.recycleDeliveryFacts(current!.pairId,peer.id).filter(message=>['held','accepted'].includes(message.state)).length}):undefined;
  }

  summary(peer:Peer,incident:ManagedIncident):unknown {
    const stage=incident.recycle;
    if(!stage)return;
    const basis=basisOf(stage);
    const fields={peerName:peer.name,peerHost:'codex' as const,trigger:copyTrigger(stage.trigger),since:basis?.decision.since,
      work:basis&&codexRecycleWorkKind(basis.decision.work),
      ...(stage.phase==='held'?{stage:copyStage(stage),reasonText:stage.reason,userStep:recoveryUserStep(peer,incident),suspend:stage.suspend,retryPending:stage.retryPending,
        leftover:stage.leftover||undefined}:{})};
    return {phase:stage.phase,trigger:stage.trigger,revision:stage.phase==='held'&&(stage.retryPending||stage.observationGap?.exhausted)?undefined:basis?.decision.revision,work:basis?.decision.work,
      since:basis?.decision.since,old_generation:basis?.old,reason:stage.phase==='held'?stage.reason:undefined,
      original_source:basis?.profile?{lineage:basis.profile.lineage,rollout:basis.profile.file,version:basis.profile.launch?.version}:undefined,
      status_text:stage.phase==='handback'?stuckStatusText(stage.fresh?'reconnecting':'handed_to_recovery',fields):stage.phase==='held'&&stage.code==='codex_recycle_partner_unreachable'?stuckUserLine('no_partner',fields):
        stage.phase==='held'&&stage.code==='incident_changed'?stuckStatusText('suspended',fields):stage.phase==='held'&&stage.code==='codex_recycle_stop_deadline'?stuckUserLine('stop_failed',fields):
        stage.phase==='held'&&stage.paused&&['launch_requested','verifying','retiring'].includes(stage.paused.phase)?stuckUserLine('launch_unknown',fields):
        stage.phase==='held'&&stage.paused?.phase==='stopped'?stuckStatusText('cleanup_blocked',{...fields,userStep:RECYCLE_CLEANUP_USER_STEP}):
        stage.phase==='held'&&stage.paused?.phase==='stop_requested'?stuckStatusText('old_unknown',fields):stuckStatusText(stage.phase,fields)};
  }
}
