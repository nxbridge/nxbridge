import { randomUUID } from 'node:crypto';
import { BridgeError, describeError } from '../../shared/errors.js';
import type { ManagedIncident, ManagedRecovery } from './managed-recovery.js';
import { evidenceUnverifiedReason, type AuthorityLoss, type SuspendReason } from '../stuck-session-text.js';

export type CodexRecycleTrigger = 'native_thread_wedged' | 'cold_exhausted' | 'loaded_unleased' | 'loaded_unleased_expired';
export interface CodexRecycleGeneration { runtimeId:string; endpoint:string; pid:number; procStart:string; bootId?:string }
export interface CodexRecycleProfileRef { sourceId:string; sessionId:string; file:string; settingsKey:string; lineage?:'native_catalog'|'saved_rollout';
  launch?:{executable:string;version:string;configHome:string;method:string;profileId?:string;commandKey?:string};
}
export interface CodexRecycleLaunch { runtimeId:string }
export interface CodexRecycleInput { id:string; senderId:string }
export interface CodexRecycleLedger { held:string[]; uncertain:CodexRecycleInput[]; uncertainSince?:number }
export interface CodexRecycleWork {
  activeTurnIds:string[];
  progress:'idle'|'progressing'|'stalled'|'unknown';
  lastProgressAt?:number;
  roles:string[];
  unresolved:string[];
  unattested?:{pid:number;procStart:string;role:string;parentPid?:number;image?:string}[];
}
export type CodexRecycleEvidence =
  | { kind:'available'; old:CodexRecycleGeneration; profile:CodexRecycleProfileRef; work:CodexRecycleWork }
  | { kind:'natural'; idleLoaded?:true }
  | { kind:'held'; code:string; reason:string };
type Available = Extract<CodexRecycleEvidence,{kind:'available'}>;
export interface CodexRecycleDecision {
  revision:string; facts:string; work:CodexRecycleWork; since:number; noticeId:string; autoEligible:boolean; noticePosted?:true;
}
export interface CodexRecycleAcceptance { incidentId:string; evidenceRevision:string; reason:string; by?:'partner'|'user' }
interface ObservationGap { since:number; attempts:number; reason:string; exhausted?:true; lapsedRevision?:string }
interface Stage { step:string; trigger:CodexRecycleTrigger; observationGap?:ObservationGap }
export interface CodexRecycleBasis { old:CodexRecycleGeneration; profile:CodexRecycleProfileRef; decision:CodexRecycleDecision; acceptance?:CodexRecycleAcceptance }
type Basis=CodexRecycleBasis;
interface Attempt extends Basis { id:string; at:number; stopDeadline:number; ledger:CodexRecycleLedger }
interface StopRetry { reason:string; basis?:Basis }
type StopRequested = { phase:'stop_requested'; attempt:Attempt; signal?:'unsubmitted'|'possibly_submitted'; stopRetry?:StopRetry };
type Stopped = { phase:'stopped'; attempt:Attempt };
type LaunchRequested = { phase:'launch_requested'; attempt:Attempt; launch:CodexRecycleLaunch };
type Verifying = { phase:'verifying'; attempt:Attempt; launch:CodexRecycleLaunch; fresh:CodexRecycleGeneration; resumeSubmitted:boolean };
type Consumed = StopRequested | Stopped | LaunchRequested | Verifying;
type Retiring = { phase:'retiring'; verification:Verifying; failure:{code:string;reason:string} };
const deadlineReason={alive:'停止期限已过，原进程仍在运行。',dead:'停止期限已过，已确认原进程退出，替代后台尚未启动。',
  unknown:'停止期限已过，原进程的退出结果仍未确认。'};
export type CodexRecycleOperation = Stage & (
  | { phase:'observing'; basis?:Basis; retrySample?:string }
  | { phase:'awaiting_partner'; basis:Basis; retrySample?:string }
  | { phase:'preflight'; basis:Basis; stopRetry?:StopRetry }
  | Consumed
 | { phase:'handback'; attempt:Attempt; fresh?:CodexRecycleGeneration }
  | { phase:'held'; code:string; reason:string; basis?:Basis; paused?:Consumed|Retiring; suspend?:SuspendReason; leftover?:number; retrySample?:string; retryPending?:true }
  | { phase:'ready'; attempt:Attempt; fresh:CodexRecycleGeneration; finalized:boolean }
);
type NextOperation = CodexRecycleOperation extends infer T ? T extends Stage ? Omit<T,keyof Stage> : never : never;
export interface CodexRecyclePorts {
                                        
  observe(trigger:CodexRecycleTrigger,hasHeldInput?:()=>boolean):Promise<CodexRecycleEvidence>;
                                             
  guard(incident:ManagedIncident):void;
  partnerReachable():boolean;
  deliveredTurnCompletedSince(at:number):boolean;
  ledger():CodexRecycleLedger;
                                        
  notify(incident:ManagedIncident,basis:Basis,ledger:CodexRecycleLedger):void;
  acceptanceLapsed?(incident:ManagedIncident,revision:string):void;
  persistProfile(evidence:Available):void;
  inspectOld(old:CodexRecycleGeneration):Promise<'alive'|'dead'|'unknown'>;
  stop(old:CodexRecycleGeneration,deadline:number,guard:()=>void,markSignal:()=>void,force:boolean):Promise<void>;
  retireOld(old:CodexRecycleGeneration,guard:()=>void,work:CodexRecycleWork):Promise<void>;
  prepareLaunch(profile:CodexRecycleProfileRef):Promise<CodexRecycleLaunch>;
  launch(intent:CodexRecycleLaunch,guard:()=>void,profile:CodexRecycleProfileRef):Promise<void>;
  inspectLaunch(intent:CodexRecycleLaunch):Promise<
    { kind:'matched'; fresh:CodexRecycleGeneration } | { kind:'unstarted'|'absent'|'unknown' }
  >;
  verify(fresh:CodexRecycleGeneration,profile:CodexRecycleProfileRef,options:{
    resumeSubmitted:boolean; markSubmitted:()=>void; guard:()=>void; uncertainSince?:number;
  }):Promise<void>;
  admit?(incident:ManagedIncident,fresh:CodexRecycleGeneration):void;
  prepareUnsubscribed?(fresh:CodexRecycleGeneration,profile:CodexRecycleProfileRef):Promise<void>;
  admitUnsubscribed?():void;
  recoveredSubscription?():CodexRecycleGeneration|undefined;
  foldHandbackReceipts?(profile:CodexRecycleProfileRef,ledger:CodexRecycleLedger):Promise<()=>void>;
  authorityLost?(incident:ManagedIncident,loss:AuthorityLoss):void;
                                           
  finish(incident:ManagedIncident,ready:Extract<CodexRecycleOperation,{phase:'ready'}>):void;
}
export interface CodexRecycleResult { kind:'natural'|'current'|'stale'; incident:ManagedIncident; retryObservation?:true }

export function sameCodexRecycleGeneration(a:CodexRecycleGeneration,b:CodexRecycleGeneration):boolean {
  return a.runtimeId===b.runtimeId&&a.endpoint===b.endpoint&&a.pid===b.pid&&a.procStart===b.procStart&&a.bootId===b.bootId;
}

                                        
export function codexRecycleFenced(incident:ManagedIncident|undefined):boolean {
  if(incident?.state==='closed')return false;
  const operation=incident?.recycle;
  if(!operation)return false;
  if(operation.phase==='handback')return false;
  if(operation.phase==='ready')return !operation.finalized;
  if(operation.phase==='held')return !!operation.paused;
  return !['observing','awaiting_partner'].includes(operation.phase);
}

export function codexRecycleWorkKind(work:CodexRecycleWork):'active_turn'|'unknown_work'|'idle' {
  if(work.activeTurnIds.length)return 'active_turn';
  return work.progress!=='idle'||work.roles.length||work.unresolved.length?'unknown_work':'idle';
}
const busy=(work:CodexRecycleWork)=>codexRecycleWorkKind(work)!=='idle';
const uniqueSorted=(values:string[])=>[...new Set(values)].sort();
const stalledAfter=15*60*1000;
const observationGapAttempts=6,observationGapMs=5*60*1000;
const recycleEvidenceHeldCode='codex_recycle_evidence_unverified';
function withProgress(evidence:Available,previous:Basis|undefined,now:number):Available {
  const {work}=evidence;
  if(!work.activeTurnIds.length||work.progress!=='unknown')return evidence;
  const prior=previous&&sameCodexRecycleGeneration(previous.old,evidence.old)&&
    JSON.stringify(uniqueSorted(previous.decision.work.activeTurnIds))===JSON.stringify(uniqueSorted(work.activeTurnIds))?previous.decision.work:undefined;
  const event=work.lastProgressAt!==undefined&&work.lastProgressAt<=now?work.lastProgressAt:undefined;
  const lastProgressAt=event===undefined?prior?.lastProgressAt:Math.max(event,prior?.lastProgressAt??0);
  const advanced=event!==undefined&&(prior?.lastProgressAt===undefined||event>prior.lastProgressAt);
  const progress=lastProgressAt===undefined?'unknown':now-lastProgressAt>=stalledAfter||prior?.progress==='stalled'&&!advanced?'stalled':'progressing';
  return {...evidence,work:{...work,lastProgressAt,progress}};
}
function decisionFacts(evidence:Available,progressGate:boolean):string {
  const {work,old,profile}=evidence;
  return JSON.stringify([
    old.runtimeId,old.endpoint,old.pid,old.procStart,old.bootId,profile.sourceId,profile.sessionId,profile.file,profile.settingsKey,
    uniqueSorted(work.activeTurnIds),work.progress==='stalled',uniqueSorted(work.roles),uniqueSorted(work.unresolved),progressGate,
  ]);
}
function heldError(error:unknown):{code:string;reason:string} {
  return {code:error instanceof BridgeError?error.code:'codex_recycle_failed',reason:describeError(error).slice(0,1200)};
}
export function recycleEvidenceReadFailure(source:string,error:unknown):Extract<CodexRecycleEvidence,{kind:'held'}> {
  return {kind:'held',code:error instanceof BridgeError?error.code:recycleEvidenceHeldCode,
    reason:(error instanceof BridgeError?describeError(error):evidenceUnverifiedReason(source,describeError(error))).slice(0,1200)};
}
export const isCodexRecycleTransientObservation=(code:string)=>['timeout','rpc_backpressure','disconnected','restart_profile_pending_tail'].includes(code);

export class CodexRecycle {
  private operations=new Map<string,{id:string; work:Promise<CodexRecycleResult>}>();
  constructor(private recovery:ManagedRecovery,private now=Date.now) {}
  get pending():boolean{return this.operations.size>0;}


  async advance(incident:ManagedIncident,trigger:CodexRecycleTrigger,ports:CodexRecyclePorts,acceptance?:CodexRecycleAcceptance):Promise<CodexRecycleResult> {
    const pending=this.operations.get(incident.peerId);
    if(pending){
      if(!acceptance&&pending.id===incident.id)return pending.work;
      await pending.work;
      return this.advance(incident,trigger,ports,acceptance);
    }
    const work=Promise.resolve().then(()=>this.run(incident,trigger,ports,acceptance));
    this.operations.set(incident.peerId,{id:incident.id,work});
    try{return await work;}finally{this.operations.delete(incident.peerId);}
  }

  private async run(original:ManagedIncident,trigger:CodexRecycleTrigger,ports:CodexRecyclePorts,acceptance?:CodexRecycleAcceptance):Promise<CodexRecycleResult> {
    let incident=this.recovery.get(original.peerId)??original,operation=incident.recycle;
    const stopRetry=operation?.phase==='preflight'||operation?.phase==='stop_requested'&&operation.signal==='unsubmitted'?operation.stopRetry:undefined;
    const stopFailed:Extract<CodexRecycleOperation,{phase:'held'}>|undefined=operation?.phase==='held'&&operation.code==='codex_recycle_stop_deadline'?operation:
      operation&&stopRetry?{phase:'held',code:'codex_recycle_stop_deadline',step:operation.step,trigger:operation.trigger,...stopRetry}:undefined;
    const retrySource=stopFailed&&{reason:stopFailed.reason,basis:stopFailed.basis&&{...stopFailed.basis,acceptance:undefined}};
    let retryObservation: true|undefined, gap=operation?.observationGap;
    const result=(kind:CodexRecycleResult['kind']='current'):CodexRecycleResult=>({kind,incident,retryObservation});
    if(incident.id!==original.id||incident.pairId!==original.pairId||incident.sessionId!==original.sessionId||incident.state==='closed')return result('stale');
    const commit=(next:NextOperation|undefined,sideEffect?:(current:ManagedIncident)=>void,observationGap?:ObservationGap)=>{
      incident=this.recovery.updateRecycle(incident,operation,next?{...next,trigger,step:randomUUID(),observationGap}:undefined,sideEffect);
      operation=incident.recycle;
    };
    const guard=()=>{
      const current=this.recovery.get(incident.peerId);
      if(!current||current.state==='closed'||current.id!==incident.id||current.pairId!==incident.pairId||current.sessionId!==incident.sessionId||current.recycle?.step!==operation?.step)
        throw new BridgeError('incident_changed','恢复阶段、会话或配对已变，已暂停回收。');
      ports.guard(current);
    };
    const progressGate=()=>{
      const last=this.recovery.lastRecycle(incident.peerId);
      return last===undefined||ports.deliveredTurnCompletedSince(last);
    };
    const hold=(code:string,reason:string,paused?:Consumed|Retiring,basis?:Basis,suspend?:SuspendReason,leftover?:number)=>{
      if(operation?.phase==='held'&&operation.code===code&&operation.reason===reason&&operation.paused===paused
        &&operation.basis?.decision.revision===basis?.decision.revision)return;
      commit({phase:'held',code,reason,paused,basis,suspend,leftover});
    };
    const fail=(error:unknown,paused?:Consumed|Retiring)=>{
      const {code,reason}=heldError(error);if(code==='codex_recycle_authority_lost')throw error;
      const detail=error as {suspend?:SuspendReason;leftover?:number}|undefined;
      hold(code,reason,paused,undefined,detail?.suspend,detail?.leftover);
    };
    const awaitPartner=(basis:Basis)=>{
      const posted=basis.decision.noticePosted;
      const waiting={...basis,decision:{...basis.decision,noticePosted:true as const}};
      commit({phase:'awaiting_partner',basis:waiting},posted?undefined:current=>ports.notify(current,waiting,ports.ledger()));
    };
    const finish=()=>{
      if(operation?.phase!=='ready'||operation.finalized)return;
      const ready=operation;
      guard();commit({...ready,finalized:true},current=>ports.finish(current,ready));
    };
    const reobserve=async()=>{
      guard();
      const evidence=await ports.observe(trigger).catch(error=>{
        const detail=heldError(error);if(!isCodexRecycleTransientObservation(detail.code))throw error;
        return {kind:'held' as const,...detail};
      });
      guard();return evidence;
    };
    const observeAgain=(basis?:Basis,retrySample?:string,observationGap=basis?.acceptance?gap:undefined)=>commit(stopFailed?
      {...stopFailed,basis:basis??(stopFailed.basis&&{...stopFailed.basis,acceptance:undefined}),retrySample}:{phase:'observing',basis,retrySample},undefined,observationGap);
    const lapseObservation=({since,attempts,reason,lapsedRevision}:ObservationGap,basis?:Basis)=>{
      const exhausted:ObservationGap={since,attempts,reason,exhausted:true,lapsedRevision:lapsedRevision??basis?.acceptance?.evidenceRevision};
      const revision=exhausted.lapsedRevision;
      const notify=revision&&!lapsedRevision?(current:ManagedIncident)=>ports.acceptanceLapsed?.(current,revision):undefined;
      if(stopFailed){
        if(!stopFailed.observationGap?.exhausted||operation!==stopFailed||stopFailed.retrySample){
          const basis=stopFailed.basis&&{...stopFailed.basis,acceptance:undefined};
          commit({...stopFailed,basis,retryPending:true,retrySample:undefined},notify,exhausted);
        }
      }else if(operation?.phase!=='held'||!operation.observationGap?.exhausted||operation.basis){
        const elapsed=this.now()-since;
        const detail=elapsed>=observationGapMs?`已持续约 ${Math.floor(elapsed/60000)} 分钟`:`连续 ${attempts} 次取证失败`;
        const heldReason=operation?.phase==='held'&&operation.observationGap?.exhausted?operation.reason:evidenceUnverifiedReason('回收证据',`${detail}：${reason}`);
        commit({phase:'held',code:recycleEvidenceHeldCode,reason:heldReason},notify,exhausted);
      }
      retryObservation=true;
    };
    const lapseIfOverdue=(basis?:Basis)=>{
      if(!basis?.acceptance||!gap||gap.exhausted||this.now()-gap.since<observationGapMs)return false;
      lapseObservation(gap,basis);return true;
    };
    const postponeObservation=(reason:string,basis?:Basis)=>{
      if(!gap?.exhausted)gap={since:gap?.since??this.now(),attempts:(gap?.attempts??0)+1,reason};
      if(gap.exhausted||gap.attempts>=observationGapAttempts||this.now()-gap.since>=observationGapMs){lapseObservation(gap,basis);return;}
                                 
      if(!stopFailed&&basis?.decision.noticePosted&&(operation?.phase==='awaiting_partner'||operation?.phase==='preflight'||
        operation?.phase==='stop_requested'&&operation.signal==='unsubmitted')){
        commit({phase:'awaiting_partner',basis},undefined,gap);
      }else observeAgain(basis,undefined,gap);
      retryObservation=true;
    };
    try{
      if(operation?.phase==='ready'){finish();return result();}
      if(operation?.phase==='handback'){
        const handback=operation,fresh=ports.recoveredSubscription?.();
        if(!fresh||fresh.runtimeId===operation.attempt.old.runtimeId||fresh.pid===operation.attempt.old.pid&&fresh.procStart===operation.attempt.old.procStart)
          return result('natural');
        if(handback.fresh&&!sameCodexRecycleGeneration(handback.fresh,fresh)&&await ports.inspectOld(handback.fresh).catch(()=>'unknown')!=='dead')
          return result('natural');
        const fold=await ports.foldHandbackReceipts?.(handback.attempt.profile,handback.attempt.ledger);
        const current=ports.recoveredSubscription?.();
        if(!current||!sameCodexRecycleGeneration(current,fresh))return result('natural');
        guard();
        commit({...handback,fresh,phase:'ready',finalized:false},fold);finish();return result();
      }
      const paused=operation?.phase==='held'?operation.paused:undefined;
      let stage:Consumed|undefined=operation?.phase==='held'?(paused?.phase==='retiring'?paused.verification:paused):
        operation&&'attempt' in operation?operation:undefined;
      if(stage){
        trigger=operation!.trigger;
      }else{
        const awaiting=operation?.phase==='awaiting_partner'||operation?.phase==='preflight'||operation?.phase==='observing'?operation.basis:
          operation?.phase==='held'?operation.basis:undefined;
        const acceptedHere=acceptance&&awaiting&&(operation?.phase==='awaiting_partner'||operation?.phase==='held'&&operation.code==='codex_recycle_stop_deadline'&&!operation.retryPending||
          operation?.phase==='held'&&operation.code==='codex_recycle_partner_unreachable'&&acceptance.by==='user')&&
          acceptance.incidentId===incident.id&&acceptance.evidenceRevision===awaiting.decision.revision;
        if(acceptance&&awaiting?.acceptance&&!acceptedHere&&(acceptance.incidentId!==incident.id||acceptance.evidenceRevision!==awaiting.acceptance.evidenceRevision))
          return result('stale');
        const approved=acceptedHere?acceptance:awaiting?.acceptance;
        const queued=approved&&awaiting?{...awaiting,acceptance:approved}:undefined;
        if(!operation)commit({phase:'observing'});
        const observed=await reobserve();
        const evidence=observed.kind==='available'?withProgress(observed,awaiting,this.now()):observed;
        if(evidence.kind==='held'){
          if(isCodexRecycleTransientObservation(evidence.code)){
            postponeObservation(evidence.reason,queued??(operation?.phase==='awaiting_partner'?awaiting:undefined));
          }else if(operation?.phase==='held'&&operation.code==='codex_recycle_stop_deadline'){
            if(operation.retrySample||operation.basis?.acceptance)observeAgain(operation.basis&&{...operation.basis,acceptance:undefined});
          }else hold(evidence.code,evidence.reason);
          return result(acceptance?'stale':'current');
        }
        if(evidence.kind==='natural'||trigger==='loaded_unleased'&&(!evidence.work.activeTurnIds.length||!ports.ledger().held.length)){
          commit(undefined);return result(acceptance?'stale':'natural');
        }
        if(lapseIfOverdue(queued))return result(acceptance?'stale':'current');
        const gate=!stopFailed&&progressGate(),facts=decisionFacts(evidence,gate);
        const previous=awaiting?.decision;
        const decision:CodexRecycleDecision=previous?.facts===facts?
          {...previous,work:evidence.work}:{facts,revision:randomUUID(),noticeId:randomUUID(),since:previous?.since??this.now(),work:evidence.work,autoEligible:gate};
        const basis:Basis={old:evidence.old,profile:evidence.profile,decision};
                                                      
        if(queued&&(awaiting?.acceptance||previous?.facts!==facts)){
          const sample=operation?.phase==='observing'||operation?.phase==='awaiting_partner'?operation.retrySample:stopFailed?.retrySample;
          if(sample!==facts){
            if(operation?.phase==='awaiting_partner'&&previous?.facts===facts)commit({phase:'awaiting_partner',basis:queued,retrySample:facts},undefined,gap);
            else observeAgain(queued,facts);
            return result(acceptance?'stale':'current');
          }
          if(previous?.facts!==facts&&!stopFailed&&(busy(evidence.work)||!gate)){
            if(ports.partnerReachable())awaitPartner(basis);
            else hold('codex_recycle_partner_unreachable','后台 Codex 可能还有工作，当前搭档无法确认是否回收。',undefined,basis);
            return result(acceptance?'stale':'current');
          }
        }
        const valid=approved&&decision.revision===approved.evidenceRevision?approved:undefined;
        if((busy(evidence.work)||!gate)&&!valid){
          if(stopFailed){
            if(!stopFailed.retryPending&&previous?.facts===facts){
              if(operation!==stopFailed||stopFailed.retrySample||gap)commit({...stopFailed,retrySample:undefined});
            }else if(stopFailed.retrySample===facts){
              const retryDecision=stopFailed.retryPending&&previous?.facts===facts?
                {...decision,revision:randomUUID(),noticeId:randomUUID(),noticePosted:undefined}:decision;
              commit({...stopFailed,basis:{...basis,decision:retryDecision},retryPending:undefined,retrySample:undefined});
            }else commit({...stopFailed,retrySample:facts},undefined,gap?.exhausted?gap:undefined);
            return result(acceptance?'stale':'current');
          }
          if(!ports.partnerReachable())hold('codex_recycle_partner_unreachable','后台 Codex 可能还有工作，当前搭档无法确认是否回收。',undefined,basis);
          else if(operation?.phase!=='awaiting_partner'||operation.basis.decision.revision!==decision.revision){
            const unverified=operation?.phase==='held'&&gap?.exhausted?operation:undefined;
            if((operation?.phase==='observing'||unverified)&&previous?.facts===facts)awaitPartner(basis);
            else if(unverified)commit({...unverified,basis},undefined,gap);
            else commit({phase:'observing',basis});
          }
          else if(gap||previous?.work.lastProgressAt!==decision.work.lastProgressAt||previous?.work.progress!==decision.work.progress)
            commit({phase:'awaiting_partner',basis});
          return result(acceptance?'stale':'current');
        }
        if(acceptance&&!valid)return result('stale');
        if(valid)basis.acceptance=valid;
        commit({phase:'preflight',basis,stopRetry:retrySource},undefined,gap);
        const sampled=await reobserve();
        const fresh=sampled.kind==='available'?withProgress(sampled,basis,this.now()):sampled;
        if(fresh.kind==='held'){
          if(isCodexRecycleTransientObservation(fresh.code))postponeObservation(fresh.reason,valid?basis:undefined);
          else observeAgain();
          return result(acceptance?'stale':'current');
        }
        const ledger=ports.ledger();
        if(fresh.kind==='natural'||trigger==='loaded_unleased'&&(!fresh.work.activeTurnIds.length||!ledger.held.length)){
          commit(undefined);return result(acceptance?'stale':'natural');
        }
        if(lapseIfOverdue(basis))return result(acceptance?'stale':'current');
        const currentFacts=decisionFacts(fresh,gate&&progressGate());
        if(currentFacts!==facts){
          observeAgain(valid?basis:undefined,valid?currentFacts:undefined);
          return result(acceptance?'stale':'current');
        }
        ports.persistProfile(fresh);
        const at=this.now(),attempt:Attempt={...basis,decision:{...decision,work:fresh.work},id:randomUUID(),at,stopDeadline:at+15000,ledger};
        stage={phase:'stop_requested',attempt,signal:'unsubmitted',stopRetry:retrySource};
        commit(stage,undefined,gap);
      }
      if(stage.phase==='stop_requested'){
        const {attempt}=stage,state=await ports.inspectOld(attempt.old).catch(()=>'unknown' as const);
        guard();
        if(stage.signal==='unsubmitted'&&(state!=='alive'||this.now()>=attempt.stopDeadline)){
          observeAgain();return result('stale');
        }
        if(state==='alive'&&this.now()>=attempt.stopDeadline){
          const decision={...attempt.decision,autoEligible:false,
            facts:decisionFacts({kind:'available',old:attempt.old,profile:attempt.profile,work:attempt.decision.work},false)};
          commit({phase:'held',code:'codex_recycle_stop_deadline',reason:deadlineReason[state],basis:{old:attempt.old,profile:attempt.profile,decision},retryPending:true});return result();
        }
        if(state!=='dead'){
          if(state!=='alive'||stage.signal!=='unsubmitted'){
            hold('codex_recycle_old_unknown','原进程尚未核验退出，已保留原代次及消息。',stage);return result();
          }
          const sampled=await reobserve();
          const fresh=sampled.kind==='available'?withProgress(sampled,attempt,this.now()):sampled;
                                             
          const gate=attempt.decision.autoEligible;
          const latestFacts=fresh.kind==='available'?decisionFacts(fresh,gate):undefined;
          const acceptedBasis=attempt.acceptance?{old:attempt.old,profile:attempt.profile,decision:attempt.decision,acceptance:attempt.acceptance}:undefined;
          if(fresh.kind==='held'&&isCodexRecycleTransientObservation(fresh.code)){
            postponeObservation(fresh.reason,acceptedBasis);return result();
          }
          if(lapseIfOverdue(acceptedBasis))return result();
          if(acceptedBasis&&latestFacts!==undefined&&latestFacts!==attempt.decision.facts){
            observeAgain(acceptedBasis,latestFacts);return result();
          }
          if(fresh.kind!=='available'||latestFacts!==attempt.decision.facts){
            throw new BridgeError('codex_recycle_work_changed','发出停止请求前，原会话的工作或归属证据已改变。');
          }
                                                
          const unattested=[...attempt.decision.work.unattested??[],...fresh.work.unattested??[]];
          const merged=[...new Map(unattested.map(holder=>[holder.pid+':'+holder.procStart,holder])).values()];
          if(merged.length!==(attempt.decision.work.unattested?.length??0)){
            stage={...stage,attempt:{...attempt,decision:{...attempt.decision,work:{...attempt.decision.work,unattested:merged}}}};
            commit(stage);
          }else if(operation?.phase==='held')commit(stage);
          let stopping=stage;
          const authorize=()=>{
            guard();
            if(stopping.signal==='unsubmitted'&&lapseIfOverdue(acceptedBasis))
              throw new BridgeError('codex_recycle_approval_lapsed','回收确认已经过期，未发出停止信号。');
            if(trigger==='loaded_unleased'&&!ports.ledger().held.length)
              throw new BridgeError('codex_recycle_work_changed','停止前的原消息已不再等待，未停止已加载的会话。');
          };
          await ports.stop(attempt.old,attempt.stopDeadline,authorize,()=>{
            authorize();
            if(stopping.signal==='possibly_submitted')return;
            stopping={...stopping,signal:'possibly_submitted'};
            commit(stopping,()=>this.recovery.recordRecycle(incident.peerId,this.now()));
          },!!attempt.acceptance&&busy(attempt.decision.work));
          guard();
        }
        stage={phase:'stopped',attempt:stage.attempt};commit(stage);
      }
      if(stage.phase==='stopped'){
        const {attempt}=stage;
        try{await ports.retireOld(attempt.old,guard,attempt.decision.work);guard();}
        catch(error){fail(error,stage);return result();}
        let launch:CodexRecycleLaunch;
        try{launch=await ports.prepareLaunch(attempt.profile);guard();}
        catch(error){
          if(error instanceof BridgeError&&['incident_changed','codex_recycle_authority_lost'].includes(error.code)){fail(error,stage);return result();}
          guard();commit({phase:'handback',attempt});return result('natural');
        }
        stage={phase:'launch_requested',attempt,launch};commit(stage);
      }
      if(stage.phase==='launch_requested'){
        const {attempt,launch}=stage;
        guard();let state=await ports.inspectLaunch(launch);guard();
        if(state.kind==='unstarted'){
          if(operation?.phase==='held')commit(stage);
          try{await ports.launch(launch,guard,attempt.profile);guard();state=await ports.inspectLaunch(launch);guard();}
          catch(error){fail(error,stage);return result();}
        }
        if(state.kind==='absent'){
          commit({phase:'handback',attempt});return result('natural');
        }
        if(state.kind!=='matched'){
          hold('codex_recycle_launch_unknown','替代进程的启动结果未确认，正在核对原启动记录。',stage);
          return result();
        }
        if(state.fresh.runtimeId!==launch.runtimeId||state.fresh.pid===attempt.old.pid&&state.fresh.procStart===attempt.old.procStart)
          throw new BridgeError('runtime_changed','替代进程代次与本次启动记录不符，未恢复。');
        stage={phase:'verifying',attempt,launch,fresh:state.fresh,resumeSubmitted:false};commit(stage);
      }
      if(stage.phase==='verifying'){
        let verifying=stage;
        const markSubmitted=()=>{
          guard();
          if(verifying.resumeSubmitted)throw new BridgeError('codex_recycle_verify_unknown','恢复请求已提交过，先核对原结果。');
          verifying={...verifying,resumeSubmitted:true};
          commit(verifying);
        };
        try{
          guard();await ports.verify(verifying.fresh,verifying.attempt.profile,{resumeSubmitted:verifying.resumeSubmitted,markSubmitted,guard,
            uncertainSince:verifying.attempt.ledger.uncertainSince});guard();
          commit({phase:'ready',attempt:verifying.attempt,fresh:verifying.fresh,finalized:false},current=>ports.admit?.(current,verifying.fresh));
        }catch(error){
          if(error instanceof BridgeError&&error.code==='codex_recycle_ack_missing'&&ports.prepareUnsubscribed&&ports.admitUnsubscribed){
            try{
              await ports.prepareUnsubscribed(verifying.fresh,verifying.attempt.profile);guard();
              commit({phase:'handback',attempt:verifying.attempt,fresh:verifying.fresh},ports.admitUnsubscribed);
              return result('natural');
            }catch(unverified){fail(unverified,verifying);return result();}
          }
          fail(error,verifying);return result();
        }
        finish();
      }
      return result();
    }catch(error){
      if(error instanceof BridgeError&&error.code==='codex_recycle_approval_lapsed')return result();
      if(error instanceof BridgeError&&error.code==='codex_recycle_authority_lost'){
        const loss=(error as BridgeError&{loss?:AuthorityLoss}).loss;
        this.recovery.closeRecycle(incident,loss?()=>ports.authorityLost?.(incident,loss):undefined);
        incident=this.recovery.get(incident.peerId)!;return result('stale');
      }
      const stopping=operation?.phase==='stop_requested'?operation:operation?.phase==='held'&&operation.paused?.phase==='stop_requested'?operation.paused:undefined;
      if(stopping?.signal==='unsubmitted'){
        try{observeAgain();}catch(changed){if(!(changed instanceof BridgeError)||changed.code!=='incident_changed')throw changed;}
        return result('stale');
      }
      if(stopping&&error instanceof BridgeError&&['incident_changed','codex_recycle_work_changed','foreground_active','session_changed','codex_recycle_owner_unverified'].includes(error.code)){
        try{
          fail(error,stopping);
        }catch(changed){if(!(changed instanceof BridgeError)||changed.code!=='incident_changed')throw changed;return result('stale');}
        return result();
      }
      if(error instanceof BridgeError&&error.code==='incident_changed'){
        try{
          if(operation?.phase==='launch_requested')fail(error,operation);
          else if(operation?.phase==='preflight'){
                                           
            observeAgain();
          }
        }catch(changed){if(!(changed instanceof BridgeError)||changed.code!=='incident_changed')throw changed;}
        return result('stale');
      }
      if(operation?.phase==='ready'||operation?.phase==='handback')throw error;
      const paused=operation&&'attempt' in operation?operation:operation?.phase==='held'?operation.paused:undefined;
      fail(error,paused);return result();
    }
  }
}
