import {isDeepStrictEqual} from 'node:util';
import type {RpcClient} from '../../ipc/rpc.js';
import {BridgeError} from '../../shared/errors.js';
import {delay} from '../../shared/time.js';
import {builtinCodexPermission,normalizedPermission,readSessionProfile,recordedTakeoverModelSettings,sandboxCandidate,withPersistedCodexPermission,type SessionProfile} from './codex-session-profile.js';
import {sameTakeoverSandbox,sameTakeoverPermission,sameTakeoverScope,type AcceptedTakeoverPermission,type TakeoverPermissionEvidence,type TakeoverRestoreUnavailable} from '../takeover-permission.js';

export const TAKEOVER_APPLIED_WAIT_MS=5000;

export function takeoverPolicyEvidence(profile:SessionProfile):TakeoverPermissionEvidence|undefined{
  const applied=profile.appliedTakeoverPermission;
  if(!profile.complete||!profile.verified||!profile.file||!applied)return;
  const policy={...applied.policy,sandbox:normalizedPermission(applied.policy.sandbox)};
  return{sessionId:profile.sessionId,file:profile.file,offset:applied.offset,cwd:applied.cwd,roots:applied.roots,policy,
    ...(applied.modelSettings?{modelSettings:applied.modelSettings}:{}),
    ...(!policy.active?{legacy:sandboxCandidate({...profile,settings:{...profile.settings,cwd:applied.cwd,permissionProfile:policy.sandbox,sandbox:applied.sandbox}})}:{})};
}

export async function readTakeoverPolicy(file:string,sessionId:string,after?:number,wait=after===undefined?200:TAKEOVER_APPLIED_WAIT_MS):Promise<{evidence:TakeoverPermissionEvidence;profile:SessionProfile}>{
  const deadline=Date.now()+wait;
  do{
    const profile=await readSessionProfile(file,sessionId),evidence=takeoverPolicyEvidence(profile);
    if(evidence&&(after===undefined||evidence.offset>after))return{evidence,profile};
    if(Date.now()>=deadline||after===undefined&&!profile.pendingTail)break;
    await delay(25);
  }while(true);
  throw new BridgeError('policy_unverified','原会话缺少完整的权限生效记录。');
}

                                 
function permissionDefinition(value:unknown):unknown{
  if(!value||typeof value!=='object'||Array.isArray(value))return value;
  return Object.fromEntries(Object.entries(value).filter(([,entry])=>entry!==null).map(([key,entry])=>[key,permissionDefinition(entry)]));
}

async function currentNamedDefinition(rpc:RpcClient,evidence:TakeoverPermissionEvidence,id:string):Promise<unknown>{
  const config=await rpc.request('config/read',{cwd:evidence.cwd},{timeout:2000});
  const configured=config.config?.permissions?.[id];
  if(!configured)return;
  const {description:_,...policy}=configured;
  return permissionDefinition(policy);
}

                                            
export async function captureNamedTakeoverDefinition(rpc:RpcClient,evidence:TakeoverPermissionEvidence,profile:SessionProfile):Promise<TakeoverPermissionEvidence['named']>{
  const active=evidence.policy.active;
  if(!active||builtinCodexPermission(active.id)||active.extends)return;
  const definition=withPersistedCodexPermission({...profile,activePermission:active,settings:{...profile.settings,permissionProfile:evidence.policy.sandbox,
    approvalPolicy:evidence.policy.approval,approvalsReviewer:evidence.policy.reviewer}}).livePermissionConfig?.['permissions.'+active.id];
  if(!definition)return;
  const configured=await currentNamedDefinition(rpc,evidence,active.id);
  if(!isDeepStrictEqual(definition,configured))return;
  return{definition};
}

export function takeoverRestoreUnavailable(original:TakeoverPermissionEvidence,current:TakeoverPermissionEvidence):TakeoverRestoreUnavailable|undefined{
  if(!sameTakeoverScope(original,current))return 'policy_changed';
  const {active}=original.policy;
  if(!sameTakeoverSandbox(original.policy,current.policy)&&(active?active.extends||!builtinCodexPermission(active.id)&&!original.named:!original.legacy))
    return 'policy_not_representable';
}

export async function takeoverRestoreArgs(rpc:RpcClient,original:TakeoverPermissionEvidence,current:TakeoverPermissionEvidence):Promise<Record<string,unknown>>{
  const unavailable=takeoverRestoreUnavailable(original,current);
  if(unavailable)throw new BridgeError(unavailable,'无法在当前宿主中精确恢复原权限定义。');
  const {policy}=original,settings:Record<string,unknown>={};
  if(!isDeepStrictEqual(policy.approval,current.policy.approval))settings.approvalPolicy=policy.approval;
  if(!isDeepStrictEqual(policy.reviewer,current.policy.reviewer))settings.approvalsReviewer=policy.reviewer;
  if(sameTakeoverSandbox(policy,current.policy))return settings;
  if(!policy.active&&original.legacy){settings.sandboxPolicy=original.legacy;return settings;}
  if(policy.active&&builtinCodexPermission(policy.active.id)){settings.permissions=policy.active.id;return settings;}
  if(policy.active&&original.named){
    const definition=await currentNamedDefinition(rpc,original,policy.active.id);
    if(isDeepStrictEqual(definition,original.named.definition)){
      settings.permissions=policy.active.id;return settings;
    }
  }
  throw new BridgeError('policy_not_representable','无法在当前宿主中精确恢复原权限定义。');
}

export function restartTakeoverEvidence(profile:SessionProfile):TakeoverPermissionEvidence|undefined{
  if(!profile.file||profile.takeoverPermissionConflict)return;
  const saved=profile.settings;
                                            
  return takeoverPolicyEvidence(profile)??(profile.verified&&profile.complete&&saved.permissionProfile&&saved.approvalPolicy!=null
    &&saved.approvalsReviewer!=null&&typeof saved.cwd==='string'&&Array.isArray(saved.runtimeWorkspaceRoots)?{
      sessionId:profile.sessionId,file:profile.file,offset:profile.settingsOffset??0,cwd:saved.cwd,roots:saved.runtimeWorkspaceRoots,
      policy:{approval:saved.approvalPolicy,reviewer:saved.approvalsReviewer,sandbox:normalizedPermission(saved.permissionProfile),active:profile.live?.activePermissionProfile??profile.activePermission??null},
    }:undefined);
}

                                     
export async function acceptedTakeoverProfile(profile:SessionProfile,acceptance?:AcceptedTakeoverPermission):Promise<SessionProfile>{
  if(!acceptance||profile.sessionId!==acceptance.final.sessionId)return profile;
  const old=restartTakeoverEvidence(profile);
  if(!old||!acceptance.previous.some(value=>sameTakeoverScope(old,value)&&sameTakeoverPermission(old.policy,value.policy)))return profile;
  const {evidence,current}=await readTakeoverPolicy(acceptance.final.file,profile.sessionId).then(result=>({evidence:result.evidence,current:result.profile})).catch(()=>({evidence:undefined,current:undefined}));
  if(!evidence||!current||evidence.offset<acceptance.final.offset||!sameTakeoverScope(evidence,acceptance.final)||!sameTakeoverPermission(evidence.policy,acceptance.final.policy))return profile;
  const {policy}=evidence,settings:SessionProfile['settings']={...profile.settings,approvalPolicy:policy.approval,approvalsReviewer:policy.reviewer,permissionProfile:policy.sandbox};
  delete settings.sandbox;
  if(current.settings.sandbox!==undefined)settings.sandbox=current.settings.sandbox;
  const accepted={...profile,settings,activePermission:policy.active,appliedTakeoverPermission:current.appliedTakeoverPermission,
    live:profile.live?{...profile.live,approvalPolicy:policy.approval,approvalsReviewer:policy.reviewer,activePermissionProfile:policy.active}:undefined,livePermissionConfig:undefined};
  return withPersistedCodexPermission(recordedTakeoverModelSettings(accepted,current));
}
