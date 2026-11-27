                                   
export interface TakeoverPermission {
  approval:unknown;
  reviewer:unknown;
  sandbox:unknown;
  active:{id:string;extends?:string}|null;
}

export interface TakeoverPermissionEvidence {
  sessionId:string;
  file:string;
  offset:number;
  cwd:string;
  roots:unknown[];
  policy:TakeoverPermission;
  legacy?:Record<string,unknown>;
  named?:{definition:unknown};
  modelSettings?:TakeoverModelSettings;
}

export interface TakeoverModelSettings {model:string;modelProvider:string;reasoningEffort:string|null;serviceTier:string|null}
export interface TakeoverRestoreIntent {
  at:number;
  before:TakeoverPermissionEvidence;
}

export type TakeoverPermissionState='pending'|'restore_pending'|'restoring'|'kept'|'restored'|'failed'|'superseded';
export type TakeoverRestoreUnavailable='policy_changed'|'policy_not_representable';
export interface TakeoverPermissionView {
  change_id:string;
  state:TakeoverPermissionState;
  changed_fields:{field:'approval_policy'|'approvals_reviewer'|'sandbox_permissions';before:unknown;after:unknown;detail_only?:true}[];
  error_code?:string;
  restore_available:boolean;
  restore_unavailable_reason?:TakeoverRestoreUnavailable;
  error_phase?:'before_write'|'after_write';
  actual_policy?:{field:TakeoverPermissionView['changed_fields'][number]['field'];value:unknown}[];
  correction?:true;
  recheck_via_open?:true;
}
import {isDeepStrictEqual} from 'node:util';
import {sameDirectory} from '../shared/files.js';


export interface AcceptedTakeoverPermission {
  previous:TakeoverPermissionEvidence[];
  final:TakeoverPermissionEvidence;
}

export const sameTakeoverSandbox=(left:TakeoverPermission,right:TakeoverPermission)=>isDeepStrictEqual(left.active,right.active)&&isDeepStrictEqual(left.sandbox,right.sandbox);
export const sameTakeoverPermission=(left:TakeoverPermission,right:TakeoverPermission)=>sameTakeoverSandbox(left,right)&&isDeepStrictEqual(left.approval,right.approval)&&isDeepStrictEqual(left.reviewer,right.reviewer);
export const sameTakeoverHistory=(left:TakeoverPermissionEvidence,right:TakeoverPermissionEvidence)=>left.sessionId===right.sessionId&&sameDirectory(left.file,right.file);
export const sameTakeoverScope=(left:TakeoverPermissionEvidence,right:TakeoverPermissionEvidence)=>sameTakeoverHistory(left,right)&&sameDirectory(left.cwd,right.cwd)&&isDeepStrictEqual(left.roots,right.roots);
export const sameTakeoverModelSettings=(left:TakeoverPermissionEvidence,right:TakeoverPermissionEvidence)=>
  !!left.modelSettings&&!!right.modelSettings&&isDeepStrictEqual(left.modelSettings,right.modelSettings);

function displaySandbox(policy:TakeoverPermission){
  const sandbox=policy.sandbox as {type?:string;network?:string};
  return{profile_id:policy.active?.id,extends:policy.active?.extends,type:sandbox.type,network:sandbox.network};
}

export function takeoverActualPolicy(policy:TakeoverPermission):NonNullable<TakeoverPermissionView['actual_policy']>{
  return [{field:'approval_policy',value:policy.approval},{field:'approvals_reviewer',value:policy.reviewer},{field:'sandbox_permissions',value:displaySandbox(policy)}];
}

export function takeoverPermissionFields(before:TakeoverPermission,after:TakeoverPermission):TakeoverPermissionView['changed_fields']{
  const fields:TakeoverPermissionView['changed_fields']=[];
  if(!isDeepStrictEqual(before.approval,after.approval))fields.push({field:'approval_policy',before:before.approval,after:after.approval});
  if(!isDeepStrictEqual(before.reviewer,after.reviewer))fields.push({field:'approvals_reviewer',before:before.reviewer,after:after.reviewer});
  if(!sameTakeoverSandbox(before,after)){
    const previous=displaySandbox(before),next=displaySandbox(after);
    fields.push({field:'sandbox_permissions',before:previous,after:next,...(isDeepStrictEqual(previous,next)?{detail_only:true}:{})});
  }
  return fields;
}
