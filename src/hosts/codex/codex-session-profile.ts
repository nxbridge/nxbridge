import { closeSync, createReadStream, fstatSync, globSync, openSync, statSync, type BigIntStats } from 'node:fs';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import {BridgeError,RuntimeRecoveryHold,describeError} from '../../shared/errors.js';
import { sameDirectory } from '../../shared/files.js';
import type { RpcClient } from '../../ipc/rpc.js';
import {delay} from '../../shared/time.js';
import {setImmediate as yieldToIo} from 'node:timers/promises';
import {CodexServiceTierError,nativeServiceTier,observedServiceTier} from './codex-service-tier.js';

export interface SessionPolicy {
  source:'persisted'|'conservative';sandbox:unknown;approval:unknown;reviewer:unknown;
  unknown:string[];narrowed:{rule:string;effect:string}[];reason?:'policy_unknown'|'policy_partial';
}
export interface SessionProfile {
  sessionId:string;file?:string;offset:number;settingsOffset?:number;complete:boolean;verified:boolean;meta:Record<string,any>;settings:Record<string,any>;
  pendingTail?:true;
  live?:Record<string,any>;livePermissionConfig?:Record<string,unknown>;
  activePermission?:{id:string;extends?:string}|null;
  appliedServiceTier?:{offset:number;present:boolean;value?:string|null};
  turn?:{id:string;state:'running'|'ready'|'paused';at:number};
  rolloutWork?:{turnOffset:number;lastWorkOffset:number;lastWorkAt?:number};
  usage?:{inputTokens:number;at:number};
  compaction?:{id:string;at:number};
}
interface Cache {profile:SessionProfile;stat:BigIntStats;invalid:boolean}
const caches=new Map<string,Cache>();
const pendingProfiles=new Map<string,{stat:BigIntStats;work:Promise<SessionProfile>}>();
const sameFile=(a:BigIntStats,b:BigIntStats)=>a.dev===b.dev&&a.ino===b.ino;
const sameVersion=(a:BigIntStats,b:BigIntStats)=>sameFile(a,b)&&a.size===b.size&&a.mtimeNs===b.mtimeNs;
const fields={model:'model',model_provider:'modelProvider',model_provider_id:'modelProvider',approval_policy:'approvalPolicy',approvals_reviewer:'approvalsReviewer',permission_profile:'permissionProfile',sandbox_policy:'sandbox',cwd:'cwd',runtime_workspace_roots:'runtimeWorkspaceRoots',effort:'reasoningEffort',reasoning_effort:'reasoningEffort',collaboration_mode:'collaborationMode',service_tier:'serviceTier',disabled_plugin_ids:'disabledPluginIds'} as const;
function knownTier(profile:SessionProfile):string|undefined{
  const live=profile.live&&Object.hasOwn(profile.live,'serviceTier'),applied=profile.appliedServiceTier;
                                             
  const value=live&&profile.live!.serviceTier===null&&applied?(applied.present?applied.value:null):live?profile.live!.serviceTier:profile.settings.serviceTier;
  const observed=observedServiceTier(value);
                                            
  return observed===undefined?undefined:nativeServiceTier(observed??'standard');
}
async function verifyResumedTier(rpc:RpcClient,id:string,profile:SessionProfile,resumed:any,current:SessionProfile,file?:string,guard?:()=>void):Promise<SessionProfile>{
  const expected=knownTier(profile);
  if(expected===undefined||observedServiceTier(resumed.serviceTier)===observedServiceTier(expected))return current;
  const saved=observedServiceTier(expected)!;
  const mismatch=(value:string|null|undefined)=>new CodexServiceTierError({code:value===undefined?'service_tier_unverified':'service_tier_mismatch',saved,...(typeof value==='string'?{live:value}:{})});
  if(resumed.serviceTier!==null)throw mismatch(observedServiceTier(resumed.serviceTier));
  if(!file)throw new CodexServiceTierError({code:'service_tier_unverified',saved});
  let actual:Awaited<ReturnType<typeof readLiveServiceTier>>;
  try{actual=await readLiveServiceTier(rpc,id,resumed,current,guard);}
  catch(error){if(error instanceof RuntimeRecoveryHold)throw error;throw new CodexServiceTierError({code:'service_tier_unverified',saved},describeError(error));}
  if(actual.serviceTier!==observedServiceTier(expected))throw mismatch(actual.profile?.appliedServiceTier?.present?actual.serviceTier:undefined);
  resumed.serviceTier=nativeServiceTier(actual.serviceTier!);
  return actual.profile!;
}

                                            
export async function readSessionProfile(file:unknown,sessionId:string,options:{trackWork?:boolean}={}):Promise<SessionProfile>{
  if(typeof file!=='string'||!file)return emptyProfile(sessionId);
  const key=JSON.stringify([file,sessionId,!!options.trackWork]);
  let fd:number|undefined,stat:BigIntStats;
  try{fd=openSync(file,'r');stat=fstatSync(fd,{bigint:true});}
  catch{if(fd!==undefined)closeSync(fd);return{...emptyProfile(sessionId),file};}
  const pending=pendingProfiles.get(key);
  if(pending){
    closeSync(fd);
    if(sameVersion(pending.stat,stat))return structuredClone(await pending.work);
                                           
    await pending.work;
    return readSessionProfile(file,sessionId,options);
  }
  const work=readProfile(file,sessionId,key,fd,stat,options.trackWork).finally(()=>pendingProfiles.delete(key));
  pendingProfiles.set(key,{stat,work});
  return structuredClone(await work);
}
const emptyProfile=(sessionId:string):SessionProfile=>({sessionId,offset:0,complete:true,verified:false,meta:{},settings:{}});
async function readProfile(file:string,sessionId:string,key:string,fd:number,stat:BigIntStats,trackWork?:boolean):Promise<SessionProfile>{
  const empty=()=>emptyProfile(sessionId);let stream:ReturnType<typeof createReadStream>|undefined;
  try{
    const size=Number(stat.size),previous=caches.get(key);
    if(!Number.isSafeInteger(size))return{...empty(),file};
    if(previous&&sameVersion(previous.stat,stat))return previous.invalid?{...empty(),file,complete:previous.profile.complete}:previous.profile;
    const append=previous&&sameFile(previous.stat,stat)&&stat.size>previous.stat.size;
    const cache=append?structuredClone(previous.profile):{...empty(),file};
    let invalid=append?previous.invalid:false;
    let tail=Buffer.alloc(0),fairnessBytes=0;
    if(size>cache.offset){
      stream=createReadStream(file,{fd,start:cache.offset,end:size-1});
      for await(const chunk of stream){
        fairnessBytes+=chunk.length;if(fairnessBytes>=1024*1024){await yieldToIo();fairnessBytes=0;}
        tail=tail.length?Buffer.concat([tail,chunk]):chunk;let newline:number;
        while((newline=tail.indexOf(10))>=0){
          const line=tail.subarray(0,newline).toString('utf8');tail=tail.subarray(newline+1);cache.offset+=newline+1;
          if(!line.trim())continue;
          if(cache.verified&&!/"(?:session_meta|turn_context|thread_settings_applied|task_started|task_complete|turn_complete|turn_aborted|token_count|compacted)"/.test(line)
            &&!(trackWork&&/"(?:exec_command_(?:begin|end)|mcp_tool_call_(?:begin|end)|function_call(?:_output)?|custom_tool_call(?:_output)?|patch_apply_(?:begin|end)|agent_message|agent_reasoning|reasoning|assistant)"/.test(line))){
            if(!line.trimEnd().endsWith('}'))invalid=true;
            continue;
          }
          let row:any;try{row=JSON.parse(line);}catch{invalid=true;continue;}
                                            
          if(!cache.verified){
            if(row.type!=='session_meta'||row.payload?.id!==sessionId){invalid=true;continue;}
            const m=row.payload;cache.meta={id:m.id,cwd:m.cwd,model_provider:m.model_provider,cli_version:m.cli_version,history_mode:m.history_mode,
              ...(trackWork?{session_id:m.session_id,parent_thread_id:m.parent_thread_id,attached_thread:m.attached_thread,thread_source:m.thread_source,source:m.source}:{})};cache.verified=true;
          }else if(row.type==='session_meta')continue;
          else{
            const at=Date.parse(row.timestamp);
            if(row.type==='compacted'&&Number.isFinite(at)){cache.compaction={id:[stat.dev,stat.ino,cache.offset].join(':'),at};delete cache.usage;}
            if(row.type==='event_msg'&&row.payload?.type==='token_count'&&Number.isFinite(at)){
              const inputTokens=row.payload.info?.last_token_usage?.input_tokens;
              if(Number.isSafeInteger(inputTokens)&&inputTokens>=0&&at>=(cache.compaction?.at??0))cache.usage={inputTokens,at};
            }
            if(row.type==='event_msg'&&['task_started','task_complete','turn_complete','turn_aborted'].includes(row.payload?.type)&&typeof row.payload.turn_id==='string'){
              if(Number.isFinite(at)){
                cache.turn={id:row.payload.turn_id,state:row.payload.type==='task_started'?'running':row.payload.type==='turn_aborted'?'paused':'ready',at};
                if(trackWork)(cache.rolloutWork??={turnOffset:0,lastWorkOffset:0}).turnOffset=cache.offset;
              }
            }
            if(trackWork&&(row.type==='event_msg'&&/^(?:exec_command|mcp_tool_call|patch_apply)_(?:begin|end)$/.test(row.payload?.type)
              ||row.type==='response_item'&&/^(?:function_call|custom_tool_call)(?:_output)?$/.test(row.payload?.type))){
              const work=cache.rolloutWork??={turnOffset:0,lastWorkOffset:0};
              work.lastWorkOffset=cache.offset;
              if(Number.isFinite(at))work.lastWorkAt=at;
            }
            if(trackWork&&Number.isFinite(at)&&(row.type==='event_msg'&&['agent_message','agent_reasoning'].includes(row.payload?.type)
              ||row.type==='response_item'&&(row.payload?.type==='reasoning'||row.payload?.type==='message'&&row.payload?.role==='assistant'))){
                                                
              const work=cache.rolloutWork??={turnOffset:0,lastWorkOffset:0};
              work.lastWorkAt=Math.max(work.lastWorkAt??0,at);
            }
            const value=row.type==='turn_context'?row.payload:row.type==='event_msg'&&row.payload?.type==='thread_settings_applied'&&row.payload.thread_id===sessionId?row.payload.thread_settings:undefined;
            if(!value||typeof value!=='object')continue;
            cache.settingsOffset=cache.offset;
            if(row.type==='event_msg'){
              const tier=observedServiceTier(value.service_tier);
              cache.appliedServiceTier={offset:cache.offset,present:Object.hasOwn(value,'service_tier'),...(tier===undefined?{}:{value:tier})};
                                               
              if(!cache.appliedServiceTier.present)delete cache.settings.serviceTier;
            }
                                                       
            if(Object.hasOwn(value,'active_permission_profile')){
              const active=value.active_permission_profile;
              cache.activePermission=active===null||active===undefined?null:
                typeof active.id==='string'&&active.id?{id:active.id,...(typeof active.extends==='string'?{extends:active.extends}:{})}:undefined;
            }else if(row.type==='event_msg'||Object.hasOwn(value,'permission_profile')||Object.hasOwn(value,'sandbox_policy'))cache.activePermission=null;
            for(const [source,target] of Object.entries(fields))if(Object.hasOwn(value,source))cache.settings[target]=value[source];
                                                         
            if(Object.hasOwn(value,'permission_profile')&&!Object.hasOwn(value,'sandbox_policy'))delete cache.settings.sandbox;
          }
        }
      }
      if(!sameFile(stat,statSync(file,{bigint:true})))return{...empty(),file};
    }
    cache.complete=cache.offset===size;
    if(cache.complete)delete cache.pendingTail;else cache.pendingTail=true;
    caches.delete(key);caches.set(key,{profile:cache,stat,invalid});while(caches.size>24)caches.delete(caches.keys().next().value!);
    if(invalid)return{...empty(),file,complete:cache.complete};
    return cache;
  }catch{return{...empty(),file};}finally{if(stream)stream.destroy();else closeSync(fd);}
}

export function sessionFile(home:string,sessionId:string):string|undefined{
  if(!/^[a-zA-Z0-9_-]+$/.test(sessionId))throw new BridgeError('session_invalid','会话 ID 不能包含路径或通配符。');
  const files=[...new Set(['sessions/**/rollout-*-'+sessionId+'.jsonl','sessions/'+sessionId+'.jsonl','archived_sessions/**/rollout-*-'+sessionId+'.jsonl'].flatMap(pattern=>globSync(pattern,{cwd:home}).map(file=>path.resolve(home,file))))];
  if(files.length>1)throw new BridgeError('session_ambiguous','原配置目录存在多个同 ID 历史文件，请核对后再托管。');
  return files[0];
}

function canonical(value:any):any{
  if(Array.isArray(value))return value.map(canonical);
  if(!value||typeof value!=='object')return value;
  const output:Record<string,unknown>={};for(const key of Object.keys(value).sort())output[key]=canonical(value[key]);return output;
}
                              
export function normalizedPermission(value:any):any{
  if(!value||typeof value!=='object')return value;
  const result=structuredClone(value);
  if(Array.isArray(result.file_system?.entries))result.file_system.entries=[...new Map(result.file_system.entries.map((entry:any)=>{const v=canonical(entry);return[JSON.stringify(v),v];})).entries()].sort(([a],[b])=>String(a).localeCompare(String(b))).map(([,v])=>v);
  return canonical(result);
}
const samePermission=(a:unknown,b:unknown)=>isDeepStrictEqual(normalizedPermission(a),normalizedPermission(b));
const rank:Record<string,number>={none:0,read:1,write:2};
function permissionPlan(profile:SessionProfile){
  const saved=profile.settings,unknown:string[]=[],narrowed:SessionPolicy['narrowed']=[],filesystem:Record<string,string>={};
  let permission=saved.permissionProfile,network=false;
  if(permission?.type==='disabled')return{options:{sandbox:'danger-full-access'},permission,policy:{source:'persisted',sandbox:permission,approval:saved.approvalPolicy??'on-request',reviewer:saved.approvalsReviewer??'user',unknown,narrowed} as SessionPolicy};
  if(permission?.type==='managed'&&permission.file_system?.type==='restricted'&&Array.isArray(permission.file_system.entries)){
    for(const entry of permission.file_system.entries){
      const key=entry.path?.type==='path'&&typeof entry.path.path==='string'?entry.path.path:entry.path?.type==='special'&&['root','slash_tmp','tmpdir','project_roots'].includes(entry.path.value?.kind)?':'+entry.path.value.kind:undefined;
      if(!key||rank[entry.access]===undefined){unknown.push('filesystem');break;}
      if(entry.missing_path_behavior&&entry.missing_path_behavior!=='skip'){unknown.push('filesystem');break;}
      if(entry.missing_path_behavior==='skip'){
        narrowed.push({rule:key,effect:entry.access==='write'?'省略仅对现存路径生效的写权限':'将仅对现存路径生效的限制保持为固定限制'});
        if(entry.access==='write')continue;
      }
      if(filesystem[key]!==undefined&&filesystem[key]!==entry.access){narrowed.push({rule:key,effect:'同路径规则取较严格权限'});if(rank[filesystem[key]!]!<rank[entry.access]!)continue;}
      filesystem[key]=entry.access;
    }
    if(permission.network==='enabled'||permission.network==='restricted')network=permission.network==='enabled';else unknown.push('network');
    if(Object.keys(permission).some(key=>!['type','file_system','network'].includes(key))){unknown.push('network');network=false;}
  }else if(permission?.type==='managed'&&permission.file_system?.type==='unrestricted'){
    filesystem[':root']='write';network=permission.network==='enabled';if(!['enabled','restricted'].includes(permission.network))unknown.push('network');
  }else unknown.push('filesystem','network');
  if(unknown.includes('filesystem')){for(const key of Object.keys(filesystem))delete filesystem[key];filesystem[':root']='none';}
                                          
  if(!permission&&saved.sandbox){const sandbox=saved.sandbox;if(sandbox.type==='danger-full-access'||sandbox.type==='dangerFullAccess')return{options:{sandbox:'danger-full-access'},permission:{type:'disabled'},policy:{source:'persisted',sandbox,approval:saved.approvalPolicy??'on-request',reviewer:saved.approvalsReviewer??'user',unknown:[],narrowed} as SessionPolicy};}
  if(saved.approvalPolicy===undefined)unknown.push('approval');if(saved.approvalsReviewer===undefined)unknown.push('reviewer');
  const entries=Object.entries(filesystem).map(([key,access])=>({path:key.startsWith(':')?{type:'special',value:{kind:key.slice(1)}}:{type:'path',path:key},access}));
  permission={type:'managed',file_system:{type:'restricted',entries},network:network?'enabled':'restricted'};
  const reason=unknown.includes('filesystem')?'policy_unknown':unknown.length||narrowed.length?'policy_partial':undefined;
  return{options:{permissions:'nxbridge_transfer',config:{'permissions.nxbridge_transfer':{filesystem,network:{enabled:network}},default_permissions:'nxbridge_transfer'}},permission,policy:{source:unknown.length?'conservative':'persisted',sandbox:permission,approval:saved.approvalPolicy??'on-request',reviewer:saved.approvalsReviewer??'user',unknown:[...new Set(unknown)],narrowed,...(reason?{reason}:{})} as SessionPolicy};
}

function sandboxCandidate(profile:SessionProfile):any{
  if(profile.settings.sandbox){const p=structuredClone(profile.settings.sandbox);p.type=({read_only:'readOnly','read-only':'readOnly',workspace_write:'workspaceWrite','workspace-write':'workspaceWrite',danger_full_access:'dangerFullAccess','danger-full-access':'dangerFullAccess',external_sandbox:'externalSandbox','external-sandbox':'externalSandbox'} as any)[p.type]??p.type;
    for(const [a,b] of [['network_access','networkAccess'],['writable_roots','writableRoots'],['exclude_tmpdir_env_var','excludeTmpdirEnvVar'],['exclude_slash_tmp','excludeSlashTmp']])if(p[a!]!==undefined){p[b!]=p[a!];delete p[a!];}return p;
  }
  const p=profile.settings.permissionProfile,entries=p?.file_system?.entries;
  if(p?.type==='disabled')return{type:'dangerFullAccess'};
  if(p?.type!=='managed'||!Array.isArray(entries)||!['enabled','restricted'].includes(p.network))return;
  if(entries.length===1&&entries[0]?.path?.value?.kind==='root'&&entries[0].access==='read')return{type:'readOnly',networkAccess:p.network==='enabled'};
                                                            
  if(entries.some((e:any)=>e.access==='none'||e.path?.type==='special'&&!['root','slash_tmp','tmpdir'].includes(e.path.value?.kind)))return;
  if(!entries.some((e:any)=>e.path?.value?.kind==='root'&&e.access==='read')||!entries.some((e:any)=>e.path?.type==='path'&&e.path.path===profile.settings.cwd&&e.access==='write'))return;
  const cwd=profile.settings.cwd,protectedNames=new Set(['.git','.agents','.codex','.aws']);
  if(entries.some((e:any)=>e.access==='read'&&e.path?.type==='path'&&(e.missing_path_behavior!=='skip'||!protectedNames.has(path.basename(e.path.path)))))return;
  return{type:'workspaceWrite',writableRoots:[...new Set(entries.filter((e:any)=>e.access==='write'&&e.path?.type==='path'&&e.path.path!==cwd).map((e:any)=>e.path.path))],networkAccess:p.network==='enabled',excludeTmpdirEnvVar:!entries.some((e:any)=>e.path?.value?.kind==='tmpdir'&&e.access==='write'),excludeSlashTmp:!entries.some((e:any)=>e.path?.value?.kind==='slash_tmp'&&e.access==='write')};
}

export const builtinCodexPermission=(id:string)=>[':read-only',':workspace',':danger-full-access'].includes(id);
                                         
export function withPersistedCodexPermission(profile:SessionProfile):SessionProfile {
  const active=profile.activePermission;
  if(!active||active.extends)return profile;
  const live={activePermissionProfile:active};
  if(builtinCodexPermission(active.id))return {...profile,live};
  if(!/^[\w-]+$/.test(active.id))return profile;
  const plan=permissionPlan(profile),definition='config' in plan.options?plan.options.config?.['permissions.nxbridge_transfer']:undefined;
  if(!definition||plan.policy.unknown.length||plan.policy.narrowed.length||!samePermission(plan.permission,profile.settings.permissionProfile))return profile;
  return {...profile,live,livePermissionConfig:{['permissions.'+active.id]:definition}};
}

async function persistedAfter(rpc:RpcClient,id:string,file:string,offset:number,settings:Record<string,unknown>,guard?:()=>void):Promise<SessionProfile>{
  await rpc.request('thread/settings/update',{threadId:id,...settings},{mutation:true,beforeSend:guard});
  return readAppliedSessionSettings(file,id,offset);
}

export async function readAppliedSessionSettings(file:unknown,id:string,offset?:number):Promise<SessionProfile>{
  const deadline=Date.now()+5000;
  do{const current=await readSessionProfile(file,id);if(current.verified&&current.complete&&(offset===undefined||(current.settingsOffset??0)>offset))return current;
    if(offset===undefined||typeof file!=='string')break;
    await delay(25);
  }while(Date.now()<deadline);
  throw new BridgeError('policy_verification_failed','宿主设置尚未完整落盘，已暂停投递；请稍后重试。');
}

export function persistedServiceTier(profile:SessionProfile):string|undefined{return observedServiceTier(knownTier(profile))??undefined;}

                                              
export async function readLiveServiceTier(rpc:RpcClient,id:string,loaded:any,current?:SessionProfile,guard?:()=>void|Promise<void>):Promise<{serviceTier:string|null|undefined;profile?:SessionProfile}>{
  if(loaded.serviceTier!==null)return{serviceTier:observedServiceTier(loaded.serviceTier),profile:current};
  if(loaded.approvalPolicy===undefined)throw new BridgeError('profile_unverified','原生未回报当前审批策略，无法在保持原设置的前提下核验服务档位。');
  const file=current?.file??loaded.thread?.path;
  if(typeof file!=='string'||!file)return{serviceTier:null,profile:current};
  const previous=current??await readSessionProfile(file,id);
  await guard?.();
                                             
  const profile=await persistedAfter(rpc,id,file,previous.offset,{approvalPolicy:loaded.approvalPolicy});
  const tier=profile.appliedServiceTier;
  if(!tier||tier.offset<=previous.offset)throw new BridgeError('profile_unverified','原生未提供新的服务档位生效记录，消息继续保留。');
  return{serviceTier:tier.present?tier.value:null,profile};
}

                                                            
export async function snapshotLiveSession(rpc:RpcClient,id:string,file:string):Promise<SessionProfile>{
  const previous=await readSessionProfile(file,id);
  const {args}=planCodexResume(id,previous,previous.settings.cwd??'',{exact:true});
  const live=await rpc.request('thread/resume',args,{mutation:true});
  if(live.thread?.id!==id)throw new BridgeError('session_changed','实时快照返回了其他线程。');
  if(live.approvalPolicy===undefined)throw new BridgeError('profile_unavailable','原生未回报当前审批策略，无法安全保存重启快照；未停止原运行时。');
  const profile=await persistedAfter(rpc,id,file,previous.offset,{approvalPolicy:live.approvalPolicy});
  if(live.serviceTier===null&&(!profile.appliedServiceTier||profile.appliedServiceTier.offset<=previous.offset))
    throw new BridgeError('profile_unavailable','原生未提供新的服务档位生效记录，未停止原运行时。');
  profile.live=Object.fromEntries(['model','modelProvider','reasoningEffort','serviceTier','approvalPolicy','approvalsReviewer','cwd','runtimeWorkspaceRoots','activePermissionProfile','sandbox'].filter(key=>live[key]!==undefined).map(key=>[key,live[key]]));
  const active=profile.live.activePermissionProfile?.id;
  if(typeof active==='string'&&active.startsWith('nxbridge_')){
    const plan=permissionPlan(profile),definition='config' in plan.options?plan.options.config?.['permissions.nxbridge_transfer']:undefined;
    if(!definition||!samePermission(plan.permission,profile.settings.permissionProfile))throw new BridgeError('profile_unavailable','临时命名权限缺少可精确保留的完整定义，未停止运行时。');
    profile.livePermissionConfig={['permissions.'+active]:definition};
  }
  if(!profile.live.activePermissionProfile?.id&&!sandboxCandidate(profile))throw new BridgeError('profile_unavailable','实时权限既无命名 profile，也不能由原生旧策略精确保留，未停止运行时。');
  return profile;
}

                                      
export function requireExactRestartProfile(profile:SessionProfile):void{
  const live=profile.live;
  if(!profile.verified||!profile.complete||!live||!profile.settings.permissionProfile
    ||typeof live.model!=='string'||!live.model||!Object.hasOwn(live,'reasoningEffort')||knownTier(profile)===undefined
    ||!['model','reasoningEffort'].every(key=>isDeepStrictEqual(live[key],profile.settings[key])))
    throw new BridgeError('restart_profile_unverified','原线程缺少可精确保留的实时权限、模型、推理强度或服务档位，未自动停止。');
}

                                      
export async function verifyRestartProfileCurrent(profile:SessionProfile,options:{exact?:boolean;current?:SessionProfile}={}):Promise<void>{
  const current=options.current??await readSessionProfile(profile.file,profile.sessionId);
  if(!current.verified&&current.pendingTail)throw new BridgeError('restart_profile_pending_tail','原会话历史正在写入首条记录，等待下一次核验。');
  const regressed=(current.settingsOffset??0)<(profile.settingsOffset??0);
  if(!current.verified||!current.complete||regressed){
    if(current.pendingTail&&!regressed)throw new BridgeError('restart_profile_pending_tail','原会话历史正在写入下一条记录，等待下一次核验。');
    throw new BridgeError('restart_profile_unverified','原会话历史暂时无法完整核验，已保留消息，稍后再核对。');
  }
  if((options.exact||(current.settingsOffset??0)>(profile.settingsOffset??0))&&(
      !samePermission(current.settings.permissionProfile,profile.settings.permissionProfile)
      ||knownTier(current)!==knownTier(profile)
      ||options.exact&&profile.activePermission!==undefined&&!isDeepStrictEqual(current.activePermission,profile.activePermission)
      ||(options.exact?['model','modelProvider','reasoningEffort','cwd','approvalPolicy','approvalsReviewer','runtimeWorkspaceRoots','collaborationMode','disabledPluginIds']:
        ['model','modelProvider','reasoningEffort','cwd','approvalPolicy','approvalsReviewer','runtimeWorkspaceRoots']).some(key=>!isDeepStrictEqual(current.settings[key],profile.settings[key]))))
    throw new BridgeError('restart_profile_changed','原会话在自动重启快照后写入了不同的设置，已保留消息，等待确认原会话设置。');
  if(current.pendingTail)throw new BridgeError('restart_profile_pending_tail','原会话历史正在写入下一条记录，等待下一次核验。');
}

                                       
export async function verifyResumedSession(rpc:RpcClient,id:string,original:SessionProfile,guard?:()=>void,cwd=original.settings.cwd??original.meta.cwd??''):Promise<{resumed:any;policy:SessionPolicy;profile:SessionProfile}>{
  const {args,plan}=planCodexResume(id,original,cwd,{exact:true});
  const resumed=await rpc.request('thread/resume',args,{mutation:true,beforeSend:guard});
  if(resumed.thread?.id!==id)throw new BridgeError('session_changed','恢复结果的线程 ID 与原会话不符，未投递。');
  const file=original.file??resumed.thread?.path;
  const current=await readSessionProfile(file,id);guard?.();
  observedResumedTier(resumed,current,original);
  const exact=original.settings.permissionProfile&&samePermission(current.settings.permissionProfile,original.settings.permissionProfile);
  if(!current.verified||!current.complete||!exact&&!samePermission(current.settings.permissionProfile,plan.permission))throw new BridgeError('policy_verification_failed','上次权限核验被中断，当前权限与原始或严格恢复方案不符，消息继续保留。');
  if(original.live&&(!exact||resumed.activePermissionProfile?.id!==original.live.activePermissionProfile?.id))
    throw new BridgeError('policy_verification_failed','上次重启后的实时权限或原生命名 profile 已改变，消息继续保留。');
  if((current.settingsOffset??0)<=(original.settingsOffset??0))throw new BridgeError('policy_verification_incomplete','上次恢复尚无新的权限落盘证据，等待安全冷恢复；消息继续保留。');
  for(const key of ['model','modelProvider','reasoningEffort','cwd','approvalPolicy','approvalsReviewer','runtimeWorkspaceRoots'])if(original.settings[key]!==undefined&&!isDeepStrictEqual(resumed[key],original.settings[key]))throw new BridgeError('policy_verification_failed','中断后的运行设置与原会话不符：'+key+'。');
  return{resumed,profile:current,policy:exact?{...plan.policy,source:'persisted',sandbox:current.settings.permissionProfile,unknown:[],narrowed:[],reason:undefined}:plan.policy};
}

                                    
export function planCodexResume(id:string,original:SessionProfile,cwd:string,options:{exact?:boolean}={}){
  if(original.verified&&!original.complete)throw new BridgeError('policy_verification_failed','原会话权限记录尚未写完，未恢复或投递。');
  const plan=permissionPlan(original),saved=original.settings,config={...('config' in plan.options?plan.options.config:{}),...(saved.reasoningEffort!==undefined&&saved.reasoningEffort!==null?{model_reasoning_effort:saved.reasoningEffort}:{})};
  const args:Record<string,unknown>={threadId:id,excludeTurns:true,...plan.options,config,cwd:saved.cwd??cwd,approvalPolicy:plan.policy.approval,approvalsReviewer:plan.policy.reviewer};
  const candidate=sandboxCandidate(original);
  if(original.live||options.exact&&candidate){
    const active=original.live?.activePermissionProfile;
    delete args.sandbox;delete args.permissions;args.config={...original.livePermissionConfig,...(saved.reasoningEffort!==undefined?{model_reasoning_effort:saved.reasoningEffort}:{})};
    if(active?.id)args.permissions=active.id;
    else{
      const legacy=candidate,mode=legacy&&({readOnly:'read-only',workspaceWrite:'workspace-write',dangerFullAccess:'danger-full-access'} as Record<string,string>)[legacy.type];
      if(!mode)throw new BridgeError('profile_unavailable','实时权限缺少可精确保留的原生表示。');
      args.sandbox=mode;
      if(legacy.type==='workspaceWrite')Object.assign(args.config!,{sandbox_workspace_write:{writable_roots:legacy.writableRoots??[],network_access:legacy.networkAccess??false,exclude_tmpdir_env_var:legacy.excludeTmpdirEnvVar??false,exclude_slash_tmp:legacy.excludeSlashTmp??false}});
      if(legacy.type==='readOnly')Object.assign(args.config!,{sandbox_read_only:{network_access:legacy.networkAccess??false}});
    }
  }
  for(const key of ['model','modelProvider','runtimeWorkspaceRoots'])if(saved[key]!==undefined)args[key]=saved[key];
  const tier=knownTier(original);if(tier!==undefined)args.serviceTier=tier;
  if(options.exact&&(!original.verified||!original.complete||original.sessionId!==id||!args.cwd||typeof saved.model!=='string'||!saved.model||typeof saved.reasoningEffort!=='string'||!saved.reasoningEffort
    ||tier===undefined||saved.approvalPolicy===undefined||saved.approvalsReviewer===undefined
    ||!original.live&&!candidate&&(!saved.permissionProfile||!samePermission(saved.permissionProfile,plan.permission))))
    throw new RuntimeRecoveryHold('owned_profile_unverified','还没能确认后台 Codex 原来的模型、推理强度、服务档位和安全设置，为免换成别的设置，暂不恢复；消息已保留。');
  return{args,plan};
}

                                        
export function observedResumedTier(resumed:any,current:SessionProfile,original:SessionProfile):string|null|undefined{
  const expected=observedServiceTier(knownTier(original)),wire=observedServiceTier(resumed.serviceTier);
  const actual=wire===null&&current.verified&&current.complete?observedServiceTier(knownTier(current)):wire;
  if(expected!==undefined&&actual!==expected)throw new CodexServiceTierError({code:actual===undefined||actual===null?'service_tier_unverified':'service_tier_mismatch',saved:expected??undefined,...(typeof actual==='string'?{live:actual}:{})});
  if(wire===null&&typeof actual==='string')resumed.serviceTier=nativeServiceTier(actual);
  return actual;
}

function resumeAckFile(original:SessionProfile,ack:any):string{
  const id=original.sessionId;
  if(ack?.thread?.id!==id||ack.thread.status?.type==='notLoaded')
    throw new BridgeError('session_changed','原生恢复没有确认加载原会话，消息继续保留。');
  const file=ack.thread.path;
  if(typeof file!=='string'||!original.file||!sameDirectory(file,original.file))
    throw new BridgeError('policy_verification_failed','恢复回执没有指向原会话历史，消息继续保留。');
  return file;
}
function firstResumeAck(original:SessionProfile,ack:any,cwd:string){
  const {plan}=planCodexResume(original.sessionId,original,cwd,{exact:true});
  for(const key of ['model','modelProvider','reasoningEffort','cwd','approvalPolicy','approvalsReviewer','runtimeWorkspaceRoots']){
    if(original.settings[key]===undefined||!isDeepStrictEqual(ack[key],original.settings[key]))
      throw new BridgeError('policy_verification_failed','恢复回执未核验原会话设置：'+key+'。');
  }
  const active=original.live?.activePermissionProfile??original.activePermission??null,returned=ack.activePermissionProfile;
  if((returned?.id??null)!==(active?.id??null)||(returned?.extends??null)!==(active?.extends??null))
    throw new BridgeError('policy_verification_failed','恢复回执未核验原生命名权限，消息继续保留。');
  return {plan};
}
                                             
function recordedTakeoverModelSettings(original:SessionProfile,current:SessionProfile):SessionProfile{
  const values=Object.fromEntries(['model','reasoningEffort','serviceTier'].filter(key=>current.settings[key]!==undefined).map(key=>[key,current.settings[key]]));
  const settings={...original.settings,...values};
  const collaboration=original.settings.collaborationMode,mirrored=collaboration?.settings;
                                                   
  const collaborationMode=mirrored?{...collaboration,settings:{...mirrored,
    ...(Object.hasOwn(mirrored,'model')?{model:settings.model}:{}),
    ...(Object.hasOwn(mirrored,'reasoning_effort')?{reasoning_effort:settings.reasoningEffort}:{})}}:collaboration;
  return {...original,settings:{...settings,...(collaboration?{collaborationMode}:{})},
    ...(Object.hasOwn(values,'serviceTier')?{appliedServiceTier:current.appliedServiceTier}:{}),
    ...(original.live?{live:{...original.live,...values}}:{})};
}
                                              
export function planTakenOverCodexResume(id:string,original:SessionProfile,current:SessionProfile,cwd:string){
  const profile=recordedTakeoverModelSettings(original,current);
  return {...planCodexResume(id,profile,cwd,{exact:true}),profile};
}
                                          
function takenOverModelSettings(original:SessionProfile,ack:any,current:SessionProfile):SessionProfile{
  const tier=ack.serviceTier===null?knownTier(current):ack.serviceTier;
  if(!['model','reasoningEffort'].every(key=>isDeepStrictEqual(ack[key],current.settings[key]))
    ||observedServiceTier(tier)===undefined||observedServiceTier(tier)!==observedServiceTier(knownTier(current)))
    throw new BridgeError('takeover_profile_unverified','接手窗口的模型设置尚未在原会话历史中得到确认，消息继续保留。');
  return recordedTakeoverModelSettings(original,current);
}

                                                   
export async function materializeCodexResumeSettings(rpc:RpcClient,original:SessionProfile,ack:any,cwd:string,guard:()=>void):Promise<void>{
  const id=original.sessionId,file=resumeAckFile(original,ack),{plan}=firstResumeAck(original,ack,cwd);
  await persistedAfter(rpc,id,file,original.offset,{approvalPolicy:plan.policy.approval},guard);
}

                                        
export async function verifyCodexResumeAck(original:SessionProfile,ack:any,options:{
  cwd:string;guard?:()=>void;requireFreshSettings?:boolean;adoptLiveModelSettings?:true;
}):Promise<{profile:SessionProfile;policy:SessionPolicy}>{
  const id=original.sessionId,file=resumeAckFile(original,ack);
  options.guard?.();
  const current=await readAppliedSessionSettings(file,id,options.requireFreshSettings?original.offset:undefined);
  const expected=options.adoptLiveModelSettings?takenOverModelSettings(original,ack,current):original;
  const {plan}=firstResumeAck(expected,ack,options.cwd);
  await verifyRestartProfileCurrent(expected,{exact:true,current});
  options.guard?.();
  observedResumedTier(ack,current,expected);
  return {profile:current,policy:{...plan.policy,source:'persisted',sandbox:current.settings.permissionProfile,unknown:[],narrowed:[],reason:undefined}};
}

                              
export async function resumeSession(rpc:RpcClient,id:string,options:{file?:string;home?:string;cwd:string;profile?:SessionProfile;verify?:boolean;beforeResume?:()=>void;onResponse?:()=>void;onResumed?:(thread:any)=>void;guard?:()=>void}):Promise<{resumed:any;policy:SessionPolicy;profile:SessionProfile}>{
  const original=options.profile??await readSessionProfile(options.file??(options.home?sessionFile(options.home,id):undefined),id),saved=original.settings;
  const {args,plan}=planCodexResume(id,original,options.cwd,{exact:!!options.beforeResume});
  let resumed:any;
  try{resumed=await rpc.request('thread/resume',args,{timeout:30000,mutation:!!options.beforeResume,beforeSend:options.beforeResume,onResponse:options.onResponse});}
  catch(error){if(plan.policy.unknown.includes('filesystem')&&/sandbox helper|codex-linux-sandbox|failed to load AGENTS/i.test(String((error as Error).message)))throw new BridgeError('policy_unknown_unstartable','原会话权限未知，原生宿主无法在禁止读取的策略下初始化。消息继续保留；请在正常窗口明确设置权限并完成一次交互后重试。');throw error;}
  if(resumed.thread?.id!==id)throw new BridgeError('session_changed','恢复结果的线程 ID 与原会话不符，未投递。');
  options.onResumed?.(resumed.thread);
  for(const key of ['model','modelProvider','reasoningEffort','cwd','approvalPolicy','approvalsReviewer','runtimeWorkspaceRoots'])if(saved[key]!==undefined&&!isDeepStrictEqual(resumed[key],saved[key]))throw new BridgeError('policy_verification_failed','宿主未保留原会话的 '+key+'，已暂停投递。');
  const file=options.file??resumed.thread.path??original.file;
  let current=await readSessionProfile(file,id);current=await verifyResumedTier(rpc,id,original,resumed,current,file,options.guard);
  if(options.verify===false)return{resumed,policy:plan.policy,profile:original};
  if(typeof file!=='string'||!file)throw new BridgeError('policy_verification_failed','无法读取原生权限生效证据，已暂停投递。');
  const candidate=original.live?.activePermissionProfile?.id?undefined:sandboxCandidate(original);
  const settings:Record<string,unknown>={approvalPolicy:plan.policy.approval};
  for(const key of ['collaborationMode','disabledPluginIds'])if(saved[key]!==undefined)settings[key]=saved[key];
                                         
  if(candidate)settings.sandboxPolicy=candidate;
  options.guard?.();
  current=await persistedAfter(rpc,id,file,current.offset,settings);
  if(original.live){
    if(!samePermission(current.settings.permissionProfile,original.settings.permissionProfile)||resumed.activePermissionProfile?.id!==original.live.activePermissionProfile?.id)throw new BridgeError('policy_verification_failed','重启后的实时权限或原生命名 profile 已改变，已暂停投递。');
    return{resumed,profile:current,policy:{...plan.policy,source:'persisted',sandbox:current.settings.permissionProfile,unknown:[],narrowed:[],reason:undefined}};
  }
  if(candidate&&samePermission(current.settings.permissionProfile,original.settings.permissionProfile)){
    const policy={...plan.policy,source:'persisted' as const,sandbox:current.settings.permissionProfile,narrowed:[],...(plan.policy.unknown.length?{reason:'policy_partial' as const}:{reason:undefined})};return{resumed,policy,profile:current};
  }
  if(candidate&&!samePermission(current.settings.permissionProfile,plan.permission)){
    if(options.beforeResume)throw new BridgeError('policy_verification_failed','恢复后 Codex 报告的权限设置和原来的不一致，为安全起见暂停投递；消息已保留（运行 nxbridge status 查看）。');
                                             
    options.guard?.();
    const repaired=await rpc.request('thread/resume',args,{timeout:30000,mutation:!!options.beforeResume});if(repaired.thread?.id!==id)throw new BridgeError('session_changed','重新核对时 Codex 会话已经变了，消息没有送出。');
    current=await readSessionProfile(file,id);options.guard?.();current=await persistedAfter(rpc,id,file,current.offset,{approvalPolicy:plan.policy.approval});
    current=await verifyResumedTier(rpc,id,original,repaired,current,file,options.guard);
  }
  if(!samePermission(current.settings.permissionProfile,plan.permission)||current.settings.approvalPolicy!==undefined&&!isDeepStrictEqual(current.settings.approvalPolicy,plan.policy.approval)||current.settings.approvalsReviewer!==undefined&&!isDeepStrictEqual(current.settings.approvalsReviewer,plan.policy.reviewer))throw new BridgeError('policy_verification_failed','无法核验不放宽权限的恢复结果，已暂停投递。');
  return{resumed,policy:plan.policy,profile:current};
}
