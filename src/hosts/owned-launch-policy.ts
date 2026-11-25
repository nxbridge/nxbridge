import { randomBytes } from 'node:crypto';
import path from 'node:path';
import {BridgeError} from '../shared/errors.js';
import {type PermissionClass,type ModelProfile} from '../types.js';
import type { RpcClient } from '../ipc/rpc.js';
import { classifyCodexPermission, codexRuntimeEvidence } from './codex/codex-permission.js';
import {delay} from '../shared/time.js';
import { claudeProfileSettings } from '../broker/profiles/profile-arguments.js';
import { persistentSocketDirectory } from '../process/posix/persistent-socket-directory.js';
import { probeClaudeInboxCapability } from './claude/claude-inbox-capability.js';
import type { HostLaunch } from './host-launch.js';
import { nativeServiceTier, observedServiceTier } from './codex/codex-service-tier.js';

                                
export function ownedCodexPolicy(source:PermissionClass):Record<string,unknown>{
  const automatic=source!=='bypass',approval=automatic?'on-request':'never',sandbox=automatic?'workspace-write':'danger-full-access',reviewer=automatic?'auto_review':'user';
  return{approvalPolicy:approval,sandbox,approvalsReviewer:reviewer,config:{approval_policy:approval,sandbox_mode:sandbox,approvals_reviewer:reviewer}};
}

export async function startOwnedCodex(rpc:RpcClient,cwd:string,source:PermissionClass,beforeLaunch?:()=>void|Promise<void>,beforeCreate?:()=>void|Promise<void>,profile:ModelProfile={}):Promise<any>{
  let automatic=source!=='bypass',result:any,submitted=false;
  const start=async()=>{submitted=false;await beforeLaunch?.();await beforeCreate?.();submitted=true;const policy=ownedCodexPolicy(automatic?'prompting':'bypass');return rpc.request('thread/start',{cwd,serviceName:'nxbridge',...policy,...(profile.model?{model:profile.model}:{}),...(profile.serviceTier===undefined?{}:{serviceTier:nativeServiceTier(profile.serviceTier)}),config:{...policy.config as object,...(profile.effort!==undefined?{model_reasoning_effort:profile.effort}:{})}},{mutation:true,timeout:30000});};
  const verifyTier=()=>{if(profile.serviceTier!==undefined&&observedServiceTier(result.serviceTier)!==profile.serviceTier)throw new BridgeError('profile_service_tier_unavailable',`Codex 没有采用指定的服务档位 ${profile.serviceTier}，未提交任务。`);};
  try{result=await start();}catch(error){
                                           
    if(!submitted||!automatic||String((error as {code?:unknown}).code)!=='-32602'||/service[_ ]?tier/i.test(String(error)))throw error;
    automatic=false;result=await start();
  }
  try{
    verifyTier();
    if(automatic&&!(result.approvalsReviewer==='auto_review'&&result.approvalPolicy==='on-request')){
                                                         
      await beforeLaunch?.();const abandoned=result.thread.id;result=undefined;
      await unloadFailedOwnedCodex(rpc,abandoned,'原生自动审核能力未采用');
      automatic=false;result=await start();verifyTier();
    }
    const evidence=codexRuntimeEvidence(result);
    if(profile.model!==undefined&&result.model!==profile.model||profile.effort!==undefined&&result.reasoningEffort!==profile.effort)throw new BridgeError('profile_verification_failed','宿主未采用新后台要求的模型或 effort，原任务未提交。');
    if(automatic?classifyCodexPermission(evidence)!=='prompting':classifyCodexPermission(evidence)!=='bypass')throw new BridgeError('autonomous_policy_unavailable','宿主没有采用新后台要求的无人审批策略，未提交任务；请检查原生管理策略。');
                                               
    await beforeLaunch?.();await rpc.request('thread/settings/update',{threadId:result.thread.id,approvalPolicy:result.approvalPolicy},{mutation:true});
    return result;
  }catch(error){
    await unloadFailedOwnedCodex(rpc,result?.thread?.id,error);
    if(result?.thread?.id&&(error as BridgeError).code==='profile_service_tier_unavailable')throw new BridgeError('profile_service_tier_unavailable',`Codex 没有采用指定的服务档位 ${profile.serviceTier}，这个新会话已撤掉，消息留着没发。可以换一个档位，或把 partners.codex.profile_source 设为 host 改用你自己的配置。`);
    throw error;
  }
}

                                   
export async function unloadFailedOwnedCodex(rpc:RpcClient,threadId:unknown,error:unknown):Promise<void>{
  if(typeof threadId!=='string')return;
  try{
    await rpc.request('thread/delete',{threadId},{mutation:true});const until=Date.now()+5000;
    while(true){
      let cursor:string|undefined,loaded=false;
      do{const page=await rpc.request<any>('thread/loaded/list',{limit:100,...(cursor?{cursor}:{})});loaded||=(page.data??[]).includes(threadId);cursor=page.nextCursor??undefined;}while(cursor&&!loaded);
      if(!loaded)return;
      if(Date.now()>=until)throw new BridgeError('thread_unload_pending','原生空线程仍在内存中，未创建另一个后台。');await delay(50);
    }
  }
  catch(cleanup){throw new BridgeError('owned_launch_cleanup_failed','新后台启动失败且空线程尚未确认卸载，未提交任务。原错误：'+String(error)+'；卸载错误：'+String(cleanup));}
}

export function ownedClaudeArgs(source:PermissionClass,profile:ModelProfile={}):string[]{
  const settings={...(source==='bypass'?{}:{crossSessionInbound:'accept'}),...claudeProfileSettings(profile)};
  return['--permission-mode','bypassPermissions',...(Object.keys(settings).length?['--settings',JSON.stringify(settings)]:[])];
}

export function ownedClaudeInboxArgs(home:string):string[]{
  if(process.platform==='win32')return[];
                                      
  const name=`ci-${randomBytes(8).toString('hex')}.sock`;
  return['--messaging-socket-path',path.join(persistentSocketDirectory(home,name),name)];
}

                                     
export async function claudeInboxLaunchArgs(home:string,launch:HostLaunch,required=false):Promise<string[]>{
  const args=ownedClaudeInboxArgs(home);if(!args.length)return args;
  if(await probeClaudeInboxCapability(launch,args[1]!))return args;
  if(required)throw new BridgeError('claude_inbox_migration_unsupported','所选 Claude 不支持指定持久收件地址，已保留原会话；更新同一启动入口后可重新核验。');
  return[];
}
