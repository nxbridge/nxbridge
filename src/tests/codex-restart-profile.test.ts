import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { appendFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { completeRestartProfile, pendingRestartVerification, prepareRestartProfile } from '../hosts/codex/codex-restart-profile.js';
import { readSessionProfile, resumeSession, verifyResumedSession, type SessionProfile } from '../hosts/codex/codex-session-profile.js';
import { removeManagedRuntime, runtimeFor, saveManagedRuntime, type ManagedRuntime } from '../hosts/managed-runtimes.js';
import { managedMarker } from '../install/installation.js';
import type { RpcClient } from '../ipc/rpc.js';
import { processIdentity } from '../process/process.js';
import { temporary } from './support/temporary.js';

const permission={type:'managed',file_system:{type:'restricted',entries:[{path:{type:'special',value:{kind:'root'}},access:'read'}]},network:'restricted'};
const settings={model:'fixture-model',reasoningEffort:'high',serviceTier:'fast',permissionProfile:permission,approvalPolicy:'on-request',approvalsReviewer:'user'};
const snapshot=(sessionId:string):SessionProfile=>({sessionId,offset:0,settingsOffset:0,verified:true,complete:true,meta:{},settings,
  live:{activePermissionProfile:{id:'nxbridge_fixture'},...settings},livePermissionConfig:{'permissions.nxbridge_fixture':{filesystem:{':root':'read'},network:{enabled:false}}}});
const row=(type:string,payload:object)=>JSON.stringify({type,payload})+'\n';
const persisted={permission_profile:permission,model:settings.model,reasoning_effort:settings.reasoningEffort,service_tier:'priority',approval_policy:settings.approvalPolicy,approvals_reviewer:settings.approvalsReviewer};
const update=(sessionId:string,threadSettings:object)=>row('event_msg',{type:'thread_settings_applied',thread_id:sessionId,thread_settings:threadSettings});

test('旧 Codex 账本移除后仍核验原代次、恢复原权限并在替代代次核验后结清',async t=>{
  const home=temporary(t),peerId=randomUUID(),sessionId=randomUUID(),current=await processIdentity(process.pid);
  const source:ManagedRuntime={id:randomUUID(),peerId,sessionId,configHome:home,owner:managedMarker(home),kind:'owned',state:'ready',
    endpoint:'unix://old.sock',pid:current.pid,procStart:current.startedAt,createdAt:1};
  const file=path.join(home,'session.jsonl');
  writeFileSync(file,row('session_meta',{id:sessionId})+update(sessionId,persisted),'utf8');
  const {live,livePermissionConfig}=snapshot(sessionId),original={...await readSessionProfile(file,sessionId),live,livePermissionConfig};
  assert.throws(()=>prepareRestartProfile(home,source,{...original,live:{...live,reasoningEffort:'low'}}),{code:'restart_profile_unverified'});
  assert.throws(()=>prepareRestartProfile(home,source,{...original,live:undefined}),{code:'restart_profile_unverified'});
  const preserve=prepareRestartProfile(home,source,original);
  assert.equal(await pendingRestartVerification(home,peerId,sessionId,home),undefined,'停止授权取消前不会写入快照');
  preserve();
  await assert.rejects(pendingRestartVerification(home,peerId,sessionId,home),{code:'runtime_launch_unconfirmed'});
  assert.equal(await pendingRestartVerification(home,peerId,randomUUID(),home),undefined);
  await assert.rejects(pendingRestartVerification(home,peerId,sessionId,temporary(t)),{code:'original_home_changed'});
  source.procStart+='-old';saveManagedRuntime(home,source);prepareRestartProfile(home,source,original)();
  removeManagedRuntime(runtimeFor(home,{id:source.id})!);
  const expected={sessionId,profile:JSON.parse(JSON.stringify(original)),restartSourceId:source.id};
  const restored=await pendingRestartVerification(home,peerId,sessionId,home);
  assert.deepEqual(restored,expected);
  completeRestartProfile(home,{...source,sessionVerification:restored});
  assert.deepEqual(await pendingRestartVerification(home,peerId,sessionId,home),expected,'旧代次不能结清自己的配置快照');
  const replacement={...source,id:randomUUID(),endpoint:'unix://new.sock',sessionVerification:restored};
  saveManagedRuntime(home,replacement);
  const ready=runtimeFor(home,{id:replacement.id})!.value;
  assert.deepEqual(ready.sessionVerification,expected);
  const calls:{method:string;params:any}[]=[];
  const rpc={request:async(method:string,params:unknown)=>{
    calls.push({method,params});
    if(method==='thread/settings/update')appendFileSync(file,update(sessionId,persisted),'utf8');
    return{thread:{id:sessionId,path:file},...live,serviceTier:'priority'};
  }} as unknown as RpcClient;
  await resumeSession(rpc,sessionId,{profile:ready.sessionVerification!.profile,cwd:home});
  assert.equal(calls[0]?.method,'thread/resume');
  assert.equal(calls[0]?.params.threadId,sessionId);
  assert.equal(calls[0]?.params.permissions,'nxbridge_fixture');
  assert.deepEqual(calls[0]?.params.config,{...livePermissionConfig,model_reasoning_effort:settings.reasoningEffort});
  assert.equal(calls[0]?.params.model,settings.model);
  assert.equal(calls[0]?.params.serviceTier,'priority');
  assert.deepEqual(await pendingRestartVerification(home,peerId,sessionId,home),expected,'替代线程写入配置后再次中断，仍可恢复原始快照');
  completeRestartProfile(home,ready);
  assert.equal(await pendingRestartVerification(home,peerId,sessionId,home),undefined);
  assert.deepEqual(runtimeFor(home,{id:replacement.id})!.value.sessionVerification,expected,'清除替代记录中的核验字段前仍可接续');
});

test('停止预约中断后原代继续使用不同配置，保留会话并拒绝覆盖',async t=>{
  const home=temporary(t),peerId=randomUUID(),sessionId=randomUUID(),current=await processIdentity(process.pid),file=path.join(home,'session.jsonl');
  const source:ManagedRuntime={id:randomUUID(),peerId,sessionId,configHome:home,owner:managedMarker(home),kind:'owned',state:'ready',
    endpoint:'unix://fixture.sock',pid:current.pid,procStart:current.startedAt+'-old',createdAt:1};
  writeFileSync(file,row('session_meta',{id:sessionId})+update(sessionId,persisted),'utf8');
  const {live,livePermissionConfig}=snapshot(sessionId);
  prepareRestartProfile(home,source,{...await readSessionProfile(file,sessionId),live,livePermissionConfig})();
  appendFileSync(file,update(sessionId,{...persisted,model:'next-fixture-model'}),'utf8');
  await assert.rejects(pendingRestartVerification(home,peerId,sessionId,home),{code:'restart_profile_changed'});
});

test('中断后核验要求原生命名权限身份及生效配置同时一致',async t=>{
  const home=temporary(t),sessionId=randomUUID(),file=path.join(home,'session.jsonl'),original={...snapshot(sessionId),file};
  const persist=(permissionProfile:unknown)=>writeFileSync(file,[
    {type:'session_meta',payload:{id:sessionId}},
    {type:'event_msg',payload:{type:'thread_settings_applied',thread_id:sessionId,thread_settings:{permission_profile:permissionProfile}}},
  ].map(line=>JSON.stringify(line)).join('\n')+'\n','utf8');
  const client=(id:string,serviceTier='fast')=>({request:async()=>({thread:{id:sessionId,path:file},...settings,serviceTier,activePermissionProfile:{id}})}) as unknown as RpcClient;
  persist(permission);
  assert.equal((await verifyResumedSession(client('nxbridge_fixture'),sessionId,original)).resumed.model,settings.model);
  await assert.rejects(verifyResumedSession(client('other-profile'),sessionId,original),{code:'policy_verification_failed'});
  await assert.rejects(verifyResumedSession(client('nxbridge_fixture','flex'),sessionId,original),{code:'profile_verification_failed'});
});
