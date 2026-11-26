import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { appendFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { completeRestartProfile, pendingRestartVerification, prepareRestartProfile } from '../hosts/codex/codex-restart-profile.js';
import { readSessionProfile, resumeSession, snapshotLiveSession, verifyResumedSession, requireExactRestartProfile, readLiveServiceTier, type SessionProfile } from '../hosts/codex/codex-session-profile.js';
import { removeManagedRuntime, runtimeFor, saveManagedRuntime, type ManagedRuntime } from '../hosts/managed-runtimes.js';
import { managedMarker } from '../install/installation.js';
import type { RpcClient } from '../ipc/rpc.js';
import { processIdentity } from '../process/process.js';
import { temporary } from './support/temporary.js';

const permission={type:'managed',file_system:{type:'restricted',entries:[{path:{type:'special',value:{kind:'root'}},access:'read'}]},network:'restricted'};
const settings={model:'fixture-model',reasoningEffort:'high',serviceTier:'fast',permissionProfile:permission,approvalPolicy:'on-request',approvalsReviewer:'user',cwd:process.cwd()};
const snapshot=(sessionId:string):SessionProfile=>({sessionId,offset:0,settingsOffset:0,verified:true,complete:true,meta:{},settings,
  live:{activePermissionProfile:{id:'nxbridge_fixture'},...settings},livePermissionConfig:{'permissions.nxbridge_fixture':{filesystem:{':root':'read'},network:{enabled:false}}}});
const row=(type:string,payload:object)=>JSON.stringify({type,payload})+'\n';
const persisted={permission_profile:permission,model:settings.model,reasoning_effort:settings.reasoningEffort,service_tier:'priority',approval_policy:settings.approvalPolicy,approvals_reviewer:settings.approvalsReviewer,cwd:settings.cwd};
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
  await assert.rejects(verifyResumedSession(client('nxbridge_fixture','flex'),sessionId,original),{code:'profile_verification_failed',pauseDetail:{code:'service_tier_mismatch',saved:'fast',live:'flex'}});
});

test('暖连接的中断核验缺少精确档位时不发任何恢复或设置请求',async()=>{
  const id=randomUUID(),saved=snapshot(id);
  const incomplete={...saved,settings:{...saved.settings,serviceTier:undefined},live:{...saved.live,serviceTier:undefined}};
  const rpc={request:async()=>assert.fail('宿主默认档位可能已改变')} as unknown as RpcClient;
  await assert.rejects(verifyResumedSession(rpc,id,incomplete),{code:'owned_profile_unverified',phase:'prepare'});
});

test('原生接口投影为 null 的档位，用同轮新落盘原值核验；不同值和缺失字段不能借旧合并值通过',async t=>{
  for(const actualTier of ['opaque-tier-a','other-opaque',undefined]){
    const home=temporary(t),sessionId=randomUUID(),file=path.join(home,'tier.jsonl');
    writeFileSync(file,row('session_meta',{id:sessionId})+update(sessionId,{...persisted,service_tier:'opaque-tier-a'}),'utf8');
    const original=await readSessionProfile(file,sessionId),calls:{method:string;params:any}[]=[];
    const rpc={request:async(method:string,params:any)=>{
      calls.push({method,params});
      if(method==='thread/settings/update'){
        assert.equal(Object.hasOwn(params,'serviceTier'),false,'证明前不能先写期望档位');
        const {service_tier:_,...base}=persisted;
        appendFileSync(file,update(sessionId,{...base,...(actualTier===undefined?{}:{service_tier:actualTier})}),'utf8');
      }
      return{thread:{id:sessionId,path:file},...settings,serviceTier:null,activePermissionProfile:{id:'nxbridge_fixture'}};
    }} as unknown as RpcClient;
    if(actualTier==='opaque-tier-a'){
      const result=await resumeSession(rpc,sessionId,{profile:original,cwd:home});
      assert.equal(result.profile.settings.serviceTier,'opaque-tier-a');assert.ok(result.profile.appliedServiceTier!.offset>original.offset);
    }else{
      await assert.rejects(resumeSession(rpc,sessionId,{profile:original,cwd:home}),{code:'profile_verification_failed',pauseDetail:{code:actualTier===undefined?'service_tier_unverified':'service_tier_mismatch',saved:'opaque-tier-a',...(actualTier===undefined?{}:{live:actualTier})}});
      if(actualTier===undefined)assert.equal((await readSessionProfile(file,sessionId)).settings.serviceTier,undefined,'完整设置不带档位时丢弃过时的合并值');
    }
    assert.equal(calls[0]?.params.serviceTier,'opaque-tier-a');
  }
});

test('实时快照的 null 投影不覆盖同次落盘档位；已加载的中断恢复也能独立核验',async t=>{
  const home=temporary(t),sessionId=randomUUID(),file=path.join(home,'live-tier.jsonl'),calls:{method:string;params:any}[]=[];
  writeFileSync(file,row('session_meta',{id:sessionId})+update(sessionId,{...persisted,service_tier:'opaque-tier-a'}),'utf8');
  const rpc={request:async(method:string,params:any)=>{
    calls.push({method,params});
    if(method==='thread/settings/update'){
      assert.equal(Object.hasOwn(params,'serviceTier'),false);
      appendFileSync(file,update(sessionId,{...persisted,service_tier:'opaque-tier-a'}),'utf8');
    }
    return{thread:{id:sessionId,path:file},...settings,serviceTier:null,activePermissionProfile:{id:'nxbridge_fixture'}};
  }} as unknown as RpcClient;
  const original=await snapshotLiveSession(rpc,sessionId,file);assert.equal(original.live!.serviceTier,null);assert.equal(original.appliedServiceTier!.value,'opaque-tier-a');
  requireExactRestartProfile(original);
  calls.length=0;await resumeSession(rpc,sessionId,{profile:original,cwd:home});assert.equal(calls[0]?.params.serviceTier,'opaque-tier-a');
  calls.length=0;const restored=await verifyResumedSession(rpc,sessionId,original);assert.equal(restored.profile.settings.serviceTier,'opaque-tier-a');
  assert.deepEqual(calls.map(call=>call.method),['thread/resume']);assert.equal(calls[0]?.params.serviceTier,'opaque-tier-a');
  assert.ok(restored.profile.appliedServiceTier!.offset>original.offset);
});

test('旧宿主未回报当前审批策略时无法安全生成档位证明，不提交空设置或改用默认策略',async t=>{
  const home=temporary(t),sessionId=randomUUID(),file=path.join(home,'old-host.jsonl');
  writeFileSync(file,row('session_meta',{id:sessionId})+update(sessionId,{...persisted,service_tier:'opaque-tier-a'}),'utf8');
  const rpc={request:async()=>assert.fail('缺少当前审批策略时不得发送任何设置写入')} as unknown as RpcClient;
  await assert.rejects(readLiveServiceTier(rpc,sessionId,{thread:{id:sessionId,path:file},serviceTier:null}),{code:'profile_unverified'});
  const snapshotRpc={request:async(method:string)=>{assert.equal(method,'thread/resume');return{thread:{id:sessionId,path:file},serviceTier:null};}} as unknown as RpcClient;
  await assert.rejects(snapshotLiveSession(snapshotRpc,sessionId,file),{code:'profile_unavailable'});
});
