import test,{type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {randomUUID} from 'node:crypto';
import {writeFileSync,appendFileSync} from 'node:fs';
import {CodexAdapter} from '../hosts/codex/adapter.js';
import {CodexNativeRpc} from '../hosts/codex/codex-native-rpc.js';
import {readSessionProfile} from '../hosts/codex/codex-session-profile.js';
import {saveManagedRuntime} from '../hosts/managed-runtimes.js';
import {managedMarker} from '../install/installation.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {Store} from '../storage/store.js';
import {temporary} from './support/temporary.js';
import {registration} from './support/registration.js';

const row=(type:string,payload:unknown)=>JSON.stringify({type,payload})+'\n';

async function fixture(t:TestContext,kind:'warm'|'cold'|'recycle',event:'before'|'after'|'busy'|'none'){
  const home=temporary(t),store=new Store(':memory:'),adapter=new CodexAdapter(DEFAULT_CONFIG,home);
  t.after(()=>{adapter.close();store.close();});
  const id='resume-order',file=path.join(home,'history.jsonl');
  const settings={cwd:home,runtime_workspace_roots:[home],model:'model-a',model_provider:'fixture',reasoning_effort:'high',service_tier:'priority',
    approval_policy:'never',approvals_reviewer:'user',permission_profile:{type:'disabled'}};
  const applied=()=>row('event_msg',{type:'thread_settings_applied',thread_id:id,thread_settings:settings});
  writeFileSync(file,row('session_meta',{id})+applied(),'utf8');
  const profile=await readSessionProfile(file,id),registered=store.register(registration('codex',id,{cwd:home}));
  store.updatePeer(registered.id,{owner:'nxbridge',state:'ready'});
  const peer=store.peer(registered.id),endpoint=store.endpoint(peer.id)!,runtimeId=randomUUID();
  Object.assign(endpoint,{runtimeId,pid:process.pid,procStart:'fixture-native',configHome:home,nativeSession:{transcriptPath:file}});store.saveEndpoint(endpoint);
  const runtime={id:runtimeId,peerId:peer.id,sessionId:id,configHome:home,owner:managedMarker(home),kind:'owned' as const,state:'ready' as const,
    pid:process.pid,procStart:'fixture-native',endpoint:endpoint.address,createdAt:Date.now(),
    ...(kind==='recycle'?{sessionVerification:{sessionId:id,profile,restartSourceId:'original-source'}}:{})};
  saveManagedRuntime(home,runtime);
  const calls:string[]=[];
  const rpc=new CodexNativeRpc(message=>{
    calls.push(message.method!);
    if(message.method==='thread/settings/update')appendFileSync(file,applied(),'utf8');
    const result=message.method==='thread/loaded/list'?{data:kind==='warm'?[id]:[]}:message.method==='thread/resume'?
      {thread:{id,path:file,cwd:home,status:{type:'idle'}},cwd:home,runtimeWorkspaceRoots:[home],model:'model-a',modelProvider:'fixture',
        reasoningEffort:'high',serviceTier:'priority',approvalPolicy:'never',approvalsReviewer:'user',sandbox:{type:'dangerFullAccess'}}:{};
    queueMicrotask(()=>{
      if(message.method==='thread/resume'&&event==='before')unload();
      rpc.receive({id:message.id,result});
      if(message.method==='thread/resume'&&event==='after')unload();
      if(message.method==='thread/resume'&&event==='busy')rpc.receive({method:'turn/started',params:{threadId:id}});
    });
  },()=>{},endpoint.address);
  adapter.connect=async()=>rpc;
  (adapter as any).installPool(endpoint.address,rpc,runtime);
  adapter.bindOwnedRoutes(memberId=>memberId===peer.id?{peer,endpoint}:undefined);
  t.mock.method(adapter as any,'verifyColdResumeRuntime',async()=>{});
  const unload=()=>rpc.receive({method:'thread/unloaded',params:{threadId:id}});
  const fresh={runtimeId,endpoint:endpoint.address,pid:runtime.pid,procStart:runtime.procStart};
  const source={sourceId:'original-source',sessionId:id,file,settingsKey:'fixture'};
  return {adapter,peer,endpoint,rpc,calls,unload,fresh,source,profile,runtime};
}

for(const kind of ['warm','cold'] as const)for(const event of ['before','after','busy'] as const)
test('恢复回执只覆盖更早的失效，后到卸载和忙碌仍有效：'+kind+'/'+event,async t=>{
  const f=await fixture(t,kind,event),options=kind==='warm'?{subscribeLoaded:async()=>()=>{}}:undefined;
  if(kind==='cold'&&event==='after'){
    await assert.rejects(f.adapter.attach(f.peer,f.endpoint,options),{code:'owned_subscription_waiting'});
  }else await f.adapter.attach(f.peer,f.endpoint,options);
  assert.equal(f.adapter.hasSubscription(f.peer,f.endpoint),event!=='after');
  assert.equal(f.calls.filter(method=>method==='thread/resume').length,1);
  if(event==='busy')assert.equal((f.adapter as any).nativeStates.get(f.endpoint.address+'|'+f.peer.sessionId).state,'busy');
});

for(const event of ['before','after','busy'] as const)test('回收替代后台的原始回执不能清掉后到状态：'+event,async t=>{
  const f=await fixture(t,'recycle',event),ports=f.adapter.recycleHost(f.peer,f.endpoint);
  let submissions=0;
  const options={guard:()=>{},markSubmitted:()=>{submissions++;},resumeSubmitted:false};
  if(event==='after'){
    await assert.rejects(ports.verify(f.fresh,f.source,options),{code:'runtime_changed'});
    await assert.rejects(ports.verify(f.fresh,f.source,{...options,resumeSubmitted:true}),{code:'runtime_changed'});
    assert.equal(submissions,1);assert.equal(f.calls.filter(method=>method==='thread/resume').length,1);
  }else{
    await ports.verify(f.fresh,f.source,options);
    const admitted=ports.admission(f.fresh);assert.equal(admitted.input.state,event==='busy'?'busy':'ready');
    ports.subscribe({peer:f.peer,endpoint:f.endpoint},admitted);
    assert.equal(f.adapter.hasSubscription(f.peer,f.endpoint),true);
    assert.equal((f.adapter as any).nativeStates.get(f.endpoint.address+'|'+f.peer.sessionId).state,event==='busy'?'busy':'ready');
    f.unload();assert.equal(f.adapter.hasSubscription(f.peer,f.endpoint),false);
  }
});

for(const event of ['before','after'] as const)test('精确重启恢复也按自己的回执结算失效，后到卸载仍中止：'+event,async t=>{
  const f=await fixture(t,'cold',event);let responseRevision=0;
  const restored=(f.adapter as any).restoreRestartTarget(f.rpc,f.peer.sessionId,f.profile,f.runtime,f.peer,()=>{responseRevision=(f.adapter as any).nativeStateRevision;});
  if(event==='after')await assert.rejects(restored,{code:'owned_subscription_waiting'});
  else{
    await restored;
    assert.ok(responseRevision>(f.adapter as any).nativeStates.get(f.endpoint.address+'|'+f.peer.sessionId).revision);
    assert.equal((f.adapter as any).invalidated.has(f.endpoint.address+'|'+f.peer.sessionId),false);
  }
});

test('回收核验完成后至接入前卸载时，准入不能复活订阅',async t=>{
  const f=await fixture(t,'recycle','none'),ports=f.adapter.recycleHost(f.peer,f.endpoint);
  await ports.verify(f.fresh,f.source,{guard:()=>{},markSubmitted:()=>{},resumeSubmitted:false});
  f.unload();
  assert.throws(()=>ports.admission(f.fresh),{code:'codex_recycle_verify_unknown'});
  assert.equal(f.adapter.hasSubscription(f.peer,f.endpoint),false);
});
