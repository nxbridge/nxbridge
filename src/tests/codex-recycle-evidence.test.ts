import assert from 'node:assert/strict';
import { writeFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import type { CodexThread } from '../hosts/codex/adapter.js';
import { catalogCodexThreads, observeCodexThreadIdentity } from '../hosts/codex/codex-evidence.js';
import { codexRecycleRuntimeGeneration, collectCodexRecycleEvidence, inspectCodexRecycleOld, planExactCodexRecycle, readCodexRecycleScope, readCodexRecycleThreadWork } from '../hosts/codex/codex-recycle.js';
import { managedMarker } from '../install/installation.js';
import { saveManagedRuntime, type ManagedRuntime } from '../hosts/managed-runtimes.js';
import type { Endpoint, Peer } from '../types.js';
import { readSessionProfile, verifyCodexResumeAck, withPersistedCodexPermission, type SessionProfile } from '../hosts/codex/codex-session-profile.js';
import { RpcClient, type RpcMessage } from '../ipc/rpc.js';
import { temporary } from './support/temporary.js';
import { ownedWindowsConsoleInfrastructure } from '../process/runtime-workload.js';

function inspector(loaded:string[],rows:CodexThread[]) {
  const requests:RpcMessage[]=[];
  const rpc=new RpcClient(request=>{
    requests.push(request);
    queueMicrotask(()=>{
      const data=request.method==='thread/loaded/list'?loaded:request.method==='thread/list'?rows:assert.fail('使用了非独立线程接口');
      rpc.receive({id:request.id,result:{data}});
    });
  },()=>{},'mock');
  return {rpc,requests};
}

test('PID 已由不同代次占用时确认旧后台已退出，不触碰当前进程',async t=>{
  const home=temporary(t),runtime:ManagedRuntime={id:randomUUID(),endpoint:'ws://127.0.0.1:12345',pid:process.pid,procStart:'1',owner:managedMarker(home),
    kind:'owned',state:'ready',configHome:home,dedicated:true,createdAt:Date.now()};
  saveManagedRuntime(home,runtime);
  assert.equal(await inspectCodexRecycleOld(home,codexRecycleRuntimeGeneration(runtime)),'dead');
  assert.equal(process.kill(process.pid,0),true);
});
const root={id:'root',cwd:tmpdir()};
const child={id:'child',cwd:tmpdir(),source:{subAgent:{spawn:{parentThreadId:'parent'}}}};
const parent={id:'parent',cwd:tmpdir(),parentThreadId:root.id};

test('普通重连先核对原生工作和待投递消息，只有两者都存在才核验回收归属',async t=>{
  const home=temporary(t),peer={id:'peer',host:'codex',sessionId:root.id} as Peer,endpoint={address:'mock'} as Endpoint;
  async function inspect(data:unknown,hasHeldInput:boolean){
    const requests:string[]=[];
    const rpc=new RpcClient(request=>{
      requests.push(request.method!);
      queueMicrotask(()=>rpc.receive({id:request.id,result:{data:request.method==='thread/loaded/list'?[root.id]:data}}));
    },()=>{},'mock');
    const result=await collectCodexRecycleEvidence({home,peer,endpoint,trigger:'loaded_unleased',connect:async()=>rpc,
      hasHeldInput:()=>hasHeldInput,preflight:async()=>assert.fail('归属未核验不能进入设置预检')});
    assert.deepEqual(requests,['thread/loaded/list','thread/turns/list']);
    return result.evidence;
  }
  assert.deepEqual(await inspect([{id:'done',status:'completed'}],true),{kind:'natural',idleLoaded:true});
  assert.deepEqual(await inspect(undefined,true),{kind:'natural'});
  assert.deepEqual(await inspect([{id:'active',status:'inProgress'}],false),{kind:'natural'});
  assert.match(JSON.stringify(await inspect([{id:'active',status:'inProgress'}],true)),/codex_recycle_owner_unverified/);
});

test('独立目录覆盖间接父链且不查询有问题的线程控制队列',async()=>{
  const {rpc,requests}=inspector(['root','child'],[root,parent,child]);
  const scope=await readCodexRecycleScope(rpc,root.id);
  assert.deepEqual([...scope.loaded],['root','child']);
  assert.deepEqual([...scope.threads.keys()],['root','child','parent']);
  assert.deepEqual(requests.map(request=>request.method),['thread/loaded/list','thread/list','thread/list']);
});

test('独立范围使用原生状态目录，不触发大历史修补，普通查询仍保留修补模式',async()=>{
  const rpc=new RpcClient(request=>queueMicrotask(()=>{
    if(request.method==='thread/list'&&request.params.useStateDbOnly!==true){
      rpc.receive({id:request.id,error:{code:'timeout',message:'JSONL 大历史修补未能在查询期限内完成'}});return;
    }
    rpc.receive({id:request.id,result:{data:request.method==='thread/loaded/list'?['root','child']:[root,parent,child]}});
  }),()=>{},'mock');
  assert.deepEqual([...(await readCodexRecycleScope(rpc,root.id)).threads.keys()],['root','child','parent']);
  const ordinary=inspector([],[root]);
  assert.equal((await catalogCodexThreads(ordinary.rpc,new Set([root.id]))).get(root.id)?.id,root.id);
  assert.equal(ordinary.requests[0]!.params.useStateDbOnly,undefined);
});

test('目录缺少已加载线程或父辈时保留原进程，连接缓存不能补齐原生证据',async()=>{
  for(const rows of [[root],[root,child]]){
    const {rpc}=inspector(['root','child'],rows);
    observeCodexThreadIdentity(rpc,child);observeCodexThreadIdentity(rpc,parent);
    await assert.rejects(readCodexRecycleScope(rpc,root.id),{code:'codex_recycle_lineage_unverified'});
  }
});

test('同代次包含无关根线程时没有回收整个后台的权限',async()=>{
  const other={id:'foreign',cwd:tmpdir()};
  const {rpc}=inspector(['root','child','foreign'],[root,parent,child,other]);
  await assert.rejects(readCodexRecycleScope(rpc,root.id),{code:'shared_runtime'});
});

test('原生目录隐藏纯工具根线程时只接受该根原始历史的独立身份',async()=>{
  const {rpc}=inspector(['root','child'],[parent,child]);
  const ownedRootRollout:SessionProfile={sessionId:'root',file:'original',offset:100,complete:true,verified:true,
    meta:{id:'root',session_id:'root',cwd:root.cwd,source:'cli'},settings:{}};
  assert.equal((await readCodexRecycleScope(rpc,root.id,{ownedRootRollout})).originalRootOmitted,true);
  const incomplete=inspector(['root','child'],[parent]).rpc;
  await assert.rejects(readCodexRecycleScope(incomplete,root.id,{ownedRootRollout}),{code:'codex_recycle_lineage_unverified'});
  const unknownRoot=inspector(['root','child'],[parent,child]).rpc;
  await assert.rejects(readCodexRecycleScope(unknownRoot,root.id,{ownedRootRollout:{
    ...ownedRootRollout,meta:{...ownedRootRollout.meta,session_id:undefined},
  }}),{code:'codex_recycle_lineage_unverified'});
});

test('目录查询末页未核验时，原历史不能补充未知的全局身份范围',async()=>{
  const rpc=new RpcClient(request=>queueMicrotask(()=>rpc.receive({id:request.id,result:
    request.method==='thread/loaded/list'?{data:['root']}:{data:[],nextCursor:'repeated'}})),()=>{},'mock');
  const ownedRootRollout:SessionProfile={sessionId:'root',file:'original',offset:100,verified:true,complete:true,
    meta:{id:'root',session_id:'root',cwd:root.cwd,source:'cli'},settings:{}};
  await assert.rejects(readCodexRecycleScope(rpc,root.id,{ownedRootRollout}),{code:'codex_recycle_lineage_unverified'});
});

test('目录隐藏的原根仍独立核对工作，活动或缺少工作证据不冒充空闲',async()=>{
  const profile:SessionProfile={sessionId:root.id,offset:100,complete:true,verified:true,meta:{},settings:{}};
  const inspect=(data:unknown)=>{
    const rpc=new RpcClient(request=>queueMicrotask(()=>rpc.receive({id:request.id,result:{data}})),()=>{},'mock');
    return readCodexRecycleThreadWork(rpc,root,profile,true,true);
  };
  assert.deepEqual(await inspect([{id:'active',status:'inProgress'}]),{active:'active',recent:undefined});
  assert.deepEqual(await inspect([]),{unknown:'terminal_history_unavailable'});
  assert.deepEqual(await inspect(undefined),{unknown:'turns_unavailable'});
});

test('Windows 控制台证明要求同一 Job、直接原生祖先及系统程序',{skip:process.platform!=='win32'},()=>{
  const image=path.join(process.env.SystemRoot!,'System32','conhost.exe');
  const native={pid:100,parentPid:99,name:'codex.exe',startedAt:'200'};
  const consoleHost={pid:101,parentPid:100,name:'conhost.exe',image,startedAt:'201'};
  const scope={inWindowsJob:true,nativeChain:[native]};
  assert.equal(ownedWindowsConsoleInfrastructure(consoleHost,scope),true);
  assert.equal(ownedWindowsConsoleInfrastructure({...consoleHost,parentPid:102},scope),false);
  assert.equal(ownedWindowsConsoleInfrastructure({...consoleHost,startedAt:'199'},scope),false);
  assert.equal(ownedWindowsConsoleInfrastructure({...consoleHost,image:path.join(tmpdir(),'conhost.exe')},scope),false);
  assert.equal(ownedWindowsConsoleInfrastructure(consoleHost,{...scope,inWindowsJob:false}),false);
});

test('回收计划要求原 provider 和根目录，并精确保留原命名权限定义',()=>{
  const cwd=tmpdir(),settings={
    cwd,model:'original-model',modelProvider:'original-provider',reasoningEffort:'high',serviceTier:'priority',
    approvalPolicy:'never',approvalsReviewer:'user',runtimeWorkspaceRoots:[cwd],permissionProfile:{type:'disabled'},
  };
  const profile:SessionProfile={sessionId:'root',offset:1,verified:true,complete:true,meta:{},settings};
  const {model,serviceTier,modelProvider,runtimeWorkspaceRoots,sandbox}=planExactCodexRecycle(profile,cwd);
  assert.deepEqual({model,serviceTier,modelProvider,runtimeWorkspaceRoots,sandbox},{
    model:'original-model',serviceTier:'priority',modelProvider:'original-provider',runtimeWorkspaceRoots:[cwd],sandbox:'danger-full-access',
  });
  for(const key of ['modelProvider','runtimeWorkspaceRoots'])assert.throws(()=>planExactCodexRecycle({
    ...profile,settings:{...settings,[key]:undefined},
  },cwd),{code:'owned_profile_unverified'});
  const live={activePermissionProfile:{id:'original'}},definition={filesystem:{':root':'read'},network:{enabled:false}};
  const permissionProfile={type:'managed',network:'restricted',file_system:{type:'restricted',entries:[{
    path:{type:'special',value:{kind:'root'}},access:'read',
  }]}};
  const named={...profile,settings:{...settings,permissionProfile},live};
  assert.throws(()=>planExactCodexRecycle(named,cwd),{code:'owned_profile_unverified'});
  const args=planExactCodexRecycle({...named,livePermissionConfig:{'permissions.original':definition}},cwd);
  assert.equal(args.permissions,'original');
  assert.deepEqual(args.config,{'permissions.original':definition,model_reasoning_effort:'high'});
  const retained=withPersistedCodexPermission({...profile,settings:{...settings,permissionProfile},activePermission:{id:'original'}});
  assert.deepEqual(planExactCodexRecycle(retained,cwd),args);
  const inherited=withPersistedCodexPermission({...profile,settings:{...settings,permissionProfile},activePermission:{id:'original',extends:'other'}});
  assert.throws(()=>planExactCodexRecycle(inherited,cwd),{code:'owned_profile_unverified'});
  const builtin=withPersistedCodexPermission({...profile,activePermission:{id:':danger-full-access'}});
  assert.equal(planExactCodexRecycle(builtin,cwd).permissions,':danger-full-access');
});

test('末轮结束后原生历史出现的工具工作不会当成空闲',async t=>{
  const file=path.join(temporary(t),'rollout.jsonl'),at='2026-11-26T00:00:00.000Z';
  const line=(type:string,payload:object)=>JSON.stringify({timestamp:at,type,payload})+'\n';
  writeFileSync(file,line('session_meta',{id:root.id,cwd:root.cwd})+
    line('event_msg',{type:'task_started',turn_id:'turn'})+
    line('turn_context',{active_permission_profile:{id:'original'},permission_profile:{type:'disabled'}})+
    line('response_item',{type:'function_call_output',call_id:'before'})+
    line('event_msg',{type:'task_complete',turn_id:'turn'}),'utf8');
  const complete=await readSessionProfile(file,root.id,{trackWork:true});
  assert.equal(complete.turn?.state,'ready');
  assert.deepEqual(complete.activePermission,{id:'original'});
  assert.ok(complete.rolloutWork&&complete.rolloutWork.lastWorkOffset<complete.rolloutWork.turnOffset);
  writeFileSync(file,line('event_msg',{type:'exec_command_begin',call_id:'after'}),{encoding:'utf8',flag:'a'});
  const later=await readSessionProfile(file,root.id,{trackWork:true});
  assert.ok(later.rolloutWork&&later.rolloutWork.lastWorkOffset>later.rolloutWork.turnOffset);
});

test('模型和工具事件更新进展，单独的设置或用量落盘不更新时间',async t=>{
  const file=path.join(temporary(t),'rollout.jsonl');
  const line=(minute:number,type:string,payload:object)=>JSON.stringify({timestamp:`2026-11-26T00:0${minute}:00.000Z`,type,payload})+'\n';
  writeFileSync(file,line(0,'session_meta',{id:root.id,cwd:root.cwd})+line(0,'event_msg',{type:'task_started',turn_id:'turn'})+
    line(1,'response_item',{type:'reasoning'})+line(2,'event_msg',{type:'token_count'})+line(3,'turn_context',{}),'utf8');
  const reasoning=await readSessionProfile(file,root.id,{trackWork:true});
  assert.equal(reasoning.rolloutWork?.lastWorkAt,Date.parse('2026-11-26T00:01:00.000Z'));
  assert.equal(reasoning.rolloutWork?.lastWorkOffset,0);
  writeFileSync(file,line(4,'response_item',{type:'function_call_output',call_id:'tool'}),{encoding:'utf8',flag:'a'});
  const tool=await readSessionProfile(file,root.id,{trackWork:true});
  assert.equal(tool.rolloutWork?.lastWorkAt,Date.parse('2026-11-26T00:04:00.000Z'));
  assert.ok(tool.rolloutWork&&tool.rolloutWork.lastWorkOffset>tool.rolloutWork.turnOffset);
});

test('只读恢复核验要求同一原生回执、完整落盘权限和原命名 profile',async t=>{
  const cwd=temporary(t),file=path.join(cwd,'rollout.jsonl'),id='resume-root',at='2026-11-26T00:00:00.000Z';
  const line=(type:string,payload:object)=>JSON.stringify({timestamp:at,type,payload})+'\n';
  const permission_profile={type:'managed',network:'restricted',file_system:{type:'restricted',entries:[{
    path:{type:'special',value:{kind:'root'}},access:'read',
  }]}};
  const snapshot={cwd,model:'original-model',model_provider_id:'source-provider',reasoning_effort:'high',
    approval_policy:'never',approvals_reviewer:'user',runtime_workspace_roots:[cwd],service_tier:'opaque',
    active_permission_profile:{id:'original'},permission_profile};
  const settings=(value:object)=>line('event_msg',{type:'thread_settings_applied',thread_id:id,thread_settings:value});
  writeFileSync(file,line('session_meta',{id,cwd})+settings(snapshot),'utf8');
  const original=withPersistedCodexPermission(await readSessionProfile(file,id));
  const ack={thread:{id,path:file,status:{type:'idle'}},model:'original-model',modelProvider:'source-provider',reasoningEffort:'high',
    approvalPolicy:'never',approvalsReviewer:'user',runtimeWorkspaceRoots:[cwd],cwd,activePermissionProfile:{id:'original'},serviceTier:null};
  const verified=await verifyCodexResumeAck(original,ack,{cwd});
  assert.deepEqual(verified.profile.settings.permissionProfile,permission_profile);
  assert.equal(ack.serviceTier,'opaque','原生空投影由原样落盘的服务档位补证');
  await assert.rejects(verifyCodexResumeAck(original,{...ack,runtimeWorkspaceRoots:[]},{cwd}),{code:'policy_verification_failed'});
  await assert.rejects(verifyCodexResumeAck(original,{...ack,activePermissionProfile:{id:'new'}},{cwd}),{code:'policy_verification_failed'});
  writeFileSync(file,settings({...snapshot,permission_profile:{type:'disabled'}}),{encoding:'utf8',flag:'a'});
  await assert.rejects(verifyCodexResumeAck(original,ack,{cwd,requireFreshSettings:true}),{code:'restart_profile_changed'});
});
