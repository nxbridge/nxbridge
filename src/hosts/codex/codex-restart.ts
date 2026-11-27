import {createReadStream,existsSync} from 'node:fs';
import {randomUUID} from 'node:crypto';
import { createInterface } from 'node:readline';
import { planCodexResume, readSessionProfile, resumeSession, snapshotLiveSession, verifyRestartProfileCurrent, type SessionPolicy, type SessionProfile } from './codex-session-profile.js';
import {BridgeError,RuntimeRecoveryHold,describeError} from '../../shared/errors.js';
import {type Endpoint,type Peer} from '../../types.js';
import {readJson} from '../../shared/files.js';
import {removeOwnedSocket} from '../../ipc/home.js';
import type { ProcessIdentity } from '../../process/process.js';
import type { RpcClient } from '../../ipc/rpc.js';
import { catalogCodexThreads, readCodexThread } from './codex-evidence.js';
import { stopOwnedProcess } from '../../process/process-stop.js';
import { retireDeadOwnedProcess, ownedProcesses, ownedRecordState, releaseOwnedProcess, type OwnedProcessRecord } from '../../process/owned-processes.js';
import { runtimeFor, removeManagedRuntime, saveManagedRuntime, writeExistingRuntime, type ManagedRuntime, type RuntimeRecord } from '../managed-runtimes.js';
import { assertRuntimeWorkloadIdle, configuredMcpInfrastructure, mcpCommands, nxbridgeMcpCommands, nxbridgeRuntimeInfrastructure } from '../../process/runtime-workload.js';
import type { TargetSelector } from '../../broker/pairing/admin-target.js';
import { verifyCodexNativeGeneration, verifyDedicatedCodex } from './codex-isolation.js';
import { codexThreadIdle } from './codex-thread-state.js';
import { codexParent, codexSubagent } from './codex-lineage.js';
import type { CodexThread } from './adapter.js';
import { completeRestartProfile, prepareRestartProfile } from './codex-restart-profile.js';
import type { CodexServiceTierError } from './codex-service-tier.js';
import { timeCodexRestart, traceCodexRestartReplacement } from './codex-restart-timing.js';

export interface CodexRestartResult {endpoint:string;oldPid:number;pid:number;resumed:any;notes?:string[]}
export interface RestartSelector extends TargetSelector {partner?:true}
export type CodexChildWorkMethod='thread/queue/list'|'thread/backgroundTerminals/list';
export interface CodexChildWorkPage {data:unknown[];nextCursor?:string|null}
interface Runtime {endpoint:string;pid:number;procStart?:string}
interface CodexStopOptions {
  peer:Peer;endpoint:Endpoint;home:string;connect:(address:string)=>Promise<RpcClient>;guard:()=>void;
  profile?:SessionProfile;inspect?:(address:string)=>Promise<RpcClient>;verifyDelivery?:(thread:any)=>Promise<void>;observeThread?:(thread:any)=>void;onStopped?:()=>void;
  acquire?: (rpc:RpcClient,runtime:ManagedRuntime)=>Promise<SessionProfile>;
  borrowLostSocket?:(saved:ManagedRuntime,native:ProcessIdentity)=>Promise<RpcClient>;
  beforeStop?:()=>void|Promise<void>;
  restoring?:boolean;
  childControl?:(rpc:RpcClient,method:CodexChildWorkMethod,id:string,beforeSend:()=>void)=>Promise<CodexChildWorkPage>;
}

function runtimeOwners(home:string,saved:ManagedRuntime):OwnedProcessRecord[]{
  return ownedProcesses(home).records.filter(value=>value.proof.launchId===saved.id||saved.kind==='legacy'&&value.proof.launchId===saved.endpoint
    ||value.role==='codex-runtime'&&value.identity.pid===saved.pid&&value.identity.startedAt===saved.procStart);
}

export function restartArguments(args:string[]):RestartSelector{
  if(args.length===2&&args[0]==='codex'&&args[1]==='--partner')return{partner:true};
  const flags={'--session':'session_id','--member':'peer_id','--pair':'pair_id'} as const,key=flags[args[1] as keyof typeof flags];
  if(args[0]!=='codex'||![1,3].includes(args.length)||(args.length===3&&(!key||!args[2]||args[2].startsWith('-'))))throw new BridgeError('invalid_arguments','用法：nxbridge restart codex [--partner | --session <会话ID> | --member <成员ID> | --pair <配对ID>]；--partner 仅供当前 Claude 窗口重启自己的托管搭档。');
  return args.length===3?{[key!]:args[2]}:{};
}

export function manualCodexRestart(peer:Peer,platform:NodeJS.Platform=process.platform):string{
                                          
  const quote=(value:string)=>"'"+value.replace(/'/g,platform==='win32'?"''":"'\"'\"'")+"'";
  const args=['codex','--no-daemon','--cd',quote(peer.cwd),...(peer.model?['--model',quote(peer.model)]:[]),...(peer.effort!==undefined?['-c',quote('model_reasoning_effort='+JSON.stringify(String(peer.effort)))]:[]),'resume',quote(peer.sessionId!)];
  return `会话 ${peer.sessionId} 由用户窗口管理，未执行重启。\n等待任务、问题和审批结束后，在原窗口输入 /quit，再在终端执行：\n${args.join(' ')}\n此命令用独立原生运行时恢复原 ID；请勿同时打开原会话的两个窗口。`;
}

function idle(thread:any,allowUnloaded=false):void{
  if(!codexThreadIdle(thread)&&!(allowUnloaded&&thread.status?.type==='notLoaded'&&!thread.status.activeFlags?.length&&!thread.turns?.some((turn:any)=>turn.status==='inProgress')))throw new BridgeError('session_busy','目标仍在运行或等待输入/审批，未执行重启；请在空闲后重试。');
}

                                        
export async function loadedCodexThreads(rpc:RpcClient):Promise<Set<string>>{
  const loaded=new Set<string>(),cursors=new Set<string>(),deadline=performance.now()+5000;
  let cursor:string|undefined;
  const unverified=()=>new BridgeError('restart_verification_failed','没能完整读取 Codex 正在加载的会话列表。');
  for(let pageIndex=0;pageIndex<10;pageIndex++){
    const remaining=Math.ceil(deadline-performance.now());
    if(remaining<=0)throw unverified();
    const page=await timeCodexRestart('native_loaded_threads',()=>rpc.request('thread/loaded/list',{limit:100,...(cursor?{cursor}:{})},{timeout:remaining}));
    if(!Array.isArray(page.data))throw unverified();
    for(const value of page.data){const threadId=typeof value==='string'?value:value?.id;if(typeof threadId!=='string'||!threadId)throw unverified();loaded.add(threadId);}
    if(page.nextCursor==null)return loaded;
    if(typeof page.nextCursor!=='string'||!page.nextCursor||cursors.has(page.nextCursor))throw unverified();
    cursor=page.nextCursor;cursors.add(cursor!);
  }
  throw unverified();
}

                               
export function confirmCodexThreadAbsent(rpc:RpcClient,id:string):void{
  if(!rpc.closed)(rpc as RpcClient&{confirmThreadAbsent?:(id:string)=>void}).confirmThreadAbsent?.(id);
}

export async function codexThreadLoaded(rpc:RpcClient,id:string):Promise<boolean>{
  if((await loadedCodexThreads(rpc)).has(id))return true;
  confirmCodexThreadAbsent(rpc,id);return false;
}

                                      
export async function assertCodexRuntimeScope(rpc:RpcClient,id:string,options:{inspectOnly?:boolean;probe?:(id:string)=>Promise<void>;observed?:(ids:ReadonlySet<string>)=>void}={}):Promise<void>{
  const loaded=await loadedCodexThreads(rpc);if(!loaded.delete(id))confirmCodexThreadAbsent(rpc,id);
  options.observed?.(loaded);
  const threads=await timeCodexRestart('native_thread_catalog',()=>catalogCodexThreads(rpc,loaded));
  const unverified=()=>new BridgeError('restart_verification_failed','没能确认 Codex 子任务的身份或状态，未执行重启。');
  const read=async(threadId:string)=>{
    let thread=threads.get(threadId);
    if(!thread){
      thread=(await catalogCodexThreads(rpc,new Set([threadId]))).get(threadId);
      if(!thread||thread.id!==threadId)throw unverified();
      threads.set(threadId,thread);
    }
    return thread;
  };
  for(const threadId of loaded){
    const metadata=await read(threadId),seen=new Set<string>();let current=metadata,verified=false;
    for(let depth=0;depth<64;depth++){
      if(seen.has(current.id))throw unverified();seen.add(current.id);
      if(!codexSubagent(current))throw new BridgeError('shared_runtime','这个后台还运行着另一个 Codex 会话，未执行重启。');
      const parent=codexParent(current);if(!parent)throw unverified();
      if(parent===id){verified=true;break;}current=await read(parent);
    }
    if(!verified)throw unverified();
    if(!codexThreadIdle(metadata)&&!(metadata.status?.type==='notLoaded'&&!metadata.status.activeFlags?.length))
      throw new BridgeError('subagent_active','Codex 还有子代理在运行，未执行重启；等它们结束后再试一次。');
    if(options.inspectOnly)continue;
    if(!options.probe){
      if(!await codexThreadLoaded(rpc,threadId))continue;
      throw new BridgeError('subagent_active','Codex 子代理尚未卸载，暂不能核验其后台工作已结束；稍后再试。');
    }
    if(!metadata.ephemeral){
      const page=await timeCodexRestart('native_turn_summary',()=>rpc.request('thread/turns/list',{threadId,limit:1,sortDirection:'desc',itemsView:'summary'},{timeout:3000}));
      if(!Array.isArray(page.data))throw unverified();
      const latest=page.data[0];
      if(latest?.status==='inProgress')throw new BridgeError('subagent_active','Codex 子代理的轮次尚未结束，未执行重启。');
      if(latest)await timeCodexRestart('terminal_proof_child',()=>durable({...metadata,turns:[latest]}));
    }
    await options.probe(threadId);
  }
}

function currentChildControls(home:string,runtime:ManagedRuntime):RuntimeRecord{
  const record=runtimeFor(home,{id:runtime.id,endpoint:runtime.endpoint});
  if(record){
    const current=record.value;
    if(current.kind===runtime.kind&&current.state!=='failed'&&current.owner===runtime.owner&&current.peerId===runtime.peerId&&current.pid&&current.procStart
      &&current.pid===runtime.pid&&current.procStart===runtime.procStart)return record;
  }
  throw new BridgeError('runtime_changed','核验子代理时原生进程代次或归属已改变。');
}

                                   
export function forgetAbsentChildControls(home:string,runtime:ManagedRuntime,present:(id:string)=>boolean):void{
  const record=currentChildControls(home,runtime),current=record.value,checks=current.childControlChecks;
  if(!checks||checks.pid!==current.pid||checks.procStart!==current.procStart)return;
  const pending=Object.fromEntries(Object.entries(checks.pending).filter(([id])=>present(id)));
  if(Object.keys(checks.pending).length===Object.keys(pending).length)return;
  if(Object.keys(pending).length)checks.pending=pending;else delete current.childControlChecks;
  writeExistingRuntime(record);
}

                                     
export async function probeCodexChildWork(home:string,runtime:ManagedRuntime,id:string,loaded:()=>Promise<boolean>,read:(method:CodexChildWorkMethod,beforeSend:()=>void)=>Promise<CodexChildWorkPage>,guard:()=>void):Promise<void>{
  const hold=()=>new BridgeError('subagent_control_unverified','此子代理的原生工作核验未确认。已保留原会话；等它实际卸载后再重试。');
  for(const method of ['thread/queue/list','thread/backgroundTerminals/list'] as const){
    if(!await loaded()){
      forgetAbsentChildControls(home,runtime,value=>value!==id);return;
    }
    const token=randomUUID();let sent=false,confirmed=false;
    try{
      const result=await read(method,()=>{
        guard();
        const record=currentChildControls(home,runtime),current=record.value,saved=current.childControlChecks;
        const pending=saved&&saved.pid===current.pid&&saved.procStart===current.procStart?saved.pending:{};
        if(Object.hasOwn(pending,id))throw hold();
        current.childControlChecks={pid:current.pid!,procStart:current.procStart!,pending:{...pending,[id]:token}};
        writeExistingRuntime(record);sent=true;
      });
      if(!Array.isArray(result?.data)||result.nextCursor!=null)throw hold();
      confirmed=true;
      if(result.data.length)throw method==='thread/queue/list'
        ?new BridgeError('delivery_unsettled','子代理还有排队输入，未执行重启。')
        :new BridgeError('session_busy','子代理还有后台终端任务，未执行重启。');
    }catch(error){
                                          
      if(!sent||confirmed)throw error;
      throw hold();
    }finally{
      if(sent&&confirmed){
        const {value:current}=currentChildControls(home,runtime);
        if(current.childControlChecks?.pending[id]===token)forgetAbsentChildControls(home,current,value=>value!==id);
      }
    }
  }
}

export async function assertNoPendingCodexWork(rpc:RpcClient,id:string,options:{allowUnloaded?:boolean}={}):Promise<void>{
  const unloaded=Symbol();
  const [queue,terminals]=await Promise.all([
    timeCodexRestart('native_queue',()=>rpc.request('thread/queue/list',{threadId:id,limit:1})),
    timeCodexRestart('native_terminals',()=>rpc.request('thread/backgroundTerminals/list',{threadId:id,limit:1})).catch(error=>{
      if(options.allowUnloaded&&error instanceof BridgeError&&error.message==='thread not found: '+id)return unloaded;
      throw error;
    }),
  ]);
  if(!Array.isArray(queue?.data)||terminals!==unloaded&&!Array.isArray(terminals?.data))throw new BridgeError('native_work_unverified','原生队列或后台终端列表无法核验，未停止运行时。');
  if(queue.data.length)throw new BridgeError('delivery_unsettled','原生线程还有排队输入，未执行重启；请先处理队列。');
  if(terminals!==unloaded&&terminals.data.length)throw new BridgeError('session_busy','目标还有后台终端任务，未执行重启；请先等待任务结束。');
  if(queue.nextCursor||terminals!==unloaded&&terminals.nextCursor)throw new BridgeError('native_work_unverified','原生队列或后台终端列表未完整返回，未停止运行时。');
  if(terminals===unloaded){
                                              
    const thread=(await rpc.request('thread/read',{threadId:id,includeTurns:false})).thread;
    if(thread?.id!==id||thread.status?.type!=='notLoaded'||thread.status.activeFlags?.length)throw new BridgeError('session_busy','线程已重新加载或状态无法核验，未停止运行时。');
    if(await codexThreadLoaded(rpc,id))throw new BridgeError('session_busy','Codex 会话在核对期间又被加载了，这次没有停止它。');
  }
}

async function durable(thread:any):Promise<void>{
  const last=thread.turns?.at(-1);
  if(!last||!['completed','failed','interrupted'].includes(last.status)||!thread.path||!existsSync(thread.path))throw new BridgeError('history_not_persisted','目标没有可核验的完整持久历史，未执行重启。');
                                            
  const stream=createReadStream(thread.path,{encoding:'utf8'}),lines=createInterface({input:stream,crlfDelay:Infinity});
  try{
    for await(const line of lines){
      let record:any;try{record=JSON.parse(line);}catch{continue;}
      if(record.type==='event_msg'&&['task_complete','turn_complete','turn_aborted'].includes(record.payload?.type)&&record.payload.turn_id===last.id)return;
    }
  }finally{lines.close();stream.destroy();}
  throw new BridgeError('history_not_persisted','末轮结束记录尚未落盘，未执行重启；请稍后重试。');
}

export async function restartCodexRuntime(options:CodexStopOptions&{
  start:()=>Promise<string>;onVerifying?:(runtimeId:string,endpoint:string)=>void;onPolicy?:(policy:SessionPolicy,runtimeId:string,endpoint:string)=>void;
  restore:(rpc:RpcClient,id:string,profile:SessionProfile,runtime:ManagedRuntime)=>ReturnType<typeof resumeSession>;
}):Promise<CodexRestartResult>{
  const stopped=await timeCodexRestart('old_runtime',()=>stopCodexRuntime({...options,restoring:true})),id=options.peer.sessionId!;
  try{
    const address=await timeCodexRestart('start_replacement',()=>options.start()),fresh=await timeCodexRestart('connect_replacement',()=>options.connect(address));
    const pending=runtimeFor(options.home,{endpoint:address});if(!pending)throw new BridgeError('runtime_missing','新运行时缺少启动记录。');
    traceCodexRestartReplacement(pending.value.id,pending.value.pid,pending.value.procStart);
    if(!pending.value.sessionVerification){pending.value.sessionVerification={sessionId:id,profile:stopped.profile};saveManagedRuntime(options.home,pending.value);}
    options.onVerifying?.(pending.value.id,address);
    const restored=await timeCodexRestart('restore_replacement',()=>options.restore(fresh,id,stopped.profile,pending.value));
    const runtime=runtimeFor(options.home,{endpoint:address});if(!runtime?.value.pid)throw new BridgeError('runtime_missing','新运行时缺少已核验的进程记录。');
    timeCodexRestart('host_post_ack',()=>{
      options.onPolicy?.(restored.policy,runtime.value.id,address);
      completeRestartProfile(options.home,runtime.value);delete runtime.value.sessionVerification;saveManagedRuntime(options.home,runtime.value);
    });
    return{endpoint:address,oldPid:stopped.oldPid,pid:runtime.value.pid,resumed:restored.resumed,notes:stopped.notes};
  }catch(error){throw Object.assign(new BridgeError('restart_incomplete',`旧托管进程已停止，但原会话 ${id} 未完整恢复；已暂停自动投递。原因：`+describeError(error)),{pauseDetail:(error as Partial<CodexServiceTierError>|null)?.pauseDetail});}
}

                                              
export async function stopCodexRuntime(options:CodexStopOptions):Promise<{oldPid:number;notes:string[];profile:SessionProfile}>{
  const {peer,endpoint,home,connect,guard}=options,id=peer.sessionId!,record=runtimeFor(home,{endpoint:endpoint.address});
  if(!record)throw new BridgeError('unowned_runtime','没有 nxbridge 托管运行时记录，未执行重启；用户 daemon 或远程宿主需要原生重载。');
  const filename=record.file,saved=record.value;
  if(timeCodexRestart('owners_read',()=>ownedProcesses(home)).records.some(r=>r.releasedToUser&&(r.identity.pid===saved.pid||r.proof.launchId===saved.id||r.proof.launchId===saved.endpoint)))throw new BridgeError('shared_runtime','这个 App Server 还有你的窗口在用，nxbridge 不会重启它。请在对应的 Codex 窗口里退出后重新打开。');
  const verify=async()=>{
    const current=existsSync(filename)?readJson<ManagedRuntime>(filename):undefined;
    if(saved.endpoint!==endpoint.address||!current||current.pid!==saved.pid||current.procStart!==saved.procStart||current.endpoint!==saved.endpoint
      ||(options.beforeStop||options.borrowLostSocket)&&(current.id!==saved.id||current.owner!==saved.owner||current.peerId!==peer.id||current.sessionId&&current.sessionId!==id))
      throw new BridgeError('runtime_changed','运行时归属记录已经变化，未执行重启。');
    return timeCodexRestart('native_generation',()=>verifyCodexNativeGeneration(saved));
  };
  const originalNative=await verify();
  if(!options.borrowLostSocket)await timeCodexRestart('dedicated_endpoint',()=>verifyDedicatedCodex(saved.endpoint,originalNative));
  const rpc=await timeCodexRestart('connect_old',()=>options.borrowLostSocket?options.borrowLostSocket(saved,originalNative):(options.inspect??connect)(endpoint.address));
  try{
  const inspectThread=(thread:any,allowUnloaded=false)=>{options.observeThread?.(thread);idle(thread,allowUnloaded);};
  if(options.acquire&&await codexThreadLoaded(rpc,id))throw new RuntimeRecoveryHold('owned_subscription_waiting','要等后台 Codex 这个会话闲下来、被 Codex 卸载后，才能完成迁移前的核对；消息已保留。');
  await timeCodexRestart('runtime_scope',()=>assertCodexRuntimeScope(rpc,id,{inspectOnly:!!options.childControl,observed:loaded=>forgetAbsentChildControls(home,saved,threadId=>loaded.has(threadId))}));
                                  
  const acquired=await timeCodexRestart('acquire_subscription',()=>options.acquire?.(rpc,saved));
  const readThread=()=>timeCodexRestart('native_thread_check',()=>readCodexThread(rpc,id));
  const live={thread:await readThread()};inspectThread(live.thread,true);await timeCodexRestart('terminal_proof_first',()=>durable(live.thread));
  let profile:SessionProfile;
  try{
    profile=options.profile??acquired??await timeCodexRestart('profile_snapshot',()=>codexThreadIdle(live.thread)?snapshotLiveSession(rpc,id,live.thread.path!):readSessionProfile(live.thread.path,id));
    if(!profile.verified||!profile.complete)throw new BridgeError('profile_unavailable','无法核验原会话完整持久设置，未停止原运行时。');
    if(options.restoring)planCodexResume(id,profile,peer.cwd,{exact:true});
  }catch(error){
    if(options.restoring&&error instanceof RuntimeRecoveryHold&&error.code==='owned_profile_unverified')
      throw new RuntimeRecoveryHold(error.code,'无法确认后台 Codex 原来的模型、推理强度、服务档位和安全设置，为免换成别的设置，这次没有重启，原来的进程照常运行。');
    throw error;
  }
  const unloaded=live.thread.status?.type==='notLoaded';
  await verify();await assertNoPendingCodexWork(rpc,id,{allowUnloaded:unloaded});
  const verified=await readThread();inspectThread(verified,true);
  if(unloaded&&verified.status?.type!=='notLoaded')throw new BridgeError('session_busy','已卸载线程在停止核验期间重新加载，未停止运行时。');
  if(options.verifyDelivery){
    await timeCodexRestart('terminal_proof_final',()=>durable(verified));
    await timeCodexRestart('delivery_proof',()=>options.verifyDelivery!(verified));
  }guard();
  let stopSubmitted=false;
  try{
    await timeCodexRestart('runtime_scope',()=>assertCodexRuntimeScope(rpc,id,{observed:loaded=>forgetAbsentChildControls(home,saved,threadId=>loaded.has(threadId)),
      ...(options.childControl?{probe:(threadId:string)=>probeCodexChildWork(home,saved,threadId,()=>codexThreadLoaded(rpc,threadId),
        (method,beforeSend)=>options.childControl!(rpc,method,threadId,beforeSend),guard)}:{})}));
    inspectThread(await readThread(),true);
    let owners:OwnedProcessRecord[]|undefined;
    if(options.beforeStop||options.borrowLostSocket){
      const config=(await timeCodexRestart('native_config',()=>rpc.request('config/read',{cwd:peer.cwd},{timeout:3000}))).config;
      const commands=[...mcpCommands(config?.mcp_servers),...(saved.configHome?nxbridgeMcpCommands(home,'codex',saved.configHome):[])];
      const native=await verify();
      owners=timeCodexRestart('owners_read',()=>runtimeOwners(home,saved));
      await timeCodexRestart('runtime_workload',()=>assertRuntimeWorkloadIdle({native,owners:owners!,infrastructure:async candidate=>await nxbridgeRuntimeInfrastructure(home,candidate)||configuredMcpInfrastructure(native,commands,candidate)}));
      await assertNoPendingCodexWork(rpc,id,{allowUnloaded:unloaded});
      if(options.borrowLostSocket&&await options.borrowLostSocket(saved,native)!==rpc)
        throw new BridgeError('codex_socket_idle_unverified','原连接在空闲核验后发生变化，未停止原会话。');
      if(!options.borrowLostSocket)await timeCodexRestart('dedicated_endpoint',()=>verifyDedicatedCodex(saved.endpoint,native));
    }
    guard();
    const preserve=options.beforeStop||options.borrowLostSocket?prepareRestartProfile(home,saved,profile):undefined;
    if(options.beforeStop)await timeCodexRestart('before_stop',()=>options.beforeStop!());
    if(options.restoring)await timeCodexRestart('profile_recheck',()=>verifyRestartProfileCurrent(profile));
    if(options.restoring||options.beforeStop)guard();
    if(rpc.closed)throw new BridgeError('disconnected','原生连接在停止前断开，未停止原会话。');
    preserve?.();
    stopSubmitted=true;
    await timeCodexRestart('stop_process',()=>stopOwnedProcess({pid:saved.pid!,startedAt:saved.procStart!},{timeout:15000}));
    timeCodexRestart('old_runtime_offline',()=>options.onStopped?.());
    rpc.close();
    const current=readJson<Runtime>(filename);
    if(current.pid!==saved.pid||current.procStart!==saved.procStart||current.endpoint!==saved.endpoint)throw new BridgeError('runtime_changed','运行时记录已被其他操作更新，停止自动恢复；原会话 ID 保留。');
    const notes:string[]=[];
    try{await timeCodexRestart('cleanup_socket',()=>removeOwnedSocket(home,saved.endpoint));}catch(error){notes.push('旧 socket 清理未完成：'+describeError(error));}
    await timeCodexRestart('cleanup_owners',async()=>{
      const records=owners??timeCodexRestart('owners_read',()=>runtimeOwners(home,saved));
      for(const proof of records.sort((a,b)=>Number(!!(b.windowsJob||b.posixGroup))-Number(!!(a.windowsJob||a.posixGroup))||b.createdAt-a.createdAt))
        if(await ownedRecordState(proof)==='dead')await retireDeadOwnedProcess(proof);else await releaseOwnedProcess(proof);
    });
    timeCodexRestart('cleanup_runtime_record',()=>removeManagedRuntime(record));
    return{oldPid:saved.pid!,notes,profile};
  }catch(error){if(!stopSubmitted)throw error;throw new BridgeError('restart_incomplete',`旧托管进程已收到停止请求，但原会话 ${id} 未完整恢复；已暂停目标自动投递，请检查原宿主后再恢复。原因：`+describeError(error));}
  }finally{if(options.inspect)await rpc.close();}
}
