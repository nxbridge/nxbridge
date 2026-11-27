import WebSocket from 'ws';
import { backendCodexClient, codexServerVersion, officialCodexClient } from './codex-client-identity.js';
import net from 'node:net';
import { proxySocket } from '../../ipc/proxy-socket.js';
import {openSync,closeSync,readSync} from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { codexThreadIdle } from './codex-thread-state.js';
import { profileChangeFailure, type EffectiveModelProfile } from '../../broker/profiles/partner-profile.js';
import { nativeServiceTier, observedServiceTier, type CodexServiceTierError } from './codex-service-tier.js';
import { createHash, randomUUID } from 'node:crypto';
import { spawnPosixSupervisor, posixSupervisorExit } from '../../process/posix/posix-supervisor.js';
import { RpcClient, clearRpcFaults, type RpcMessage } from '../../ipc/rpc.js';
import {BridgeError,DeliveryError,RuntimeRecoveryHold,describeError} from '../../shared/errors.js';
import {DEFAULT_RETENTION} from '../../config/defaults.js';
import {HEALTH_WAKE_NOTICE,WAKE_NOTICE,type BridgeConfig,type CodexPermissionEvidence,type Endpoint,type Peer,type PeerState,type RegisterInput} from '../../types.js';
import { rotateLog } from '../../storage/logs.js';
import {sameDirectory} from '../../shared/files.js';
import {bridgeHome} from '../../shared/local-paths.js';
import {delay} from '../../shared/time.js';
import {removeOwnedSocket,socketDirectory} from '../../ipc/home.js';
import {codexSocketTargetMissing,currentCodexSocketAlias,proveUnixCodexSocket,type UnixSocketProof} from '../../ipc/unix-socket.js';
import {spawnCli} from '../../process/host-command.js';
import {processIdentity,type ProcessIdentity} from '../../process/process.js';
import { assertCodexRuntimeScope, codexThreadLoaded, forgetAbsentChildControls, loadedCodexThreads, restartCodexRuntime, type CodexChildWorkMethod, type CodexChildWorkPage, type CodexRestartResult } from './codex-restart.js';
import {prepareOwnedLaunch,recordHostLaunch,selectHostLaunch,spawnSelectedHost,type HostLaunch,type HostLaunchOptions} from '../host-launch.js';
import { nativeDescendant } from '../../process/process.js';
import { providerRetryInfo } from '../../broker/health/turn-health.js';
import {probeAppServer,probePeerRuntime} from '../../broker/health/probe.js';
import {type RuntimeAvailability} from '../../broker/health/contracts.js';
import {declareOwnedNative,retireDeadOwnedProcess,ownedProcesses,ownedRecordState,prepareProcessRegistry,releaseOwnedProcess,releaseRecordedProcesses,rememberOwnedChild,rememberOwnedHost,type OwnedProcessRecord} from '../../process/owned-processes.js';
import {ownedProcessState,type OwnedIdentity} from '../../process/process-stop.js';
import { managedMarker } from '../../install/installation.js';
import { managedRuntimes, runtimeFor, saveManagedRuntime, writeExistingRuntime, removeManagedRuntime, managedRuntimeState, managedRuntimeNativeState, managedRuntimeToken,createRuntimeToken,localCodexEndpoint,assertManagedRuntimeEndpoint,type ManagedRuntime, type RuntimeRecord } from '../managed-runtimes.js';
import { launchIndependentWindows } from '../../process/windows/windows-background.js';
import { requireDedicatedCodex, verifyCodexNativeGeneration, verifyDedicatedCodex, verifyDedicatedCodexArguments, requireRestoreBootstrap, restoreBootstrapKey, CODEX_RESTORE_BOOTSTRAP } from './codex-isolation.js';
import {hookTrustFromList} from './codex-hook-trust.js';
import {configClient} from './stdio-codex.js';
import {persistentSocketDirectory} from '../../process/posix/persistent-socket-directory.js';
import {completeRestartProfile,pendingRestartVerification} from './codex-restart-profile.js';
import { selectSessionLaunch } from '../session-launch.js';
import { recordManagedSessionLaunch } from '../launch-profiles.js';
import { restoreState } from '../../broker/pairing/pair-restore.js';
import { backgroundState, requireBackgroundRetry } from '../background-state.js';
import { catchUpCodexHistory, readCodexHistory, type HistoryCursor, type HistoryEvidence } from './codex-history.js';
import { catalogCodexThreads, observeCodexThreadIdentity, readCodexThread, readCodexItems } from './codex-evidence.js';
import { classifyCodexPermission, codexRuntimeEvidence } from './codex-permission.js';
import { startOwnedCodex, unloadFailedOwnedCodex } from '../owned-launch-policy.js';
import type { PermissionClass } from '../../types.js';
import { materializeCodexResumeSettings, resumeSession, readSessionProfile, sessionFile, verifyCodexResumeAck, verifyResumedSession, verifyRestartProfileCurrent, readAppliedSessionSettings, persistedServiceTier, withPersistedCodexPermission, type SessionProfile, type SessionPolicy } from './codex-session-profile.js';
import { CodexOwnedRoutes, exitedOwnedCodexRoute, ownedCodexLease, type OwnedCodexRoute, type OwnedCodexLaunchProof } from './codex-owned-routes.js';
import { CodexNativeRpc } from './codex-native-rpc.js';
import { codexParent, codexSubagent, codexUserWindow } from './codex-lineage.js';
import { codexThreadState } from './codex-thread-state.js';
import { queueCodexWake } from './queue-wake.js';
import type { CodexTurnCause,CodexTurnOriginProvider } from './codex-turn-origin.js';
import type {CustodyAcquisition,CustodyOffer,ParkedCustody} from '../../broker/broker-custody.js';
import {acquireCodexGroups,custodyInvalidation,handoffUnavailable,parkCodexGroups,type ProvisionalCodexGroup} from './codex-handoff.js';
import {isDeepStrictEqual} from 'node:util';
import { collectCodexRecycleEvidence, inspectCodexRecycleLaunch, inspectCodexRecycleOld, persistCodexRecycleProfile, planExactCodexRecycle, retireCodexRecycleOld, stopCodexRecycleOld, type CodexRecycleObservation } from './codex-recycle.js';
import type { CodexRecycleGeneration, CodexRecyclePorts, CodexRecycleProfileRef, CodexRecycleTrigger } from '../../broker/recovery/codex-recycle.js';

interface PreparedRuntimeLaunch {launch:HostLaunch;authenticatedLoopback:boolean}
type RecycleObservation=Extract<CodexRecycleObservation,{runtime:ManagedRuntime}>;
type RecycleHostPorts=Pick<CodexRecyclePorts,'observe'|'persistProfile'|'inspectOld'|'stop'|'retireOld'|'prepareLaunch'|'launch'|'inspectLaunch'|'verify'>;
interface RecycleAdmission {input:RegisterInput;rpc:RpcClient;runtime:ManagedRuntime;policy:SessionPolicy;receipts:HistoryEvidence['receipts']}
const recycleCommandKey=(launch:HostLaunch)=>createHash('sha256').update(JSON.stringify(launch.executable)).digest('hex');
export interface CodexRestartOptions {
  identityMigration?:boolean;verificationRecovery?:boolean;lostSocketRecovery?:boolean;requesterSession?:string;
  verifyDelivery?:(thread:CodexThread)=>Promise<void>;beforeStop?:()=>void|Promise<void>;beforeLaunch?:()=>void|Promise<void>;
}
interface AppServerConnectionProof {runtimeId:string;image:string;socket:UnixSocketProof}

export interface CodexThread {
  id:string;cwd:string;name?:string;ephemeral?:boolean;parentThreadId?:string|null;forkedFromId?:string|null;threadSource?:string;path?:string;
  status?:{type:string;activeFlags?:string[]};source?:any;turns?:any[];
  model?:string|null;reasoningEffort?:string|null;progressEvidence?:boolean;
}

export interface CodexContextReceipt {messageId:string;turnId:string;itemId:string}

export function codexContextReceipts(item:any,turnId:unknown):CodexContextReceipt[]{
  if(typeof item?.id!=='string'||!item.id||typeof turnId!=='string'||!turnId)return[];
  let output:any;
  if(item.type==='functionCallOutput'&&item.namespace==='claude'&&['receive_messages','read_messages'].includes(item.name)&&typeof item.output==='string'){
    try{output=JSON.parse(item.output);}catch{return[];}
  }else if(item.type==='mcpToolCall'&&item.server==='claude'&&['receive_messages','read_messages'].includes(item.tool)&&item.status==='completed'&&!item.error){
    const content=item.result?.content;
    if(!Array.isArray(content))return[];
    output={messages:content.flatMap((entry:any)=>{if(entry?.type!=='text'||typeof entry.text!=='string')return[];try{const value=JSON.parse(entry.text);return Array.isArray(value.messages)?value.messages:[];}catch{return[];}})};
  }else return[];
  if(!Array.isArray(output?.messages))return[];
  const ids=new Set<string>();
  for(const message of output.messages)if(typeof message?.message_id==='string'&&message.message_id)ids.add(message.message_id);
  return[...ids].map(messageId=>({messageId,turnId,itemId:item.id}));
}

                                                    
export function codexAgentPath(sessionId:string,agentId:string,transcript:string,parentTranscript:string):string|undefined{
                                                                  
  const metadata=(filename:string)=>{
    const fd=openSync(filename,'r');
    try{
      const bytes=Buffer.allocUnsafe(4096),chunks:Buffer[]=[];let length:number;
      while((length=readSync(fd,bytes,0,bytes.length,null))>0){
        const end=bytes.subarray(0,length).indexOf(10);chunks.push(Buffer.from(bytes.subarray(0,end<0?length:end)));
        if(end>=0)break;
      }
      const record=JSON.parse(Buffer.concat(chunks).toString('utf8'));return record.type==='session_meta'?record.payload:undefined;
    }finally{closeSync(fd);}
  };
  const parent=metadata(parentTranscript),meta=metadata(transcript);
  if(parent?.id!==sessionId||parent?.session_id!==sessionId||meta?.id!==agentId||meta?.session_id!==sessionId)throw new BridgeError('unverified_parent','子代理及根元数据与 hook 身份不符。');
  return typeof meta.agent_path==='string'?meta.agent_path:undefined;
}

export async function connectAppServer(endpoint:string,config:BridgeConfig,bearerToken?:string,home=bridgeHome(),options:{readOnly?:boolean;signal?:AbortSignal}={}):Promise<RpcClient>{
  return (options.readOnly?backendCodexClient:officialCodexClient)(()=>openAppServerTransport(endpoint,config,bearerToken,home,options.signal));
}

async function openAppServerTransport(endpoint:string,config:BridgeConfig,bearerToken:string|undefined,home:string,signal?:AbortSignal):Promise<RpcClient>{
  signal?.throwIfAborted();
  let proxy:ReturnType<typeof spawnCli>|undefined;
  const launchId=randomUUID();let cleanup:Promise<void>|undefined;
  const stopProxy=()=>{if(!proxy)return;cleanup??=releaseRecordedProcesses(home,record=>record.proof.launchId===launchId).then(notes=>{for(const note of notes)process.stderr.write(note+'\n');}).catch(error=>{process.stderr.write('proxy 回收未完成：'+describeError(error)+'\n');});};
                                           
                                                  
                                     
  const options:WebSocket.ClientOptions&{closeTimeout:number}={handshakeTimeout:5000,closeTimeout:1000,maxPayload:16*1024*1024};
  if(!endpoint.startsWith('unix://')){
    await assertManagedRuntimeEndpoint(home,endpoint);
    const token=bearerToken??managedRuntimeToken(home,endpoint)??(config.codexAuthTokenEnv?process.env[config.codexAuthTokenEnv]:undefined);
    if(config.codexAuthTokenEnv&&!token)throw new BridgeError('token_missing',`缺少环境变量 ${config.codexAuthTokenEnv}，无法认证远程 App Server。`);
    if(token)options.headers={Authorization:`Bearer ${token}`};
  }
  let url=endpoint;
  if(endpoint.startsWith('unix://')){
    const filename=endpoint.slice('unix://'.length);url='ws://localhost';
    if(process.platform==='win32'){
      await prepareProcessRegistry(home);
      proxy=spawnCli('codex',['app-server','proxy','--sock',filename],{stdio:['pipe','pipe','pipe']},config,home);
      proxy.stderr?.on('data',()=>{});
      const stream=proxySocket(proxy);
                                                
      let streamError:Error|undefined;
      stream.on('error',error=>{streamError=error;});
                                           
      options.createConnection=()=>stream.setTimeout(options.handshakeTimeout??0) as unknown as net.Socket;
      try{
        await rememberOwnedHost(home,proxy,'codex','proxy',launchId);
        if(stream.destroyed)throw streamError??stream.errored??new BridgeError('proxy_closed','App Server proxy 在连接建立前已关闭。');
      }catch(error){stream.destroy();stopProxy();if(proxy.exitCode===null&&proxy.signalCode===null)proxy.kill();throw error;}
    }else options.createConnection=()=>net.createConnection({path:filename,timeout:options.handshakeTimeout});
  }else if(!/^wss?:\/\//.test(endpoint))throw new BridgeError('invalid_endpoint','App Server 地址必须是 unix:// 或 ws(s)://。');
  const socket=new WebSocket(url,options);
  const abort=()=>socket.terminate(),detachAbort=()=>signal?.removeEventListener('abort',abort);
  signal?.addEventListener('abort',abort,{once:true});
  await new Promise<void>((resolve,reject)=>{
    socket.once('open',resolve);socket.once('error',error=>{stopProxy();reject(error);});
    if(signal?.aborted)abort();
  }).catch(error=>{detachAbort();throw error;});
                                                 
  const rpc=new CodexNativeRpc(message=>{if(socket.bufferedAmount>4*1024*1024)throw new BridgeError('rpc_backpressure','Codex 那边的写入还在排队，稍后自动重试。');socket.send(JSON.stringify(message));},()=>socket.close(),endpoint);
  codexTransports.set(rpc,{id:randomUUID(),closed:new Promise(resolve=>socket.once('close',()=>resolve())),terminate:abort,detachAbort});
  let lastInput=Date.now(),misses=0,pongTimer:NodeJS.Timeout|undefined;
  const heartbeat=setInterval(()=>{
    if(rpc.closed||socket.readyState!==WebSocket.OPEN||Date.now()-lastInput<30000||pongTimer)return;
    socket.ping();pongTimer=setTimeout(()=>{pongTimer=undefined;if(++misses>=2){rpc.disconnected('原生心跳连续未确认。','heartbeat_timeout');socket.terminate();}},10000);pongTimer.unref();
  },30000);heartbeat.unref();
  const received=()=>{lastInput=Date.now();misses=0;if(pongTimer){clearTimeout(pongTimer);pongTimer=undefined;}};
  const clean=()=>{clearInterval(heartbeat);if(pongTimer)clearTimeout(pongTimer);};socket.on('pong',received);
  socket.on('message',data=>{received();const at=performance.now();try{const message=JSON.parse(data.toString());rpc.receive(message,{bytes:Buffer.isBuffer(data)?data.length:Buffer.byteLength(data.toString()),parseMs:performance.now()-at});}catch{rpc.disconnected('原生响应不是有效 JSON。','invalid_json');socket.close();}});
  socket.on('close',(code,reason)=>{clean();detachAbort();rpc.disconnected(reason.toString()||'连接已断开。',code===1009?'payload_too_large':'disconnected');stopProxy();});
  socket.on('error',(error:any)=>{clean();rpc.disconnected(error?.code==='WS_ERR_UNSUPPORTED_MESSAGE_LENGTH'?'原生响应超过 16 MiB 大小保护。':'原生连接中断。',error?.code==='WS_ERR_UNSUPPORTED_MESSAGE_LENGTH'?'payload_too_large':'disconnected');});
  return rpc;
}

const codexTransports=new WeakMap<RpcClient,{id:string;closed:Promise<void>;terminate:()=>void;detachAbort:()=>void}>();
const creationControls=new Set(['thread/read','thread/resume','thread/settings/update','thread/name/set','thread/delete']);
type ColdResumePlan={attempt:1|2;broker:OwnedIdentity};

                                               
async function deliveryStep<T>(work:()=>Promise<T>,signal?:AbortSignal,uncertain:boolean|(()=>boolean)=false):Promise<T>{
  const submitted=()=>typeof uncertain==='function'?uncertain():uncertain;
  if(signal?.aborted)throw new DeliveryError('stopping','协调器正在关闭，已停止等待系统通知。',submitted());
  if(!signal)return work();
  let abort!:()=>void;
  try{return await new Promise<T>((resolve,reject)=>{
    abort=()=>reject(new DeliveryError('stopping','协调器正在关闭，已停止等待系统通知。',submitted()));
    signal.addEventListener('abort',abort,{once:true});
    work().then(resolve,reject);
  });}finally{signal.removeEventListener('abort',abort);}
}

interface CodexRuntimeProfile extends Pick<CodexThread,'model'|'reasoningEffort'> {serviceTier?:string|null;approvalPolicy?:unknown;approvalsReviewer?:unknown;autoReviewEnabled?:boolean;sandbox?:{type?:string}}

export function threadRegistration(thread:CodexThread,endpoint:string,profile:CodexRuntimeProfile={}):RegisterInput|undefined{
  if(thread.ephemeral)return undefined;
  const parent=codexParent(thread);
  const model=profile.model===undefined?thread.model:profile.model,effort=profile.reasoningEffort===undefined?thread.reasoningEffort:profile.reasoningEffort;
  const serviceTier=observedServiceTier(profile.serviceTier);
  const evidence:CodexPermissionEvidence=codexRuntimeEvidence(profile);
  return {host:'codex',sessionId:thread.id,cwd:thread.cwd,name:thread.name??'Codex',kind:codexSubagent(thread)?'subagent':thread.forkedFromId?'fork':'root',parentId:parent,
    ...(model===undefined?{}:{model}),...(effort===undefined?{}:{effort}),...(serviceTier===undefined?{}:{serviceTier}),
    state:codexThreadState(thread),
    endpoint:{mode:'app-server',address:endpoint,epoch:endpoint,pid:null,procStart:null,...(thread.path?{nativeSession:{transcriptPath:thread.path}}:{}),...(Object.keys(evidence).length?{codexPermission:evidence}:{})}};
}

export class CodexAdapter extends EventEmitter {
  modelProfile:(peer:Peer)=>import('../../types.js').ModelProfile=()=>({});
  turnOrigin?:CodexTurnOriginProvider;
  private disabledMembers=new Set<string>();
  private disabledThreads=new Set<string>();
  setEnabled(peer:Peer,endpoint:Endpoint|undefined,enabled:boolean,disconnect=false):void{
    const key=endpoint?endpoint.address+'|'+peer.sessionId:undefined;
    if(enabled){this.disabledMembers.delete(peer.id);if(key)this.disabledThreads.delete(key);}
    else{this.disabledMembers.add(peer.id);if(key&&endpoint){this.disabledThreads.add(key);this.nativeStates.delete(key);this.ownedRoutes.revoke(endpoint.address,peer.sessionId??undefined);}if(disconnect&&endpoint)this.clients.get(endpoint.address)?.close();}
  }
  private requireMember(id?:string):void{if(id&&this.disabledMembers.has(id))throw new BridgeError('member_disabled','此成员的 nxbridge 已关闭，未访问原生宿主。');}
  private clients=new Map<string,RpcClient>();
  private ownedRoutes=new CodexOwnedRoutes();
  private coldControls=new WeakMap<()=>void,{rpc:RpcClient;id:string}>();
  private childControls=new WeakMap<()=>void,{rpc:RpcClient;method:CodexChildWorkMethod;id:string}>();
  private verifyingControls=new WeakMap<RpcClient,string>();
  private migrationLeases=new WeakMap<RpcClient,{id:string;current:()=>boolean}>();
  bindOwnedRoutes(source:(memberId:string)=>OwnedCodexRoute|undefined,identity?:(memberId:string)=>string|undefined):void{this.ownedRoutes.bind(source,identity);}
  routeExited(peer:Peer,endpoint:Endpoint,launch?:OwnedCodexLaunchProof):Promise<boolean>{return exitedOwnedCodexRoute(this.home,{peer,endpoint},launch);}
  reconcileOwnedRoute(route:OwnedCodexRoute,prepare:()=>Promise<unknown>):Promise<unknown>|undefined{return this.ownedRoutes.reconcile(route,prepare);}
  private connectionProofs=new WeakMap<RpcClient,AppServerConnectionProof>();
  rpcMetrics(){return[...this.clients.values()].reduce((sum,rpc)=>({connections:sum.connections+1,responses:sum.responses+rpc.metrics.responses,bytes:sum.bytes+rpc.metrics.bytes,parseMs:sum.parseMs+rpc.metrics.parseMs,largestBytes:Math.max(sum.largestBytes,rpc.metrics.largestBytes),timeouts:sum.timeouts+rpc.metrics.timeouts,rejected:sum.rejected+rpc.metrics.rejected}),{connections:0,responses:0,bytes:0,parseMs:0,largestBytes:0,timeouts:0,rejected:0});}
  private connecting=new Map<string,Promise<RpcClient>>();
  private attached=new Set<string>();
  private subscriptionOwners=new Map<string,{peer:Peer;endpoint:Endpoint}>();
  private attaching=new Map<string,Promise<RpcClient>>();
  private threadOperations=new Map<string,Promise<unknown>>();
  private verifying=new Set<string>();
  private invalidated=new Set<string>();
  private starting=new Map<string,Promise<string>>();
  private declaredNatives=new Set<string>();
  private restarting=new Map<string,Promise<CodexRestartResult>>();
  private tokens=new Map<string,string>();
  private configHomes=new Map<string,string>();
  private discovering=new Map<string,Promise<void>>();
  private discoveringLoaded=new Set<Promise<void>>();
  private observing=new Set<Promise<unknown>>();
  private handoffPaused=false;
  private deferredNative?:{bytes:number;observations:Map<symbol|string,{bytes:number;observe:()=>void}>;overflow:()=>void};
  private handoffPrepared:{offer:CustodyOffer;route:OwnedCodexRoute;rpc:CodexNativeRpc}[]=[];
  private discovered=new Map<string,number>();
  private nativeStates=new Map<string,{state:PeerState;revision:number}>();
  private nativeStateRevision=0;
  private verifiedLineage=new Map<string,CodexThread>();
  private brokerIdentity?:Promise<OwnedIdentity>;
  private coldResumeSealed=false;
  private recycleAcks=new Map<string,{rpc:RpcClient;ack:any}>();
                                    
  constructor(private config:BridgeConfig,private home=bridgeHome(),private startupBroker?:OwnedIdentity){super();}
  pauseHandoff(onOverflow:()=>void):{pending:()=>boolean;resume:()=>void}{
    this.handoffPaused=true;
    this.deferredNative={bytes:0,observations:new Map(),overflow:onOverflow};
    return {pending:()=>!!(this.connecting.size||this.attaching.size||this.starting.size||this.restarting.size||this.threadOperations.size||this.discovering.size||this.discoveringLoaded.size||this.observing.size||this.ownedRoutes.pendingWork||
      [...this.clients.values()].some(rpc=>rpc instanceof CodexNativeRpc&&rpc.hasHandoffPending())),resume:()=>{
      const observations=this.deferredNative;
      this.deferredNative=undefined;this.handoffPrepared=[];this.handoffPaused=false;
      if(observations)for(const {observe} of observations.observations.values())observe();
    }};
  }
  private observeAfterHandoff(message:RpcMessage|undefined,observe:()=>void,progressKey?:string):void{
    const deferred=this.deferredNative;
    if(this.handoffPaused&&deferred){
      const key=progressKey??Symbol(),previous=deferred.observations.get(key);
      const bytes=message?Buffer.byteLength(JSON.stringify(message)):0;
      const total=deferred.bytes+bytes-(previous?.bytes??0);
      if((previous||deferred.observations.size<1024)&&total<=4*1024*1024){
                                           
        if(previous)deferred.observations.delete(key);
        deferred.observations.set(key,{bytes,observe});deferred.bytes=total;return;
      }
                                                 
      deferred.overflow();
    }
    if(!this.handoffPaused)observe();
  }
  private handoffRuntime(route:OwnedCodexRoute,offer:CustodyOffer):ManagedRuntime{
    const lease=ownedCodexLease(route),{peer,endpoint}=route;
    const runtime=runtimeFor(this.home,{id:offer.runtimeId})?.value;
    if(!lease||!Object.entries(lease).every(([key,value])=>offer[key as keyof CustodyOffer]===value)||!this.ownedRoutes.current(route)||
      endpoint.processKind==='foreground'||endpoint.nativeSession?.interactive||!['root','fork'].includes(peer.kind)||this.disabledMembers.has(peer.id)||
      !runtime||runtime.kind!=='owned'||!runtime.dedicated||runtime.state!=='ready'||runtime.peerId!==peer.id||runtime.sessionId!==offer.sessionId||
      runtime.endpoint!==offer.address||runtime.pid!==offer.pid||runtime.procStart!==offer.procStart||!runtime.configHome||
      offer.configHome&&!sameDirectory(runtime.configHome,offer.configHome)||endpoint.configHome&&!sameDirectory(runtime.configHome,endpoint.configHome)||
      !localCodexEndpoint(this.home,offer.address))throw handoffUnavailable();
    return runtime;
  }
  async prepareHandoff(routes:readonly OwnedCodexRoute[],signal:AbortSignal):Promise<void>{
    const results=await Promise.all(routes.map(async(route)=>{
      const lease=ownedCodexLease(route),rpc=this.clients.get(route.endpoint.address);
      if(!lease||!(rpc instanceof CodexNativeRpc)||!this.ownedRoutes.permits(route,rpc))return;
      const transport=codexTransports.get(rpc);if(!transport)return;
      const offer:CustodyOffer={...lease,id:randomUUID(),groupId:transport.id};
      let runtime:ManagedRuntime;
      try{runtime=this.handoffRuntime(route,offer);await verifyDedicatedCodex(offer.address,await verifyCodexNativeGeneration(runtime));}
      catch{return;}
      if(signal.aborted||!this.ownedRoutes.permits(route,rpc))return;
      offer.configHome=runtime.configHome;
      try{
        const known=sessionFile(runtime.configHome!,offer.sessionId),file=route.endpoint.nativeSession?.transcriptPath??known;
        if(!file||!known||!sameDirectory(known,file))throw handoffUnavailable();
        const profile=withPersistedCodexPermission(await readSessionProfile(file,offer.sessionId));
        planExactCodexRecycle(profile,route.peer.cwd);
        if(!signal.aborted&&this.ownedRoutes.permits(route,rpc))offer.profile=profile;
      }catch{                                    }
      return{offer,route,rpc};
    }));
    if(!signal.aborted)this.handoffPrepared=results.filter(row=>row!==undefined);
  }
  parkHandoff(_routes:readonly OwnedCodexRoute[]):ParkedCustody{
    const parked=this.handoffPrepared.filter(({route,rpc})=>this.clients.get(route.endpoint.address)===rpc&&this.ownedRoutes.permits(route,rpc));
    const retained=new Set(parked.map(row=>row.rpc as RpcClient));
    this.deferredNative=undefined;this.handoffPrepared=[];this.coldResumeSealed=true;this.turnOrigin=undefined;
    for(const rpc of this.clients.values()){
      if(rpc instanceof CodexNativeRpc){rpc.threadGuard=undefined;rpc.seal();}
      for(const event of ['notification','request','threadAbsent','disconnected'])rpc.removeAllListeners(event);
      if(!retained.has(rpc))rpc.close();
    }
    this.clients.clear();this.attached.clear();this.nativeStates.clear();this.subscriptionOwners.clear();this.ownedRoutes.clear();
    return parkCodexGroups(parked,rpc=>{rpc.close();codexTransports.get(rpc)?.terminate();});
  }
  async acquireHandoff(custody:CustodyAcquisition){
    return acquireCodexGroups(custody,offers=>this.openHandoffGroup(offers,custody.signal));
  }
  private async openHandoffGroup(offers:readonly CustodyOffer[],signal:AbortSignal):Promise<ProvisionalCodexGroup>{
    const first=offers[0]!,address=first.address;
    const route=(offer:CustodyOffer)=>{
      const current=this.ownedRoutes.selected(offer.memberId);if(!current)throw handoffUnavailable();
      return {current,runtime:this.handoffRuntime(current,offer)};
    };
    for(const offer of offers){const {runtime}=route(offer);await verifyDedicatedCodex(address,await verifyCodexNativeGeneration(runtime));}
    signal.throwIfAborted();
    const rpc=await connectAppServer(address,this.config,undefined,this.home,{signal});
    if(!(rpc instanceof CodexNativeRpc)){rpc.close();throw handoffUnavailable();}
    const transport=codexTransports.get(rpc)!;
    const lost=new Set<string>(),acknowledged=new Map<string,{thread:CodexThread;revision:number}>(),states=new Map<string,{state:PeerState;revision:number}>();
    const absent=(id:string)=>lost.add(id);
    const observe=(message:RpcMessage)=>{
      const id=custodyInvalidation(message);if(id){absent(id);return;}
      const p=message.params??{};if(typeof p.threadId!=='string')return;
      let state:PeerState|undefined;
      if(message.method==='thread/status/changed')state=codexThreadState({status:p.status});
      else if(message.method==='turn/started')state='busy';
      else if(message.method==='turn/completed')state=p.turn?.status==='interrupted'?'paused':'ready';
      if(state)states.set(p.threadId,{state,revision:++this.nativeStateRevision});
    };
    rpc.on('notification',observe);rpc.on('threadAbsent',absent);
    const validate=()=>{
      if(signal.aborted||rpc.closed||this.clients.has(address)||offers.some(offer=>lost.has(offer.sessionId)))throw handoffUnavailable();
      for(const offer of offers)route(offer);
    };
    const close=async()=>{transport.terminate();await transport.closed;};
    return{validate,close,acquire:async(offer,verify)=>{
      const {current}=route(offer),profile=offer.profile!;
      if(profile.sessionId!==offer.sessionId)throw handoffUnavailable();
      await verifyRestartProfileCurrent(profile,{exact:true});
      const args=planExactCodexRecycle(profile,current.peer.cwd);
      await verify();
      const revision=this.nativeStateRevision;
      const ack=await rpc.resumeForHandoff(args,{timeout:8000,beforeSend:validate});
      validate();
      await verifyCodexResumeAck(profile,ack,{cwd:current.peer.cwd,guard:validate});
      acknowledged.set(offer.id,{thread:ack.thread,revision});
    },promote:()=>{
      validate();
      const adopted=offers.map(offer=>({offer,...route(offer),ack:acknowledged.get(offer.id)!}));
      if(adopted.some(row=>!row.ack))throw handoffUnavailable();
      transport.detachAbort();rpc.off('notification',observe);rpc.off('threadAbsent',absent);
      this.installPool(address,rpc,adopted[0]!.runtime);this.configHomes.set(address,adopted[0]!.runtime.configHome!);
      for(const {offer,current,ack} of adopted){
        const key=address+'|'+offer.sessionId;
        this.invalidated.delete(key);this.attached.add(key);this.subscriptionOwners.set(key,current);
        this.ownedRoutes.subscribed(current,rpc);observeCodexThreadIdentity(rpc,ack.thread);
        const latest=states.get(offer.sessionId);
        this.nativeStates.set(key,latest&&latest.revision>ack.revision?latest:{state:codexThreadState(ack.thread),revision:++this.nativeStateRevision});
      }
    }};
  }
  async probe(peer:Peer,endpoint?:Endpoint):Promise<RuntimeAvailability>{
    this.requireMember(peer.id);
    return probePeerRuntime(peer,endpoint,this.home,async()=>{
      if(!endpoint)return false;
      const rpc=this.clients.get(endpoint.address);
      if(rpc&&!rpc.closed){
        if(peer.owner==='nxbridge'&&!this.ownedRoutes.permits({peer,endpoint},rpc))return true;
        try{const result=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false},{timeout:750});return typeof result.thread?.id==='string'?result.thread.id===peer.sessionId:undefined;}
        catch(error){return rpc.closed?false:undefined;}
      }
      const token=this.tokens.get(endpoint.address)??managedRuntimeToken(this.home,endpoint.address)??(this.config.codexAuthTokenEnv?process.env[this.config.codexAuthTokenEnv]:undefined);
      return probeAppServer(endpoint.address,token);
    });
  }
  async connect(endpoint:string,bearerToken?:string):Promise<RpcClient>{
    if(/[\\/]app-server-(?:control|daemon)[\\/]/i.test(endpoint))throw new BridgeError('shared_daemon_read_only','共享 daemon 只能通过只读发现连接访问，不能交给专用投递适配器。');
    if(bearerToken)this.tokens.set(endpoint,bearerToken);
    const existing=this.clients.get(endpoint);if(existing&&!existing.closed)return existing;
    const pending=this.connecting.get(endpoint);if(pending)return pending;
    const ready=(async()=>{
      const proof=await this.prepareConnectionProof(endpoint);
      const runtime=runtimeFor(this.home,{endpoint})?.value;
      const rpc=await connectAppServer(endpoint,this.config,this.tokens.get(endpoint),this.home);
      if(proof)try{
        const live=await processIdentity(proof.socket.pid),current=runtimeFor(this.home,{endpoint})?.value;
        if(!rpc.closed&&current?.id===proof.runtimeId&&current.pid===live.pid&&current.procStart===live.startedAt
          &&live.startedAt===proof.socket.generation&&live.image===proof.image&&currentCodexSocketAlias(proof.socket))
          this.connectionProofs.set(rpc,proof);
      }catch{                                          }
      if(rpc.closed)throw new BridgeError('disconnected','原生连接在确认进程代次时断开。');
      this.installPool(endpoint,rpc,runtime);
      if(!this.handoffPaused)try{const value=await rpc.request('config/read',{includeLayers:true},{timeout:3000}),user=value.layers?.find((layer:any)=>layer.name?.type==='user'&&typeof layer.name.file==='string');if(!this.handoffPaused&&this.clients.get(endpoint)===rpc){if(user)this.configHomes.set(endpoint,path.dirname(user.name.file));this.emit('providerRetries',{address:endpoint,budget:providerRetryInfo(value.config)});}}catch{}
      if(rpc.closed)throw new BridgeError('disconnected','和 Codex 的连接在建立时断开了，会自动重试。');
      if(!this.handoffPaused)void this.discoverLoaded(endpoint,rpc).catch(error=>this.emit('diagnostic','还没找到对应的 Codex 会话：'+describeError(error)));
      return rpc;
    })();
    this.connecting.set(endpoint,ready);try{return await ready;}finally{this.connecting.delete(endpoint);}
  }
  private installPool(endpoint:string,rpc:RpcClient,runtime?:ManagedRuntime):void{
      clearRpcFaults(endpoint);this.clients.set(endpoint,rpc);
      this.installThreadGuard(rpc,endpoint,(id,method)=>this.observationPermit(endpoint,id,rpc,undefined,method));
      const samePhysical=()=>!this.clients.has(endpoint)||this.clients.get(endpoint)===rpc;
      const forgetChild=(id:string)=>{
        if(this.handoffPaused||!runtime?.peerId||!runtime.pid||!runtime.procStart||this.clients.get(endpoint)!==rpc)return;
        try{forgetAbsentChildControls(this.home,runtime,value=>value!==id);}
        catch(error){if(!(error instanceof BridgeError)||error.code!=='runtime_changed')this.emit('diagnostic',describeError(error));}
      };
      const observeNotification=(message:RpcMessage)=>{
        const id=message.params?.threadId;
        if(typeof id==='string'&&(['thread/unloaded','thread/closed','thread/archived'].includes(message.method??'')||message.method==='thread/status/changed'&&message.params.status?.type==='notLoaded'))forgetChild(id);
        this.notify(endpoint,message);
      };
      rpc.on('threadAbsent',(id:string)=>{
        if(this.clients.get(endpoint)!==rpc||!id)return;
        const key=endpoint+'|'+id;
        this.ownedRoutes.revoke(endpoint,id,rpc);this.attached.delete(key);this.subscriptionOwners.delete(key);this.invalidated.add(key);
        this.nativeStates.set(key,{state:'offline',revision:++this.nativeStateRevision});
        this.observeAfterHandoff(undefined,()=>{if(samePhysical()){forgetChild(id);this.emit('state',id,'offline',endpoint);}});
      });
      rpc.on('notification',(message:RpcMessage)=>{if(this.clients.get(endpoint)===rpc){
        if(!this.handoffPaused){observeNotification(message);return;}
        const id=custodyInvalidation(message);
        if(id){const key=endpoint+'|'+id;this.ownedRoutes.revoke(endpoint,id,rpc);this.attached.delete(key);this.subscriptionOwners.delete(key);this.invalidated.add(key);}
        const {threadId,turnId}=message.params??{};
        if(typeof threadId==='string'&&typeof turnId==='string'&&/^item\/(?:agentMessage|reasoning|commandExecution)\/.*(?:Delta|delta)$/.test(message.method??'')){
                                                 
          const progress={method:message.method,params:{threadId,turnId}};
          this.observeAfterHandoff(progress,()=>{if(this.clients.get(endpoint)===rpc)this.notify(endpoint,progress);},JSON.stringify([endpoint,threadId,turnId]));
          return;
        }
        this.observeAfterHandoff(message,()=>{if(samePhysical())observeNotification(message);});
      }});
      rpc.on('request',(message:RpcMessage)=>{
        if(this.clients.get(endpoint)!==rpc)return;
                                   
        this.observeAfterHandoff(message,()=>{
          if(!samePhysical())return;
          this.emit('native',{address:endpoint,message});
          if(message.params?.threadId){this.nativeStates.set(endpoint+'|'+message.params.threadId,{state:'needsHuman',revision:++this.nativeStateRevision});this.emit('state',message.params.threadId,'needsHuman',endpoint);}
        });
      });
      rpc.on('disconnected',()=>{
        this.ownedRoutes.revoke(endpoint,undefined,rpc);
        for(const [id,ack] of this.recycleAcks)if(ack.rpc===rpc)this.recycleAcks.delete(id);
        if(this.clients.get(endpoint)!==rpc)return;
        this.clients.delete(endpoint);
        for(const key of this.nativeStates.keys())if(key.startsWith(endpoint+'|'))this.nativeStates.delete(key);
        for(const key of new Set([...this.attached,...this.attaching.keys()]))if(key.startsWith(endpoint+'|')){this.attached.delete(key);this.subscriptionOwners.delete(key);this.invalidated.add(key);}
        this.observeAfterHandoff(undefined,()=>{if(samePhysical())this.emit('disconnected',endpoint);});
      });
  }
  private installThreadGuard(rpc:RpcClient,address:string,leased:(id:string,method:string)=>object|boolean|undefined):void{
    if(!(rpc instanceof CodexNativeRpc))return;
    rpc.threadGuard=(method,id,options)=>{
      const permit=leased(id,method);
      if(permit)return typeof permit==='object'?permit:undefined;
      const cold=options.beforeSend&&this.coldControls.get(options.beforeSend);
      if(method==='thread/resume'&&cold?.rpc===rpc&&cold.id===id)return;
      const child=options.beforeSend&&this.childControls.get(options.beforeSend);
      if(child?.rpc===rpc&&child.method===method&&child.id===id)return;
      if(this.clients.get(address)===rpc&&this.verifyingControls.get(rpc)===id&&method==='thread/settings/update'&&!this.invalidated.has(address+'|'+id))return;
      throw new RuntimeRecoveryHold('owned_subscription_waiting','后台 Codex 还没重新接上，这次没有发给它；消息已保留，接上后自动送达。');
    };
  }
  private async prepareConnectionProof(endpoint:string):Promise<AppServerConnectionProof|undefined>{
    if(process.platform!=='darwin'||!endpoint.startsWith('unix://'))return;
    try{
      const runtime=runtimeFor(this.home,{endpoint})?.value;
      if(runtime?.kind!=='owned'||!runtime.peerId||runtime.state==='failed'||!runtime.pid||!runtime.procStart)return;
      const native=await verifyCodexNativeGeneration(runtime);
      if(!native.image)return;
      await verifyDedicatedCodexArguments(endpoint,native);
      const socket=await proveUnixCodexSocket(endpoint.slice(7),native);
      if(currentCodexSocketAlias(socket))return{runtimeId:runtime.id,image:native.image,socket};
    }catch{                               }
  }
  private async borrowLostConnection(peer:Peer,endpoint:Endpoint,runtime:ManagedRuntime,native:ProcessIdentity):Promise<RpcClient>{
    const unavailable=()=>new BridgeError('codex_socket_idle_unverified','Codex 物理连接文件已丢失，无法从既有可信连接核验原生空闲及归属，保留原会话。');
    const rpc=this.clients.get(endpoint.address),proof=rpc&&this.connectionProofs.get(rpc),identity=endpoint.native??endpoint;
    if(process.platform!=='darwin'||peer.owner!=='nxbridge'||endpoint.daemon||runtime.kind!=='owned'||runtime.peerId!==peer.id
      ||runtime.state!=='ready'||runtime.sessionId&&runtime.sessionId!==peer.sessionId||endpoint.runtimeId!==runtime.id
      ||!rpc||rpc.closed||!proof||proof.runtimeId!==runtime.id||native.pid!==identity.pid||native.startedAt!==identity.procStart
      ||proof.socket.pid!==native.pid||proof.socket.generation!==native.startedAt||proof.image!==native.image
      ||!codexSocketTargetMissing(endpoint.address.slice(7),proof.socket))throw unavailable();
    await verifyDedicatedCodexArguments(endpoint.address,native);
    if(rpc.closed||this.clients.get(endpoint.address)!==rpc||!codexSocketTargetMissing(endpoint.address.slice(7),proof.socket))throw unavailable();
    return rpc;
  }
  discoverLoaded(endpoint:string,rpc=this.clients.get(endpoint)):Promise<void>{
    if(this.handoffPaused||!rpc||rpc.closed)return Promise.resolve();
    const work=this.readLoaded(endpoint,rpc).finally(()=>this.discoveringLoaded.delete(work));
    this.discoveringLoaded.add(work);return work;
  }
  private async readLoaded(endpoint:string,rpc:RpcClient):Promise<void>{
    const loaded=await loadedCodexThreads(rpc);
    if(this.handoffPaused)return;
    const scope=this.ownedNativeScope(runtimeFor(this.home,{endpoint})?.value);
    const serialized:string[]=[],scoped=new Set<string>();
    for(const id of loaded)if(this.canObserveThread(endpoint,id,rpc,scope))serialized.push(id);else if(scope?.isChild(id))scoped.add(id);
    const revision=this.nativeStateRevision;
    const [catalog]=await Promise.all([catalogCodexThreads(rpc,scoped,{independent:true}),Promise.all(serialized.map(id=>this.discoverThread(endpoint,id,rpc)))]);
    if(this.clients.get(endpoint)===rpc)for(const thread of catalog.values())if(thread.status?.type!=='notLoaded')this.record(thread,endpoint,undefined,undefined,revision);
  }
  private discoverThread(endpoint:string,id:string,rpc=this.clients.get(endpoint)):Promise<void>{
    const key=endpoint+'|'+id,pending=this.discovering.get(key);if(pending)return pending;
    if(this.handoffPaused||!rpc||rpc.closed||typeof id!=='string'||this.disabledThreads.has(key)||(this.discovered.get(key)??0)>Date.now())return Promise.resolve();
    const scope=this.ownedNativeScope(runtimeFor(this.home,{endpoint})?.value),serialized=this.canObserveThread(endpoint,id,rpc,scope);
    if(!serialized&&!scope?.isChild(id))return Promise.resolve();
    const revision=this.nativeStateRevision;
    const work=(serialized?rpc.request('thread/read',{threadId:id,includeTurns:false},{timeout:3000}).then(r=>r.thread):catalogCodexThreads(rpc,new Set([id]),{independent:true}).then(catalog=>catalog.get(id)))
      .then(thread=>{if(!this.handoffPaused&&this.clients.get(endpoint)===rpc&&thread?.id===id&&thread.status?.type!=='notLoaded'){this.record(thread,endpoint,undefined,undefined,revision);this.discovered.set(key,Date.now()+1000);}})
      .catch(()=>{this.discovered.set(key,Date.now()+1000);}).finally(()=>{if(this.discovering.get(key)===work)this.discovering.delete(key);});
    this.discovering.set(key,work);if(this.discovered.size>256)this.discovered.delete(this.discovered.keys().next().value!);return work;
  }
  private ownedNativeScope(runtime?:ManagedRuntime){
    if(!runtime?.peerId)return;
    const route=this.ownedRoutes.selected(runtime.peerId);
    const rpc=this.clients.get(runtime.endpoint);
    const root=this.ownedRoutes.sessionId(runtime.peerId)??route?.peer.sessionId??runtime.sessionId
      ??(rpc?this.ownedRoutes.nativeRoot(runtime.endpoint,rpc,runtime):undefined);
                                  
    const isChild=(id:string)=>!!root&&id!==root&&id!==runtime.sessionId&&id!==route?.peer.sessionId;
    return{runtime,route,root,isChild};
  }
  private canObserveThread(address:string,id:string,rpc:RpcClient,scope=this.ownedNativeScope(runtimeFor(this.home,{endpoint:address})?.value)):boolean{
    return !!this.observationPermit(address,id,rpc,scope);
  }
  private observationPermit(address:string,id:string,rpc:RpcClient,scope=this.ownedNativeScope(runtimeFor(this.home,{endpoint:address})?.value),method='thread/read'):object|boolean{
    const creating=this.ownedRoutes.creationIdentity(address,id,rpc,scope?.runtime);
                                        
    if(creating)return creationControls.has(method)?creating:false;
    if(!scope)return true;
    const {runtime,route,root}=scope;
    if(route&&id===root){
      if(route.peer.sessionId!==id||route.endpoint.address!==address||!this.ownedRoutes.nativeHolds(address,id,rpc,runtime))return false;
      return this.ownedRoutes.leaseIdentity(route,rpc)??false;
    }
    return (id===root||scope.isChild(id))&&this.ownedRoutes.nativeHolds(address,id,rpc,runtime);
  }
  private record(thread:CodexThread,endpoint:string,profile?:CodexRuntimeProfile,memberId?:string,readRevision=this.nativeStateRevision):void {
    if(this.verifying.has(endpoint+'|'+thread.id))return;
    if(this.disabledThreads.has(endpoint+'|'+thread.id)||memberId&&this.disabledMembers.has(memberId))return;
    const key=endpoint+'|'+thread.id,nativeState=this.nativeStates.get(key);
    if(thread.status?.type==='notLoaded'&&(!nativeState||nativeState.revision<=readRevision)){this.ownedRoutes.revoke(endpoint,thread.id);this.attached.delete(key);this.subscriptionOwners.delete(key);this.invalidated.add(key);}
    const registration=threadRegistration(thread,endpoint,profile);
                                             
    if(registration&&nativeState&&nativeState.revision>readRevision)registration.state=nativeState.state;
    if(registration){const home=this.configHomes.get(endpoint);if(home)registration.endpoint.configHome=home;const runtime=runtimeFor(this.home,{endpoint})?.value;if(runtime){recordManagedSessionLaunch(this.home,thread.id,thread.cwd,runtime);registration.endpoint.runtimeId=runtime.id;if(runtime.pid&&runtime.procStart){registration.endpoint.pid=runtime.pid;registration.endpoint.procStart=runtime.procStart;}}this.emit('register',registration,memberId);}
  }
  private notify(endpoint:string,message:RpcMessage):void {
    if(this.disabledThreads.has(endpoint+'|'+(message.params?.threadId??message.params?.thread?.id)))return;
    this.emit('native',{address:endpoint,message});
    const p=message.params??{};
    if(message.method==='thread/started'&&p.thread){
      const rpc=this.clients.get(endpoint);if(rpc)observeCodexThreadIdentity(rpc,p.thread);
      this.nativeStates.set(endpoint+'|'+p.thread.id,{state:codexThreadState(p.thread),revision:++this.nativeStateRevision});this.record(p.thread,endpoint);
    }
    if(['thread/closed','thread/archived','thread/unloaded'].includes(message.method??'')&&typeof p.threadId==='string'){
      this.ownedRoutes.revoke(endpoint,p.threadId);
      const key=endpoint+'|'+p.threadId;this.attached.delete(key);this.subscriptionOwners.delete(key);this.invalidated.add(key);
      this.nativeStates.set(key,{state:'offline',revision:++this.nativeStateRevision});this.emit('state',p.threadId,'offline',endpoint);
    }
    if(message.method==='thread/status/changed'){
      if(p.status?.type!=='notLoaded')void this.discoverThread(endpoint,p.threadId);
      if(p.status?.type==='notLoaded'){
        this.ownedRoutes.revoke(endpoint,p.threadId);
        const key=endpoint+'|'+p.threadId;this.attached.delete(key);this.subscriptionOwners.delete(key);this.invalidated.add(key);
      }
      const state=codexThreadState({status:p.status});
      this.nativeStates.set(endpoint+'|'+p.threadId,{state,revision:++this.nativeStateRevision});this.emit('state',p.threadId,state,endpoint);
    }
    if(message.method==='turn/started'){
      this.nativeStates.set(endpoint+'|'+p.threadId,{state:'busy',revision:++this.nativeStateRevision});
      this.emit('turnStarted',p.threadId,endpoint);
                                               
      const revision=this.nativeStateRevision;
      const rpc=this.clients.get(endpoint);
      if(rpc&&this.canObserveThread(endpoint,p.threadId,rpc)){
        const work=rpc.request('thread/read',{threadId:p.threadId,includeTurns:false}).then(r=>{
          if(!this.handoffPaused&&this.clients.get(endpoint)===rpc&&r.thread){this.record(r.thread,endpoint,undefined,undefined,revision);this.emit('profile',p.threadId,{model:r.thread.model,effort:r.thread.reasoningEffort},endpoint);}
        }).catch(()=>{}).finally(()=>this.observing.delete(work));
        this.observing.add(work);
      }
    }
    if(message.method==='turn/completed'&&p.turn?.status==='interrupted'){this.nativeStates.set(endpoint+'|'+p.threadId,{state:'paused',revision:++this.nativeStateRevision});this.emit('interrupted',p.threadId,endpoint,p.turn.id);}
    if(message.method==='item/completed'){
      this.emit('item',p);
      if(typeof p.threadId==='string')for(const receipt of codexContextReceipts(p.item,p.turnId))this.emit('contextObserved',{...receipt,sessionId:p.threadId,address:endpoint});
    }
  }
                                     
  private threadOperation<T>(peer:Peer,endpoint:Endpoint,work:()=>Promise<T>):Promise<T>{
    const key=endpoint.address+'|'+peer.sessionId;
    const next=(this.threadOperations.get(key)??Promise.resolve()).catch(()=>{}).then(work).finally(()=>{if(this.threadOperations.get(key)===next)this.threadOperations.delete(key);});
    this.threadOperations.set(key,next);return next;
  }
  async observe(peer:Peer,endpoint:Endpoint):Promise<void>{
    this.requireMember(peer.id);
    if(endpoint.configHome)this.configHomes.set(endpoint.address,endpoint.configHome);
    const rpc=await this.connect(endpoint.address),revision=this.nativeStateRevision;
    if(peer.kind==='subagent'&&peer.sessionId&&!this.canObserveThread(endpoint.address,peer.sessionId,rpc)){
      const id=peer.sessionId,loaded=await loadedCodexThreads(rpc);
      const thread=loaded.has(id)?(await catalogCodexThreads(rpc,new Set([id]),{independent:true})).get(id):undefined;
      if(this.clients.get(endpoint.address)!==rpc||rpc.closed)return;
      if(thread&&codexSubagent(thread))this.record(thread,endpoint.address,undefined,peer.id,revision);
      else if(!loaded.has(id)&&(this.nativeStates.get(endpoint.address+'|'+id)?.revision??0)<=revision)this.emit('state',id,'offline',endpoint.address);
      return;
    }
    let result:any;
    try{result=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false});}
    catch(error){
      if(peer.kind!=='subagent'||String((error as {code?:unknown}).code)!=='-32600'||describeError(error)!=='thread not loaded: '+peer.sessionId)throw error;
      if(rpc.closed||this.clients.get(endpoint.address)&&this.clients.get(endpoint.address)!==rpc)return;
      this.emit('state',peer.sessionId,'offline',endpoint.address);return;
    }
    this.requireMember(peer.id);
    if(rpc.closed||this.clients.get(endpoint.address)&&this.clients.get(endpoint.address)!==rpc)return;
    if(result.thread?.id!==peer.sessionId)throw new BridgeError('session_changed','观察返回的线程与目标不符。');
    this.record(result.thread,endpoint.address,undefined,peer.id,revision);
  }
  async observeActive(peer:Peer,endpoint:Endpoint):Promise<void>{
    await this.threadOperation(peer,endpoint,async()=>{
      this.requireMember(peer.id);
      const rpc=await this.connect(endpoint.address),revision=this.nativeStateRevision;
      const result=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false});
      if(result.thread?.id!==peer.sessionId)throw new BridgeError('session_changed','运行中观察返回的线程与目标不符。');
      this.record(result.thread,endpoint.address,undefined,peer.id,revision);
      const native=this.nativeStates.get(endpoint.address+'|'+peer.sessionId);
      if(!codexUserWindow(peer)||codexSubagent(result.thread)||result.thread.status?.type!=='active'||native&&native.revision>revision&&!['busy','needsHuman'].includes(native.state))return;
      await this.attachThread(peer,endpoint);
    });
  }
  async restoreRegisteredRoute(peer:Peer,endpoint:Endpoint,attachOptions:()=>{verify:boolean;prepare:()=>void}):Promise<void>{
    if(codexUserWindow(peer))await this.observe(peer,endpoint);
    else await this.attach(peer,endpoint,attachOptions());
  }
  bootstrapProbeKey(peer:Peer,configHome:string|undefined,proof?:Pick<HostLaunchOptions,'executable'|'profileId'>):string{
    return restoreBootstrapKey(selectHostLaunch('codex',this.config,{home:this.home,sessionId:peer.sessionId!,cwd:peer.cwd,configHome,
      ...(proof?.executable?{executable:proof.executable,profileId:proof.profileId}:{})}));
  }
  async attach(peer:Peer,endpoint:Endpoint,options:{verify?:boolean;prepare?:()=>void}={}):Promise<RpcClient>{
    return this.threadOperation(peer,endpoint,()=>this.attachThread(peer,endpoint,options));
  }
  private async attachThread(peer:Peer,endpoint:Endpoint,options:{verify?:boolean;prepare?:()=>void}={}):Promise<RpcClient>{
    this.requireMember(peer.id);
    const key=endpoint.address+'|'+peer.sessionId;
    const pending=this.attaching.get(key);if(pending)return pending;
    const work=(async()=>{
      const runtime=runtimeFor(this.home,{endpoint:endpoint.address})?.value,native=endpoint.native??endpoint;
      const existing=this.clients.get(endpoint.address);
      if(existing)this.ownedRoutes.adoptCreation({peer,endpoint},existing);
      const external=!runtime&&existing&&this.ownedRoutes.permits({peer,endpoint},existing);
      if(peer.owner==='nxbridge'&&!external&&(runtime?.kind!=='owned'||runtime.state!=='ready'||runtime.peerId!==peer.id
        ||runtime.sessionId&&runtime.sessionId!==peer.sessionId||!runtime.pid||!runtime.procStart
        ||endpoint.runtimeId&&endpoint.runtimeId!==runtime.id||native.pid&&native.pid!==runtime.pid||native.procStart&&native.procStart!==runtime.procStart))
        throw new RuntimeRecoveryHold('owned_route_unverified','还没确认这个后台 Codex 就是 nxbridge 启动的那个；消息已保留，确认后自动送达。');
      if(endpoint.configHome)this.configHomes.set(endpoint.address,endpoint.configHome);
      const rpc=await this.connect(endpoint.address);
      this.requireMember(peer.id);
      this.ownedRoutes.adoptCreation({peer,endpoint},rpc);
      const currentRoute=()=>{
        options.prepare?.();
        const selected=this.ownedRoutes.selected(peer.id);
        if(rpc.closed||this.clients.get(endpoint.address)!==rpc||peer.owner==='nxbridge'
          &&(selected?selected.peer.sessionId!==peer.sessionId||selected.endpoint.address!==endpoint.address||selected.endpoint.epoch!==endpoint.epoch
            ||selected.endpoint.bindingEpoch!==endpoint.bindingEpoch||endpoint.runtimeId&&selected.endpoint.runtimeId!==endpoint.runtimeId
            ||(endpoint.native??endpoint).procStart&&(selected.endpoint.native??selected.endpoint).procStart!==(endpoint.native??endpoint).procStart
            :!options.prepare&&!this.ownedRoutes.current({peer,endpoint})))
          throw new RuntimeRecoveryHold('owned_subscription_waiting','和后台 Codex 的连接刚刚变了，正在重新确认；消息已保留。');
      };
      if(peer.owner==='nxbridge')currentRoute();
      if((peer.owner==='nxbridge'?this.ownedRoutes.permits({peer,endpoint},rpc):this.attached.has(key))&&!options.verify){
        if(!['offline','unknown'].includes(peer.state))return rpc;
                                              
        const revision=this.nativeStateRevision,current=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false});
        this.requireMember(peer.id);if(peer.owner==='nxbridge')currentRoute();
        if(current.thread?.id!==peer.sessionId)throw new BridgeError('session_changed','重新接上时 Codex 返回的不是原来的会话，消息没有送出。');
        this.record(current.thread,endpoint.address,undefined,peer.id,revision);
        if(current.thread?.status?.type!=='notLoaded'&&(peer.owner!=='nxbridge'||this.ownedRoutes.permits({peer,endpoint},rpc)))return rpc;
      }
      this.invalidated.delete(key);
      let result:any,subscribed=true,revision=this.nativeStateRevision,verifying=false;
      try{
        let cold=false;
        if(peer.owner==='nxbridge'&&runtime?.kind==='owned'&&runtime.peerId===peer.id){
          if(!this.ownedRoutes.permits({peer,endpoint},rpc)){
                                                   
            try{
              cold=!await codexThreadLoaded(rpc,peer.sessionId!);
            }
            catch(error){throw new RuntimeRecoveryHold('owned_subscription_waiting','暂时无法确认后台 Codex 的会话状态；消息已保留，稍后自动重试。'+describeError(error));}
            if(!cold)throw Object.assign(new RuntimeRecoveryHold('owned_subscription_waiting','后台 Codex 还没重新接上，要等它空闲下来再按原设置恢复；消息已保留，恢复后自动送达。'),{recycleEvidence:'loaded_unleased'});
          }
        }
        if(peer.owner==='nxbridge'&&runtime?.kind==='owned'&&runtime.peerId===peer.id&&(cold||runtime.sessionVerification)){
          verifying=true;
          const id=peer.sessionId!;
          if(runtime.sessionVerification&&runtime.sessionVerification.sessionId!==id)throw new RuntimeRecoveryHold('owned_subscription_waiting','上次恢复的记录和这个 Codex 会话对不上；消息已保留，没有送出。');
          const original=runtime.sessionVerification?.profile??await readSessionProfile(endpoint.nativeSession?.transcriptPath??sessionFile(runtime.configHome??endpoint.configHome??this.configHomes.get(endpoint.address)??'',id),id);
          this.verifying.add(key);
          try{
            const detectedHome=this.configHomes.get(endpoint.address);
            if(runtime.sessionVerification?.restartSourceId&&runtime.configHome&&detectedHome&&!sameDirectory(detectedHome,runtime.configHome)){
              this.emit('sessionVerifying',{memberId:peer.id,sessionId:peer.sessionId,runtimeId:runtime.id,endpoint:endpoint.address});
              throw new BridgeError('original_home_changed','替代 Codex 进程使用了不同的配置目录，未加载原会话。');
            }
            const checkRuntime=()=>{
              currentRoute();this.requireMember(peer.id);
              const record=runtimeFor(this.home,{endpoint:endpoint.address}),current=record?.value;
              if(current?.id!==runtime.id||current.kind!=='owned'||current.state!=='ready'||current.peerId!==peer.id
                ||current.sessionId&&current.sessionId!==id||current.pid!==runtime.pid||current.procStart!==runtime.procStart)
                throw new RuntimeRecoveryHold('owned_subscription_waiting','恢复过程中后台 Codex 进程换了；消息已保留，重新确认后再送。');
              return record!;
            };
            const initialRevision=this.nativeStateRevision;let plan:ColdResumePlan|undefined;
            if(cold)try{
              plan=await this.prepareColdResume(checkRuntime(),id);
              if(runtime.sessionVerification?.restartSourceId)await verifyRestartProfileCurrent(original);
              await this.verifyColdResumeRuntime(runtime);
                                                     
              if(await codexThreadLoaded(rpc,id))throw new BridgeError('session_busy','Codex 会话在核对期间被重新加载了。');
            }
            catch(error){if(error instanceof RuntimeRecoveryHold)throw error;throw new RuntimeRecoveryHold('owned_cold_resume_waiting','没能确认原 Codex 会话已卸载，暂不恢复；消息已保留。'+describeError(error));}
            revision=this.nativeStateRevision;checkRuntime();
            let acknowledged=false;
            const guard=()=>{
              const current=checkRuntime();
              if(!acknowledged||this.invalidated.has(key))throw new RuntimeRecoveryHold('owned_subscription_waiting','恢复后没能重新接上后台 Codex；消息已保留，会自动重试。');
              return current;
            };
            const startVerification=()=>this.emit('sessionVerifying',{memberId:peer.id,sessionId:id,runtimeId:runtime.id,endpoint:endpoint.address});
            if(!cold)startVerification();
            const restored=plan?await this.resumeOwnedCold(rpc,id,{profile:original,file:original.file,home:endpoint.configHome??detectedHome,cwd:peer.cwd,plan,
              beforeResume:()=>{
                const current=checkRuntime(),nativeState=this.nativeStates.get(key);
                if(this.ownedRoutes.permits({peer,endpoint},rpc)||nativeState&&nativeState.revision>initialRevision&&nativeState.state!=='offline')
                  throw new RuntimeRecoveryHold('owned_cold_resume_waiting','恢复时原 Codex 会话已重新加载，暂不恢复；消息已保留。');
                this.invalidated.delete(key);current.value.sessionVerification??={sessionId:id,profile:original};return current;
              },
              onReserved:startVerification,
              onResumed:thread=>{acknowledged=thread.status?.type!=='notLoaded';guard();},guard,
            }):await verifyResumedSession(rpc,id,original,()=>{checkRuntime();this.ownedRoutes.require({peer,endpoint},rpc);},peer.cwd);
            const {value:current}=cold?guard():checkRuntime();
            this.emit('sessionPolicy',{memberId:peer.id,sessionId:peer.sessionId,runtimeId:runtime.id,policy:restored.policy});
            completeRestartProfile(this.home,current);delete current.sessionVerification;saveManagedRuntime(this.home,current);
            result=restored.resumed;
          }catch(error){this.emit('sessionVerificationFailed',{memberId:peer.id,sessionId:peer.sessionId,runtimeId:runtime.id,error:{code:error instanceof BridgeError?error.code:'policy_verification_failed',message:describeError(error),pauseDetail:(error as Partial<CodexServiceTierError>|null)?.pauseDetail}});if(error instanceof RuntimeRecoveryHold)throw error;throw new RuntimeRecoveryHold(error instanceof BridgeError?error.code:'owned_verification_waiting','恢复后的设置还没核对完；消息已保留，核对完再送。'+describeError(error));}finally{this.verifying.delete(key);}
        }else{
          revision=this.nativeStateRevision;
          if(peer.owner==='nxbridge'){
            this.ownedRoutes.require({peer,endpoint},rpc);
            result=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false});
            subscribed=false;
          }else result=await rpc.request('thread/resume',{threadId:peer.sessionId,excludeTurns:true});
        }
      }
      catch(error){
                                                         
                                           
        if(verifying||!(error instanceof Error)||!['no rollout found','list_turns is not supported yet'].some(reason=>error.message.includes(reason)))throw error;
        if(!await codexThreadLoaded(rpc,peer.sessionId!))throw error;
        this.requireMember(peer.id);
        revision=this.nativeStateRevision;result=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false});
        subscribed=false;
      }
      if(peer.owner==='nxbridge')currentRoute();
      if(result.thread?.id!==peer.sessionId)throw new BridgeError('session_changed','重新接上时 Codex 返回的不是原来的会话。');
      const permission=codexPermission(result);if(permission!=='unknown')this.emit('permission',peer.sessionId,permission,endpoint.address);
      if(!this.invalidated.has(key)&&!rpc.closed){
        this.record(result.thread,endpoint.address,result,peer.id,revision);
                                                  
        if(subscribed&&result.thread.status?.type!=='notLoaded'){
          this.attached.add(key);this.subscriptionOwners.set(key,{peer:{...peer},endpoint:{...endpoint}});
          if(peer.owner==='nxbridge'){
            const nativeState=this.nativeStates.get(key);
            this.nativeStates.set(key,{state:nativeState&&nativeState.revision>revision?nativeState.state:codexThreadState(result.thread),revision:++this.nativeStateRevision});
            const runtime=runtimeFor(this.home,{endpoint:endpoint.address})?.value;
            if(runtime?.peerId===peer.id){
              const selected=this.ownedRoutes.selected(peer.id);
              if(selected&&selected.peer.sessionId===peer.sessionId&&selected.endpoint.address===endpoint.address&&selected.endpoint.runtimeId===runtime.id)this.ownedRoutes.subscribed(selected,rpc);
              else this.ownedRoutes.subscribed({peer,endpoint:{...endpoint,runtimeId:runtime.id,pid:runtime.pid??endpoint.pid,procStart:runtime.procStart??endpoint.procStart}},rpc);
            }
          }
        }
      }
      return rpc;
    })();
    this.attaching.set(key,work);try{return await work;}finally{if(this.attaching.get(key)===work)this.attaching.delete(key);}
  }
  private async verifyColdResumeRuntime(runtime:ManagedRuntime):Promise<void>{
    if(!runtime.dedicated||!runtime.pid||!runtime.procStart)throw new BridgeError('runtime_endpoint_unverified','无法确认后台 Codex 的进程只属于 nxbridge。');
    const native=await verifyCodexNativeGeneration(runtime);
    await verifyDedicatedCodex(runtime.endpoint,native);
  }
  private unconfirmedColdResume(runtime:ManagedRuntime,id:string):ManagedRuntime['coldResumeUnconfirmed']{
    const pending=runtime.coldResumeUnconfirmed;
    return pending?.sessionId===id&&pending.pid===runtime.pid&&pending.procStart===runtime.procStart?pending:undefined;
  }
  private coldResumeAttempt(runtime:ManagedRuntime,id:string):1|2{
    const previous=this.unconfirmedColdResume(runtime,id);
    if(!previous)return 1;
    if((previous.attempts??1)===1&&previous.transportClosed)return 2;
    throw this.coldResumeHold();
  }
  private async prepareColdResume(record:RuntimeRecord,id:string):Promise<ColdResumePlan>{
    const broker=this.startupBroker??await(this.brokerIdentity??=processIdentity(process.pid).then(({pid,startedAt})=>({pid,startedAt})).catch(error=>{this.brokerIdentity=undefined;throw error;}));
    const original=record.value,previous=this.unconfirmedColdResume(original,id);
    if(previous&&!previous.transportClosed){
      const owner=previous.broker;
      let exited=false;
      if(owner){
        if(owner.pid!==broker.pid||owner.startedAt!==broker.startedAt)exited=await ownedProcessState(owner)==='dead';
      }else if(this.startupBroker){
                                                   
        const ownership=ownedProcesses(this.home);
        const predecessors=ownership.records.filter(r=>r.role==='broker'&&(r.identity.pid!==broker.pid||r.identity.startedAt!==broker.startedAt));
        exited=!ownership.notes.length&&(await Promise.all(predecessors.map(r=>ownedProcessState(r.identity)))).every(state=>state==='dead');
      }
      if(exited){
        const current=runtimeFor(this.home,{id:original.id,endpoint:original.endpoint}),pending=current&&this.unconfirmedColdResume(current.value,id);
        if(this.coldResumeSealed||!current||!pending||pending.pid!==previous.pid||pending.procStart!==previous.procStart
          ||pending.attempts!==previous.attempts||pending.connectionId!==previous.connectionId||pending.broker?.pid!==owner?.pid||pending.broker?.startedAt!==owner?.startedAt)throw this.coldResumeWait();
        pending.transportClosed=true;writeExistingRuntime(current);record=current;
      }
    }
    return{broker,attempt:this.coldResumeAttempt(record.value,id)};
  }
  private coldResumeHold():RuntimeRecoveryHold{
    return Object.assign(new RuntimeRecoveryHold('owned_subscription_waiting','后台 Codex 的恢复还没能确认成功，原来的进程和消息都保留着。自动恢复的次数已用完，或者旧连接还没确认关闭；要等这个 Codex 进程退出后才能再恢复。一直这样的话，告诉 Claude，由它帮你处理。'),{recycleEvidence:'cold_exhausted'});
  }
  private coldResumeWait():RuntimeRecoveryHold{
    return new RuntimeRecoveryHold('owned_subscription_waiting','还没确认后台 Codex 的这个会话已经卸载，先不恢复；消息已保留，确认后会自动恢复。');
  }
  private async closeColdResumeTransport(rpc:RpcClient,onClosed?:()=>void):Promise<boolean>{
    const closed=codexTransports.get(rpc)?.closed;rpc.close();
    if(!closed)return false;
    let timer:NodeJS.Timeout|undefined;
    const observed=closed.then(()=>{
      if(!this.coldResumeSealed)try{onClosed?.();}catch(error){this.emit('diagnostic','记录原生连接关闭失败：'+describeError(error));}
      return true;
    });
    try{return await Promise.race([observed,new Promise<boolean>(resolve=>{timer=setTimeout(()=>resolve(false),1500);})]);}
    finally{clearTimeout(timer);}
  }
  private async resumeOwnedCold(rpc:RpcClient,id:string,options:Omit<Parameters<typeof resumeSession>[2],'beforeResume'>&{plan:ColdResumePlan;beforeResume:()=>RuntimeRecord;onReserved?:()=>void}):ReturnType<typeof resumeSession>{
    let reserved:{runtime:ManagedRuntime;attempt:1|2;connectionId:string}|undefined;
    const update=(attempt:NonNullable<typeof reserved>,apply:(value:ManagedRuntime)=>void)=>{
      const {runtime}=attempt,record=runtimeFor(this.home,{id:runtime.id,endpoint:runtime.endpoint});
      const pending=record&&this.unconfirmedColdResume(record.value,id);
      if(!record||pending?.attempts!==attempt.attempt||pending.connectionId!==attempt.connectionId||record.value.pid!==runtime.pid||record.value.procStart!==runtime.procStart)
        throw new BridgeError('runtime_changed','核对恢复时后台 Codex 进程换了，暂不恢复。');
      apply(record.value);writeExistingRuntime(record);
    };
    const beforeResume=()=>{
      const record=options.beforeResume(),value=record.value;
      const previous=this.unconfirmedColdResume(value,id),attempt=this.coldResumeAttempt(value,id);
      if(this.coldResumeSealed||attempt!==options.plan.attempt)throw this.coldResumeWait();
      if(!value.pid||!value.procStart)throw new BridgeError('runtime_changed','恢复前没法确认后台 Codex 是不是原来那个进程，暂不恢复。');
      const connectionId=codexTransports.get(rpc)?.id??randomUUID();
      value.coldResumeUnconfirmed={sessionId:id,pid:value.pid,procStart:value.procStart,attempts:attempt,broker:options.plan.broker,connectionId};
      writeExistingRuntime(record);reserved={runtime:value,attempt,connectionId};
      try{options.onReserved?.();}catch(error){
        update(reserved,current=>{if(previous)current.coldResumeUnconfirmed=previous;else delete current.coldResumeUnconfirmed;});
        reserved=undefined;throw error;
      }
    };
    this.coldControls.set(beforeResume,{rpc,id});
    try{
      const restored=await resumeSession(rpc,id,{...options,beforeResume,onResumed:thread=>{
        options.onResumed?.(thread);
        if(thread.status?.type==='notLoaded')throw this.coldResumeWait();
        this.verifyingControls.set(rpc,id);
      }});
      if(reserved)update(reserved,current=>{delete current.coldResumeUnconfirmed;});
      return restored;
    }catch(error){
                                        
      if(reserved){
        const failed=reserved;
        await this.closeColdResumeTransport(rpc,()=>update(failed,current=>{current.coldResumeUnconfirmed!.transportClosed=true;}));
        if(failed.attempt===2)throw this.coldResumeHold();
      }
      throw error;
    }finally{this.coldControls.delete(beforeResume);this.verifyingControls.delete(rpc);}
  }
  async wake(peer:Peer,endpoint:Endpoint,signal?:AbortSignal,beforeSubmit?:()=>Promise<boolean>,message:string|(()=>string)=WAKE_NOTICE,started?:(turnId:string)=>void):Promise<void>{
    let submitted=false;
    await deliveryStep(()=>this.threadOperation(peer,endpoint,async()=>{
      if(signal?.aborted)return;
      let rpc:RpcClient;
      try{rpc=await this.attachThread(peer,endpoint);}catch(error){if(error instanceof RuntimeRecoveryHold||error instanceof BridgeError&&error.code.startsWith('codex_identity_'))throw error;throw new DeliveryError('attach_failed',describeError(error));}
      const cause=typeof message==='function'||message===HEALTH_WAKE_NOTICE?'notice':'partner';
      const origin=await this.turnOrigin?.(peer,cause);
      if(signal?.aborted||beforeSubmit&&!await beforeSubmit()||signal?.aborted)return;
      this.requireMember(peer.id);this.ownedRoutes.require({peer,endpoint},rpc);
      const result=await rpc.request<any>('turn/start',{threadId:peer.sessionId,input:[{type:'text',text:typeof message==='function'?message():message}],...origin},{mutation:true,beforeSend:()=>{submitted=true;}});
      if(typeof result?.turn?.id==='string'){this.emit('turnOriginObserved',peer.id,result.turn.id,cause);started?.(result.turn.id);}
      if(peer.owner!=='nxbridge'&&!this.attached.has(endpoint.address+'|'+peer.sessionId)){
        try{await this.attachThread(peer,endpoint);}catch(error){this.emit('diagnostic','唤醒已提交，后续线程订阅需要重连；正文仍等待正常取件。');}
      }
    }),signal,()=>submitted);
  }
  private async completionNative(endpoint:Endpoint):Promise<void>{
    const address=endpoint.address,{pid,procStart}=endpoint.native??endpoint;
    const runtime=runtimeFor(this.home,{endpoint:address})?.value;
    if(!pid||!procStart||!localCodexEndpoint(this.home,address)||endpoint.runtimeId&&runtime?.id!==endpoint.runtimeId
      ||runtime&&(runtime.state!=='ready'||runtime.pid!==pid||runtime.procStart!==procStart))throw new DeliveryError('wake_route_unverified','父会话的原生后台代次无法核验。');
    const native=await verifyCodexNativeGeneration({pid,procStart});
    await verifyDedicatedCodex(address,native);
  }
                                             
  async wakeSubagents(peer:Peer,endpoint:Endpoint,signal:AbortSignal|undefined,beforeSubmit:()=>Promise<boolean>):Promise<string|undefined>{
    return this.threadOperation(peer,endpoint,async()=>{
      if(signal?.aborted||this.disabledMembers.has(peer.id)||peer.host!=='codex'||!peer.sessionId||!['root','fork'].includes(peer.kind)
        ||endpoint.peerId!==peer.id||!endpoint.active||endpoint.pendingContext||endpoint.mode!=='app-server'||endpoint.daemon)return;
      if(peer.owner==='nxbridge'&&!this.hasSubscription(peer,endpoint))return;
      try{await this.completionNative(endpoint);}catch(error){if(signal?.aborted)return;throw new DeliveryError((error as BridgeError).code??'wake_route_unverified',describeError(error));}
      if(signal?.aborted||this.disabledMembers.has(peer.id))return;
      let rpc:RpcClient;
      try{rpc=await this.connect(endpoint.address);}catch(error){if(signal?.aborted)return;if(error instanceof BridgeError&&error.code.startsWith('codex_identity_'))throw error;throw new DeliveryError('attach_failed',describeError(error));}
      const key=endpoint.address+'|'+peer.sessionId,revision=this.nativeStates.get(key)?.revision;
      const changed=()=>signal?.aborted||this.disabledMembers.has(peer.id)||rpc.closed||this.nativeStates.get(key)?.revision!==revision
        ||peer.owner==='nxbridge'&&(!this.ownedRoutes.permits({peer,endpoint},rpc)||this.clients.get(endpoint.address)!==rpc);
      if(changed())return;
      let thread:CodexThread;
      try{({thread}=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false},{timeout:5000}));}
      catch(error){if(signal?.aborted)return;if((error as BridgeError).code==='-32600'&&describeError(error)==='thread not loaded: '+peer.sessionId){this.emit('state',peer.sessionId,'offline',endpoint.address);return;}throw error;}
      if(thread?.id!==peer.sessionId)throw new DeliveryError('session_changed','唤醒读取返回的线程与父会话不符。');
      if(changed())return;
      if(thread.ephemeral||codexSubagent(thread))throw new DeliveryError('session_changed','唤醒读取不再是已登记的父线程。');
      if(!codexThreadIdle(thread)){this.emit('state',peer.sessionId,codexThreadState(thread),endpoint.address);return;}
      const origin=await this.turnOrigin?.(peer,'children');
      if(!await beforeSubmit()||changed())return;
      let result:any;
      let submitted=false;
      try{result=await deliveryStep(()=>rpc.request('turn/start',{threadId:peer.sessionId,input:[{type:'text',text:HEALTH_WAKE_NOTICE}],...origin},{mutation:true,beforeSend:()=>{submitted=true;}}),signal,()=>submitted);}
      catch(error){if(error instanceof DeliveryError)throw error;throw Object.assign(new DeliveryError('wake_result_unknown','未取得原生唤醒结果。',true),{cause:error});}
      if(typeof result?.turn?.id!=='string'||!result.turn.id)throw new DeliveryError('wake_result_unknown','原生唤醒缺少轮次回执，结果未知。',true);
      return result.turn.id;
    });
  }
  async queueWake(peer:Peer,endpoint:Endpoint,beforeSpawn:()=>void|Promise<void>,onSpawn:()=>void,message=HEALTH_WAKE_NOTICE,completionNotice?:()=>string):Promise<void>{
    return this.threadOperation(peer,endpoint,async()=>{
      this.requireMember(peer.id);
      const origin=await this.turnOrigin?.(peer,'queue');
      await queueCodexWake(this.home,this.config,peer,endpoint,{clientUserMessageId:origin?.clientUserMessageId??randomUUID(),message,completionNotice,
        beforeSubmit:async()=>{await beforeSpawn();this.requireMember(peer.id);},
        ...(endpoint.mode==='app-server'||endpoint.daemon?{requestRemote:async(params:any,authorize:()=>Promise<void>)=>{
          const rpc=await this.connect(endpoint.address);await authorize();if(!endpoint.daemon)this.ownedRoutes.require({peer,endpoint},rpc);
          return rpc.request('thread/queue/add',params,{mutation:true,beforeSend:onSpawn});
        }}:{})});
    });
  }
                                            
  async queueWakeReady(peer:Peer,endpoint:Endpoint):Promise<boolean>{
    const rpc=this.clients.get(endpoint.address),key=endpoint.address+'|'+peer.sessionId,revision=this.nativeStates.get(key)?.revision;
    if(!rpc||rpc.closed||this.disabledMembers.has(peer.id)||!peer.sessionId||peer.owner==='nxbridge'&&endpoint.mode==='app-server'&&!endpoint.daemon&&!this.ownedRoutes.permits({peer,endpoint},rpc))return false;
    try{
      const {thread}=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false},{timeout:5000});
      return !rpc.closed&&!this.disabledMembers.has(peer.id)&&this.nativeStates.get(key)?.revision===revision
        &&(peer.owner!=='nxbridge'||endpoint.daemon||this.ownedRoutes.permits({peer,endpoint},rpc))
        &&thread?.id===peer.sessionId&&!thread.ephemeral&&!codexSubagent(thread)&&codexThreadIdle(thread);
    }catch{return false;}
  }
  async reconcile(peer:Peer,endpoint:Endpoint,options:{since?:number}={}):Promise<CodexContextReceipt[]>{
    this.requireMember(peer.id);
    if(peer.owner==='nxbridge'&&!this.hasSubscription(peer,endpoint)){
      const runtime=runtimeFor(this.home,{endpoint:endpoint.address})?.value;
      if(!localCodexEndpoint(this.home,endpoint.address)||runtime?.kind!=='owned'||runtime.peerId!==peer.id)return [];
      const file=endpoint.nativeSession?.transcriptPath??sessionFile(runtime.configHome??endpoint.configHome??'',peer.sessionId!);
      const history=await readCodexHistory(file,peer.sessionId!,options.since===undefined?{tail:true}:{since:options.since});
      this.emit('historyCompatibility',{sessionId:peer.sessionId,address:endpoint.address,status:history.status,reason:history.reason});
      return history.receipts;
    }
    const rpc=await this.connect(endpoint.address);
    this.requireMember(peer.id);
    const thread=await readCodexThread(rpc,peer.sessionId!);
    const receipts:CodexContextReceipt[]=[];
    if(localCodexEndpoint(this.home,endpoint.address)){const history=await readCodexHistory(thread.path,peer.sessionId!,options.since===undefined?{tail:true}:{since:options.since});this.emit('historyCompatibility',{sessionId:peer.sessionId,address:endpoint.address,status:history.status,reason:history.reason});receipts.push(...history.receipts);}
    else for(const entry of (await readCodexItems(rpc,peer.sessionId!)).items){const item=entry.item??entry;receipts.push(...codexContextReceipts(item,entry.turnId??thread.turns?.at(-1)?.id));}
    return receipts;
  }
  async injectionState(peer:Peer,endpoint:Endpoint,options:{cursor?:HistoryCursor;tail?:boolean;since?:number}={}){
    this.requireMember(peer.id);
    const rpc=codexUserWindow(peer)?await this.connect(endpoint.address):await this.attach(peer,endpoint);this.requireMember(peer.id);
    const thread=await readCodexThread(rpc,peer.sessionId!);
    const history:HistoryEvidence=localCodexEndpoint(this.home,endpoint.address)?await readCodexHistory(thread.path,peer.sessionId!,options):{status:'unavailable',receipts:[],calls:[],reason:'远程端点没有本机持久历史证明'};
    this.emit('historyCompatibility',{sessionId:peer.sessionId,address:endpoint.address,status:history.status,reason:history.reason});
    return{thread,history};
  }
  async inject(peer:Peer,endpoint:Endpoint,callId:string,output:string):Promise<void>{
    return this.threadOperation(peer,endpoint,async()=>{
      const rpc=await this.attachThread(peer,endpoint);
      this.requireMember(peer.id);this.ownedRoutes.require({peer,endpoint},rpc);
      await rpc.request('thread/inject_items',{threadId:peer.sessionId,items:[{type:'function_call',call_id:callId,name:'receive_messages',namespace:'claude',arguments:'{}'},{type:'function_call_output',call_id:callId,name:'receive_messages',namespace:'claude',output}]},{mutation:true});
    });
  }
  async continueInjected(peer:Peer,endpoint:Endpoint,cause:CodexTurnCause='partner'):Promise<string>{
    return this.threadOperation(peer,endpoint,()=>this.continueThread(peer,endpoint,cause));
  }
  private async continueThread(peer:Peer,endpoint:Endpoint,cause:CodexTurnCause='partner'):Promise<string>{
    this.requireMember(peer.id);
    const rpc=await this.attachThread(peer,endpoint);this.requireMember(peer.id);
    const origin=await this.turnOrigin?.(peer,cause);this.requireMember(peer.id);this.ownedRoutes.require({peer,endpoint},rpc);
    const result=await rpc.request('turn/start',{threadId:peer.sessionId,input:[],...origin},{mutation:true});
                                             
    if(peer.owner!=='nxbridge'&&!this.attached.has(endpoint.address+'|'+peer.sessionId))try{await this.attachThread(peer,endpoint);}catch(error){this.emit('diagnostic','已让 Codex 接着做，但接收它进展的连接需要重连：'+describeError(error));}
    this.emit('turnOriginObserved',peer.id,result.turn.id,cause);return result.turn.id;
  }
  async ensureEndpoint(cwd=process.cwd(),peerId?:string,requesterSession?:string,sessionId?:string,configHome?:string,beforeLaunch?:()=>void|Promise<void>,affinity?:Pick<ManagedRuntime,'executable'|'profileId'>):Promise<string>{
    this.requireMember(peerId);
    const key=peerId??'foreground:'+path.resolve(configHome??this.config.codexHome??''),restarting=this.restarting.get(key);if(restarting)return(await restarting).endpoint;
    if(this.config.codexEndpoint){await this.connect(this.config.codexEndpoint);return this.config.codexEndpoint;}
    const pending=this.starting.get(key);if(pending){if(peerId)return pending;await pending;return this.ensureEndpoint(cwd,peerId,requesterSession,sessionId,configHome,beforeLaunch,affinity);}
    const work=this.ensureEndpointInner(cwd,peerId,sessionId,configHome,requesterSession,beforeLaunch,affinity).finally(()=>{this.starting.delete(key);});this.starting.set(key,work);return work;
  }
  async resumeForeground(endpoint:string,configHome:string):Promise<void>{
    this.configHomes.set(endpoint,configHome);await this.connect(endpoint);await this.discoverLoaded(endpoint);
  }
  private async readChildWork(rpc:RpcClient,runtime:ManagedRuntime,peer:Peer,endpoint:Endpoint,method:CodexChildWorkMethod,id:string,reserve:()=>void):Promise<CodexChildWorkPage>{
    const beforeSend=()=>{
      const migration=this.migrationLeases.get(rpc);
      const permitted=migration?migration.id===peer.sessionId&&migration.current():this.clients.get(endpoint.address)===rpc&&this.ownedRoutes.permits({peer,endpoint},rpc);
      if(id===peer.sessionId||rpc.closed||!permitted)
        throw new RuntimeRecoveryHold('owned_subscription_waiting','原托管主线程尚无当前原生订阅，未核验子代理工作。');
      const current=runtimeFor(this.home,{peerId:peer.id,endpoint:endpoint.address})?.value;
      if(current?.id!==runtime.id||current.pid!==runtime.pid||current.procStart!==runtime.procStart)
        throw new BridgeError('runtime_changed','核验子代理前原生进程代次已变化。');
      reserve();
    };
    this.childControls.set(beforeSend,{rpc,method,id});
    try{return await rpc.request(method,{threadId:id,limit:1},{timeout:1500,beforeSend});}
    finally{this.childControls.delete(beforeSend);}
  }
  private async acquireMigrationRoot(rpc:RpcClient,runtime:ManagedRuntime,peer:Peer,endpoint:Endpoint,guard:()=>void):Promise<SessionProfile>{
    const id=peer.sessionId!,{pid,procStart}=runtime;
    const verify=()=>{
      guard();this.requireMember(peer.id);
      const current=runtimeFor(this.home,{endpoint:endpoint.address});
      if(rpc.closed||!current||current.value.id!==runtime.id||current.value.owner!==runtime.owner||current.value.kind!==runtime.kind
        ||current.value.state!=='ready'||current.value.peerId!==peer.id||current.value.sessionId&&current.value.sessionId!==id
        ||current.value.pid!==pid||current.value.procStart!==procStart)throw new BridgeError('runtime_changed','迁移核对期间后台 Codex 进程换了或归属变了，没有执行迁移。');
      return current;
    };
    const hold=()=>this.coldResumeWait();
    const current=verify(),plan=await this.prepareColdResume(current,id);
    const file=endpoint.nativeSession?.transcriptPath??sessionFile(runtime.configHome??endpoint.configHome??'',id);
    const savedProfile=current.value.sessionVerification;
    if(savedProfile&&savedProfile.sessionId!==id)throw hold();
    const profile=savedProfile?.profile??await readSessionProfile(file,id);
    let reloaded=false,acknowledged=false;
    rpc.on('notification',(message:RpcMessage)=>{
      if(message.method==='thread/started'&&message.params?.thread?.id===id||message.params?.threadId===id&&message.method==='thread/status/changed'&&message.params.status?.type!=='notLoaded')reloaded=true;
      if(acknowledged&&message.params?.threadId===id&&(['thread/closed','thread/unloaded','thread/archived'].includes(message.method??'')||message.method==='thread/status/changed'&&message.params.status?.type==='notLoaded'))rpc.close();
    });
    rpc.on('threadAbsent',(absent:string)=>{if(absent===id&&acknowledged)rpc.close();});
    if(await codexThreadLoaded(rpc,id))throw hold();
    const lease=()=>{try{verify();return acknowledged&&!rpc.closed;}catch{return false;}};
    await this.resumeOwnedCold(rpc,id,{profile,file:profile.file,home:runtime.configHome??endpoint.configHome,cwd:peer.cwd,plan,
      beforeResume:()=>{
        const record=verify();if(reloaded||this.hasSubscription(peer,endpoint))throw hold();
        record.value.sessionVerification??={sessionId:id,profile};return record;
      },
      onResumed:thread=>{
        if(thread.status?.type==='notLoaded')throw hold();acknowledged=true;
        this.migrationLeases.set(rpc,{id,current:lease});
      },guard:()=>{if(!lease())throw hold();},
    });
    verify();
    return profile;
  }
  async restart(peer:Peer,endpoint:Endpoint,guard:(stage?:'prepare')=>void,options:CodexRestartOptions={}):Promise<CodexRestartResult>{
    this.requireMember(peer.id);
    if(this.restarting.has(peer.id)||this.starting.has(peer.id))throw new BridgeError('restart_in_progress','目标托管运行时正在启动或重启，请稍后重试。');
    const original=runtimeFor(this.home,{endpoint:endpoint.address})?.value,interrupted=options.verificationRecovery?original:undefined;
    if(options.identityMigration&&(peer.owner!=='nxbridge'||!peer.sessionId||original?.peerId!==peer.id||endpoint.daemon||!localCodexEndpoint(this.home,endpoint.address)))throw new BridgeError('unowned_runtime','无法确认这个后台 Codex 只属于 nxbridge，迁移没有停止它。');
    if(options.verificationRecovery&&(peer.owner!=='nxbridge'||!interrupted?.dedicated||interrupted.peerId!==peer.id||interrupted.sessionVerification?.sessionId!==peer.sessionId))throw new BridgeError('unowned_runtime','缺少待核验原线程的独占运行时证明，未停止任何进程。');
    const automatic=options.beforeStop!==undefined||options.lostSocketRecovery===true;
    if(automatic&&(options.identityMigration||options.verificationRecovery||peer.owner!=='nxbridge'||!peer.sessionId
      ||original?.kind!=='owned'||original.peerId!==peer.id||!original.configHome||original.sessionVerification||endpoint.daemon))
      throw new BridgeError('unowned_runtime','当前会话缺少可核验的独立运行时，未自动关闭。');
    const revision=this.nativeStateRevision;
    const work=(async()=>{
      let prepared:PreparedRuntimeLaunch|undefined;
      if(automatic){
        guard('prepare');
        if(options.lostSocketRecovery){
          if(!original!.pid)throw new BridgeError('codex_socket_idle_unverified','物理连接文件已丢失，缺少原生进程证据，保留原会话。');
          await this.borrowLostConnection(peer,endpoint,original!,await verifyCodexNativeGeneration(original!));
        }
        prepared=await this.prepareRuntimeLaunch(peer.cwd,peer.id,peer.sessionId!,original!.configHome,options.requesterSession,original);
        const control=await configClient(this.config,this.home,prepared.launch);
        try{
          const result=await control.request('hooks/list',{cwds:[peer.cwd]},{timeout:5000});
          if(hookTrustFromList((result.data??[]).flatMap((row:{hooks?:[]})=>row.hooks??[]))==='required')
            throw new BridgeError('trust_required','所选 Codex 的 nxbridge hooks 需要重新信任，未停止原会话。');
        }finally{await control.close();}
        if(process.platform!=='win32')persistentSocketDirectory(this.home,'ps-XXXXXX/native.sock');
        if(!prepared.authenticatedLoopback)await socketDirectory(this.home);
        guard('prepare');
      }
      return restartCodexRuntime({peer,endpoint,home:this.home,connect:address=>this.connect(address),...(interrupted?.sessionVerification?{profile:interrupted.sessionVerification.profile}:{}),verifyDelivery:options.verifyDelivery,onStopped:()=>this.emit('runtimeStopped',endpoint),...(options.identityMigration?{
        inspect:async(address:string)=>{
          const rpc=await connectAppServer(address,this.config,this.tokens.get(address),this.home,{readOnly:true});
          this.installThreadGuard(rpc,address,id=>{const lease=this.migrationLeases.get(rpc);return lease?.id===id&&lease.current()?lease:undefined;});
          return rpc;
        },acquire:(rpc:RpcClient,runtime:ManagedRuntime)=>this.acquireMigrationRoot(rpc,runtime,peer,endpoint,guard),
        observeThread:(thread:CodexThread)=>{const state=threadRegistration(thread,endpoint.address)?.state;if(state)this.emit('state',thread.id,state,endpoint.address);},
      }:{}),
        ...(options.lostSocketRecovery?{borrowLostSocket:(runtime,native)=>this.borrowLostConnection(peer,endpoint,runtime,native)}:{}),
        ...(peer.owner==='nxbridge'&&original?.peerId===peer.id&&(original.kind==='owned'||original.kind==='legacy'&&options.identityMigration)?{childControl:(rpc:RpcClient,method:CodexChildWorkMethod,id:string,beforeSend:()=>void)=>this.readChildWork(rpc,original,peer,endpoint,method,id,beforeSend)}:{}),
        beforeStop:options.beforeStop,start:()=>this.ensureEndpointInner(peer.cwd,peer.id,peer.sessionId??undefined,endpoint.configHome,options.requesterSession,options.beforeLaunch,original,prepared),
        restore:(rpc,id,profile,runtime)=>this.restoreRestartTarget(rpc,id,profile,runtime,peer),
        guard,onVerifying:(runtimeId,address)=>this.emit('sessionVerifying',{memberId:peer.id,sessionId:peer.sessionId,runtimeId,endpoint:address}),onPolicy:(policy,runtimeId)=>this.emit('sessionPolicy',{memberId:peer.id,sessionId:peer.sessionId,runtimeId,policy})});
    })();
    this.restarting.set(peer.id,work);
    try{
      const result=await work;
      try{
        this.record(result.resumed.thread,result.endpoint,result.resumed,peer.id,revision);
        this.attached.add(result.endpoint+'|'+peer.sessionId);
        const rpc=this.clients.get(result.endpoint),runtime=runtimeFor(this.home,{endpoint:result.endpoint})?.value;
        if(rpc&&runtime)this.ownedRoutes.subscribed({peer,endpoint:{...endpoint,address:result.endpoint,epoch:result.endpoint,runtimeId:runtime.id,pid:runtime.pid??null,procStart:runtime.procStart??null,native:undefined}},rpc);
        const permission=codexPermission(result.resumed);if(permission!=='unknown')this.emit('permission',peer.sessionId,permission,result.endpoint);
      }catch(error){throw new BridgeError('restart_incomplete','原 ID 已恢复，但宿主状态登记失败；已暂停目标自动投递。原因：'+describeError(error));}
      return result;
    }catch(error){const runtime=runtimeFor(this.home,{peerId:peer.id})?.value;if(runtime?.sessionVerification)this.emit('sessionVerificationFailed',{memberId:peer.id,sessionId:peer.sessionId,runtimeId:runtime.id,error:{code:error instanceof BridgeError?error.code:'policy_verification_failed',message:describeError(error),pauseDetail:(error as Partial<CodexServiceTierError>|null)?.pauseDetail}});throw error;}finally{this.restarting.delete(peer.id);}
  }
  private async restoreRestartTarget(rpc:RpcClient,id:string,profile:SessionProfile,runtime:ManagedRuntime,peer:Peer):ReturnType<typeof resumeSession>{
    const key=runtime.endpoint+'|'+id,revision=this.nativeStateRevision;
    const custody=()=>{
      this.requireMember(peer.id);
      const current=runtimeFor(this.home,{id:runtime.id,endpoint:runtime.endpoint});
      if(!current||this.clients.get(runtime.endpoint)!==rpc||rpc.closed||current.value.peerId!==peer.id||current.value.pid!==runtime.pid||current.value.procStart!==runtime.procStart)
        throw new BridgeError('runtime_changed','恢复前发现新的后台 Codex 归属变了，暂不恢复。');
      return current;
    };
    const plan=await this.prepareColdResume(custody(),id);
    if(await codexThreadLoaded(rpc,id))throw this.coldResumeWait();
    return this.resumeOwnedCold(rpc,id,{profile,cwd:peer.cwd,home:runtime.configHome,plan,beforeResume:()=>{
      const current=custody(),nativeState=this.nativeStates.get(key);
      if(nativeState&&nativeState.revision>revision&&nativeState.state!=='offline')throw this.coldResumeWait();
      this.invalidated.delete(key);return current;
    },guard:()=>{custody();if(this.invalidated.has(key))throw this.coldResumeWait();}});
  }
  inspectRecycleOld(old:CodexRecycleGeneration){return inspectCodexRecycleOld(this.home,old);}

  finalizeRecycle(peerId:string,sessionId:string,fresh:CodexRecycleGeneration,sourceId:string):void{
    const runtime=runtimeFor(this.home,{id:fresh.runtimeId,endpoint:fresh.endpoint})?.value;
    if(runtime?.kind!=='owned'||runtime.peerId!==peerId||runtime.sessionId!==sessionId||runtime.pid!==fresh.pid||runtime.procStart!==fresh.procStart||runtime.bootId!==fresh.bootId
      ||runtime.sessionVerification?.sessionId!==sessionId||runtime.sessionVerification.restartSourceId!==sourceId)return;
    completeRestartProfile(this.home,runtime);delete runtime.sessionVerification;
    if(runtime.state==='starting')runtime.state='ready';
    saveManagedRuntime(this.home,runtime);
  }
                                                   
  recycleHost(peer:Peer,endpoint:Endpoint,requesterSession?:string):RecycleHostPorts&{admission:(fresh:CodexRecycleGeneration)=>RecycleAdmission;prepareUnsubscribed:(fresh:CodexRecycleGeneration,source:CodexRecycleProfileRef)=>Promise<RegisterInput>;subscribe:(route:OwnedCodexRoute,receipt:RecycleAdmission)=>void}{
    const id=peer.sessionId!,home=this.home;
    let observed:RecycleObservation|undefined,prepared:PreparedRuntimeLaunch|undefined,planned:string|undefined,receipt:RecycleAdmission|undefined;
    const exactLaunch=async(source:NonNullable<CodexRecycleProfileRef['launch']>)=>{
      const candidate=await this.prepareRuntimeLaunch(peer.cwd,peer.id,id,source.configHome,requesterSession,source);
      const {launch}=candidate;
      if(!sameDirectory(launch.configHome,source.configHome)||!sameDirectory(launch.executable.command,source.executable)
        ||launch.method!==source.method||source.profileId!==launch.profile?.id
        ||(source.commandKey?source.commandKey!==recycleCommandKey(launch):launch.executable.prefix.length>0))
        throw new BridgeError('launch_provenance_missing','原后台的安装入口或启动配置已经改变，未回收。');
      const version=launch.version?.match(/\d+\.\d+\.\d+(?:-[\w.-]+)?/)?.[0];
      if(!version||!source.version.includes(version)||!/^0\.(?:16[6-9]|1[7-9]\d|[2-9]\d\d)\./.test(version))
        throw new BridgeError('profile_version_unsupported','原后台版本尚不能核验精确接续协议，未回收。');
      launch.version=version;return candidate;
    };
    const getNew=(fresh:CodexRecycleGeneration)=>{
      const record=runtimeFor(home,{id:fresh.runtimeId,endpoint:fresh.endpoint});
      if(!record||record.value.peerId!==peer.id||record.value.sessionId!==id||record.value.pid!==fresh.pid||record.value.procStart!==fresh.procStart)
        throw new BridgeError('runtime_changed','替代后台的归属或代次已改变。');
      return record.value;
    };
    return {
      observe:async(trigger:CodexRecycleTrigger,hasHeldInput?:()=>boolean)=>{
        const inspection=await collectCodexRecycleEvidence({home,peer,endpoint,trigger,hasHeldInput,
          connect:address=>connectAppServer(address,this.config,this.tokens.get(address),home,{readOnly:true}),
          preflight:async(runtime,_profile,_args,rpc)=>{
            const version=codexServerVersion(rpc);
            if(!version||!runtime.version?.includes(version))throw new BridgeError('profile_version_unsupported','原驻留后台与记录的安装版本不符。');
            prepared=await exactLaunch({configHome:runtime.configHome!,executable:runtime.executable!,method:runtime.launchMethod!,
              version:runtime.version,profileId:runtime.profileId,commandKey:runtime.launchCommandKey});
            if(prepared.launch.version!==version)throw new BridgeError('profile_version_unsupported','原安装入口已换成其他版本。');
          }});
        if(inspection.runtime)observed=inspection;
        return inspection.evidence;
      },
      persistProfile:proof=>{
        if(!observed||observed.evidence.profile.settingsKey!==proof.profile.settingsKey)throw new BridgeError('restart_profile_changed','原设置证据已经改变。');
        persistCodexRecycleProfile(home,observed);
      },
      inspectOld:old=>this.inspectRecycleOld(old),
      stop:(old,deadline,_guard,markSignal,force)=>stopCodexRecycleOld(home,old,deadline,markSignal,force),
      retireOld:async(old,guard,work)=>{
        guard();
        this.clients.get(old.endpoint)?.close();
        await retireCodexRecycleOld(home,old,guard,work);
      },
      prepareLaunch:async source=>{
        if(!source.launch)throw new BridgeError('launch_provenance_missing','原启动记录不完整，未启动替代后台。');
        prepared=await exactLaunch(source.launch);
        planned=randomUUID();return {runtimeId:planned};
      },
      launch:async(intent,guard,source)=>{
        guard();
        if(!prepared||planned!==intent.runtimeId){
          if(!source.launch)throw new BridgeError('launch_provenance_missing','原启动记录不完整，未启动替代后台。');
          prepared=await exactLaunch(source.launch);guard();
        }
        prepared.launch.env.NXBRIDGE_OWNED_LAUNCH='codex:'+intent.runtimeId;
        await this.ensureEndpointInner(peer.cwd,peer.id,id,prepared.launch.configHome,requesterSession,guard,undefined,prepared,guard);
      },
      inspectLaunch:async intent=>{
        const record=runtimeFor(home,{id:intent.runtimeId});
        if(record?.value.state==='starting'&&record.value.peerId===peer.id&&record.value.sessionId===id&&record.value.sessionVerification?.sessionId===id){
          const original=record.value;
          const candidate=ownedProcesses(home).records.filter(owner=>owner.proof.launchId===intent.runtimeId&&owner.peerId===peer.id&&owner.role==='codex-runtime'&&!owner.releasedToUser);
          if(candidate.length===1){
            const native=await this.declareRuntimeNative(original,candidate[0]).catch(()=>undefined);
            if(native){await verifyDedicatedCodex(original.endpoint,native);original.dedicated=true;saveManagedRuntime(home,original);}
          }
        }
        return inspectCodexRecycleLaunch(home,peer,intent);
      },
      verify:async(fresh,source,options)=>{
        const runtime=getNew(fresh),profile=runtime.sessionVerification?.profile;
        if(!profile||runtime.sessionVerification?.restartSourceId!==source.sourceId)throw new BridgeError('restart_profile_unverified','未取得替代后台的原设置记录。');
        const key=fresh.endpoint+'|'+id;
        this.verifying.add(key);
        let rpc:RpcClient|undefined,verified=false;
        try{
          let ack:any;
          const prior=this.recycleAcks.get(fresh.runtimeId);
          if(options.resumeSubmitted){
            if(!prior||prior.rpc.closed||this.clients.get(fresh.endpoint)!==prior.rpc)
              throw new BridgeError('codex_recycle_ack_missing','无法证明此前恢复回执仍持有当前订阅，已保留替代后台。');
            ({rpc,ack}=prior);
          }else{
            rpc=await this.connect(fresh.endpoint);options.guard();
            if(await codexThreadLoaded(rpc,id))throw new BridgeError('codex_recycle_verify_unknown','替代后台意外加载原会话，未重复提交恢复。');
            const args=planExactCodexRecycle(profile,peer.cwd),connection=rpc;
            const beforeSend=()=>{options.guard();getNew(fresh);this.invalidated.delete(key);options.markSubmitted();};
            this.coldControls.set(beforeSend,{rpc,id});
            ack=await rpc.request('thread/resume',args,{mutation:true,timeout:30000,beforeSend});
            this.recycleAcks.set(fresh.runtimeId,{rpc:connection,ack});
          }
          const guard=()=>{
            options.guard();getNew(fresh);
            if(rpc!.closed||this.clients.get(fresh.endpoint)!==rpc||this.invalidated.has(key))
              throw new BridgeError('runtime_changed','替代后台的首次恢复连接已经失效。');
          };
          guard();this.verifyingControls.set(rpc,id);
          try{await materializeCodexResumeSettings(rpc,profile,ack,peer.cwd,guard);}
          finally{this.verifyingControls.delete(rpc);}
          const {policy}=await verifyCodexResumeAck(profile,ack,{cwd:peer.cwd,guard:options.guard,requireFreshSettings:true});
          const input=threadRegistration(ack.thread,fresh.endpoint,ack);
          if(!input)throw new BridgeError('session_changed','未核验替代后台中的原根会话。');
          const history=options.uncertainSince===undefined?undefined:await catchUpCodexHistory(profile.file,id,options.uncertainSince);
          options.guard();getNew(fresh);
          input.endpoint={...input.endpoint,configHome:runtime.configHome,runtimeId:runtime.id,pid:fresh.pid,procStart:fresh.procStart};
          receipt={input,rpc,runtime,policy,receipts:history?.receipts??[]};verified=true;
        }finally{
          this.verifying.delete(key);
          if(!verified&&rpc&&!this.recycleAcks.has(fresh.runtimeId))await this.closeColdResumeTransport(rpc);
        }
      },
      admission:fresh=>{
        if(!receipt||receipt.rpc.closed||receipt.runtime.id!==fresh.runtimeId||this.clients.get(fresh.endpoint)!==receipt.rpc)
          throw new BridgeError('codex_recycle_verify_unknown','原生已核验连接在接入前关闭，恢复继续保留。');
        getNew(fresh);return receipt;
      },
      prepareUnsubscribed:async(fresh,source)=>{
        const runtime=getNew(fresh),launch=source.launch,proof=runtime.sessionVerification;
        const observed=await inspectCodexRecycleLaunch(home,peer,{runtimeId:fresh.runtimeId});
        if(observed.kind!=='matched'||observed.fresh.pid!==fresh.pid||observed.fresh.procStart!==fresh.procStart||observed.fresh.endpoint!==fresh.endpoint
          ||!launch||!proof||proof.sessionId!==id||proof.restartSourceId!==source.sourceId||!runtime.configHome||!sameDirectory(runtime.configHome,launch.configHome)
          ||!runtime.executable||!sameDirectory(runtime.executable,launch.executable)||runtime.launchMethod!==launch.method||runtime.launchCommandKey!==launch.commandKey
          ||runtime.profileId!==launch.profileId||!runtime.version?.includes(launch.version))
          throw new BridgeError('codex_recycle_verify_unknown','无法核验替代后台的原生归属或原启动设置，已保留进程。');
        await verifyRestartProfileCurrent(proof.profile,{exact:true});getNew(fresh);
        return {host:'codex',sessionId:id,cwd:peer.cwd,name:peer.name,kind:peer.kind,state:'offline',endpoint:{mode:'app-server',
          address:fresh.endpoint,epoch:fresh.runtimeId,pid:fresh.pid,procStart:fresh.procStart,configHome:runtime.configHome,runtimeId:fresh.runtimeId}};
      },
      subscribe:(route,admitted)=>{
        this.ownedRoutes.subscribed(route,admitted.rpc);
        this.recycleAcks.delete(admitted.runtime.id);
        const key=route.endpoint.address+'|'+id;this.attached.add(key);this.invalidated.delete(key);
      },
    };
  }
  private async declareRuntimeNative(saved:ManagedRuntime,wrapper?:OwnedProcessRecord){
    if(!wrapper){
      const records=ownedProcesses(this.home).records.filter(value=>value.proof.launchId===saved.id||saved.kind==='legacy'&&value.proof.launchId===saved.endpoint);
      const containers=records.filter(value=>value.windowsJob||value.posixGroup);
      if(containers.length>1)throw new BridgeError('process_launch_unverified','同次启动有多个容器，未声明原生宿主。');
      wrapper=containers[0]??records.find(value=>value.identity.pid===saved.wrapperPid&&value.identity.startedAt===saved.wrapperProcStart);
    }
    const expected=saved.pid&&saved.procStart?{pid:saved.pid,startedAt:saved.procStart}:wrapper?.native;
    const native=expected?await verifyCodexNativeGeneration({pid:expected.pid,procStart:expected.startedAt}):wrapper?await nativeDescendant(wrapper.identity.pid,'codex',saved.createdAt,saved.endpoint,wrapper):undefined;
    if(!native)return;
    const key=saved.id+':'+native.pid+':'+native.startedAt;
    if(wrapper&&!this.declaredNatives.has(key)){await declareOwnedNative(wrapper,native);this.declaredNatives.add(key);}
    if(saved.pid!==native.pid||saved.procStart!==native.startedAt){saved.pid=native.pid;saved.procStart=native.startedAt;if(saved.kind==='owned')saveManagedRuntime(this.home,saved);}
    return native;
  }
  private async prepareRuntimeLaunch(cwd:string,peerId?:string,sessionId?:string,configHome?:string,requesterSession?:string,affinity?:Pick<ManagedRuntime,'executable'|'profileId'>):Promise<PreparedRuntimeLaunch>{
    const launch=await prepareOwnedLaunch(await selectSessionLaunch('codex',this.config,{home:this.home,cwd,sessionId,configHome,requesterSession,executable:affinity?.executable,profileId:affinity?.profileId}));
    if(peerId&&sessionId&&restoreState({host:'codex',sessionId,owner:'nxbridge'} as Peer,launch.configHome)==='gone')throw new BridgeError('session_not_found','原配置目录中没有该 Codex 会话，请先恢复原会话文件或在原生窗口恢复。');
    const capabilities=await requireDedicatedCodex(launch),authenticatedLoopback=['linux','win32'].includes(process.platform)&&capabilities.authenticatedWebsocket;
    if(peerId&&sessionId)await requireRestoreBootstrap(launch);
    return{launch,authenticatedLoopback};
  }
  private async ensureEndpointInner(cwd=process.cwd(),peerId?:string,sessionId?:string,configHome?:string,requesterSession?:string,beforeLaunch?:()=>void|Promise<void>,affinity?:Pick<ManagedRuntime,'executable'|'profileId'>,prepared?:PreparedRuntimeLaunch,recycleGuard?:()=>void):Promise<string>{
    this.requireMember(peerId);
    let records=managedRuntimes(this.home).filter(r=>peerId?r.value.peerId===peerId:!r.value.peerId&&(!configHome||r.value.configHome&&path.resolve(r.value.configHome)===path.resolve(configHome)));
                                        
    for(const record of records)if(await managedRuntimeNativeState(this.home,record.value)==='dead'&&record.value.state!=='failed'){
      this.clients.get(record.value.endpoint)?.close();record.value.state='failed';if(record.value.kind==='owned')saveManagedRuntime(this.home,record.value);
    }
    if(!peerId){
      for(const record of records.filter(r=>r.value.state==='failed'&&r.value.threadUnloadDelaySecs!==0))if(await managedRuntimeNativeState(this.home,record.value)!=='dead')throw new BridgeError('runtime_launch_unconfirmed','旧后台的启动尚未核验结束，保留启动预约，不再创建第二个进程。');
                                                    
      const loaded:RuntimeRecord[]=[];
      if(sessionId&&/^[a-f0-9-]{36}$/i.test(sessionId))for(const record of records.filter(r=>r.value.state!=='failed'&&r.value.threadUnloadDelaySecs!==0)){
        if(await managedRuntimeNativeState(this.home,record.value)==='dead')continue;
        try{
          await this.declareRuntimeNative(record.value);
          const rpc=await this.connect(record.value.endpoint);
          if(await codexThreadLoaded(rpc,sessionId))loaded.push(record);
        }catch(error){this.emit('diagnostic','旧后台的会话归属尚未确认，新窗口改用当前后台：'+describeError(error));}
      }
      records=loaded.length?loaded:records.filter(r=>r.value.threadUnloadDelaySecs===0);
    }
    const usable=records.filter(r=>r.value.state!=='failed');
    if(usable.length>1)throw new BridgeError('runtime_ambiguous','成员有多个尚未核验结束的运行时，未重复启动。');
    const previous=usable[0]??records.sort((a,b)=>b.value.createdAt-a.value.createdAt)[0];
    if(previous){
      const saved=previous.value;
      if(saved.failureCode==='bootstrap_required')requireBackgroundRetry(this.home);
      if(saved.configHome)this.configHomes.set(saved.endpoint,saved.configHome);
      const state=await managedRuntimeNativeState(this.home,saved);
      if(state==='dead'){
        this.clients.get(saved.endpoint)?.close();
        if(saved.state!=='failed'){saved.state='failed';if(saved.kind==='owned')saveManagedRuntime(this.home,saved);}
        if(await managedRuntimeState(this.home,saved)==='dead'){await removeOwnedSocket(this.home,saved.endpoint);removeManagedRuntime(previous);}
      }
      else{
        if(prepared)throw new BridgeError('runtime_ambiguous','原会话已有另一个存活或尚未核验的运行时，未重复恢复。');
        const failed=saved.state==='failed';
        try{
                                              
          const native=await this.declareRuntimeNative(saved);
          if(failed&&saved.authenticatedLoopback){
            if(!native)throw new BridgeError('runtime_endpoint_unverified','原后台尚无可核验的原生代次，未恢复凭证。');
            await verifyDedicatedCodex(saved.endpoint,native);this.requireMember(peerId);
            saved.state='starting';saved.dedicated=true;saveManagedRuntime(this.home,saved);
          }
          await this.connect(saved.endpoint);
          if(saved.state!=='ready'){saved.state='ready';delete saved.error;delete saved.failureCode;if(saved.kind==='owned')saveManagedRuntime(this.home,saved);}
          return saved.endpoint;
        }catch(error){
          const containers=ownedProcesses(this.home).records.filter(record=>(record.windowsJob||record.posixGroup)&&(record.proof.launchId===saved.id||saved.kind==='legacy'&&record.proof.launchId===saved.endpoint));
          const undeclared=containers.length===1?containers[0]:undefined;
          if(!saved.pid&&!saved.procStart&&undeclared?.posixGroup&&!undeclared.native&&undeclared.parent&&await ownedProcessState(undeclared.parent)==='dead'){
                                                    
            await releaseOwnedProcess(undeclared);
            removeManagedRuntime(previous);await removeOwnedSocket(this.home,saved.endpoint);
          }else{
            if(failed&&saved.state==='starting'){saved.state='failed';saved.error=describeError(error);saveManagedRuntime(this.home,saved);}
            throw new BridgeError('runtime_launch_unconfirmed',`原运行时${state==='alive'?'仍存活但连接失败':'进程代次尚无法核验且连接失败'}，保留启动预约，不再创建第二个进程。原因：${describeError(error)}`);
          }
        }
      }
    }
    const {launch,authenticatedLoopback}=prepared??await this.prepareRuntimeLaunch(cwd,peerId,sessionId,configHome??previous?.value.configHome,requesterSession,{executable:affinity?.executable??previous?.value.executable,profileId:affinity?.profileId??previous?.value.profileId});
    this.requireMember(peerId);
    for(const note of launch.notes)this.emit('diagnostic',note);
                                                      
    const runtimeId=launch.env.NXBRIDGE_OWNED_LAUNCH!.slice('codex:'.length),directory=await socketDirectory(this.home),endpoint=authenticatedLoopback?await new Promise<string>((resolve,reject)=>{const server=net.createServer();server.once('error',reject);server.listen(0,'127.0.0.1',()=>{const port=(server.address() as net.AddressInfo).port;server.close(error=>error?reject(error):resolve('ws://127.0.0.1:'+port));});}):'unix://'+path.join(directory,'cx-'+createHash('sha256').update(runtimeId).digest('hex').slice(0,16)+'.sock');
    const sessionVerification=peerId&&sessionId?await pendingRestartVerification(this.home,peerId,sessionId,launch.configHome):undefined;
    await beforeLaunch?.();
    const saved:ManagedRuntime={id:runtimeId,...(peerId?{peerId}:{}),owner:managedMarker(this.home),kind:'owned',state:'starting',endpoint,configHome:launch.configHome,launchMethod:launch.method,createdAt:Date.now(),bootstrapRevision:backgroundState(this.home)?.revision,executable:launch.executable.command,launchCommandKey:recycleCommandKey(launch),version:launch.version??launch.profile?.version,profileId:launch.profile?.id,sessionId,threadUnloadDelaySecs:0,...(authenticatedLoopback?{authenticatedLoopback:true}:{}),...(sessionVerification?{sessionVerification}:{})};
    saveManagedRuntime(this.home,saved);
    const logfile=path.join(this.home,'run','codex-'+runtimeId+'.log'),retention={...DEFAULT_RETENTION,...this.config.retention};
    let failure:unknown,spawnAttempted=false,wrapper:OwnedProcessRecord|undefined,child:ReturnType<typeof spawnSelectedHost>|undefined;
    try{
    const authentication=authenticatedLoopback?['--ws-auth','capability-token','--ws-token-file',createRuntimeToken(this.home,saved)]:[];
    const bootstrap=peerId&&sessionId?CODEX_RESTORE_BOOTSTRAP:[];
                                         
    const nativeArgs=['--no-daemon',...bootstrap,'-c','check_for_update_on_startup=false','-c','thread_unload_delay_secs=0','app-server','--listen',endpoint,...authentication];
    rotateLog(logfile,retention.logMaxBytes,retention.logBackups);
    await prepareProcessRegistry(this.home);
    this.requireMember(peerId);
    recycleGuard?.();
    launch.env.RUST_LOG??='error';
    if(process.platform==='win32'){
      spawnAttempted=true;
      wrapper=await launchIndependentWindows({home:this.home,executable:launch.executable.command,args:[...launch.executable.prefix,...nativeArgs],cwd:launch.cwd,env:launch.env,logfile,role:'codex-runtime',launchId:runtimeId,configHome:launch.configHome,...(peerId?{peerId}:{})});
    }else{
      const fd=openSync(logfile,'a',0o600);
      try{child=spawnPosixSupervisor(launch.executable.command,[...launch.executable.prefix,...nativeArgs],{cwd:launch.cwd,env:launch.env,stdio:['ignore',fd,fd],waitForNative:true,home:this.home});spawnAttempted=true;}
      finally{closeSync(fd);}
      child.on('error',error=>{failure=error;});child.unref();
      wrapper=await rememberOwnedChild(this.home,child,'codex-runtime',{configHome:launch.configHome,posixGroup:true,...(peerId?{peerId}:{})},runtimeId);
    }
    if(wrapper){saved.wrapperPid=wrapper.identity.pid;saved.wrapperProcStart=wrapper.identity.startedAt;saveManagedRuntime(this.home,saved);}
    if(!wrapper)throw new BridgeError('runtime_launch_unconfirmed','未核验启动包装器身份，保留预约。');
    for(let i=0;i<60;i++){
      if(failure)throw failure;await delay(200);
      try{
                                         
        let identity=authenticatedLoopback||process.platform==='darwin'&&peerId?await this.declareRuntimeNative(saved,wrapper):undefined;
        const connected=await this.connect(endpoint);identity??=await this.declareRuntimeNative(saved,wrapper);
        if(!identity)throw new BridgeError('launch_owner_unverified','未确认本次启动的原生宿主。');
        let dedicated=false;try{await verifyDedicatedCodex(endpoint,identity);dedicated=true;}catch(error){this.emit('diagnostic','后台已连接，但独占性尚无法核验；仅允许非破坏性恢复：'+describeError(error));}
        const detectedHome=this.configHomes.get(endpoint);
        if(sessionVerification?.restartSourceId&&detectedHome&&!sameDirectory(detectedHome,launch.configHome))
          throw new BridgeError('original_home_changed','替代 Codex 进程使用了不同的配置目录，未加载原会话。');
        launch.configHome=detectedHome??launch.configHome;this.configHomes.set(endpoint,launch.configHome);
        Object.assign(saved,{state:'ready',pid:identity.pid,procStart:identity.startedAt,configHome:launch.configHome,dedicated,version:saved.version??codexServerVersion(connected)});saveManagedRuntime(this.home,saved);recordHostLaunch(this.home,'codex-app-server',launch,identity.pid);return endpoint;
      }catch(error){
        if(error instanceof BridgeError&&['process_launch_unverified','shared_backend','original_home_changed'].includes(error.code))throw error;
        const nativeExit=child?posixSupervisorExit(child):undefined;
        failure=child&&(child.exitCode!==null||child.signalCode!==null||nativeExit&&(nativeExit.code!==0||nativeExit.signal!==null))?error:undefined;
      }
    }
    throw new BridgeError('app_server_start_failed','App Server 启动超时，检查 '+logfile+'。');
    }catch(error){
      this.clients.get(endpoint)?.close();
      const records=ownedProcesses(this.home).records.filter(value=>value.proof.launchId===runtimeId),cleanupErrors:string[]=[];
      wrapper??=records.find(value=>value.role==='codex-runtime');
      if(wrapper&&!saved.wrapperPid){saved.wrapperPid=wrapper.identity.pid;saved.wrapperProcStart=wrapper.identity.startedAt;}
                                               
      for(const record of records.sort((a,b)=>Number(!!(b.windowsJob||b.posixGroup))-Number(!!(a.windowsJob||a.posixGroup))||b.createdAt-a.createdAt))try{await releaseOwnedProcess(record);}catch(cleanup){const note=describeError(cleanup);cleanupErrors.push(note);this.emit('diagnostic','App Server 启动失败后的回收未完成：'+note);}
      let childStopped=false;
      if(child&&!wrapper)try{
        if(child.exitCode===null&&child.signalCode===null)child.kill();
        const deadline=Date.now()+5000;while(child.exitCode===null&&child.signalCode===null&&Date.now()<deadline)await delay(25);
        childStopped=child.exitCode!==null||child.signalCode!==null;
        if(!childStopped)throw new BridgeError('process_stop_pending','本次创建的启动监督进程尚未退出。');
      }catch(cleanup){cleanupErrors.push(describeError(cleanup));}
      saved.state='failed';saved.error=describeError(error)+(cleanupErrors.length?'；启动清理未完成：'+cleanupErrors.join('；'):'');saved.failureCode=error instanceof BridgeError?error.code:undefined;
      saved.launchSettled=cleanupErrors.length===0&&(!spawnAttempted||records.length>0||childStopped||error instanceof BridgeError&&error.code==='bootstrap_required');saveManagedRuntime(this.home,saved);throw error;
    }
  }
  async create(peer:Peer,requesterSession?:string,beforeLaunch?:()=>void|Promise<void>,sourcePermission:PermissionClass='unknown',beforeCreate?:()=>void|Promise<void>):Promise<RegisterInput>{
    const endpoint=await this.ensureEndpoint(peer.cwd,peer.id,requesterSession,undefined,undefined,beforeLaunch),rpc=await this.connect(endpoint);
    const runtime=runtimeFor(this.home,{endpoint})?.value;
    const result=await startOwnedCodex(rpc,peer.cwd,sourcePermission,()=>{this.requireMember(peer.id);return beforeLaunch?.();},beforeCreate,this.modelProfile(peer),id=>{
      if(!this.invalidated.has(endpoint+'|'+id)){
        this.ownedRoutes.created({peer:{...peer,sessionId:id},endpoint:{address:endpoint,epoch:endpoint,peerId:peer.id,mode:'app-server',
          pid:runtime?.pid??null,procStart:runtime?.procStart??null,...(runtime?{runtimeId:runtime.id}:{}),active:true,seenAt:Date.now()}},rpc);
      }
    }),thread=result.thread;
    try{
      await rpc.request('thread/name/set',{threadId:thread.id,name:peer.name},{mutation:true});
      if(runtime?.peerId===peer.id){
                                                      
        const saved=runtimeFor(this.home,{id:runtime.id,endpoint});
        if(!saved||saved.value.peerId!==peer.id||saved.value.pid!==runtime.pid||saved.value.procStart!==runtime.procStart
          ||saved.value.sessionId&&saved.value.sessionId!==thread.id)throw new BridgeError('runtime_changed','新后台创建后运行时归属变了，未登记会话。');
        saved.value.sessionId=thread.id;writeExistingRuntime(saved);
      }
    }
    catch(error){await unloadFailedOwnedCodex(rpc,thread.id,error);throw error;}
    const key=endpoint+'|'+thread.id,current=!rpc.closed&&this.clients.get(endpoint)===rpc&&!this.invalidated.has(key);
    if(current)this.attached.add(key);
    const registration={...threadRegistration({...thread,name:peer.name},endpoint,result)!,permission:codexPermission(result)};registration.endpoint.configHome=this.configHomes.get(endpoint);if(runtime){recordManagedSessionLaunch(this.home,thread.id,thread.cwd,runtime);registration.endpoint.runtimeId=runtime.id;registration.endpoint.pid=runtime.pid??null;registration.endpoint.procStart=runtime.procStart??null;
    }
    if(current){
      this.ownedRoutes.creationReady({peer:{...peer,sessionId:thread.id},endpoint:{...registration.endpoint,peerId:peer.id,active:true,seenAt:Date.now()}},rpc);
      const selected=this.ownedRoutes.selected(peer.id);if(selected&&selected.peer.sessionId===thread.id)this.ownedRoutes.adoptCreation(selected,rpc);
    }
    return registration;
  }
                                       
  async setProfile(peer:Peer,endpoint:Endpoint,profile:import('../../types.js').ModelProfile,guard:()=>void):Promise<EffectiveModelProfile>{
    let submitted=false,accepted=false;
    return this.threadOperation(peer,endpoint,async()=>{
      guard();this.requireMember(peer.id);
      const runtime=runtimeFor(this.home,{endpoint:endpoint.address})?.value;
      if(peer.owner!=='nxbridge'||endpoint.daemon||runtime?.kind!=='owned'||runtime.peerId!==peer.id)throw new BridgeError('process_owned_by_user','模型切换只支持 nxbridge 自有后台运行时。');
      const rpc=await this.attachThread(peer,endpoint),current=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false});guard();
      if(current.thread?.id!==peer.sessionId)throw new BridgeError('session_changed','模型切换前线程身份已改变。');
      if(!codexThreadIdle(current.thread))throw new BridgeError('session_busy','搭档仍在工作，模型选择将在安全空闲点应用。');
      const {serviceTier,...settings}=profile;
      const withTier=serviceTier!==undefined||peer.serviceTier!==undefined;
      const offset=withTier?(await readSessionProfile(current.thread.path??endpoint.nativeSession?.transcriptPath,peer.sessionId!)).settingsOffset??0:undefined;
      guard();
      this.ownedRoutes.require({peer,endpoint},rpc);
      await rpc.request('thread/settings/update',{threadId:peer.sessionId,...settings,...(serviceTier===undefined?{}:{serviceTier:nativeServiceTier(serviceTier)})},{mutation:true,beforeSend:()=>{submitted=true;}});accepted=true;guard();
      const actual=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false});guard();
      if(actual.thread?.id!==peer.sessionId)throw new BridgeError('session_changed','模型切换返回了不同会话。');
      return this.settingsProfile(rpc,peer,endpoint,actual.thread,guard,withTier,offset);
    }).catch(error=>{throw profileChangeFailure(error,submitted,accepted);});
  }
  async readProfile(peer:Peer,endpoint:Endpoint,guard:()=>void):Promise<EffectiveModelProfile>{
    return this.threadOperation(peer,endpoint,async()=>{
      guard();this.requireMember(peer.id);const runtime=runtimeFor(this.home,{endpoint:endpoint.address})?.value;
      if(endpoint.daemon||runtime?.kind!=='owned'||runtime.peerId!==peer.id)throw new BridgeError('process_owned_by_user','只能核对自有后台运行时的设置。');
      const rpc=this.clients.get(endpoint.address);
      if(!rpc)throw new RuntimeRecoveryHold('owned_subscription_waiting','后台 Codex 还没重新接上，设置先不读取，接上后再核对。');
      this.ownedRoutes.require({peer,endpoint},rpc);
      const actual=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false});guard();
      if(actual.thread?.id!==peer.sessionId)throw new BridgeError('session_changed','设置回读返回了不同会话。');
      if(actual.thread.status?.type==='notLoaded')throw new BridgeError('profile_unverified','原会话尚未由已核验的恢复路径加载，设置等待回读。');
      return this.settingsProfile(rpc,peer,endpoint,actual.thread,guard,this.modelProfile(peer).serviceTier!==undefined||peer.serviceTier!==undefined);
    });
  }
                                                
  private async settingsProfile(rpc:RpcClient,peer:Peer,endpoint:Endpoint,thread:CodexThread,guard:()=>void,withTier:boolean,offset?:number):Promise<EffectiveModelProfile>{
    const profile={model:thread.model??undefined,effort:thread.reasoningEffort??undefined};
    if(!withTier)return profile;
    if(!codexThreadIdle(thread))throw new BridgeError('profile_unverified','无法在非空闲线程核对当前服务档位。');
    const saved=await readAppliedSessionSettings(thread.path??endpoint.nativeSession?.transcriptPath,peer.sessionId!,offset);
    guard();this.ownedRoutes.require({peer,endpoint},rpc);
    const serviceTier=persistedServiceTier(saved);
    if(serviceTier===undefined||saved.settings.model!==thread.model||saved.settings.reasoningEffort!==thread.reasoningEffort)
      throw new BridgeError('profile_unverified','持久档位缺失或不属于当前模型设置，投递继续等待核验。');
    return{...profile,serviceTier};
  }
                                                 
  async loadedRuntime(sessionId:string,configHome:string):Promise<ManagedRuntime|undefined>{
    const loaded:ManagedRuntime[]=[];
    for(const {value} of managedRuntimes(this.home)){
      if(value.state!=='ready'||!value.configHome||!sameDirectory(value.configHome,configHome)||!value.pid||!value.procStart)continue;
      if(await ownedProcessState({pid:value.pid,startedAt:value.procStart})!=='alive')continue;
      if(await this.sessionState({sessionId},{address:value.endpoint,configHome:value.configHome})==='alive')loaded.push(value);
    }
    if(loaded.length>1)throw new BridgeError('runtime_ambiguous','同一会话在多个自有后台中加载，请先在原窗口核对。');
    return loaded[0];
  }
  async sessionState(peer:Pick<Peer,'sessionId'>,endpoint:Pick<Endpoint,'address'|'configHome'>):Promise<'alive'|'dead'|'unknown'>{
    if(!peer.sessionId)return'unknown';
    const existing=this.clients.get(endpoint.address);let inspector:RpcClient|undefined;
    try{
      const rpc=existing&&!existing.closed?existing:inspector=await connectAppServer(endpoint.address,{...this.config,...(endpoint.configHome?{codexHome:endpoint.configHome}:{})},undefined,this.home,{readOnly:true});
      const loaded=await codexThreadLoaded(rpc,peer.sessionId);
      return rpc.closed||!inspector&&this.clients.get(endpoint.address)!==rpc?'unknown':loaded?'alive':'dead';
    }catch{return'unknown';}finally{inspector?.close();}
  }
                                          
  async inspectThread(runtime:ManagedRuntime,sessionId:string):Promise<CodexThread|undefined>{
    this.requireMember(runtime.peerId);
    if(runtime.state!=='ready'||!runtime.pid||!runtime.procStart||!localCodexEndpoint(this.home,runtime.endpoint))throw new BridgeError('managed_proof_unverified','自有运行时的原生代次尚未确认存活。');
    const scope=this.ownedNativeScope(runtime);
    if(scope&&!scope.isChild(sessionId)){
      const rpc=this.clients.get(runtime.endpoint);
      if(!rpc||!this.canObserveThread(runtime.endpoint,sessionId,rpc,scope))return;
      const {thread}=await rpc.request('thread/read',{threadId:sessionId,includeTurns:false},{timeout:2000});
      if(!rpc.closed&&this.clients.get(runtime.endpoint)===rpc&&thread?.id===sessionId)return thread;
      return;
    }
    const key=JSON.stringify([runtime.id,runtime.endpoint,runtime.pid,runtime.procStart,runtime.configHome,sessionId]),known=this.verifiedLineage.get(key);if(known)return known;
                                          
    if(runtime.endpoint.startsWith('unix://')&&await ownedProcessState({pid:runtime.pid,startedAt:runtime.procStart})!=='alive')throw new BridgeError('managed_proof_unverified','自有运行时的原生代次尚未确认存活。');
    const rpc=await connectAppServer(runtime.endpoint,this.config,undefined,this.home,{readOnly:true});
    try{
      const thread=scope?(await catalogCodexThreads(rpc,new Set([sessionId]),{independent:true})).get(sessionId)
        :(await rpc.request('thread/read',{threadId:sessionId,includeTurns:false},{timeout:2000})).thread;
      if(thread?.id!==sessionId)return;
                                       
      if(codexSubagent(thread)){if(this.verifiedLineage.size>=256)this.verifiedLineage.delete(this.verifiedLineage.keys().next().value!);this.verifiedLineage.set(key,thread);}return thread;
    }
    finally{rpc.close();}
  }
  async release(peer:Peer,endpoint:Endpoint,guard?:()=>void):Promise<void>{
    return this.threadOperation(peer,endpoint,()=>this.releaseThread(peer,endpoint,guard));
  }
  hasSubscription(peer:Peer,endpoint:Endpoint):boolean{const rpc=this.clients.get(endpoint.address);return !!rpc&&!rpc.closed&&(peer.owner==='nxbridge'?this.ownedRoutes.permits({peer,endpoint},rpc):this.attached.has(endpoint.address+'|'+peer.sessionId));}
  userSubscriptions():{peer:Peer;endpoint:Endpoint}[]{return[...this.subscriptionOwners].filter(([key,value])=>this.attached.has(key)&&codexUserWindow(value.peer)).map(([,value])=>value);}
  async releaseIdle(peer:Peer,endpoint:Endpoint,guard:()=>boolean,closing=false):Promise<'retained'|'released'|'unloaded'|'idle'>{
    const key=endpoint.address+'|'+peer.sessionId;
    if(this.threadOperations.has(key))return'retained';
    return this.threadOperation(peer,endpoint,async()=>{
      const rpc=this.clients.get(endpoint.address);
      if(!codexUserWindow(peer)||!rpc||rpc.closed||!guard())return'retained';
      const revision=this.nativeStateRevision;let result:any;
      try{result=await rpc.request('thread/read',{threadId:peer.sessionId,includeTurns:false},{timeout:2000});}
      catch(error){if(String((error as {code?:unknown}).code)!=='-32600'||!describeError(error).includes('thread not loaded: '+peer.sessionId))throw error;this.attached.delete(key);this.subscriptionOwners.delete(key);return guard()?'unloaded':'retained';}
      if(!guard()||result.thread?.id!==peer.sessionId)return'retained';
      const status=result.thread.status,native=this.nativeStates.get(key);
      if(native&&native.revision>revision&&!['ready','offline',...(closing?['needsHuman']:[])].includes(native.state))return'retained';
      if(status?.type==='notLoaded'){this.attached.delete(key);this.subscriptionOwners.delete(key);return'unloaded';}
      const waiting=status?.activeFlags?.some((flag:string)=>/approval|input/i.test(flag));
      if(!['idle','systemError'].includes(status?.type)&&!(closing&&waiting)||status.activeFlags?.length&&!(closing&&waiting))return'retained';
      if(!this.attached.has(key))return'idle';
      await rpc.request('thread/unsubscribe',{threadId:peer.sessionId});this.attached.delete(key);this.subscriptionOwners.delete(key);return'released';
    });
  }
  private async releaseThread(peer:Peer,endpoint:Endpoint,guard?:()=>void):Promise<void>{
                                                        
    const native=endpoint.native??endpoint;
    if(native.pid&&native.procStart&&await ownedProcessState({pid:native.pid,startedAt:native.procStart})==='dead'){
      guard?.();
      this.ownedRoutes.revoke(endpoint.address,peer.sessionId??undefined);
      this.attached.delete(endpoint.address+'|'+peer.sessionId);this.subscriptionOwners.delete(endpoint.address+'|'+peer.sessionId);return;
    }
    const rpc=await this.connect(endpoint.address);
    guard?.();
    try{
      if(peer.owner==='nxbridge'){
        const runtime=runtimeFor(this.home,{endpoint:endpoint.address})?.value;
        this.ownedRoutes.adoptCreation({peer,endpoint},rpc);
        if(runtime?runtime.peerId!==peer.id||!this.ownedRoutes.nativeHolds(endpoint.address,peer.sessionId!,rpc,runtime):!this.ownedRoutes.permits({peer,endpoint},rpc))
          throw new RuntimeRecoveryHold('owned_subscription_waiting','和后台 Codex 的连接已经断了，没法正常退订；后台会话保留着，不会被强行结束。');
      }
      await rpc.request('thread/unsubscribe',{threadId:peer.sessionId});
    }
    catch(error){
      if(await codexThreadLoaded(rpc,peer.sessionId!))throw error;
    }
    this.ownedRoutes.revoke(endpoint.address,peer.sessionId??undefined,rpc);
    this.attached.delete(endpoint.address+'|'+peer.sessionId);
    this.subscriptionOwners.delete(endpoint.address+'|'+peer.sessionId);
  }
  async releaseRuntime(protectedEndpoints:Endpoint[]=[],address?:string,retired:Peer[]=[],options:{guard?:()=>void;runtimeId?:string}={}):Promise<string|undefined>{
    const notes:string[]=[];
    const records=managedRuntimes(this.home).filter(r=>(!address||r.value.endpoint===address)&&(!options.runtimeId||r.value.id===options.runtimeId));
    for(const record of records){const note=await this.releaseOneRuntime(record,protectedEndpoints,retired,options.guard);if(note)notes.push(note);}
    return notes.length?notes.join('；'):undefined;
  }
  private async releaseOneRuntime(record:RuntimeRecord,protectedEndpoints:Endpoint[],retired:Peer[],guard?:()=>void):Promise<string|undefined>{
    const saved=record.value;
    if(protectedEndpoints.some(endpoint=>endpoint.active&&endpoint.address===saved.endpoint))return'托管 App Server 仍被用户窗口引用，保留运行时及归属记录。';
    const owned=ownedProcesses(this.home).records.filter(value=>value.proof.launchId===saved.id||
      saved.kind==='legacy'&&value.proof.launchId===saved.endpoint||
      value.role==='codex-runtime'&&value.identity.pid===saved.pid&&value.identity.startedAt===saved.procStart);
    if(owned.some(value=>value.releasedToUser))return'托管 App Server 已交给用户窗口，保留运行时及归属记录。';
    if(await managedRuntimeState(this.home,saved)==='dead'){guard?.();await removeOwnedSocket(this.home,saved.endpoint);for(const value of owned)if(await ownedRecordState(value)==='dead')await retireDeadOwnedProcess(value,'dead-generation',guard);guard?.();removeManagedRuntime(record);this.emit('runtimeStopped',{address:saved.endpoint,pid:saved.pid??null,procStart:saved.procStart??null,runtimeId:saved.id});return;}
    const nativeState=saved.pid&&saved.procStart?await ownedProcessState({pid:saved.pid,startedAt:saved.procStart}):undefined;
    if(nativeState==='unknown')return'无法核验托管 App Server 的进程代次，保留进程。';
    if(nativeState==='alive'){
      try{await verifyDedicatedCodex(saved.endpoint,await verifyCodexNativeGeneration(saved));}catch(error){return'无法确认这个后台 Codex 只属于 nxbridge，进程保留：'+describeError(error);}
                                                     
      const allowed=new Set(retired.filter(peer=>saved.kind==='owned'&&peer.id===saved.peerId&&peer.owner==='nxbridge'&&peer.host==='codex').map(peer=>peer.sessionId).filter(Boolean));
      const rpc=await this.connect(saved.endpoint);
      if(allowed.size===1){try{await assertCodexRuntimeScope(rpc,[...allowed][0]!);}catch(error){return'托管 App Server 的线程范围或未完成工作尚未核验，保留运行时：'+describeError(error);}}
      else if((await loadedCodexThreads(rpc)).size)return'后台 Codex 里还有会话没卸载（可能还在 Codex 的卸载等待期内），这次没有停止它。';
      guard?.();rpc.close();
    }
    else if(saved.dedicated!==true)return'原生后台尚无独占性证明，保留仍存活的启动包装器及其后代。';
    if(!owned.length)return'托管运行时的启动归属尚未核验，保留预约和归属记录。';
    for(const value of owned.sort((a,b)=>Number(!!(b.windowsJob||b.posixGroup))-Number(!!(a.windowsJob||a.posixGroup))||b.createdAt-a.createdAt))await releaseOwnedProcess(value,guard);
                                        
    guard?.();saved.launchSettled=true;if(saved.kind==='owned')saveManagedRuntime(this.home,saved);
    if(await managedRuntimeState(this.home,saved)!=='dead')return'托管运行时仍有无法核验的进程，保留预约和归属记录。';
    guard?.();await removeOwnedSocket(this.home,saved.endpoint);guard?.();
    removeManagedRuntime(record);this.emit('runtimeStopped',{address:saved.endpoint,pid:saved.pid??null,procStart:saved.procStart??null,runtimeId:saved.id});
  }
  close():void {this.coldResumeSealed=true;this.deferredNative=undefined;for(const rpc of this.clients.values())rpc.close();this.clients.clear();this.recycleAcks.clear();this.nativeStates.clear();this.subscriptionOwners.clear();this.ownedRoutes.clear();}
}

export function codexPermission(result:any):Peer['permission']{
  return classifyCodexPermission(codexRuntimeEvidence(result));
}
