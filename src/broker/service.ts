import path from 'node:path';
import {z} from 'zod';
import {Store} from '../storage/store.js';
import {CodexAdapter} from '../hosts/codex/adapter.js';
import {ClaudeAdapter} from '../hosts/claude/adapter.js';
import {BridgeError,describeError} from '../shared/errors.js';
import {bridgeHome} from '../shared/local-paths.js';
import {ensureHome,removeDanglingCodexSockets} from '../ipc/home.js';
import {loadConfig} from '../config/load.js';
import {installationEnabled,listenBroker,serviceOrigin} from '../ipc/ipc.js';
import {neutralEnvironmentAsync} from '../process/environment.js';
import {registerRuntimeProcess} from '../install/runtime-install.js';
import {unlinkSync,existsSync} from 'node:fs';
import {withBrokerStartup,assertStartupReservation} from './broker-startup.js';
import {forgetOwnedProcess,ownedProcesses,registerCurrentProcess} from '../process/owned-processes.js';
import {ResourceHealth,resourceSampler} from './health/resource-health.js';
import {Broker} from './coordinator.js';
import {CustodyHolder} from './custody-holder.js';
import {validateCustodyStartup} from './custody-client.js';
import type {ParkedCustody} from './broker-custody.js';
import {CUSTODY_ENV,CUSTODY_PROTOCOL,emptyCustodyResult,parseCustodyDescriptor} from './custody-protocol.js';
import {brokerStartupDiagnostics} from './startup-diagnostics.js';
import {STARTUP_SERVING_METHODS} from './startup-routes.js';

export async function runService(home=bridgeHome()):Promise<void>{
  const startupToken=process.env.NXBRIDGE_BROKER_STARTUP_TOKEN;
  const custodyEnv=process.env[CUSTODY_ENV];
  const descriptor=custodyEnv?parseCustodyDescriptor(custodyEnv):undefined;
  const trace=descriptor&&process.platform==='win32'?brokerStartupDiagnostics(home,descriptor.buildId,descriptor.expected):undefined;
  try{
                                            
  delete process.env.NXBRIDGE_BROKER_STARTUP_TOKEN;delete process.env[CUSTODY_ENV];
  const environmentWork=()=>neutralEnvironmentAsync({NXBRIDGE_HOME:home,NXBRIDGE_SERVICE_ORIGIN:process.env.NXBRIDGE_SERVICE_ORIGIN});
  const environment=await(trace?trace.measure('persistent_environment',environmentWork):environmentWork());
  for(const key of Object.keys(process.env))delete process.env[key];Object.assign(process.env,environment.env);
  await ensureHome(home);let broker:Broker|undefined,shutdown:(()=>Promise<void>)|undefined,started:Promise<void>|undefined;
  const inherited=z.array(z.object({pid:z.number().int().positive(),startedAt:z.string(),name:z.string()})).safeParse(process.env.NXBRIDGE_SERVICE_ORIGIN?JSON.parse(process.env.NXBRIDGE_SERVICE_ORIGIN):undefined);
  const origin=inherited.success?inherited.data:await serviceOrigin();
  const validate=()=>validateCustodyStartup(home,descriptor!,startupToken,origin);
  const incoming=descriptor?await(trace?trace.measure('validate_custody',validate):validate()):undefined;
  let custodyResult={...emptyCustodyResult(),held:incoming?.offered??0};
  let holder:CustodyHolder|undefined,sealing:Promise<ParkedCustody>|undefined,stopping:Promise<string[]>|undefined;
  let irreversible=false,ready=false,fullyStarted=false,closing=false;
                                           
  let server:Awaited<ReturnType<typeof listenBroker>>|undefined;
  trace?.mark('listen','begin');
  try{const listening=await withBrokerStartup(home,origin,async()=>server=await listenBroker(home,async(method,params)=>{
                                                                
    if(!broker||!ready||!fullyStarted&&!STARTUP_SERVING_METHODS.has(method))throw new BridgeError('starting','协调器正在恢复，请稍后重试。');
    if(method==='broker.handoff.prepare'){
      if(closing||stopping||!holder)throw new BridgeError('stopping','协调器正在关闭。');
      if(sealing)throw new BridgeError('handoff_busy','协调器正在交接。');
      return holder.prepare(params);
    }
    if(method==='shutdown'){
      const options=z.object({releaseOwned:z.boolean().default(false),reason:z.enum(['shutdown','uninstall','test_cleanup','external_release']).default('shutdown'),requester:z.object({pid:z.number().int().positive(),command:z.string().max(256)}).optional(),startupToken:z.string().optional(),expectedBroker:z.object({pid:z.number().int().positive(),generation:z.string().optional()}).optional()}).parse(params??{});
      if(options.expectedBroker&&(options.expectedBroker.pid!==process.pid||options.expectedBroker.generation!==undefined&&options.expectedBroker.generation!==runtime?.record.generation))throw new BridgeError('broker_changed','协调器代次已改变，未停止新的协调器。');
                                             
      if(options.reason!=='uninstall'||installationEnabled(home))await assertStartupReservation(home,options.startupToken);
      if(sealing)throw new BridgeError('handoff_busy','协调器正在交接。');
      if(stopping||closing)throw new BridgeError('stopping','协调器正在关闭。');
      holder?.close();
      const actor=options.reason+(options.requester?' (requester pid '+options.requester.pid+', '+options.requester.command+')':'');
      const {startupToken:_token,...recordedOptions}=options;store!.event('shutdown.requested',recordedOptions);
      stopping=broker.stop(options.releaseOwned,options.reason==='uninstall'?null:{reason:options.reason==='external_release'?'external_release':'broker_shutdown',actor});
      try{return{stopping:true,notes:await stopping};}
      finally{setTimeout(()=>{void shutdown?.().catch(error=>{process.stderr.write('协调器关闭未完成：'+describeError(error)+'\n');process.exitCode=1;});},20);}
    }
    const result=await broker.dispatch(method,params);
    return method==='ping'?{...result as object,handoff:{protocol:CUSTODY_PROTOCOL,...custodyResult,...(!fullyStarted?{pending:true}:{})}}:result;
  }),startupToken);if(!listening)return;server=listening;}
  catch(error){
    trace?.mark('listen','failed');
                                                   
    try{await server?.close();}catch(cleanup){process.stderr.write('协调器监听回收未完成：'+describeError(cleanup)+'\n');}
    if(error instanceof BridgeError&&error.code==='already_running')return;throw error;
  }
  if(!installationEnabled(home)){await server.close();return;}
  trace?.mark('listen','end');
  const boundServer=server;
  let runtime:Awaited<ReturnType<typeof registerRuntimeProcess>>|undefined,store:Store|undefined,ownership:Awaited<ReturnType<typeof registerCurrentProcess>>|undefined;
  let closeWork:Promise<void>|undefined;
  const closeStore=()=>{store?.close();store=undefined;};
  const close=()=>closeWork??=(async()=>{
    closing=true;holder?.close();
    await started?.catch(()=>{});
    await sealing?.catch(()=>{});
    try{if(!irreversible)await(stopping??broker?.stop());}
    finally{
                                         
      closeStore();await boundServer.close();
      if(ownership)forgetOwnedProcess(ownership);if(runtime&&existsSync(runtime.file))unlinkSync(runtime.file);
    }
  })();
  const terminate=()=>{void close().catch(error=>{process.stderr.write('协调器关闭未完成：'+describeError(error)+'\n');process.exitCode=1;});};
  shutdown=close;
  try{
    runtime=await registerRuntimeProcess(home);
    trace?.mark('register','end',undefined,runtime.record.generation);
    if(!installationEnabled(home)){await close();return;}
    store=new Store(path.join(home,'nxbridge.db'));const config=loadConfig(home);
    const removedSockets=removeDanglingCodexSockets(home);if(removedSockets.length)store.event('socket.dangling.cleaned',{paths:removedSockets});
    ownership=await registerCurrentProcess(home,'broker');
    const windowsIndependent=process.platform!=='win32'||ownedProcesses(home).records.some(value=>value.role==='broker'&&value.windowsIndependent&&value.identity.pid===process.pid&&value.identity.startedAt===ownership!.identity.startedAt);
    const resources=new ResourceHealth(resourceSampler(home));
    broker=new Broker(store,config,{codex:new CodexAdapter(config,home,{pid:ownership.identity.pid,startedAt:ownership.identity.startedAt}),claude:new ClaudeAdapter(config,home)},home,origin,{entry:runtime.record.entry,generation:runtime.record.generation,buildId:runtime.record.buildId,windowsIndependent},resources);
    holder=new CustodyHolder(home,ownership.identity,options=>{
      if(closing||stopping)throw new BridgeError('stopping','协调器正在关闭。');
      const pending=(async()=>{
        const parked=await broker!.sealForHandoff({...options,commit:()=>{options.commit();irreversible=true;}});
        try{store=undefined;await boundServer.close();return parked;}
        catch(error){parked.close();throw error;}
      })();
      sealing=pending;
      return pending.finally(()=>{sealing=undefined;});
    },terminate);
    const claim=async()=>incoming?.claim?.();
    const acquisition=await(trace?trace.measure('claim',claim):claim());
    if(acquisition)custodyResult=acquisition.result;
    try{started=broker.start(acquisition,trace,()=>{ready=true;});await started;}
    finally{if(acquisition){custodyResult=acquisition.result;acquisition.close();}}
    fullyStarted=true;
    trace?.mark('ready','end',undefined,runtime.record.generation);
    process.once('SIGINT',terminate);process.once('SIGTERM',terminate);
  }catch(error){trace?.mark('startup','failed');try{await close();}catch(cleanup){process.stderr.write('协调器启动失败后的回收未完成：'+describeError(cleanup)+'\n');}throw error;}
  }finally{trace?.finish();}
}
