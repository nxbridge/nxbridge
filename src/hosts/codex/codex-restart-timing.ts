import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
import { BridgeError, RuntimeRecoveryHold } from '../../shared/errors.js';
import type { Endpoint, Peer } from '../../types.js';

interface RestartTarget {
  peerId:string;sessionId:string|null;runtimeId?:string;pid:number|null;generation:string|null;
}
interface RestartStage {count:number;ms:number;firstAtMs:number;lastAtMs:number;failed?:number}
export interface CodexRestartTiming {
  operationId:string;requestId?:string;incidentId?:string;kind:string;brokerPid:number;brokerGeneration?:string;
  target?:RestartTarget;replacement?:{runtimeId?:string;pid?:number;generation?:string};
  outcome:'complete'|'failed'|'held';code?:string;elapsedMs:number;stages:Record<string,RestartStage>;
  processCpuMs:{user:number;system:number};threadCpuMs?:{user:number;system:number};
  eventLoop:{utilization:number;activeMs:number;p99Ms?:number;maxMs?:number};
}
interface Trace {
  target?:RestartTarget;replacement?:CodexRestartTiming['replacement'];
  began:number;active:boolean;stages:Record<string,RestartStage>;heldCode?:string;
}
const traceContext=new AsyncLocalStorage<Trace>();
const round=(ms:number)=>Math.round(ms*100)/100;
const milliseconds=({user,system}:NodeJS.CpuUsage)=>({user:round(user/1000),system:round(system/1000)});

export async function withCodexRestartTiming<T>(identity:{kind:string;requestId?:string;incidentId?:string;brokerGeneration?:string},
  record:(timing:CodexRestartTiming)=>void,work:()=>Promise<T>,reuse=false):Promise<T>{
  if(reuse&&traceContext.getStore()?.active)return work();
                                          
  const began=performance.now(),cpu=process.cpuUsage(),threadCpu=process.threadCpuUsage?.(),loop=performance.eventLoopUtilization();
  const delay=monitorEventLoopDelay({resolution:10});delay.enable();
  const trace:Trace={began,active:true,stages:{}};
  let outcome:CodexRestartTiming['outcome']='complete',code:string|undefined;
  try{return await traceContext.run(trace,work);}
  catch(error){
    outcome=error instanceof RuntimeRecoveryHold?'held':'failed';
    code=error instanceof BridgeError?error.code:'internal';
    throw error;
  }finally{
    trace.active=false;delay.disable();
    const current=performance.eventLoopUtilization(loop);
    const timing:CodexRestartTiming={operationId:identity.requestId??randomUUID(),...identity,brokerPid:process.pid,
      target:trace.target,replacement:trace.replacement,outcome:outcome==='complete'&&trace.heldCode?'held':outcome,code:code??trace.heldCode,
      elapsedMs:round(performance.now()-began),stages:trace.stages,processCpuMs:milliseconds(process.cpuUsage(cpu)),
      ...(threadCpu?{threadCpuMs:milliseconds(process.threadCpuUsage(threadCpu))}:{}),
      eventLoop:{utilization:round(current.utilization),activeMs:round(current.active),
        ...(delay.count?{p99Ms:round(delay.percentile(99)/1e6),maxMs:round(delay.max/1e6)}:{})}};
                              
    try{record(timing);}catch{                }
  }
}

export function traceCodexRestartTarget(peer:Peer,endpoint?:Endpoint):void{
  const trace=traceContext.getStore();if(!trace?.active)return;
  const native=endpoint?.native??endpoint;
  trace.target={peerId:peer.id,sessionId:peer.sessionId,runtimeId:endpoint?.runtimeId,pid:native?.pid??null,generation:native?.procStart??null};
}

export function traceCodexRestartReplacement(runtimeId?:string,pid?:number,generation?:string):void{
  const trace=traceContext.getStore();
  if(trace?.active)trace.replacement={runtimeId:runtimeId??trace.replacement?.runtimeId,pid:pid??trace.replacement?.pid,generation:generation??trace.replacement?.generation};
}

export function traceCodexRestartHeld(code:string):void{
  const trace=traceContext.getStore();if(trace?.active)trace.heldCode=code;
}

                                         
export function timeCodexRestart<T>(name:string,work:()=>T):T{
  const trace=traceContext.getStore();if(!trace?.active)return work();
  const began=performance.now(),finish=(failed=false)=>{
    if(!trace.active)return;
    const now=performance.now(),first=round(began-trace.began),last=round(now-trace.began);
    const stage=trace.stages[name]??={count:0,ms:0,firstAtMs:first,lastAtMs:last};
    stage.count++;stage.ms=round(stage.ms+now-began);
    stage.firstAtMs=Math.min(stage.firstAtMs,first);stage.lastAtMs=Math.max(stage.lastAtMs,last);
    if(failed)stage.failed=(stage.failed??0)+1;
  };
  try{
    const value=work();
    if(value instanceof Promise)return value.then(result=>{finish();return result;},error=>{finish(true);throw error;}) as T;
    finish();return value;
  }catch(error){finish(true);throw error;}
}
