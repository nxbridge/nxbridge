import {processProbeStatus} from '../process/process.js';
import {appendFile,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {monitorEventLoopDelay} from 'node:perf_hooks';

                                     
export function brokerStartupDiagnostics(home:string,buildId:string,previous:{pid:number;startedAt:string}) {
  const began=performance.now(),baseline=processProbeStatus();
  const eventLoop=monitorEventLoopDelay({resolution:10});eventLoop.enable();
  const filename=path.join(home,'run','broker-startup.log');
  let routeEntries=0,first=true,finished=false;
  let writing=Promise.resolve();
  const mark=(stage:string,edge:'begin'|'end'|'failed',memberId?:string,generation?:string)=>{
    if(finished)return;
                                
    if(memberId&&routeEntries++>=128)return;
    const probes=processProbeStatus();
    const line=JSON.stringify({type:'broker.startup',at:new Date().toISOString(),pid:process.pid,buildId,previous,
      stage,edge,elapsedMs:Math.round(performance.now()-began),memberId,generation,
      eventLoopP99Ms:Math.round(eventLoop.percentile(99)/10000)/100,eventLoopMaxMs:Math.round(eventLoop.max/10000)/100,eventLoopResolutionMs:10,
      probeCount:probes.nativeQueries-baseline.nativeQueries,probeMs:Math.round(probes.nativeDurationMs-baseline.nativeDurationMs)})+'\n';
    const write=first?writeFile:appendFile;
    first=false;
    writing=writing.then(()=>write(filename,line,'utf8')).catch(()=>{});
  };
  const measure=async<T>(stage:string,work:()=>Promise<T>,memberId?:string):Promise<T>=>{
    mark(stage,'begin',memberId);
    try{const value=await work();mark(stage,'end',memberId);return value;}
    catch(error){mark(stage,'failed',memberId);throw error;}
  };
  return {mark,measure,finish:()=>{finished=true;eventLoop.disable();}};
}

export type BrokerStartupDiagnostics=ReturnType<typeof brokerStartupDiagnostics>;
