import {connectBroker} from '../ipc/ipc.js';

interface ServingBroker {
  pid:number;
  runtime:{generation:string};
  handoff?:{pending?:boolean;transferred:number;held:number};
}
interface IpcSample {
  connectMs:number;pingMs?:number;pid?:number;generation?:string;handoff?:ServingBroker['handoff'];code?:string;
}
export const ipcBudgetMs=500;

export async function probeIpcWave(home:string,clients:number,began:number){
  const started=performance.now(),startedAt=Date.now();
  const connections=await Promise.all(Array.from({length:clients},async()=>{
    const at=performance.now();
    try{
      const rpc=await connectBroker(home,2*ipcBudgetMs);
      return {rpc,connectMs:performance.now()-at};
    }catch(error){return{connectMs:performance.now()-at,code:(error as {code?:string}).code??'unknown'};}
  }));
                                       
  const connected=connections.filter(connection=>connection.rpc&&!connection.rpc.closed).length;
  const samples=await Promise.all(connections.map(async({rpc,connectMs,code}):Promise<IpcSample>=>{
    if(!rpc)return{connectMs,code};
    const at=performance.now();
    try{
      const result=await rpc.request<ServingBroker>('ping',{}, {timeout:2*ipcBudgetMs});
      return{connectMs,pingMs:performance.now()-at,pid:result.pid,generation:result.runtime.generation,handoff:result.handoff};
    }catch(error){return{connectMs,pingMs:performance.now()-at,code:(error as {code?:string}).code??'unknown'};}
    finally{rpc.close();}
  }));
  return{atMs:Math.round(started-began),startedAt,finishedAt:Date.now(),connected,samples};
}

export function latency(values:number[]){
  const sorted=values.toSorted((a,b)=>a-b);
  return{count:sorted.length,p99Ms:Math.round(sorted[Math.ceil(sorted.length*.99)-1]??0),maxMs:Math.round(sorted.at(-1)??0)};
}

export function waveReport(wave:Awaited<ReturnType<typeof probeIpcWave>>){
  const replies:Record<string,number>={};
  for(const sample of wave.samples){
    const key=sample.code??'pid:'+sample.pid+(sample.handoff?.pending?':pending':'');
    replies[key]=(replies[key]??0)+1;
  }
  return{atMs:wave.atMs,connected:wave.connected,replies,connect:latency(wave.samples.map(s=>s.connectMs)),
    ping:latency(wave.samples.flatMap(s=>s.pingMs===undefined?[]:[s.pingMs]))};
}
