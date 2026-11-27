import type {CustodyAcquisition,CustodyOffer,ParkedCustody} from '../../broker/broker-custody.js';
import type {RpcClient,RpcMessage} from '../../ipc/rpc.js';
import {BridgeError} from '../../shared/errors.js';

export const handoffUnavailable=()=>new BridgeError('handoff_native_unverified','不能确认原生订阅连续性，保留原任务。');

export function custodyInvalidation(message:RpcMessage):string|undefined{
  const {method,params}=message;
  if(typeof params?.threadId==='string'&&(['thread/unloaded','thread/closed','thread/archived','thread/deleted'].includes(method??'')||method==='thread/status/changed'&&params.status?.type==='notLoaded'))return params.threadId;
}

export function parkCodexGroups(rows:readonly {offer:CustodyOffer;rpc:RpcClient}[],retire=(rpc:RpcClient)=>rpc.close()):ParkedCustody{
  const active=new Map(rows.map(row=>[row.offer.id,row] as const));
  const groups=new Map<RpcClient,typeof rows>();
  for(const row of rows)groups.set(row.rpc,[...groups.get(row.rpc)??[],row]);
  for(const [rpc,group] of groups){
    const invalidate=(id:string)=>{for(const row of group)if(row.offer.sessionId===id)active.delete(row.offer.id);};
    rpc.on('notification',(message:RpcMessage)=>{const id=custodyInvalidation(message);if(id)invalidate(id);});
    rpc.on('threadAbsent',invalidate);
  }
  const unresolved=new Set(active.keys());
  const close=()=>{for(const rpc of groups.keys())retire(rpc);groups.clear();active.clear();unresolved.clear();};
  return {offers:rows.map(row=>row.offer),live:id=>!!active.get(id)&&!active.get(id)!.rpc.closed,release:id=>{
    unresolved.delete(id);
    const row=rows.find(value=>value.offer.id===id);if(!row)return;
    const group=groups.get(row.rpc);if(group&&!group.some(value=>unresolved.has(value.offer.id))){groups.delete(row.rpc);retire(row.rpc);for(const member of group)active.delete(member.offer.id);}
  },close};
}

export interface ProvisionalCodexGroup {
  acquire(offer:CustodyOffer,verify:()=>Promise<void>):Promise<void>;
  validate():void;
  promote():void;
  close():Promise<void>;
}

                                       
export const HANDOFF_GROUP_BUDGET_MS=12000;
export async function acquireCodexGroups(custody:CustodyAcquisition,open:(offers:readonly CustodyOffer[],signal:AbortSignal)=>Promise<ProvisionalCodexGroup>,
  measure?:<T>(stage:string,work:()=>Promise<T>,memberId?:string)=>Promise<T>,budgetMs=HANDOFF_GROUP_BUDGET_MS):Promise<{promoted:readonly string[];transferred:number;held:number}>{
  const groups=new Map<string,CustodyOffer[]>(),addressGroups=new Map<string,string>();
  const ambiguous=new Set<string>();
  for(const offer of custody.offers){
    const previous=addressGroups.get(offer.address);
    if(previous&&previous!==offer.groupId){ambiguous.add(previous);ambiguous.add(offer.groupId);}
    addressGroups.set(offer.address,offer.groupId);
    const group=groups.get(offer.groupId)??[];group.push(offer);groups.set(offer.groupId,group);
  }
  const results=await Promise.all([...groups].map(async([group,offers])=>{
    const budget=new AbortController(),signal=AbortSignal.any([custody.signal,budget.signal]);
    const timer=setTimeout(()=>budget.abort(),budgetMs);
    let provisional:ProvisionalCodexGroup|undefined;
    const check=()=>{if(signal.aborted)throw handoffUnavailable();};
    let interrupted!:()=>void;
    const aborted=new Promise<never>((_resolve,reject)=>{
      interrupted=()=>reject(handoffUnavailable());
      signal.addEventListener('abort',interrupted,{once:true});if(signal.aborted)interrupted();
    });
    const verify=async()=>{
      check();
      if(!(await Promise.all(offers.map(offer=>custody.verify(offer.id)))).every(Boolean))throw handoffUnavailable();
      check();provisional?.validate();
    };
    const ids=offers.map(offer=>offer.id);
    try{
      const acquireGroup=async()=>{
      if(ambiguous.has(group)||offers.some(offer=>offer.address!==offers[0]!.address||!offer.profile)||new Set(ids).size!==ids.length)throw handoffUnavailable();
      await verify();
                                                    
      const opened=await(measure?measure('route_connect',()=>open(offers,signal),offers[0]!.memberId):open(offers,signal));
      if(signal.aborted){await opened.close();throw handoffUnavailable();}
      provisional=opened;
      await verify();
      for(const offer of offers){
        const acquire=()=>provisional!.acquire(offer,verify);
        await(measure?measure('route_resume',acquire,offer.memberId):acquire());
      }
      await verify();
      };
      await Promise.race([acquireGroup(),aborted]);
                                                    
      clearTimeout(timer);
      await custody.resolve(ids,'transferred');
      check();provisional!.validate();provisional!.promote();
      return offers.map(offer=>offer.memberId);
    }catch{
      budget.abort();
      await provisional?.close();
      await custody.resolve(ids,'held').catch(()=>{});
      return [];
    }finally{clearTimeout(timer);signal.removeEventListener('abort',interrupted);}
  }));
  const promoted=results.flat();
  return {promoted,transferred:promoted.length,held:custody.offers.length-promoted.length};
}
