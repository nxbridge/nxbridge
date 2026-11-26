import type { RpcClient } from '../../ipc/rpc.js';
import { RuntimeRecoveryHold } from '../../shared/errors.js';
import type { Endpoint, Peer } from '../../types.js';
import { managedRuntimeNativeState, runtimeFor } from '../managed-runtimes.js';
import { probeProcesses } from '../../process/process.js';

export interface OwnedCodexRoute {
  peer: Peer;
  endpoint: Endpoint;
}
export interface OwnedCodexLaunchProof {
  sessionId:string;runtimeId?:string;endpoint?:string;pid?:number|null;procStart?:string|null;
}

                                        
export async function exitedOwnedCodexRoute(home:string,{peer,endpoint}:OwnedCodexRoute,launch?:OwnedCodexLaunchProof):Promise<boolean>{
  const native=endpoint.native??endpoint;
  if(peer.owner!=='nxbridge'||endpoint.daemon||!endpoint.runtimeId||!native.pid||!native.procStart)return false;
  const runtime=runtimeFor(home,{id:endpoint.runtimeId})?.value;
  if(runtime)return runtime.kind==='owned'&&runtime.endpoint===endpoint.address&&runtime.peerId===peer.id
    &&(!runtime.sessionId||runtime.sessionId===peer.sessionId)&&runtime.pid===native.pid&&runtime.procStart===native.procStart
    &&await managedRuntimeNativeState(home,runtime)==='dead';
                                              
  if(!launch||launch.sessionId!==peer.sessionId||launch.runtimeId!==endpoint.runtimeId
    ||launch.endpoint&&launch.endpoint!==endpoint.address||launch.pid&&launch.pid!==native.pid||launch.procStart&&launch.procStart!==native.procStart)return false;
  return (await probeProcesses([{pid:native.pid,procStart:native.procStart}]))[0]==='dead';
}

export interface OwnedCodexLease {
  memberId:string;sessionId:string;address:string;epoch:string;bindingEpoch?:string;
  runtimeId:string;pid:number;procStart:string;
}

export function ownedCodexLease({peer,endpoint}:OwnedCodexRoute):OwnedCodexLease|undefined{
  const {pid,procStart}=endpoint.native??endpoint;
  if(!peer.sessionId||!endpoint.runtimeId||!pid||!procStart)return;
  return {memberId:peer.id,sessionId:peer.sessionId,address:endpoint.address,epoch:endpoint.epoch,
    bindingEpoch:endpoint.bindingEpoch,runtimeId:endpoint.runtimeId,pid,procStart};
}

const routeKey = ({peer,endpoint}:OwnedCodexRoute) => {
  const native=endpoint.native??endpoint;
  return JSON.stringify([peer.id,peer.owner,peer.sessionId,endpoint.address,endpoint.epoch,endpoint.bindingEpoch,endpoint.runtimeId,native.pid,native.procStart]);
};

interface Subscription {memberId:string;sessionId:string;address:string;lease?:OwnedCodexLease;rpc:RpcClient;state:'creating'|'created'|'subscribed'}
interface Pending {key:string;work:Promise<unknown>}

                                                 
export class CodexOwnedRoutes {
  private subscriptions=new Map<string,Subscription>();
  private pending=new Map<string,Pending>();
  private source?: (memberId:string)=>OwnedCodexRoute|undefined;
  private identity?: (memberId:string)=>string|undefined;

  bind(source:(memberId:string)=>OwnedCodexRoute|undefined,identity?:(memberId:string)=>string|undefined):void{this.source=source;this.identity=identity;}

  current(route:OwnedCodexRoute):boolean{
    const {peer,endpoint}=route;
    if(peer.host!=='codex'||peer.owner!=='nxbridge'||!peer.sessionId||endpoint.peerId!==peer.id||endpoint.daemon||endpoint.mode!=='app-server'||!endpoint.active)return false;
    const current=this.source?.(peer.id);
    return !this.source||!!current&&routeKey(current)===routeKey(route);
  }

  selected(id:string):OwnedCodexRoute|undefined{return this.source?.(id);}
  sessionId(id:string):string|undefined{return this.identity?.(id);}

  subscribed(route:OwnedCodexRoute,rpc:RpcClient):void{
    this.remember(route,rpc,'subscribed');
  }

                                       
  created(route:OwnedCodexRoute,rpc:RpcClient):void{
    this.remember(route,rpc,'creating');
  }

  creationReady(route:OwnedCodexRoute,rpc:RpcClient):void{
    this.remember(route,rpc,'created');
  }

  private remember(route:OwnedCodexRoute,rpc:RpcClient,state:Subscription['state']):void{
    const lease=ownedCodexLease(route);
    if(!route.peer.sessionId||rpc.closed||route.endpoint.runtimeId&&!lease)return;
    this.subscriptions.set(routeKey(route),{memberId:route.peer.id,sessionId:route.peer.sessionId,address:route.endpoint.address,lease,rpc,state});
  }

  adoptCreation(route:OwnedCodexRoute,rpc:RpcClient):boolean{
    if(rpc.closed||!this.current(route))return false;
    const native=ownedCodexLease(route);
    for(const [key,receipt] of this.subscriptions){
      if(receipt.state!=='created'||receipt.rpc!==rpc||receipt.memberId!==route.peer.id||receipt.sessionId!==route.peer.sessionId||receipt.address!==route.endpoint.address)continue;
      if(receipt.lease?(!native||receipt.lease.runtimeId!==native.runtimeId||receipt.lease.pid!==native.pid||receipt.lease.procStart!==native.procStart):!!route.endpoint.runtimeId)continue;
      this.subscriptions.delete(key);this.subscribed(route,rpc);return true;
    }
    return false;
  }

  nativeRoot(address:string,rpc:RpcClient,runtime:{id:string;peerId?:string;pid?:number;procStart?:string}):string|undefined{
    if(rpc.closed)return;
    return [...this.subscriptions.values()].find(({lease,rpc:owner,state})=>state!=='subscribed'&&owner===rpc&&lease&&lease.memberId===runtime.peerId
      &&lease.address===address&&lease.runtimeId===runtime.id&&lease.pid===runtime.pid&&lease.procStart===runtime.procStart)?.sessionId;
  }

  creationIdentity(address:string,id:string,rpc:RpcClient,runtime?:{id:string;pid?:number;procStart?:string}):object|undefined{
    if(rpc.closed)return;
    const receipt=[...this.subscriptions.values()].find(entry=>entry.state==='creating'&&entry.rpc===rpc&&entry.address===address&&entry.sessionId===id
      &&(runtime?entry.lease?.runtimeId===runtime.id&&entry.lease.pid===runtime.pid&&entry.lease.procStart===runtime.procStart:!entry.lease));
    if(!receipt)return;
    const selected=this.source?.(receipt.memberId),identity=this.identity?.(receipt.memberId);
    if(identity&&identity!==id||selected&&(selected.peer.owner!=='nxbridge'||selected.peer.sessionId&&selected.peer.sessionId!==id))return;
    return receipt;
  }

  permits(route:OwnedCodexRoute,rpc?:RpcClient):boolean{
    return !!this.leaseIdentity(route,rpc);
  }

                                          
  leaseIdentity(route:OwnedCodexRoute,rpc?:RpcClient):object|undefined{
    const lease=this.subscriptions.get(routeKey(route));
    return lease&&lease.state!=='creating'&&!lease.rpc.closed&&(!rpc||lease.rpc===rpc)&&this.current(route)?lease:undefined;
  }

  nativeHolds(address:string,threadId:string,rpc:RpcClient,runtime:{id:string;pid?:number;procStart?:string}):boolean{
    return !rpc.closed&&[...this.subscriptions.values()].some(({lease,rpc:owner,state})=>state!=='creating'&&owner===rpc&&lease?.address===address&&lease.sessionId===threadId
      &&lease.runtimeId===runtime.id&&lease.pid===runtime.pid&&lease.procStart===runtime.procStart);
  }

  require(route:OwnedCodexRoute,rpc:RpcClient):void{
    if(route.peer.owner==='nxbridge'&&!this.permits(route,rpc))throw new RuntimeRecoveryHold('owned_subscription_waiting','后台 Codex 还没重新接上；消息已保留，接上后自动送达。');
  }

  revoke(address:string,threadId?:string,rpc?:RpcClient):void{
    for(const [id,entry] of this.subscriptions)if(entry.address===address&&(!threadId||entry.sessionId===threadId)&&(!rpc||entry.rpc===rpc))this.subscriptions.delete(id);
  }

  reconcile(route:OwnedCodexRoute,attach:()=>Promise<unknown>):Promise<unknown>|undefined{
    if(!this.current(route)||this.permits(route))return;
    const id=route.peer.id,key=routeKey(route),pending=this.pending.get(id);
    if(pending)return pending.key===key?pending.work:undefined;
    const work=Promise.resolve().then(()=>this.current(route)?attach():undefined).finally(()=>{if(this.pending.get(id)?.work===work)this.pending.delete(id);});
    this.pending.set(id,{key,work});return work;
  }

  clear():void{this.subscriptions.clear();}
}
