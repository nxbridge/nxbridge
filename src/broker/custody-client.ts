import {activeRuntime,ownsRuntime,runtimeManifest} from '../install/runtime-install.js';
import {connectLocalRpc} from '../ipc/ipc.js';
import type {RpcClient} from '../ipc/rpc.js';
import {processIdentity,probeProcesses} from '../process/process.js';
import {BridgeError} from '../shared/errors.js';
import {sameDirectory} from '../shared/files.js';
import {CLI_PATH} from '../shared/package-location.js';
import {remainingTime} from '../shared/time-budget.js';
import type {CustodyAcquisition,CustodyOffer} from './broker-custody.js';
import {startupReservationDeadline} from './broker-startup.js';
import {custodyAddress,custodyDeadlines,CUSTODY_MANIFEST_ENTRY,emptyCustodyResult,sameGeneration,type CustodyDescriptor,type CustodyGeneration,type CustodyResult} from './custody-protocol.js';

export async function custodyCall<T>(home:string,descriptor:CustodyDescriptor,method:string,deadline:number,timeout=1500):Promise<T>{
  const rpc=await connectLocalRpc(custodyAddress(home,descriptor.updateId),descriptor.ticket,remainingTime(deadline,2000),2*1024*1024);
  try{return await rpc.request<T>(method,{}, {timeout:remainingTime(deadline,timeout)});}finally{rpc.close();}
}

const invalid=()=>new BridgeError('handoff_descriptor_invalid','协调器交接凭据或候选运行时已改变。');

export async function validateCustodyStartup(home:string,descriptor:CustodyDescriptor,token:string|undefined,origin:CustodyGeneration[]):Promise<{offered:number;claim?:()=>Promise<SuccessorCustody|undefined>}>{
  const pointer=activeRuntime(home);
  const running=runtimeManifest();
  if(!token||!pointer||pointer.buildId!==descriptor.buildId||running?.buildId!==descriptor.buildId||!running.files[CUSTODY_MANIFEST_ENTRY]||
    !ownsRuntime(home,pointer.directory)||!sameDirectory(pointer.entry,CLI_PATH)||!origin.some(identity=>sameGeneration(identity,descriptor.updater)))throw invalid();
  const reservedUntil=await startupReservationDeadline(home,token,descriptor.updater);
  if(reservedUntil<descriptor.expiresAt)throw invalid();
  const {deadline}=custodyDeadlines(descriptor.expiresAt);
  let offered=0;
  try{
    const status=await custodyCall<{phase:string;offered:number}>(home,descriptor,'status',deadline);
    if(status.phase!=='sealed')throw invalid();
    offered=status.offered;
  }catch(error){
    const [state]=await probeProcesses([{pid:descriptor.expected.pid,procStart:descriptor.expected.startedAt}]);
    if(state==='dead')return{offered};
    throw error;
  }
  return{offered,claim:async()=>{
                                          
    const identity=await processIdentity(process.pid);
    return SuccessorCustody.claim(home,descriptor,identity);
  }};
}

export class SuccessorCustody implements CustodyAcquisition {
  readonly signal:AbortSignal;
  readonly result:CustodyResult=emptyCustodyResult();
  readonly offers:readonly CustodyOffer[];
  private readonly deadline:number;
  private readonly acquisitionDeadline:number;
  private readonly timer:NodeJS.Timeout;
  private readonly stopped=new AbortController();
  private transferred=new Set<string>();

  static async claim(home:string,descriptor:CustodyDescriptor,identity:CustodyGeneration):Promise<SuccessorCustody|undefined>{
    const {acquisitionDeadline}=custodyDeadlines(descriptor.expiresAt);
    let rpc:RpcClient|undefined;
    try{
      rpc=await connectLocalRpc(custodyAddress(home,descriptor.updateId),descriptor.ticket,remainingTime(acquisitionDeadline,2000),2*1024*1024);
      const {offers}=await rpc.request<{offers:CustodyOffer[]}>('claim',{identity,buildId:descriptor.buildId},{timeout:remainingTime(acquisitionDeadline,3000)});
      return new SuccessorCustody(descriptor,rpc,offers);
    }catch{rpc?.close();return;}
  }

  private constructor(descriptor:CustodyDescriptor,private rpc:RpcClient,offers:CustodyOffer[]){
    const {deadline,acquisitionDeadline}=custodyDeadlines(descriptor.expiresAt);
    this.deadline=deadline;this.acquisitionDeadline=acquisitionDeadline;this.offers=offers;
    this.signal=this.stopped.signal;this.result.used=true;this.result.held=offers.length;
    this.timer=setTimeout(()=>this.stopped.abort(),Math.max(0,acquisitionDeadline-performance.now()));
    rpc.once('disconnected',()=>this.stopped.abort());
  }

  async verify(id:string):Promise<boolean>{
    if(this.signal.aborted)return false;
    try{return (await this.rpc.request<{live:boolean}>('verify',{id},{timeout:remainingTime(this.acquisitionDeadline,1500)})).live&&!this.signal.aborted;}
    catch{return false;}
  }

  async resolve(ids:readonly string[],outcome:'transferred'|'held'):Promise<void>{
    if(!ids.length)return;
    const deadline=outcome==='transferred'?this.acquisitionDeadline:this.deadline;
    const send=()=>this.rpc.request('resolve',{ids,outcome},{timeout:remainingTime(deadline,1500)});
    try{await send();}
    catch(error){
                                               
      if(this.rpc.closed||(error as {code?:string}).code!=='timeout')throw error;
      await send();
    }
    if(outcome==='transferred'){
      for(const id of ids)this.transferred.add(id);
      this.result.transferred=this.transferred.size;this.result.held=this.offers.length-this.result.transferred;
    }
  }

  close():void{clearTimeout(this.timer);this.rpc.close();}
  complete({transferred,held}:{transferred:number;held:number}):void{Object.assign(this.result,{transferred,held});}
}
