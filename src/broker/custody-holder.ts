import {randomBytes} from 'node:crypto';
import {chmodSync} from 'node:fs';
import type {Socket} from 'node:net';
import {z} from 'zod';
import {activeRuntime,ownsRuntime,runtimeManifest} from '../install/runtime-install.js';
import {socketDirectory} from '../ipc/home.js';
import {installationEnabled,localRpcServer} from '../ipc/ipc.js';
import {probeProcesses} from '../process/process.js';
import {BridgeError} from '../shared/errors.js';
import {remainingTime} from '../shared/time-budget.js';
import type {CustodySeal,ParkedCustody} from './broker-custody.js';
import {startupReservationDeadline} from './broker-startup.js';
import {custodyAddress,custodyDeadlines,custodyPrepareSchema,CUSTODY_LIMIT_MS,CUSTODY_MANIFEST_ENTRY,generationSchema,sameGeneration,type CustodyDescriptor,type CustodyGeneration} from './custody-protocol.js';

type Phase='prepared'|'draining'|'sealing'|'sealed'|'claimed'|'closed';
type Terminal='transferred'|'held';
type Seal=(cutover:CustodySeal)=>Promise<ParkedCustody>;
const rejected=()=>new BridgeError('handoff_rejected','无法核验本次协调器交接。');
const probe=(generation:CustodyGeneration,timeout:number)=>probeProcesses([{pid:generation.pid,procStart:generation.startedAt}],timeout).then(([state])=>state);

export class CustodyHolder {
  private pending=false;
  private current?:Offer;
  private closed=false;
  constructor(private home:string,private old:CustodyGeneration,private seal:Seal,private abandoned:()=>void){}
  async prepare(raw:unknown):Promise<CustodyDescriptor>{
    if(this.closed||this.pending||this.current)throw new BridgeError('handoff_busy','协调器正在关闭或已有交接。');
                              
    const parsed=custodyPrepareSchema.safeParse(raw);
    if(!parsed.success)throw rejected();
    const {startupToken,...request}=parsed.data;
    if(!sameGeneration(this.old,request.expected)||request.updater.pid===this.old.pid)throw rejected();
    this.pending=true;
    try{
      const deadline=await startupReservationDeadline(this.home,startupToken,request.updater);
      const pointer=activeRuntime(this.home);
      if(!installationEnabled(this.home)||pointer?.buildId!==request.buildId||!ownsRuntime(this.home,pointer.directory)||!runtimeManifest(pointer.directory)?.files[CUSTODY_MANIFEST_ENTRY])throw rejected();
      if(await probe(request.updater,1500)!=='alive')throw rejected();
      if(process.platform!=='win32')await socketDirectory(this.home);
      const descriptor={...request,updateId:randomBytes(8).toString('hex'),ticket:randomBytes(32).toString('hex'),expiresAt:Math.min(deadline,Date.now()+CUSTODY_LIMIT_MS)};
      const offer=new Offer(this.home,descriptor,startupToken,this.seal,(committed)=>{
        if(this.current===offer)this.current=undefined;
        if(committed)this.abandoned();
      });
      await offer.listen();
      if(this.closed){offer.close();throw rejected();}
      this.current=offer;
      return descriptor;
    }finally{this.pending=false;}
  }

  close():void{this.closed=true;this.current?.close();}
}

class Offer {
  private phase:Phase='prepared';
  private readonly controller=new AbortController();
  private readonly deadline:number;
  private readonly acquisitionDeadline:number;
  private readonly address:string;
  private readonly endpoint:ReturnType<typeof localRpcServer>;
  private expiry?:NodeJS.Timeout;
  private monitor?:NodeJS.Timeout;
  private claimant?:{identity:CustodyGeneration;socket:Socket};
  private parked?:ParkedCustody;
  private outcomes=new Map<string,Terminal>();
  committed=false;

  constructor(private home:string,private descriptor:CustodyDescriptor,private startupToken:string,private seal:Seal,private finished:(committed:boolean)=>void){
    const {deadline,acquisitionDeadline}=custodyDeadlines(descriptor.expiresAt);
    this.deadline=deadline;this.acquisitionDeadline=acquisitionDeadline;
    this.address=custodyAddress(home,descriptor.updateId);
    this.endpoint=localRpcServer(descriptor.ticket,(method,params,socket)=>this.handle(method,params,socket),true);
  }

  async listen():Promise<void>{
    try{
      remainingTime(this.acquisitionDeadline,CUSTODY_LIMIT_MS);
      await this.endpoint.listen(this.address);
      if(process.platform!=='win32')chmodSync(this.address,0o600);
      this.expiry=setTimeout(()=>this.close(),remainingTime(this.deadline,CUSTODY_LIMIT_MS));
      this.scheduleMonitor();
    }catch(error){await this.endpoint.close();throw error;}
  }

  private scheduleMonitor():void{
    this.monitor=setTimeout(async()=>{
      try{
        const identity=this.claimant?.identity??this.descriptor.updater;
        if(!installationEnabled(this.home))this.close();
        else if(await probe(identity,remainingTime(this.deadline,1000))==='dead'&&identity===(this.claimant?.identity??this.descriptor.updater))this.close();
      }catch{                                  }
      if(this.phase!=='closed')this.scheduleMonitor();
    },2000);
  }

  close():void{
    if(this.phase==='closed')return;
    this.phase='closed';clearTimeout(this.expiry);clearTimeout(this.monitor);
    this.controller.abort();
    try{this.parked?.close();}finally{void this.endpoint.close().finally(()=>this.finished(this.committed));}
  }

  private checkOpen():void{
    if(this.phase==='closed'||performance.now()>=this.deadline||!installationEnabled(this.home)||activeRuntime(this.home)?.buildId!==this.descriptor.buildId)throw rejected();
  }

  private async handle(method:string,params:unknown,socket:Socket):Promise<unknown>{
    this.checkOpen();
    if(method==='status')return{phase:this.phase,offered:this.parked?.offers.length??0};
    if(method==='abort'){
      if(this.claimant&&this.claimant.socket!==socket)throw rejected();
      this.controller.abort();
      setImmediate(()=>this.close());return{aborting:true};
    }
    if(method==='seal'&&this.phase==='prepared')return this.sealCurrent(socket);
    if(method==='claim'&&this.phase==='sealed')return this.claim(params,socket);
    if(this.phase!=='claimed'||this.claimant?.socket!==socket||!this.parked)throw rejected();
    if(method==='verify'){
      const {id}=z.object({id:z.string()}).parse(params);
      return{live:performance.now()<this.acquisitionDeadline&&!this.outcomes.has(id)&&this.parked.live(id)};
    }
    if(method==='resolve'){
      const {ids,outcome}=z.object({ids:z.array(z.string()).min(1),outcome:z.enum(['held','transferred'])}).parse(params);
      const offered=new Map(this.parked.offers.map(offer=>[offer.id,offer] as const)),selected=new Set(ids);
      const groups=new Set(ids.map(id=>offered.get(id)?.groupId));
      if(groups.has(undefined)||this.parked.offers.some(offer=>groups.has(offer.groupId)&&!selected.has(offer.id))||
        ids.some(id=>this.outcomes.has(id)?this.outcomes.get(id)!==outcome:outcome==='transferred'&&(performance.now()>=this.acquisitionDeadline||!this.parked!.live(id))))throw rejected();
      for(const id of selected)if(!this.outcomes.has(id)){
        this.outcomes.set(id,outcome);this.parked.release(id);
      }
      return{resolved:true};
    }
    throw rejected();
  }

  private async sealCurrent(socket:Socket){
    this.phase='draining';
    const abort=()=>{if(!this.committed)this.controller.abort();};
    socket.once('close',abort);
    try{
      await startupReservationDeadline(this.home,this.startupToken,this.descriptor.updater);
      if(await probe(this.descriptor.updater,remainingTime(this.acquisitionDeadline,1500))!=='alive'||this.controller.signal.aborted||socket.destroyed)throw rejected();
      const parked=await this.seal({updateId:this.descriptor.updateId,signal:this.controller.signal,commit:()=>{
        this.checkOpen();remainingTime(this.acquisitionDeadline,CUSTODY_LIMIT_MS);
        if(this.controller.signal.aborted)throw rejected();
        this.committed=true;this.phase='sealing';
      }});
      this.parked=parked;
      try{this.checkOpen();}catch(error){parked.close();throw error;}
      this.phase='sealed';return{phase:this.phase,offered:parked.offers.length};
    }catch(error){setImmediate(()=>this.close());throw error;}
    finally{socket.off('close',abort);}
  }

  private async claim(raw:unknown,socket:Socket){
    const {identity,buildId}=z.object({identity:generationSchema,buildId:z.string()}).parse(raw);
    const offered=this.descriptor;
    if(buildId!==offered.buildId||identity.pid===offered.expected.pid||identity.pid===offered.updater.pid)throw rejected();
    await startupReservationDeadline(this.home,this.startupToken,offered.updater);
    const timeout=remainingTime(this.acquisitionDeadline,1500);
    const [updater,successor]=await probeProcesses([offered.updater,identity].map(owner=>({pid:owner.pid,procStart:owner.startedAt})),timeout);
    if(updater!=='alive'||successor!=='alive')throw rejected();
    this.checkOpen();
    if(this.phase!=='sealed'||this.controller.signal.aborted||socket.destroyed)throw rejected();
    this.phase='claimed';this.claimant={identity,socket};
    socket.once('close',()=>this.close());
    return{offers:this.parked!.offers};
  }
}
