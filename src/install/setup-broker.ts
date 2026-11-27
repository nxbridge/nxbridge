import {embeddedSource} from "../shared/embedded-source.js";
import {reserveBrokerStartup,startupReservationDeadline,type StartupReservation} from '../broker/broker-startup.js';
import {createRequire} from 'node:module';
import path from 'node:path';
import {custodyCall} from '../broker/custody-client.js';
import {CUSTODY_MANIFEST_ENTRY,CUSTODY_PROTOCOL,custodyDeadlines,emptyCustodyResult,parseCustodyDescriptor,sameGeneration,type CustodyDescriptor,type CustodyGeneration,type CustodyResult} from '../broker/custody-protocol.js';
import type {brokerCall} from '../ipc/ipc.js';
import {processIdentity,probeProcesses} from '../process/process.js';
import {BridgeError} from '../shared/errors.js';
import {delay} from '../shared/time.js';
import {remainingTime} from '../shared/time-budget.js';
import type {RuntimeManifest} from './runtime-install.js';

                         
const knownPreHandoff=new Set(['0.1.1-20462655d259','0.1.1-b5073b5c0020','0.1.1-2441d4ab28d8']);
type Ping={pid:number;runtime?:{generation?:string;buildId?:string;windowsIndependent?:boolean};handoff?:{protocol:number;pending?:boolean}&CustodyResult};
type ServingGeneration={pid:number;generation?:string;buildId?:string};
export type BrokerUpdate={restarted:boolean;servingGeneration:ServingGeneration|null;handoff:{supported:boolean|null}&CustodyResult;windowsIndependent?:boolean;error?:unknown;retryCommand?:string};
const absent=(error:unknown)=>['ENOENT','ECONNREFUSED'].includes((error as NodeJS.ErrnoException).code??'');
const generation=(value:Ping):ServingGeneration=>({pid:value.pid,generation:value.runtime?.generation,buildId:value.runtime?.buildId});
const probe=(identity:CustodyGeneration,timeout=1500)=>probeProcesses([{pid:identity.pid,procStart:identity.startedAt}],timeout).then(([state])=>state);

async function waitForLegacyQuestions(home:string,reservation:StartupReservation,updater:CustodyGeneration,progress?:(message:string)=>void):Promise<void>{
  const {DatabaseSync}=createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');
  let read:InstanceType<typeof DatabaseSync>|undefined;
  const pending=()=>read!.prepare(                          embeddedSource("sql", `SELECT max(expires_at) AS expiry,count(*) AS n FROM questions WHERE status IN ('pending','awaiting_user')`)).get() as {expiry:number|null;n:number};
  const deferred=()=>new BridgeError('handoff_legacy_wait','旧协调器仍有未完成的问题。处理完成后运行 nxbridge update 接班。');
  try{
                                                
    read=new DatabaseSync(path.join(home,'nxbridge.db'),{readOnly:true,timeout:0});
    let active=pending();if(!active.n)return;
    const reservedUntil=await startupReservationDeadline(home,reservation.token,updater);
    const deadline=performance.now()+Math.min((active.expiry??Date.now())-Date.now(),reservedUntil-Date.now()-75000);
    if(deadline>performance.now())progress?.('正在等待旧协调器的问题结束，等待期限沿用原问题和本次启动预约。');
    while(active.n&&performance.now()<deadline){
      await delay(Math.min(400,Math.max(0,deadline-performance.now())));active=pending();
    }
                                 
    if(active.n)throw deferred();
  }catch(error){
    if(error instanceof BridgeError)throw error;
    throw new BridgeError('handoff_legacy_unverified','无法核验旧协调器的问题状态，已保留该进程。稍后运行 nxbridge update 重试。');
  }finally{read?.close();}
}

export async function updateBroker(home:string,{buildId,files}:RuntimeManifest,broker:typeof brokerCall,backgroundPreflight?:()=>Promise<void>,progress?:(message:string)=>void):Promise<BrokerUpdate>{
  const result:BrokerUpdate={restarted:false,servingGeneration:null,handoff:{supported:null,...emptyCustodyResult()}};
  const began=performance.now();
  let phase='preflight';
  const checkpoint=(next:string,details:Record<string,unknown>={})=>{
    phase=next;
    progress?.('协调器更新进度 '+JSON.stringify({phase,at:new Date().toISOString(),elapsedMs:Math.round(performance.now()-began),buildId,...details}));
  };
  let reservation:StartupReservation|undefined,descriptor:CustodyDescriptor|undefined;
  let started=false;
  const read=()=>broker<Ping>('ping',{}, {home,start:false});
  const recordServing=(value:Ping)=>{result.servingGeneration=generation(value);result.windowsIndependent=value.runtime?.windowsIndependent;};
  try{
    checkpoint('preflight',{updaterPid:process.pid});
    await backgroundPreflight?.();
    const updater=await processIdentity(process.pid);
    reservation=await reserveBrokerStartup(home,updater);
    checkpoint('reserved',{updaterPid:updater.pid,updaterGeneration:updater.startedAt});
    const before=await read().catch(error=>{if(!absent(error))throw error;});
    let launch:CustodyDescriptor|undefined,deadline:number|undefined;
    if(before){
      recordServing(before);
      const old={pid:before.pid,startedAt:before.runtime?.generation??''};
      if(!old.startedAt)throw new BridgeError('broker_generation_unknown','无法核验当前协调器代次，更新等待确认。');
      if(before.handoff?.protocol===CUSTODY_PROTOCOL){
        result.handoff.supported=true;
        if(!files[CUSTODY_MANIFEST_ENTRY])throw new BridgeError('handoff_candidate_unsupported','候选版本不支持协调器交接，已保留正在服务的协调器。');
        checkpoint('prepare',{old:result.servingGeneration});
        descriptor=parseCustodyDescriptor(await broker('broker.handoff.prepare',{protocol:CUSTODY_PROTOCOL,expected:old,updater,buildId,startupToken:reservation.token},{home,start:false,timeout:5000}));
        if(!sameGeneration(descriptor.expected,old)||!sameGeneration(descriptor.updater,updater)||descriptor.buildId!==buildId)
          throw new BridgeError('handoff_descriptor_invalid','交接凭据与本次安装不匹配。');
        const cutover=custodyDeadlines(descriptor.expiresAt);
        deadline=cutover.deadline;
        checkpoint('seal',{old:result.servingGeneration,expiresAt:descriptor.expiresAt});
        let readiness:{phase:string;offered:number}|undefined;
        try{readiness=await custodyCall(home,descriptor,'seal',cutover.acquisitionDeadline,45000);}
        catch(error){
          readiness=await custodyCall<typeof readiness>(home,descriptor,'status',deadline).catch(()=>undefined);
          if(readiness?.phase!=='sealed'){
            if(await probe(old)!=='dead')throw error;
            readiness=undefined;
            deadline=undefined;
          }
        }
        if(readiness){
          if(readiness.phase!=='sealed')throw new BridgeError('handoff_not_ready','原协调器尚未释放数据库和公开地址。');
          result.handoff.held=readiness.offered;
          launch=descriptor;
        }
      }else if(before.handoff===undefined&&before.runtime?.buildId&&knownPreHandoff.has(before.runtime.buildId)){
        result.handoff.supported=false;
        checkpoint('legacy_drain',{old:result.servingGeneration});
        await waitForLegacyQuestions(home,reservation,updater,progress);
        await broker('shutdown',{releaseOwned:false,startupToken:reservation.token,expectedBroker:{pid:old.pid,generation:old.startedAt}},{home,start:false,mutation:true,timeout:60000});
        const confirmation=performance.now()+15000;
        while(await probe(old,remainingTime(confirmation,1500))!=='dead')await delay(remainingTime(confirmation,200));
      }else throw new BridgeError('handoff_unsupported','无法确认当前协调器支持本次交接，更新等待确认。');
    }
    await reservation.assertCurrent();
    started=true;
    checkpoint('wait_ready',{old:result.servingGeneration,offered:result.handoff.held,withCustody:!!launch});
    let after=await broker<Ping>('ping',{}, {home,reserved:true,custody:launch,deadline});
    const serving=generation(after),settledBy=deadline??performance.now()+30000;
                                            
    while(after.handoff?.pending){
      await delay(remainingTime(settledBy,100));
      after=await broker<Ping>('ping',{}, {home,start:false,timeout:remainingTime(settledBy,1500)});
      if(after.pid!==serving.pid||after.runtime?.generation!==serving.generation)
        throw new BridgeError('broker_changed','接班结果尚未结算时协调器代次已改变。');
    }
    recordServing(after);
    if(after.runtime?.buildId!==buildId||!after.runtime.generation||before&&after.runtime.generation===before.runtime?.generation&&after.pid===before.pid)
      throw new BridgeError('broker_update_unconfirmed','当前服务代次与本次安装不匹配。');
    result.restarted=true;
    if(after.handoff?.protocol===CUSTODY_PROTOCOL){
      result.handoff.supported??=true;
      result.handoff.used=after.handoff.used;result.handoff.transferred=after.handoff.transferred;
      result.handoff.held=Math.max(result.handoff.held-result.handoff.transferred,after.handoff.held);
    }
    checkpoint('ready',{serving:result.servingGeneration,handoff:result.handoff});
  }catch(error){
    result.error=error;
    checkpoint('failed',{failedAt:phase,code:(error as {code?:string})?.code,observed:result.servingGeneration,handoff:result.handoff,
      ...(process.platform==='win32'&&descriptor&&started?{startupLog:path.join(home,'run','broker-startup.log')}:{})});
    if(error instanceof BridgeError&&['handoff_legacy_wait','handoff_legacy_unverified'].includes(error.code))result.retryCommand='nxbridge update';
    if(descriptor&&!started){
      const {deadline}=custodyDeadlines(descriptor.expiresAt);
      await custodyCall(home,descriptor,'abort',deadline).catch(()=>{});
    }
    await read().then(recordServing,()=>{result.servingGeneration=null;result.windowsIndependent=undefined;});
  }finally{await reservation?.();}
  return result;
}
