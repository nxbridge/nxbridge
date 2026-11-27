import { randomUUID } from 'node:crypto';
import { externalTerminalOrigin } from '../../hosts/user-origin.js';
import { brokerCall, serviceOrigin } from '../../ipc/ipc.js';
import {bridgeHome} from '../../shared/local-paths.js';
import {delay} from '../../shared/time.js';
import {BridgeError,DeliveryError,describeError} from '../../shared/errors.js';
import type { TargetSelector } from '../../broker/pairing/admin-target.js';
import type { RestartSelector } from '../../hosts/codex/codex-restart.js';
import { restartPendingText, restartRequestIdText, restartUnknownText } from './restart-text.js';

interface RestartResult {text:string}
type RestartQueryResult=RestartResult|{request_id:string;state:'pending'};
const pollable=new Set(['restart_result_missing','starting','stopping','handoff_retry','disconnected','timeout','broker_timeout','ENOENT','ECONNREFUSED','ECONNRESET','EPIPE']);
const withRequestText=(result:RestartResult,requestId:string):RestartResult=>({...result,text:result.text+'\n'+restartRequestIdText(requestId)});
const withRequestError=(error:unknown,requestId:string):never=>{
  if(error instanceof Error)error.message+='\n'+restartRequestIdText(requestId);
  throw error;
};
const readResult=(requestId:string,home:string,deadline?:number)=>brokerCall<RestartQueryResult>('codex.restart.result',{request_id:requestId},{home,start:false,timeout:3000,deadline});

export async function restartResultCommand(requestId:string,home=bridgeHome()):Promise<RestartResult>{
  try{
    const result=await readResult(requestId,home);
    return 'state' in result?{text:restartPendingText(requestId)}:withRequestText(result,requestId);
  }catch(error){
    const code=(error as {code?:string}).code??'';
    if(code==='restart_result_missing'||code==='restart_result_unknown')throw new DeliveryError(code,restartUnknownText(requestId),true);
    return withRequestError(error,requestId);
  }
}

export async function restartCommand(params:RestartSelector,home=bridgeHome(),onRequest?:(requestId:string)=>void):Promise<RestartResult>{
  try{return await performRestart(params,home,onRequest);}catch(error){
    if(!params.partner)throw error;
    const code=(error as {code?:string}).code??'restart_failed',retry=['session_busy','subagent_active','question_pending','delivery_unsettled','context_transition','stale_context','pair_paused','history_not_persisted','restart_in_progress'].includes(code),message=describeError(error)+(retry?' 完成待处理工作并确认空闲后再尝试一次；不要轮询重启。':' 停止自动重试，将原因告知用户。');
    if(error instanceof DeliveryError)throw new DeliveryError(code,message,error.uncertain);
    throw new BridgeError(code,message);
  }
}

async function performRestart(params:RestartSelector,home:string,onRequest?:(requestId:string)=>void):Promise<RestartResult>{
  const target=await brokerCall<{owner:string;session_id?:string;peer_id?:string;pair_id?:string;text?:string;runtime?:{pid:number;procStart:string};broker?:{pid:number;generation?:string;windowsIndependent?:boolean}}>('codex.restart.target',params,{home});
  if(target.owner==='user'&&target.text)return{text:target.text};
  const runtime=target.runtime;
  const origin=await serviceOrigin();
  if(runtime&&origin.some(value=>value.pid===runtime.pid&&value.startedAt===runtime.procStart)||target.session_id&&[process.env.CODEX_THREAD_ID,process.env.CODEX_SESSION_ID].includes(target.session_id))throw new BridgeError('self_restart','请从目标 Codex 之外的终端执行重启；当前命令属于目标宿主，停止它会同时中断调用方。');
  if(process.platform==='win32'&&!target.broker?.windowsIndependent)throw new BridgeError('bootstrap_required','协调器尚未核验为独立后台，请先在普通 PowerShell 运行 nxbridge bootstrap；本次未停止任何会话。');
  const request_id=randomUUID(),deadline=performance.now()+180000;
  onRequest?.(request_id);
  try{return withRequestText(await brokerCall<RestartResult>('codex.restart',{...(params.partner?{...params,peer_id:target.peer_id,pair_id:target.pair_id,session_id:target.session_id}:target.peer_id?{peer_id:target.peer_id}:{...params,session_id:target.session_id??params.session_id}),request_id,...(target.broker?.generation?{broker_pid:target.broker.pid,broker_generation:target.broker.generation}:{})},{home,start:false,mutation:true,timeout:60000,deadline}),request_id);}
  catch(error){
    if(!(error instanceof DeliveryError&&error.uncertain))return withRequestError(error,request_id);
                                
    let pending=false;
    while(performance.now()<deadline){
      try{
        const result=await readResult(request_id,home,deadline);
        if(!('state' in result))return withRequestText(result,request_id);
        pending=true;
      }catch(resultError){
        const {code='',operation}=resultError as {code?:string;operation?:string};
        if(operation==='codex.restart')return withRequestError(resultError,request_id);
        if(code==='restart_result_unknown'){pending=false;break;}
        if(!pollable.has(code))return withRequestError(resultError,request_id);
        if(code==='restart_result_missing')pending=false;
      }
      const remaining=deadline-performance.now();
      if(remaining>0)await delay(Math.min(500,remaining));
    }
    throw new DeliveryError(pending?'restart_pending':'restart_unconfirmed',pending?restartPendingText(request_id):restartUnknownText(request_id),true);
  }
}

export async function recoverCommand(params:TargetSelector&{turn_id?:string;preview?:boolean;recycle?:true;incident_id?:string;evidence_revision?:string;reason?:string},home=bridgeHome()):Promise<unknown>{
  if(params.recycle){
    try{return await brokerCall('codex.recover',{...params,terminal_origin:params.preview?undefined:await externalTerminalOrigin()},
      {home,start:false,mutation:!params.preview,timeout:params.preview?15000:75000});}
    catch(error){
      if(error instanceof DeliveryError&&error.uncertain)try{return await brokerCall('codex.recover',{...params,preview:true},{home,start:false,timeout:3000});}catch{}
      throw error;
    }
  }
  const target=await brokerCall<{peer_id:string;runtime?:{pid:number;procStart:string}}>('codex.recover',{...params,turn_id:undefined,preview:true},{home});
  if(params.preview)return target;
  if(!params.preview){
    const runtime=target.runtime;
    if(runtime&&(await serviceOrigin()).some(p=>p.pid===runtime.pid&&p.startedAt===runtime.procStart))throw new BridgeError('self_recovery','请从目标 Codex 之外的终端执行恢复，避免中断正在执行恢复命令的 turn。');
  }
  const request_id=randomUUID();
  try{return await brokerCall('codex.recover',{peer_id:target.peer_id,turn_id:params.turn_id,preview:false,request_id,terminal_origin:await externalTerminalOrigin()},{home,start:false,mutation:true,timeout:45000});}
  catch(error){
    if(error instanceof DeliveryError&&error.uncertain)try{return await brokerCall('codex.recover.result',{request_id},{home,start:false,timeout:3000});}catch{}
    throw error;
  }
}
