import {CUSTODY_LIMIT_MS} from '../../broker/custody-protocol.js';
import {delay} from '../../shared/time.js';
import {brokerCall} from '../../ipc/ipc.js';

type Result={status:string;expires_at?:string};
const transient=new Set(['handoff_retry','starting','stopping','ENOENT','ECONNREFUSED','ECONNRESET','EPIPE','disconnected','broker_timeout','timeout']);

                             
export async function waitForQuestion<T extends Result>(poll:(connectionDeadline?:number)=>Promise<T>):Promise<T>{
  let expiresAt=Infinity,retryUntil:number|undefined;
  for(;;){
    try{
                                                 
      const value=await poll(retryUntil);
      if(value.status!=='pending'&&value.status!=='awaiting_user')return value;
      if(value.expires_at){
        const expiry=Date.parse(value.expires_at);
        if(Number.isFinite(expiry))expiresAt=Math.min(expiresAt,performance.now()+expiry-Date.now());
      }
      retryUntil=undefined;
    }catch(error){
      if(!transient.has((error as {code?:string}).code??''))throw error;
      retryUntil??=Math.min(expiresAt,performance.now()+CUSTODY_LIMIT_MS);
      const remaining=retryUntil-performance.now();
      if(remaining<=0)throw error;
      await delay(Math.min(200,remaining));
    }
  }
}

export function waitForPartnerQuestion<T extends Result>(params:object,home?:string):Promise<T>{
  return waitForQuestion(connectionDeadline=>brokerCall<T>('question.wait',{...params,handoff:1},{home,start:false,connectionDeadline}));
}
