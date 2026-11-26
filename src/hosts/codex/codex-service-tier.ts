import {BridgeError} from '../../shared/errors.js';

                                              
export const nativeServiceTier=(value:string)=>value==='standard'?'default':value==='fast'?'priority':value;
export const observedServiceTier=(value:unknown):string|null|undefined=>
  value==='default'?'standard':value==='priority'?'fast':typeof value==='string'||value===null?value:undefined;

export interface CodexServiceTierPauseDetail {code:'service_tier_mismatch'|'service_tier_unverified';saved?:string;live?:string}
export class CodexServiceTierError extends BridgeError {
  constructor(public pauseDetail:CodexServiceTierPauseDetail,reason?:string){
    super('profile_verification_failed',(pauseDetail.code==='service_tier_mismatch'?`恢复后的服务档位与原会话不符（原来 ${pauseDetail.saved??'未指定'}，现在 ${pauseDetail.live??'未指定'}）`:'恢复后没能确认服务档位和原来一致')+'，已暂停投递，消息已保留。'+(reason?' 原因：'+reason:''));
  }
}
