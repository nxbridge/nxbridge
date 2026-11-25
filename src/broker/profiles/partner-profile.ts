import { z } from 'zod';
import type { Store } from '../../storage/store.js';
import {BridgeError,DeliveryError,describeError} from '../../shared/errors.js';
import {type BridgeConfig,type ModelProfile,type Peer} from '../../types.js';
import { captureFrozenProfile, freezesProfile, profileFields, resolveProfileDefaults, type ProfileField, type ProfileSources } from './frozen-profile.js';

export const modelProfileSchema=z.object({model:z.string().trim().min(1).optional(),effort:z.union([z.string().trim().min(1),z.number().finite().nonnegative()]).optional()});
export const codexProfileSchema=modelProfileSchema.extend({serviceTier:z.enum(['standard','fast','flex']).optional()});
export const PARTNER_PROFILE_TOOL={name:'partner_profile',description:'view/change model + effort of your current partner (nxbridge-owned bg only; user windows refused). set: model and/or effort, any value host accepts (no list; strengths/cost: partner skill). never interrupts: busy -> pending, applied at idle. result = effective (host-reported) + requested + status + delivery_held. host rejects -> partner keeps current profile; uncertain/mismatch -> delivery held till reconciled or reset. model switch resets partner prompt cache -> switch only for real reason. claude native bg: live change unsupported -> create new partner w/ profile. reset: drop member choice, live profile kept. global defaults: config propose (user confirms)',inputSchema:{type:'object' as const,properties:{action:{type:'string',enum:['get','set','reset']},model:{type:'string'},effort:{anyOf:[{type:'string'},{type:'number',minimum:0}]}},additionalProperties:false},annotations:{readOnlyHint:false,destructiveHint:false,openWorldHint:false}};
export interface PartnerProfileIntent {
  requested:ModelProfile;source:'defaults'|'explicit';status:'pending'|'applied'|'failed';
  at:number;sessionId?:string;capture?:boolean;captureEffort?:false;explicitFields?:ProfileField[];sources?:ProfileSources;context?:string;failedAt?:number;blocked?:boolean;resolved?:ModelProfile;error?:{code:string;message:string};
}
export type EffectiveModelProfile=Omit<ModelProfile,'serviceTier'>&{requestedModel?:string;serviceTier?:string|null};
const key=(peer:Peer)=>'partnerProfile:'+peer.id;
type ProfileObservation={model?:string|null;effort?:string|number|null;serviceTier?:string|null};
export const profileMatches=(wanted:ModelProfile,actual:ProfileObservation)=>profileFields.every(field=>wanted[field]===undefined||wanted[field]===actual[field]);
export const hasProfile=(profile:ModelProfile)=>profileFields.some(field=>profile[field]!==undefined);
const rejected=(code:string)=>['-32600','-32601','-32602','profile_change_unsupported','profile_service_tier_unavailable','claude_control_rejected'].includes(code);
const verificationMessage=(peer:Peer)=>`后台搭档实际用的模型、推理强度或服务档位和所选的不一致，新消息先不发。运行 nxbridge status 核对，或用 nxbridge partner-profile ${peer.host} --reset 清除这个搭档的单独选择。`;
export function profileChangeFailure(error:unknown,submitted:boolean,accepted=false):DeliveryError{
  const failure=error as {code?:unknown;phase?:string;uncertain?:boolean},code=String(failure.code??'profile_change_failed');
                                              
  const uncertain=submitted&&(accepted||failure.uncertain===true||failure.phase==='enqueue'||failure.phase!=='prepare'&&!rejected(code));
  return Object.assign(new DeliveryError(code,describeError(error),uncertain),{phase:uncertain?'enqueue':'prepare'});
}

                                     
export class PartnerProfiles {
  constructor(private store:Store,private home:string,private config:BridgeConfig){}
  intent(peer:Peer):PartnerProfileIntent|undefined{return this.store.setting<PartnerProfileIntent>(key(peer))??undefined;}
  requested(peer:Peer):ModelProfile{return this.intent(peer)?.requested??{};}
  launchError(peer:Peer,error:unknown):unknown{
    const intent=this.intent(peer),model=intent?.requested.model;
    if(peer.sessionId||intent?.sources?.model!=='recommended'||!model)return error;
    const failure=error as {code?:unknown;exitCode?:number;cause?:{stderr?:string}};
    const text=String(failure.exitCode===1?failure.cause?.stderr??'':describeError(error)).trim();
    const explicit=['model_not_found','model_not_available','model_not_supported'].includes(String(failure.code));
    const nativeRejected=failure.exitCode===1||String(failure.code)==='-32602';
    const reason=text.split(/\r?\n/).find(line=>line.includes(model)&&/\b(?:unknown model|invalid model|unsupported model|model(?:\s+.*?)?\s+(?:is\s+)?(?:not found|not available|not supported|unavailable|does not exist))\b/i.test(line));
    if(!explicit&&!(nativeRejected&&reason))return error;
    return new BridgeError('recommended_model_unavailable',`nxbridge 推荐的模型 ${model} 在当前模型服务里用不了（${(reason??text).slice(0,300)}）。可以让 AI 新建时指定别的模型，或在终端运行 nxbridge config set partners.${peer.host}.profile_source host 改用你自己的配置。`);
  }
  context(peer:Peer):string{const endpoint=this.store.endpoint(peer.id);return JSON.stringify([peer.sessionId,endpoint?.epoch,endpoint?.pid,endpoint?.procStart]);}
  clear(peer:Peer):void{this.store.setSetting(key(peer),null);}
  waiting(peer:Peer,error:unknown):void{const intent=this.intent(peer);if(intent)this.store.setSetting(key(peer),{...intent,error:{code:String((error as BridgeError).code??'profile_unverified'),message:describeError(error)}});}
  initialize(peer:Peer):void{
    if(peer.owner!=='nxbridge'||peer.sessionId||this.intent(peer))return;
    const {profile:requested,sources}=resolveProfileDefaults(this.home,this.config,peer.host);
    this.store.setSetting(key(peer),{requested,sources,source:'defaults',status:'pending',...(freezesProfile(this.config,peer.host)?{capture:true}:{}),at:Date.now()} satisfies PartnerProfileIntent);
  }
  set(peer:Peer,profile:ModelProfile,reset=false):void{
    const old=this.intent(peer),changed=profileFields.filter(field=>profile[field]!==undefined);
    const defaults=!peer.sessionId&&(reset||!old)?resolveProfileDefaults(this.home,this.config,peer.host,reset?{}:profile):{profile:reset?{}:old?.requested??{},sources:reset?{}:old?.sources??{}};
    const requested={...defaults.profile,...(reset?{}:profile)},explicitFields=reset?[]:[...new Set([...(old?.explicitFields??[]),...changed])];
    const sources:ProfileSources={...defaults.sources,...Object.fromEntries(explicitFields.map(field=>[field,'explicit']))};
    this.store.setSetting(key(peer),{requested,sources,explicitFields,source:reset?'defaults':'explicit',status:hasProfile(requested)?'pending':'applied',at:Math.max(Date.now(),(old?.at??0)+1),...(peer.sessionId?{sessionId:peer.sessionId}:{}),...(!peer.sessionId&&freezesProfile(this.config,peer.host)&&!explicitFields.includes('model')?{capture:true,...(explicitFields.includes('effort')?{captureEffort:false as const}:{})}:{})} satisfies PartnerProfileIntent);
  }
  syncConfiguration():void{
    for(const peer of this.store.peers()){const intent=this.intent(peer);if(intent?.capture&&!freezesProfile(this.config,peer.host))this.store.setSetting(key(peer),{...intent,capture:false});}
  }
  observed(peer:Peer,observation?:ProfileObservation):void{
    const intent=this.intent(peer);if(!intent||peer.owner!=='nxbridge')return;
    let changed=false;
    if(intent.capture&&observation?.model){
      captureFrozenProfile(this.home,this.config,peer.host,{model:observation.model,...(intent.captureEffort!==false&&observation.effort!==undefined&&observation.effort!==null?{effort:observation.effort}:{})});
      if(intent.captureEffort===false||observation.effort!==undefined&&observation.effort!==null){intent.capture=false;changed=true;}
    }
    const expected=intent.resolved??intent.requested;
    if((intent.status==='pending'||intent.status==='failed'&&intent.blocked)&&observation&&profileMatches(expected,observation)){intent.status='applied';intent.resolved={...expected};intent.sessionId=peer.sessionId??undefined;intent.error=undefined;intent.blocked=false;changed=true;}
    if(intent.status==='applied'&&hasProfile(expected)&&observation&&profileMatches(expected,observation)){const context=this.context(peer);if(intent.context!==context){intent.context=context;changed=true;}}
    if(intent.status==='applied'&&observation&&profileFields.some(field=>observation[field]!==undefined&&(observation[field]!==null||field==='serviceTier')&&expected[field]!==undefined&&observation[field]!==expected[field])){
      intent.status='failed';intent.blocked=true;intent.failedAt=Date.now();intent.error={code:'profile_verification_failed',message:verificationMessage(peer)};changed=true;
    }
    if(changed)this.store.setSetting(key(peer),intent);
  }
  applied(peer:Peer,actual:EffectiveModelProfile):void{
    const intent=this.intent(peer);if(!intent)return;
    const expected={...(intent.resolved??intent.requested),...(intent.requested.model!==undefined&&actual.requestedModel===intent.requested.model&&actual.model?{model:actual.model}:{})};
    this.store.updatePeer(peer.id,Object.fromEntries(profileFields.filter(field=>actual[field]!==undefined).map(field=>[field,actual[field]])));
    if(!profileMatches(expected,actual))throw new BridgeError('profile_verification_failed',verificationMessage(peer));
    this.store.setSetting(key(peer),{...intent,status:'applied',resolved:expected,sessionId:peer.sessionId??undefined,context:this.context(peer),blocked:false,failedAt:undefined,error:undefined});
    this.observed(this.store.peer(peer.id),actual);
  }
  failed(peer:Peer,error:unknown):void{
    const intent=this.intent(peer),code=String((error as BridgeError).code??'profile_change_failed');
    if(intent)this.store.setSetting(key(peer),{...intent,status:'failed',failedAt:Date.now(),blocked:code==='profile_verification_failed'||((error as {uncertain?:boolean}).uncertain??!rejected(code)),error:{code,message:code==='profile_verification_failed'?verificationMessage(peer):describeError(error)}});
  }
  blocks(peer:Peer):boolean{if(peer.owner!=='nxbridge')return false;const intent=this.intent(peer);return !!intent&&hasProfile(intent.requested)&&(intent.status==='failed'?intent.blocked!==false:intent.status==='pending'||intent.context!==this.context(peer));}
  view(peer:Peer){
    const intent=this.intent(peer),fields=profileFields.filter(field=>peer[field]!==undefined);
    const sources=Object.fromEntries(fields.filter(field=>intent?.requested[field]===undefined).map(field=>[field,'host']));
    return{effective:Object.fromEntries(fields.map(field=>[field,peer[field]])),sources:{...sources,...intent?.sources},delivery_held:this.blocks(peer),...(intent?{requested:intent.requested,status:intent.status,source:intent.source,at:intent.at,...(intent.error?{error:intent.error}:{})}:{status:'host' as const})};
  }
}
