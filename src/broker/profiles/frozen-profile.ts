import { readFileSync } from 'node:fs';
import path from 'node:path';
import { atomicWriteText } from '../../storage/atomic-file.js';
import {stateDirectory} from '../../storage/state-directory.js';
import {BridgeError,describeError} from '../../shared/errors.js';
import {type BridgeConfig,type Host,type ModelProfile} from '../../types.js';

export type { ModelProfile } from '../../types.js';
export interface FrozenProfile extends ModelProfile {at:number}
interface FrozenProfileState {version:1;profiles:Partial<Record<Host,FrozenProfile>>}
const hosts:Host[]=['claude','codex'];
export const profileFields=['model','effort','serviceTier'] as const;
export type ProfileField=(typeof profileFields)[number];
export type ProfileSources=Partial<Record<ProfileField,'recommended'|'host'|'frozen'|'config'|'explicit'>>;
const recommendations:Record<Host,ModelProfile>={
  codex:{model:'gpt-6.2-sol',effort:'xhigh',serviceTier:'standard'},
  claude:{model:'claude-opus-5-5',effort:'xhigh'},
};
const fieldsFor=(host:Host)=>host==='codex'?profileFields:profileFields.slice(0,2);
export const freezesProfile=(config:BridgeConfig,host:Host)=>config.partnerProfiles?.[host]?.source==='frozen';
const filename=(home:string)=>path.join(home,'frozen-profiles.json');
function readState(home:string):FrozenProfileState{
  let text:string;
  try{text=readFileSync(filename(home),'utf8');}
  catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')return{version:1,profiles:{}};throw error;}
  let state:FrozenProfileState;
  try{state=JSON.parse(text);}catch{throw new BridgeError('invalid_frozen_profiles',`${filename(home)}：固定的模型和推理强度记录格式无效；原文件未修改。新建依赖固定值的后台搭档会先停下。修复：删掉这个文件（下一个新建的后台搭档会重新记录），或把对应的 partners.<codex|claude>.profile_source 改回 host。`);}
  if(!state||state.version!==1||!state.profiles||typeof state.profiles!=='object'||Array.isArray(state.profiles))
    throw new BridgeError('invalid_frozen_profiles',`${filename(home)}：固定的模型和推理强度记录版本或结构无效；原文件未修改。新建依赖固定值的后台搭档会先停下。修复：删掉这个文件（下一个新建的后台搭档会重新记录），或把对应的 partners.<codex|claude>.profile_source 改回 host。`);
  return state;
}
function saveState(home:string,state:FrozenProfileState):void{
  stateDirectory(home,0o700);
  atomicWriteText(filename(home),JSON.stringify(state,null,2)+'\n');
}
export function frozenProfiles(home:string,config:BridgeConfig):Partial<Record<Host,FrozenProfile>>{
  return frozenProfileStatus(home,config).profiles;
}
export function frozenProfileStatus(home:string,config:BridgeConfig):{profiles:Partial<Record<Host,FrozenProfile>>;warnings:string[]}{
  const active=hosts.filter(host=>freezesProfile(config,host));
  if(!active.length)return{profiles:{},warnings:[]};
  let state:FrozenProfileState;try{state=readState(home);}catch(error){return{profiles:{},warnings:[describeError(error)]};}
  const profiles:Partial<Record<Host,FrozenProfile>>={};
  for(const host of active){const profile=state.profiles[host];if(profile)profiles[host]={...profile};}
  return{profiles,warnings:[]};
}
                             
export function profileDefaults(home:string,config:BridgeConfig,host:Host,explicit:ModelProfile={}):ModelProfile{
  return resolveProfileDefaults(home,config,host,explicit).profile;
}

export function resolveProfileDefaults(home:string,config:BridgeConfig,host:Host,explicit:ModelProfile={}):{profile:ModelProfile;sources:ProfileSources}{
  const defaults=config.partnerProfiles?.[host],mode=defaults?.source??'recommended',fields=fieldsFor(host);
                                                
  const captured=freezesProfile(config,host)&&(['model','effort'] as const).some(key=>explicit[key]===undefined&&defaults?.[key]===undefined)?readState(home).profiles[host]:undefined;
  const fromSource=mode==='recommended'?recommendations[host]:captured;
  const entries=fields.map(field=>[field,explicit[field]??defaults?.[field]??fromSource?.[field]] as const);
  const sources=Object.fromEntries(fields.map(field=>[field,explicit[field]!==undefined?'explicit':defaults?.[field]!==undefined?'config':mode==='recommended'?'recommended':captured?.[field]!==undefined?'frozen':'host'])) as ProfileSources;
  return{profile:Object.fromEntries(entries.filter(([,value])=>value!==undefined)),sources};
}
                                      
export function captureFrozenProfile(home:string,config:BridgeConfig,host:Host,observed:ModelProfile):void{
  if(!freezesProfile(config,host))return;
  const model=typeof observed.model==='string'&&observed.model.trim().length>0?observed.model:undefined;
  const effort=(typeof observed.effort==='string'&&observed.effort.trim().length>0||typeof observed.effort==='number'&&Number.isFinite(observed.effort))?observed.effort:undefined;
                                        
  if(model===undefined)return;
  let state:FrozenProfileState;try{state=readState(home);}catch{return;}
  const previous=state.profiles[host];
  if(previous?.model!==undefined&&previous.model!==model)return;
  if(previous?.model!==undefined&&(previous.effort!==undefined||effort===undefined))return;
  state.profiles[host]={...previous,at:previous?.at??Date.now(),model,...(previous?.effort!==undefined?{effort:previous.effort}:effort===undefined?{}:{effort})};
  saveState(home,state);
}
export function syncFrozenProfiles(home:string,config:BridgeConfig):void{
  let state:FrozenProfileState;try{state=readState(home);}catch{return;}let changed=false;
  for(const host of hosts)if(!freezesProfile(config,host)&&state.profiles[host]!==undefined){delete state.profiles[host];changed=true;}
  if(changed)saveState(home,state);
}
