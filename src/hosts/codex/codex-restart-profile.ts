import { unlinkSync } from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { managedMarker } from '../../install/installation.js';
import { currentLinuxBootId } from '../../process/owned-processes.js';
import { ownedProcessState } from '../../process/process-stop.js';
import { BridgeError } from '../../shared/errors.js';
import { atomicJson, readJson, sameDirectory } from '../../shared/files.js';
import { stateDirectory } from '../../storage/state-directory.js';
import type { ManagedRuntime } from '../managed-runtimes.js';
import { planCodexResume, requireExactRestartProfile, verifyRestartProfileCurrent, type SessionProfile } from './codex-session-profile.js';
import {acceptedTakeoverProfile,restartTakeoverEvidence} from './codex-takeover-permission.js';
import type {AcceptedTakeoverPermission,TakeoverPermissionEvidence} from '../takeover-permission.js';

interface RestartProfile {
  version:1;owner:string;peerId:string;configHome:string;sourceId:string;pid:number;procStart:string;bootId?:string;profile:SessionProfile;recycle?:true;
}
const file=(home:string,peerId:string)=>path.join(home,'run','codex-restart-'+createHash('sha256').update(peerId).digest('hex')+'.json');

function readRestartProfile(home:string,peerId:string):RestartProfile|undefined{
  try{
    const value=readJson<RestartProfile>(file(home,peerId));
    if(value.version!==1||value.owner!==managedMarker(home)||value.peerId!==peerId||!value.configHome||!value.sourceId
      ||!Number.isSafeInteger(value.pid)||value.pid<=0||!value.procStart||!value.profile?.verified||!value.profile.complete||(!value.profile.live&&!value.recycle))
      throw new BridgeError('restart_profile_unverified','自动重启的原会话配置记录无法核验，消息继续保留。');
    return value;
  }catch(error){
    if((error as NodeJS.ErrnoException).code==='ENOENT')return;
    throw error;
  }
}

export function pendingRestartTakeoverEvidence(home:string,peerId:string,sessionId:string):TakeoverPermissionEvidence|undefined{
  const previous=readRestartProfile(home,peerId);
  if(previous?.profile.sessionId!==sessionId)return;
  const evidence=restartTakeoverEvidence(previous.profile);
  if(!evidence)throw new BridgeError('restart_profile_unverified','无法核验重启档案的权限引用。');
  return evidence;
}

                                
export function prepareRestartProfile(home:string,runtime:ManagedRuntime,profile:SessionProfile,options:{recycle?:true}={}):()=>void{
  if(options.recycle)planCodexResume(profile.sessionId,profile,profile.settings.cwd??'',{exact:true});
  else requireExactRestartProfile(profile);
  if(runtime.kind!=='owned'||!runtime.peerId||!runtime.configHome||!runtime.pid||!runtime.procStart
    ||runtime.sessionId&&runtime.sessionId!==profile.sessionId)
    throw new BridgeError('restart_profile_unverified','无法精确保存原会话的实时权限及运行设置，未自动停止原进程。');
  const value:RestartProfile={version:1,owner:managedMarker(home),peerId:runtime.peerId,configHome:runtime.configHome,
    sourceId:runtime.id,pid:runtime.pid,procStart:runtime.procStart,bootId:runtime.bootId,profile,...options};
  return()=>{
    const previous=readRestartProfile(home,value.peerId);
    if(previous?.profile.sessionId===profile.sessionId&&previous.sourceId!==value.sourceId)
      throw new BridgeError('restart_profile_unverified','原会话上一次自动重启的配置尚未核验，未自动停止当前进程。');
    const filename=file(home,value.peerId);stateDirectory(path.dirname(filename),0o700);atomicJson(filename,value);
  };
}

export async function pendingRestartVerification(home:string,peerId:string,sessionId:string,configHome:string,accepted?:AcceptedTakeoverPermission):Promise<ManagedRuntime['sessionVerification']>{
  const previous=readRestartProfile(home,peerId);
  if(!previous||previous.profile.sessionId!==sessionId)return;
  if(!sameDirectory(previous.configHome,configHome))
    throw new BridgeError('original_home_changed','自动重启的配置记录属于原 Codex 配置目录，未在另一目录恢复。');
  const boot=currentLinuxBootId();
  if((!previous.bootId||!boot||previous.bootId===boot)&&await ownedProcessState({pid:previous.pid,startedAt:previous.procStart})!=='dead')
    throw new BridgeError('runtime_launch_unconfirmed','自动重启的原 Codex 进程尚未核验退出，未启动替代进程。');
  const profile=await acceptedTakeoverProfile(previous.profile,accepted);
  await verifyRestartProfileCurrent(profile,{exact:previous.recycle});
  return{sessionId,profile,restartSourceId:previous.sourceId};
}

                                         
export function completeRestartProfile(home:string,runtime:ManagedRuntime):void{
  const verification=runtime.sessionVerification;
  if(!runtime.peerId||!runtime.configHome||!verification?.restartSourceId)return;
  const previous=readRestartProfile(home,runtime.peerId);
  if(!previous||previous.sourceId!==verification.restartSourceId||previous.profile.sessionId!==verification.sessionId
    ||previous.sourceId===runtime.id||!sameDirectory(previous.configHome,runtime.configHome))return;
  unlinkSync(file(home,runtime.peerId));
}
