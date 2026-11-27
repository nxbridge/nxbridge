import path from 'node:path';
import { readFileSync, readdirSync, statSync, unlinkSync } from 'node:fs';
import { managedMarker } from '../install/installation.js';
import { retireDeadOwnedProcess, retireOwnedProcessRecord, ownedProcesses, ownedRecordState, ownedRecordPredatesBoot, ownedRecordMatchesGeneration, ownedContainerComplete, ownedContainerRetirementReady, isCurrentLinuxBoot, predatesLinuxBoot, processRegistryRoot, type OwnedProcessRecord } from './owned-processes.js';
import { probeProcesses } from './process.js';
import {DEFAULT_RETENTION} from '../config/defaults.js';
import {BridgeError,describeError} from '../shared/errors.js';
import {type RetentionConfig} from '../types.js';
import { managedRuntimes, managedRuntimeState, removeManagedRuntime, runtimeFor, type ManagedRuntime, type RuntimeRecord } from '../hosts/managed-runtimes.js';
import { capLog } from '../storage/logs.js';
import { claudeChild, type ClaudeChildRecord } from '../hosts/claude/claude-child.js';
import { nativeOwnership, nativeOwnershipRecords, type NativeOwnership } from './owned-native.js';
import { sameDirectory } from '../shared/files.js';

interface BoundGeneration {home:string;peerId?:string;launchId?:string;native?:{pid:number;startedAt:string}}
const nativeGeneration=(binding:NativeOwnership|undefined):BoundGeneration|undefined=>binding?.phase==='bound'&&!binding.releasedToUser&&typeof binding.id==='string'&&!!binding.id?
  {home:binding.home,peerId:binding.peerId,launchId:binding.id,native:binding.native}:undefined;
const runtimeGeneration=(home:string,runtime:ManagedRuntime|undefined):BoundGeneration|undefined=>runtime?.kind==='owned'&&runtime.state==='ready'&&!predatesLinuxBoot(runtime.bootId)?
  {home,peerId:runtime.peerId,launchId:runtime.id,native:runtime.pid&&runtime.procStart?{pid:runtime.pid,startedAt:runtime.procStart}:undefined}:undefined;
const childGeneration=(home:string,child:ClaudeChildRecord|undefined):BoundGeneration|undefined=>child?.phase==='active'&&child.native&&(process.platform!=='linux'||isCurrentLinuxBoot(child.bootId))?
  {home,peerId:child.peerId,launchId:child.launchId,native:child.native}:undefined;
function supersededNativeGeneration(record:OwnedProcessRecord,binding:BoundGeneration|undefined):boolean{
  if(record.releasedToUser||!binding?.native||!Number.isSafeInteger(binding.native.pid)||binding.native.pid<=0||typeof binding.native.startedAt!=='string'||!binding.native.startedAt||
    !sameDirectory(record.home,binding.home)||binding.peerId!==record.peerId)return false;
  if(record.windowsJob||record.posixGroup){
                                              
    return !!record.proof.launchId&&!!binding.launchId&&record.proof.launchId!==binding.launchId&&ownedContainerComplete(record)&&
      (!record.native||process.platform==='linux'&&ownedRecordPredatesBoot(record)||record.native.pid!==binding.native.pid||record.native.startedAt!==binding.native.startedAt);
  }
                                       
  return !record.windowsHolder&&!record.completionFile&&!record.posixControl&&!record.native&&!ownedRecordMatchesGeneration(record,binding.native);
}

                                     
export async function pruneDeadProcessRecords(home:string,options:{retention?:Partial<RetentionConfig>;referencedPeers?:Set<string>}={}):Promise<{owned:number;runtime:number;artifacts:number;notes:string[]}>{
  const snapshot=ownedProcesses(home),notes=[...snapshot.notes],runtime:{file:string;content:string;pid:number;generation:string}[]=[];
  const bindings=new Map((options.referencedPeers?.size?nativeOwnershipRecords(home):[]).map(binding=>['claude-native:'+binding.peerId,nativeGeneration(binding)]));
  let managed:RuntimeRecord[]=[];
  try{managed=managedRuntimes(home);}catch(error){notes.push('保留无法核验的托管运行时记录：'+describeError(error));}
  for(const peerId of new Set(managed.flatMap(record=>record.value.peerId?[record.value.peerId]:[])))try{
    bindings.set('codex-runtime:'+peerId,runtimeGeneration(home,runtimeFor(home,{peerId},managed)?.value));
  }catch(error){if((error as BridgeError).code!=='runtime_ambiguous')throw error;}
  const childRoles=new Set(['claude-child','claude-worker','host-control']);
  for(const peerId of new Set(snapshot.records.filter(record=>childRoles.has(record.role)).flatMap(record=>record.peerId?[record.peerId]:[]))){
    const generation=childGeneration(home,claudeChild(home,peerId));
    for(const role of childRoles)bindings.set(role+':'+peerId,generation);
  }
  const currentGeneration=(record:OwnedProcessRecord)=>record.role==='claude-native'?nativeGeneration(nativeOwnership(home,record.peerId!)):
    record.role==='codex-runtime'?runtimeGeneration(home,runtimeFor(home,{peerId:record.peerId!})?.value):childRoles.has(record.role)?childGeneration(home,claudeChild(home,record.peerId!)):undefined;
  const retention={...DEFAULT_RETENTION,...options.retention},children:{file:string;content:string;record:ClaudeChildRecord}[]=[],now=Date.now();
  let names:string[]=[];
  try{names=readdirSync(path.join(home,'run'));}catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;}
  for(const name of names.filter(value=>/^runtime-.*\.json$/.test(value))){
    const file=path.join(home,'run',name);
    try{
      const content=readFileSync(file,'utf8'),record=JSON.parse(content);
      if(record?.owner!==managedMarker(home))continue;
      if(!Number.isSafeInteger(record.pid)||record.pid<=0||typeof record.generation!=='string'||!record.generation)throw Error('运行时记录缺少有效的 PID 或进程代次');
      runtime.push({file,content,pid:record.pid,generation:record.generation});
    }catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')notes.push(`保留无法核验的运行时记录 ${file}：${describeError(error)}`);}
  }
  for(const name of names.filter(value=>/^claude-child-[a-f0-9]+\.json$/.test(value))){
    const file=path.join(home,'run',name);
    try{
      const content=readFileSync(file,'utf8'),record=JSON.parse(content) as ClaudeChildRecord;
      if(record.version===1&&record.peerId&&['stopped','released','failed'].includes(record.phase)&&Number.isFinite(record.updatedAt)&&now-record.updatedAt>retention.historyMaxAgeMs&&!options.referencedPeers?.has(record.peerId))children.push({file,content,record});
    }catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')notes.push('保留无法读取的子进程记录 '+file+'：'+describeError(error));}
  }
  const identities=new Map<string,{pid:number;procStart:string}>();
  const key=(pid:number,generation:string)=>`${pid}:${generation}`;
  for(const record of snapshot.records)if(!record.windowsJob&&!record.posixGroup)identities.set(key(record.identity.pid,record.identity.startedAt),{pid:record.identity.pid,procStart:record.identity.startedAt});
  for(const record of runtime)identities.set(key(record.pid,record.generation),{pid:record.pid,procStart:record.generation});
  for(const child of children)for(const identity of [child.record.worker,child.record.wrapper,child.record.native,child.record.foreground])if(identity)identities.set(key(identity.pid,identity.startedAt),{pid:identity.pid,procStart:identity.startedAt});
  const values=[...identities.values()],states=await probeProcesses(values),byIdentity=new Map(values.map((value,index)=>[key(value.pid,value.procStart),states[index]]));
                                                            
  const containers=new Map<string,Awaited<ReturnType<typeof ownedRecordState>>>();
  await Promise.all(snapshot.records.filter(record=>record.windowsJob||record.posixGroup).map(async record=>{containers.set(record.id,await ownedRecordState(record));}));
  const recordState=(record:OwnedProcessRecord)=>process.platform==='linux'&&ownedRecordPredatesBoot(record)?'dead':containers.get(record.id)??byIdentity.get(key(record.identity.pid,record.identity.startedAt));
  let owned=0,removedRuntime=0,artifacts=0;
  const activeLaunches=new Set(snapshot.records.filter(record=>recordState(record)!=='dead').map(record=>record.proof.launchId));
  try{
    const states=await Promise.all(managed.map(record=>managedRuntimeState(home,record.value,snapshot.records)));
    for(let i=0;i<managed.length;i++)if(states[i]!=='dead'||managed[i]!.value.peerId&&options.referencedPeers?.has(managed[i]!.value.peerId!))activeLaunches.add(managed[i]!.value.id);else if(managed[i]!.value.failureCode!=='bootstrap_required')try{removeManagedRuntime(managed[i]!);removedRuntime++;}catch(error){notes.push('失效托管运行时记录清理未完成：'+describeError(error));}
  }catch(error){notes.push('保留无法核验的托管运行时记录：'+describeError(error));}
  for(const record of snapshot.records)if(recordState(record)==='dead')try{
    const referenced=!!record.peerId&&!!options.referencedPeers?.has(record.peerId);
    if(referenced&&!supersededNativeGeneration(record,bindings.get(record.role+':'+record.peerId)))continue;
                                                
    const guard=referenced?()=>{
      if(!supersededNativeGeneration(record,currentGeneration(record)))throw new BridgeError('process_record_changed','后继原生归属已经变化，保留旧代次记录。');
    }:undefined;
    const reason=referenced?'superseded-native-generation':'dead-generation';
    if(record.windowsJob||record.posixGroup){if(!await ownedContainerRetirementReady(record)||!retireOwnedProcessRecord(record,reason,guard))continue;}
    else if(!await retireDeadOwnedProcess(record,reason,guard))continue;
    owned++;
  }catch(error){notes.push(`进程 ${record.identity.pid} 的失效归属记录清理未完成：${describeError(error)}`);}
  for(const record of runtime)if(byIdentity.get(key(record.pid,record.generation))==='dead')try{
                                     
    if(readFileSync(record.file,'utf8')===record.content){unlinkSync(record.file);removedRuntime++;}
  }catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')notes.push(`失效运行时记录 ${record.file} 清理未完成：${describeError(error)}`);}
  for(const child of children){
    const associated=snapshot.records.filter(value=>value.peerId===child.record.peerId),processes=[child.record.worker,child.record.wrapper,child.record.native,child.record.foreground].filter(value=>!!value);
    if(associated.some(record=>recordState(record)!=='dead')||processes.some(value=>byIdentity.get(key(value.pid,value.startedAt))!=='dead'))continue;
    try{if(readFileSync(child.file,'utf8')===child.content){unlinkSync(child.file);artifacts++;}}catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')notes.push('失效子进程记录清理未完成：'+describeError(error));}
  }
  const logs=names.flatMap(name=>{
    const match=/^codex-([a-f0-9-]{36})\.log$/.exec(name);if(!match)return[];
    try{return[{name,id:match[1]!,time:statSync(path.join(home,'run',name)).mtimeMs}];}catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')return[];throw error;}
  }).sort((a,b)=>b.time-a.time);
  let retained=0;
  for(const log of logs)if(!activeLaunches.has(log.id)&&(retained++>=retention.logBackups+1||now-log.time>retention.eventMaxAgeMs)){
    for(const name of names.filter(name=>name===log.name||name.startsWith(log.name+'.')&&/^\d+$/.test(name.slice(log.name.length+1))))try{unlinkSync(path.join(home,'run',name));artifacts++;}catch(error){if(!['ENOENT','EACCES','EPERM','EBUSY'].includes((error as NodeJS.ErrnoException).code??''))notes.push('运行日志清理未完成：'+describeError(error));}
  }
  for(const [name,limit] of [['broker.log',retention.logMaxBytes],['claude-child.log',1024*1024],...logs.map(log=>[log.name,retention.logMaxBytes])] as [string,number][])if(capLog(path.join(home,'run',name),limit))artifacts++;
  const retired=path.join(processRegistryRoot(),'retired');let retiredNames:string[]=[];
  try{retiredNames=readdirSync(retired);}catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')notes.push('已退出进程的归属快照无法读取：'+describeError(error));}
  for(const name of retiredNames.filter(name=>/^[a-f0-9-]{36}\.json$/.test(name)))try{
    const file=path.join(retired,name),record=JSON.parse(readFileSync(file,'utf8'));
    if(record.version===1&&record.id+'.json'===name&&record.home===path.resolve(home)&&Number.isFinite(record.retiredAt)&&now-record.retiredAt>retention.historyMaxAgeMs){if(record.completionFile)try{unlinkSync(record.completionFile);}catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;}unlinkSync(file);artifacts++;}
  }catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')notes.push('保留无法读取的已退出进程快照：'+describeError(error));}
  return{owned,runtime:removedRuntime,artifacts,notes};
}
