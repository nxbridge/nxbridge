import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, mkdirSync, readFileSync, statSync, utimesSync, writeFileSync, unlinkSync, rmdirSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { pruneDeadProcessRecords } from '../process/process-records.js';
import { pruneProcessOwnership } from '../process/process-cleanup.js';
import { processIdentity, liveProcess } from '../process/process.js';
import { forgetOwnedProcess, ownedProcesses, processRegistryRoot, recordOwnedProcess, releaseOwnedProcess, completeOwnedContainer, ownedContainerComplete, ownedContainerRetirementReady, currentLinuxBootId, processRegistryDirectory, prepareProcessRegistry, type OwnedProcessRecord } from '../process/owned-processes.js';
import {atomicJson} from '../shared/files.js';
import { managedMarker } from '../install/installation.js';
import { Store } from '../storage/store.js';
import { Broker, type Adapters } from '../broker/coordinator.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import { FakeAdapter } from './support/fake-adapter.js';
import { temporaryDirectory } from './support/temporary.js';
import { saveNativeOwnership, type NativeOwnership } from '../process/owned-native.js';
import { saveManagedRuntime, type ManagedRuntime } from '../hosts/managed-runtimes.js';
import { claudeChildFile } from '../hosts/claude/claude-child.js';
import { launchIndependentWindows } from '../process/windows/windows-background.js';
import { requireIndependentWindows } from './support/windows-job.js';
import { stopOwnedProcess } from '../process/process-stop.js';

test('周期回收删除已死及复用 PID 的旧代次记录，保留活进程、外来及损坏证据',async t=>{
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge'),run=path.join(home,'run');mkdirSync(run,{recursive:true});
  const child=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore',windowsHide:true}),closed=once(child,'close');
  t.after(async()=>{if(child.exitCode===null&&child.signalCode===null)child.kill();await closed;for(const record of ownedProcesses(home).records)if(record.identity.pid===process.pid)forgetOwnedProcess(record);await temp.cleanup();});
  const store=new Store(path.join(home,'nxbridge.db'));store.close();
  await once(child,'spawn');const parent=await processIdentity(process.pid),identity=await processIdentity(child.pid!);
  await recordOwnedProcess(home,{role:'proxy',identity,parent,proof:{kind:'spawn'}});
  await recordOwnedProcess(home,{role:'broker',identity:parent,proof:{kind:'spawn'}});
  const runtime=(name:string,pid:number,generation:string,owner=managedMarker(home))=>{const file=path.join(run,`runtime-${name}.json`);atomicJson(file,{owner,pid,generation,entry:process.execPath});return file;};
  const dead=runtime('dead',identity.pid,identity.startedAt),reused=runtime('reused',parent.pid,parent.startedAt+'0'),live=runtime('live',parent.pid,parent.startedAt),foreign=runtime('foreign',identity.pid,identity.startedAt,'other-owner');
  const broken=path.join(run,'runtime-broken.json');writeFileSync(broken,'{未完成的记录','utf8');
  child.kill();await closed;
  const result=await pruneDeadProcessRecords(home);
  assert.equal(result.owned,1);assert.equal(result.runtime,2);assert.equal(result.notes.length,1);assert.match(result.notes[0]!,/runtime-broken/);
  assert.equal(existsSync(dead),false);assert.equal(existsSync(reused),false);
  for(const file of [live,foreign,broken])assert.equal(existsSync(file),true);
  assert.deepEqual(ownedProcesses(home).records.map(record=>record.identity.pid),[process.pid]);assert.equal(liveProcess(process.pid),true);
});

test('已绑定原生后台退休同次启动的旧代次、旧会话和旧格式证明，重复维护不复活记录',async t=>{
  const temp=temporaryDirectory(),home=temp.directory,identity=await processIdentity(process.pid),peerId=randomUUID(),sessionId=randomUUID(),launchId=randomUUID();
  t.after(async()=>{for(const record of ownedProcesses(home).records)forgetOwnedProcess(record);await temp.cleanup();});
  const binding:NativeOwnership={version:1,id:launchId,home,peerId,sessionId,configHome:home,phase:'bound',createdAt:Date.now(),native:identity};saveNativeOwnership(binding);
  const current=await recordOwnedProcess(home,{role:'claude-native',identity,peerId,sessionId,configHome:home,proof:{kind:'native-session',launchId}});
  const retired=[],variants:Partial<OwnedProcessRecord>[]=[{},
    {proof:{kind:'native-session',launchId}},{sessionId:randomUUID(),configHome:path.join(home,'previous-config')},
    {proof:{kind:'legacy-ledger'}},{proof:{kind:'legacy-runtime'}},{proof:{kind:'spawn'}},
  ];
  for(const [index,variant] of variants.entries())retired.push(await recordOwnedProcess(home,{role:'claude-native',identity:{...identity,startedAt:(BigInt(identity.startedAt)+BigInt(index+1)).toString()},peerId,sessionId,configHome:process.platform==='win32'?home.toUpperCase():home,proof:{kind:'native-session',launchId:randomUUID()},...variant}));
  const options={referencedPeers:new Set([peerId])},result=await pruneDeadProcessRecords(home,options);
  assert.equal(result.owned,variants.length);assert.deepEqual(result.notes,[]);assert.deepEqual(ownedProcesses(home).records.map(record=>record.id),[current.id]);assert.equal(liveProcess(process.pid),true);
  for(const record of retired){const snapshot=JSON.parse(readFileSync(path.join(processRegistryRoot(),'retired',record.id+'.json'),'utf8'));assert.equal(snapshot.reason,'superseded-native-generation');assert.equal(snapshot.generation,record.identity.startedAt);}
  assert.equal((await pruneDeadProcessRecords(home,options)).owned,0);assert.deepEqual(ownedProcesses(home).records.map(record=>record.id),[current.id]);
});

test('保留未结算及无法核验的后继、用户归属、容器和当前原生代次的配对证明',async t=>{
  const temp=temporaryDirectory(),home=temp.directory,identity=await processIdentity(process.pid),old={...identity,startedAt:identity.startedAt+'0'},references=new Set<string>(),ids:string[]=[];
  t.after(async()=>{for(const record of ownedProcesses(home).records)forgetOwnedProcess(record);for(const peerId of references)unlinkSync(path.join(processRegistryDirectory(home),'native-'+peerId+'.json'));await temp.cleanup();});
  const cases:{binding?:Partial<NativeOwnership>;record?:Partial<OwnedProcessRecord>}[]=[
    {binding:{phase:'intent'}},{binding:{native:undefined}},{binding:{native:{pid:0,startedAt:''}}},{binding:{id:''}},{binding:{home:path.join(home,'other')}},
    {binding:{releasedToUser:true}},{record:{releasedToUser:true}},
    {record:{posixGroup:true,proof:{kind:'native-session',launchId:'current'}}},{binding:{native:old}},{record:{identity}},
    {record:{completionFile:path.join(home,'unclaimed.done')}},{record:{posixControl:path.join(home,'control.json')}},{record:{native:identity}},
  ];
  for(const entry of cases){
    const peerId=randomUUID(),sessionId=randomUUID();references.add(peerId);
    const record=await recordOwnedProcess(home,{role:'claude-native',identity:old,peerId,sessionId,configHome:home,proof:{kind:'native-session',launchId:randomUUID()},...entry.record});ids.push(record.id);
    atomicJson(path.join(processRegistryDirectory(home),'native-'+peerId+'.json'),{version:1,id:'current',home,peerId,sessionId,configHome:home,phase:'bound',createdAt:Date.now(),native:identity,...entry.binding});
    if(record.posixGroup)completeOwnedContainer(record);
  }
  const result=await pruneDeadProcessRecords(home,{referencedPeers:references});assert.equal(result.owned,0);assert.deepEqual(result.notes,[]);
  assert.deepEqual(ownedProcesses(home).records.map(record=>record.id).sort(),ids.sort());assert.equal(liveProcess(process.pid),true);
});

test('托管 Claude 的已绑定原生代次替代旧 worker、child 和控制记录',async t=>{
  const temp=temporaryDirectory(),home=temp.directory,identity=await processIdentity(process.pid),peerId=randomUUID(),sessionId=randomUUID();
  t.after(async()=>{for(const record of ownedProcesses(home).records)forgetOwnedProcess(record);await temp.cleanup();});
  mkdirSync(path.join(home,'run'),{recursive:true});
  atomicJson(claudeChildFile(home,peerId),{version:1,bootId:currentLinuxBootId(),peerId,sessionId,cwd:home,configHome:home,address:'unused-test-socket',phase:'active',hasSession:true,native:identity,worker:identity,updatedAt:Date.now()});
  for(const role of ['claude-worker','claude-child','host-control'] as const)
    await recordOwnedProcess(home,{role,identity:{...identity,startedAt:(BigInt(identity.startedAt)+1n).toString()},peerId,sessionId:randomUUID(),configHome:home,proof:{kind:'spawn'}});
  const current=await recordOwnedProcess(home,{role:'claude-worker',identity,peerId,sessionId,configHome:home,proof:{kind:'spawn'}});
  const result=await pruneDeadProcessRecords(home,{referencedPeers:new Set([peerId])});assert.equal(result.owned,3);assert.deepEqual(result.notes,[]);
  assert.deepEqual(ownedProcesses(home).records.map(record=>record.id),[current.id]);assert.equal(liveProcess(process.pid),true);
});

test('Linux 缺少启动代号的旧托管文件不能授权退休已配对成员的进程证明',{skip:process.platform!=='linux'},async t=>{
  const temp=temporaryDirectory(),home=temp.directory,identity=await processIdentity(process.pid),peerId=randomUUID(),sessionId=randomUUID();
  t.after(async()=>{for(const record of ownedProcesses(home).records)forgetOwnedProcess(record);await temp.cleanup();});
  mkdirSync(path.join(home,'run'),{recursive:true});
  atomicJson(claudeChildFile(home,peerId),{version:1,peerId,sessionId,cwd:home,address:'unused-legacy-socket',phase:'active',hasSession:true,native:identity,updatedAt:Date.now()});
  const record=await recordOwnedProcess(home,{role:'host-control',identity,peerId,sessionId,proof:{kind:'spawn'}});
  atomicJson(path.join(processRegistryDirectory(home),record.id+'.json'),{...record,bootId:'previous-boot'});
  const result=await pruneDeadProcessRecords(home,{referencedPeers:new Set([peerId])});assert.equal(result.owned,0);assert.deepEqual(result.notes,[]);
  assert.deepEqual(ownedProcesses(home).records.map(value=>value.id),[record.id]);assert.equal(liveProcess(process.pid),true);
});

test('周期维护只归档有完成凭据的旧容器，不向仍存活的持有器发送信号',async t=>{
  const temp=temporaryDirectory(),home=temp.directory,identity=await processIdentity(process.pid),peerId=randomUUID(),launchId=randomUUID();
  const holder=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore',windowsHide:true}),closed=once(holder,'close');
  t.after(async()=>{if(holder.exitCode===null&&holder.signalCode===null)holder.kill();await closed;for(const record of ownedProcesses(home).records){if(record.windowsJob||record.posixGroup)completeOwnedContainer(record);forgetOwnedProcess(record);}await temp.cleanup();});
  await once(holder,'spawn');const holderIdentity=await processIdentity(holder.pid!);
  await recordOwnedProcess(home,{role:'host-control',identity:holderIdentity,parent:identity,proof:{kind:'spawn'}});
  saveManagedRuntime(home,{id:launchId,peerId,owner:managedMarker(home),kind:'owned',state:'ready',endpoint:'current-container-test',createdAt:Date.now(),pid:identity.pid,procStart:identity.startedAt});
  const input={role:'codex-runtime' as const,identity:{...identity,startedAt:(BigInt(identity.startedAt)+1n).toString()},peerId,proof:{kind:'spawn' as const,launchId:randomUUID()},windowsJob:'isolated-completed-job',windowsHolder:holderIdentity,native:{pid:identity.pid,startedAt:(BigInt(identity.startedAt)+1n).toString()}};
  const old=await recordOwnedProcess(home,input),nonComplete=await recordOwnedProcess(home,{...input,identity,windowsJob:undefined,windowsHolder:undefined,posixGroup:true,native:identity}),current=await recordOwnedProcess(home,{...input,proof:{kind:'spawn',launchId},native:identity});
  completeOwnedContainer(old);completeOwnedContainer(current);
  const result=await pruneDeadProcessRecords(home,{referencedPeers:new Set([peerId])});
  assert.equal(result.owned,process.platform==='win32'?0:1);assert.deepEqual(result.notes,[]);assert.equal(liveProcess(holder.pid!),true);
  const remaining=ownedProcesses(home).records.map(record=>record.id);assert.equal(remaining.includes(old.id),process.platform==='win32','Windows 旧空凭据缺少可重新核验的 Job 时继续保留');assert.ok(remaining.includes(current.id));assert.ok(remaining.includes(nonComplete.id));
  assert.equal(ownedContainerComplete(old),true,'归档仍保留完成凭据');
});

test('Windows 旧推断完成凭据仍须核验 Job，存活成员及查询错误都阻止退休',{skip:process.platform!=='win32',timeout:30000},async t=>{
  if(!await requireIndependentWindows(t))return;
  const temp=temporaryDirectory(),home=temp.directory,peerId=randomUUID(),identity=await processIdentity(process.pid);
  mkdirSync(path.join(home,'run'),{recursive:true});
  const record=await launchIndependentWindows({home,executable:process.execPath,args:['-e','setInterval(()=>{},1000)'],cwd:home,env:process.env,logfile:path.join(home,'run','live.log'),role:'codex-runtime',launchId:randomUUID(),peerId});
                                         
  t.after(async()=>{await stopOwnedProcess(record.identity);completeOwnedContainer(record);await releaseOwnedProcess(record);await temp.cleanup();});
  saveManagedRuntime(home,{id:randomUUID(),peerId,owner:managedMarker(home),kind:'owned',state:'ready',endpoint:'newer-runtime',createdAt:Date.now(),pid:identity.pid,procStart:identity.startedAt});
  completeOwnedContainer(record);
  assert.equal(await ownedContainerRetirementReady({...record,identity:{...record.identity,startedAt:(BigInt(record.identity.startedAt)+1n).toString()}}),false,'身份查询失败不能当作 Job 已消失');
  const result=await pruneDeadProcessRecords(home,{referencedPeers:new Set([peerId])});assert.equal(result.owned,0);assert.deepEqual(result.notes,[]);
  assert.ok(ownedProcesses(home).records.some(value=>value.id===record.id));assert.equal(liveProcess(record.identity.pid),true);assert.equal(liveProcess(record.windowsHolder!.pid),true);
  await stopOwnedProcess(record.windowsHolder!);
  assert.equal(liveProcess(record.identity.pid),true,'持有器退出不等于无名 Job 的成员退出');
  assert.equal(await ownedContainerRetirementReady(record),false,'Job 名字消失不能补足旧完成凭据的证明');
  assert.equal((await pruneDeadProcessRecords(home,{referencedPeers:new Set([peerId])})).owned,0);
  assert.ok(ownedProcesses(home).records.some(value=>value.id===record.id));assert.equal(liveProcess(record.identity.pid),true);
});

test('Codex 旧单进程证明只由唯一就绪的自有运行时代次取代，未知和歧义仍保留',async t=>{
  const temp=temporaryDirectory(),home=temp.directory,identity=await processIdentity(process.pid),old={...identity,startedAt:identity.startedAt+'0'},references=new Set<string>(),kept:string[]=[];
  t.after(async()=>{for(const record of ownedProcesses(home).records)forgetOwnedProcess(record);await temp.cleanup();});
  const cases:{state?:ManagedRuntime['state'];kind?:ManagedRuntime['kind'];duplicate?:boolean;noNative?:boolean;current?:boolean}[]=[
    {},{state:'starting'},{state:'failed'},{kind:'legacy'},{duplicate:true},{noNative:true},{current:true},
  ];
  for(const [index,entry] of cases.entries()){
    const peerId=randomUUID(),id=randomUUID(),native=entry.current?old:identity;references.add(peerId);
    const runtime:ManagedRuntime={id,peerId,owner:managedMarker(home),kind:entry.kind??'owned',state:entry.state??'ready',endpoint:'test-runtime-'+index,createdAt:Date.now(),
      ...(!entry.noNative?{pid:native.pid,procStart:native.startedAt}:{})};saveManagedRuntime(home,runtime);
    if(entry.duplicate)saveManagedRuntime(home,{...runtime,id:randomUUID(),endpoint:runtime.endpoint+'-other'});
    const record=await recordOwnedProcess(home,{role:'codex-runtime',identity:old,peerId,sessionId:randomUUID(),configHome:path.join(home,'previous-config'),proof:{kind:'legacy-ledger'}});
    if(index)kept.push(record.id);
  }
  const result=await pruneDeadProcessRecords(home,{referencedPeers:references});assert.equal(result.owned,1);assert.deepEqual(result.notes,[]);
  assert.deepEqual(ownedProcesses(home).records.map(record=>record.id).sort(),kept.sort());assert.equal(liveProcess(process.pid),true);
});

test('进程核验期间 Codex 后继从就绪变回启动中时保留原证明',async t=>{
  const temp=temporaryDirectory(),home=temp.directory,identity=await processIdentity(process.pid),peerId=randomUUID();
  t.after(async()=>{for(const record of ownedProcesses(home).records)forgetOwnedProcess(record);await temp.cleanup();});
  const runtime:ManagedRuntime={id:randomUUID(),peerId,owner:managedMarker(home),kind:'owned',state:'ready',endpoint:'runtime-race',createdAt:Date.now(),pid:identity.pid,procStart:identity.startedAt};saveManagedRuntime(home,runtime);
  const record=await recordOwnedProcess(home,{role:'codex-runtime',identity:{...identity,startedAt:identity.startedAt+'0'},peerId,proof:{kind:'spawn'}});
  const pending=pruneDeadProcessRecords(home,{referencedPeers:new Set([peerId])});saveManagedRuntime(home,{...runtime,state:'starting'});
  const result=await pending;assert.equal(result.owned,0);assert.match(result.notes.join(' '),/后继原生归属已经变化/);assert.deepEqual(ownedProcesses(home).records.map(value=>value.id),[record.id]);
});

test('进程核验期间原生绑定被后继预约替换时保留原记录',async t=>{
  const temp=temporaryDirectory(),home=temp.directory,identity=await processIdentity(process.pid),peerId=randomUUID(),sessionId=randomUUID();
  t.after(async()=>{for(const record of ownedProcesses(home).records)forgetOwnedProcess(record);await temp.cleanup();});
  const binding:NativeOwnership={version:1,id:randomUUID(),home,peerId,sessionId,configHome:home,phase:'bound',createdAt:Date.now(),native:identity};saveNativeOwnership(binding);
  const record=await recordOwnedProcess(home,{role:'claude-native',identity:{...identity,startedAt:identity.startedAt+'0'},peerId,sessionId,configHome:home,proof:{kind:'native-session',launchId:randomUUID()}});
  const pending=pruneDeadProcessRecords(home,{referencedPeers:new Set([peerId])});saveNativeOwnership({...binding,id:randomUUID(),phase:'intent'});
  const result=await pending;assert.equal(result.owned,0);assert.match(result.notes.join(' '),/后继原生归属已经变化/);assert.deepEqual(ownedProcesses(home).records.map(value=>value.id),[record.id]);
});

test('Linux 旧启动的单进程证明不会因 PID 和 tick 恰好复用而永久保留',{skip:process.platform!=='linux'},async t=>{
  const temp=temporaryDirectory(),home=temp.directory,identity=await processIdentity(process.pid),peerId=randomUUID(),sessionId=randomUUID(),launchId=randomUUID();
  t.after(async()=>{for(const record of ownedProcesses(home).records)forgetOwnedProcess(record);await temp.cleanup();});
  saveNativeOwnership({version:1,id:launchId,home,peerId,sessionId,configHome:home,phase:'bound',createdAt:Date.now(),native:identity});
  const input={role:'claude-native' as const,identity,peerId,sessionId,configHome:home,proof:{kind:'native-session' as const,launchId}};
  const previous=await recordOwnedProcess(home,input),current=await recordOwnedProcess(home,input);
  atomicJson(path.join(processRegistryDirectory(home),previous.id+'.json'),{...previous,bootId:'previous-boot'});
  const result=await pruneDeadProcessRecords(home,{referencedPeers:new Set([peerId])});assert.equal(result.owned,1);assert.deepEqual(result.notes,[]);
  assert.deepEqual(ownedProcesses(home).records.map(record=>record.id),[current.id]);assert.equal(liveProcess(process.pid),true);
});

test('旧容器完成证明不覆盖相同 PID 和启动代次的直接进程存活证明',async t=>{
  const temp=temporaryDirectory(),home=temp.directory,identity=await processIdentity(process.pid),peerId=randomUUID(),sessionId=randomUUID();
  t.after(async()=>{for(const record of ownedProcesses(home).records)forgetOwnedProcess(record);await temp.cleanup();});
  saveNativeOwnership({version:1,id:randomUUID(),home,peerId,sessionId,configHome:home,phase:'bound',createdAt:Date.now(),native:{pid:identity.pid,startedAt:identity.startedAt+'0'}});
  const input={role:'claude-native' as const,identity,peerId,sessionId,configHome:home,proof:{kind:'native-session' as const,launchId:randomUUID()}};
  const plain=await recordOwnedProcess(home,input),container=await recordOwnedProcess(home,{...input,posixGroup:true});
  if(process.platform==='linux')atomicJson(path.join(processRegistryDirectory(home),container.id+'.json'),{...container,bootId:'previous-boot'});
  else completeOwnedContainer(container);
  const result=await pruneDeadProcessRecords(home,{referencedPeers:new Set([peerId])});
  assert.equal(result.owned,process.platform==='linux'?1:0);assert.deepEqual(result.notes,[]);assert.equal(liveProcess(process.pid),true);
  const remaining=ownedProcesses(home).records.map(record=>record.id);assert.ok(remaining.includes(plain.id),'旧容器不能覆盖直接查询仍存活的单进程证明');
  assert.equal(remaining.includes(container.id),process.platform!=='linux','缺少启动代号且同一组仍活着时，空凭据不能授权退休');
});

test('退出后保留最小归属快照但不再授权回收，周期维护只删除本安装过期快照',async t=>{
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge'),other=path.join(temp.directory,'other'),now=Date.now(),identity=await processIdentity(process.pid);
  t.after(temp.cleanup);
  const recent=await recordOwnedProcess(home,{role:'proxy',identity,proof:{kind:'spawn',launchId:'owned-proxy'}}),old=await recordOwnedProcess(home,{role:'host-control',identity,proof:{kind:'spawn'}}),foreign=await recordOwnedProcess(other,{role:'proxy',identity,proof:{kind:'spawn'}});
  for(const record of [recent,old,foreign])forgetOwnedProcess(record,'child-exited');
  assert.deepEqual(ownedProcesses(home).records,[]);assert.equal(liveProcess(process.pid),true);
  const retired=path.join(processRegistryRoot(),'retired'),file=(id:string)=>path.join(retired,id+'.json'),snapshot=JSON.parse(readFileSync(file(recent.id),'utf8'));
  assert.deepEqual(snapshot,{version:1,id:recent.id,home:path.resolve(home),role:'proxy',pid:identity.pid,generation:identity.startedAt,launchId:'owned-proxy',retiredAt:snapshot.retiredAt,reason:'child-exited'});
  assert.ok(snapshot.retiredAt>=now);forgetOwnedProcess(recent,'owned-release');assert.equal(readFileSync(file(recent.id),'utf8'),JSON.stringify(snapshot,null,2)+'\n','重复退出回调不覆盖最初原因');
  for(const record of [old,foreign])atomicJson(file(record.id),{...JSON.parse(readFileSync(file(record.id),'utf8')),retiredAt:now-8*86400000});
  const result=await pruneDeadProcessRecords(home);assert.deepEqual(result.notes,[]);assert.equal(result.artifacts,1);
  assert.equal(existsSync(file(old.id)),false);assert.equal(existsSync(file(recent.id)),true);assert.equal(existsSync(file(foreign.id)),true);
});

test('容器凭据随归属退休迁出安装目录，旧监督快照仍能确认完成并允许立即清理目录',async t=>{
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge'),identity=await processIdentity(process.pid);t.after(temp.cleanup);
  const record=await recordOwnedProcess(home,{role:'host-control',identity,proof:{kind:'spawn'},posixGroup:true}),captured=structuredClone(record);
  completeOwnedContainer(record);forgetOwnedProcess(record);
  assert.equal(existsSync(record.completionFile!),false);assert.equal(ownedContainerComplete(captured),true);
  const retired=path.join(processRegistryRoot(),'retired',record.id+'.json'),snapshot=JSON.parse(readFileSync(retired,'utf8'));
  assert.equal(snapshot.completionFile,path.join(processRegistryRoot(),'retired',record.id+'.done'));assert.equal(existsSync(snapshot.completionFile),true);
  completeOwnedContainer(captured);assert.equal(existsSync(record.completionFile!),false,'迟到回调不得重新创建安装目录内的凭据');
  const result=await pruneProcessOwnership(home);assert.deepEqual(result.notes,[]);assert.equal(result.removed,true);assert.equal(ownedContainerComplete(captured),true);
});

test('旧版和迟到的退休凭据按本 home 迁移，不让孤立 done 文件阻塞卸载',async t=>{
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge'),identity=await processIdentity(process.pid);t.after(temp.cleanup);
  const record=await recordOwnedProcess(home,{role:'host-control',identity,proof:{kind:'spawn'},posixGroup:true});forgetOwnedProcess(record);
  writeFileSync(record.completionFile!,'','utf8');
  const unknown=path.join(processRegistryDirectory(home),'unknown.done');writeFileSync(unknown,'未知证据','utf8');
  const first=await pruneProcessOwnership(home);assert.equal(first.removed,false);assert.match(first.notes.join(' '),/无法核验/);assert.equal(existsSync(unknown),true);
  assert.equal(existsSync(record.completionFile!),false);assert.equal(ownedContainerComplete(record),true);
                               
  const {unlinkSync}=await import('node:fs');unlinkSync(unknown);
  const result=await pruneProcessOwnership(home);assert.deepEqual(result.notes,[]);assert.equal(result.removed,true);
});

test('只在显式 purge 删除写在归属记录之前的空孤立完成凭据',async t=>{
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge');t.after(temp.cleanup);await prepareProcessRegistry(home);
  const orphan=path.join(processRegistryDirectory(home),randomUUID()+'.json.done');writeFileSync(orphan,'','utf8');
  const normal=await pruneProcessOwnership(home);assert.equal(normal.removed,false);assert.equal(normal.orphanReceiptsRemoved,0);assert.equal(existsSync(orphan),true);
  const purge=await pruneProcessOwnership(home,true);assert.equal(purge.removed,true);assert.deepEqual(purge.notes,[]);assert.equal(purge.orphanReceiptsRemoved,1);assert.equal(existsSync(orphan),false);
});

test('purge 保留已有归属的完成证明、损坏账本对应凭据及无法识别的文件',async t=>{
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge'),identity=await processIdentity(process.pid);t.after(temp.cleanup);
  const owned=await recordOwnedProcess(home,{role:'host-control',identity,proof:{kind:'spawn'},posixGroup:true});completeOwnedContainer(owned);
  const directory=processRegistryDirectory(home),brokenId=randomUUID(),broken=path.join(directory,brokenId+'.json'),referenced=broken+'.done',unknown=path.join(directory,'unknown.done'),content=path.join(directory,randomUUID()+'.json.done'),subdirectory=path.join(directory,randomUUID()+'.json.done'),orphan=path.join(directory,randomUUID()+'.json.done');
  writeFileSync(broken,'{未完成的归属记录','utf8');writeFileSync(referenced,'','utf8');writeFileSync(unknown,'','utf8');writeFileSync(content,'未知文件内容','utf8');mkdirSync(subdirectory);writeFileSync(orphan,'','utf8');
  try{
    const result=await pruneProcessOwnership(home,true);assert.equal(result.removed,false);assert.equal(result.orphanReceiptsRemoved,1);assert.equal(ownedContainerComplete(owned),true);assert.equal(existsSync(path.join(processRegistryRoot(),'retired',owned.id+'.done')),true);
    for(const file of [broken,referenced,unknown,content,subdirectory])assert.equal(existsSync(file),true);assert.equal(existsSync(orphan),false);
  }finally{for(const file of [broken,referenced,unknown,content])if(existsSync(file))unlinkSync(file);rmdirSync(subdirectory);}
});

test('旧版临时完成目录已丢失时，确认退出的事实仍能保存并允许退休',async t=>{
  const temp=temporaryDirectory(),home=temp.directory,identity=await processIdentity(process.pid);t.after(temp.cleanup);
  const record=await recordOwnedProcess(home,{role:'host-control',identity,proof:{kind:'spawn'},posixGroup:true,completionFile:path.join(home,'missing-temporary','complete.done')});
  completeOwnedContainer(record);assert.equal(ownedContainerComplete(record),true);assert.equal(existsSync(path.dirname(record.completionFile!)),false);
  forgetOwnedProcess(record);assert.equal(ownedContainerComplete(record),true);assert.deepEqual((await pruneProcessOwnership(home)).notes,[]);
});

test('协调器启动维护回收失效运行时记录，关闭等待维护完成',async t=>{
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge'),run=path.join(home,'run');mkdirSync(run,{recursive:true});
  const identity=await processIdentity(process.pid),file=path.join(run,'runtime-stale.json');atomicJson(file,{owner:managedMarker(home),pid:identity.pid,generation:identity.startedAt+'0',entry:process.execPath});
  const store=new Store(path.join(home,'nxbridge.db')),broker=new Broker(store,DEFAULT_CONFIG,{codex:new FakeAdapter(),claude:new FakeAdapter()} as unknown as Adapters,home);
  t.after(async()=>{await broker.stop();store.close();await temp.cleanup();});
  await broker.start();await broker.stop();assert.equal(existsSync(file),false);
  assert.ok(store.events().some(event=>event.type==='process.records.maintained'));
});

test('卸载本 home 的归属目录时保留仍含其他安装证据的共用根',async t=>{
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge'),root=path.join(temp.directory,'registry'),other=path.join(root,'other-home','proof.json'),previous=process.env.NXBRIDGE_PROCESS_REGISTRY;
  process.env.NXBRIDGE_PROCESS_REGISTRY=root;mkdirSync(path.dirname(other),{recursive:true});writeFileSync(other,'其他安装的证据','utf8');
  t.after(async()=>{if(previous===undefined)delete process.env.NXBRIDGE_PROCESS_REGISTRY;else process.env.NXBRIDGE_PROCESS_REGISTRY=previous;await temp.cleanup();});
  const result=await pruneProcessOwnership(home);assert.deepEqual(result.notes,[]);assert.equal(existsSync(other),true);assert.equal(existsSync(root),true);
});

test('维护回收过期孤立子进程记录和旧日志，保留引用、活代次及最近诊断',async t=>{
  const temp=temporaryDirectory(),home=path.join(temp.directory,'bridge'),run=path.join(home,'run');mkdirSync(run,{recursive:true});
  t.after(async()=>{for(const record of ownedProcesses(home).records)if(record.identity.pid===process.pid)forgetOwnedProcess(record);await temp.cleanup();});
  const identity=await processIdentity(process.pid),active=randomUUID(),now=Date.now(),old=now-10*86400000;
  await recordOwnedProcess(home,{role:'codex-runtime',identity,proof:{kind:'spawn',launchId:active}});
  const files=new Map<string,string>();
  for(const [index,peerId] of ['orphan','referenced','live','active-phase'].entries()){
    const file=path.join(run,'claude-child-'+(index+1)+'.json');files.set(peerId,file);atomicJson(file,{version:1,peerId,updatedAt:old,phase:peerId==='active-phase'?'active':'released',...(peerId==='live'?{native:identity}:{})});
  }
  const logs=[active,...Array.from({length:5},()=>randomUUID())].map((id,index)=>{const file=path.join(run,'codex-'+id+'.log');writeFileSync(file,'x'.repeat(200)+'tail','utf8');const stamp=new Date(id===active?old:now-index*1000);utimesSync(file,stamp,stamp);return file;});
  const result=await pruneDeadProcessRecords(home,{referencedPeers:new Set(['referenced']),retention:{logMaxBytes:64}});
  assert.deepEqual(result.notes,[]);assert.equal(existsSync(files.get('orphan')!),false);
  for(const id of ['referenced','live','active-phase'])assert.equal(existsSync(files.get(id)!),true);
  for(const file of logs.slice(0,4)){assert.equal(existsSync(file),true);assert.ok(statSync(file).size<=64);assert.ok(readFileSync(file,'utf8').endsWith('tail'));}
  for(const file of logs.slice(4))assert.equal(existsSync(file),false);
});
