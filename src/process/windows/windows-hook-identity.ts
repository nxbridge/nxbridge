import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, writeFile, rename, rm } from 'node:fs/promises';
import { AsyncLocalStorage } from 'node:async_hooks';
import path from 'node:path';
import {bridgeHome} from '../../shared/local-paths.js';
import {readJson} from '../../shared/files.js';
import {BridgeError,describeError} from '../../shared/errors.js';
import { remainingTime } from '../../shared/time-budget.js';
import type { ProcessAncestry, ProcessIdentity } from '../process.js';
import { WINDOWS_IDENTITY_REVISION } from './identity-revision.js';

const execute=promisify(execFile);
declare const __NXBRIDGE_IDENTITY_REVISION__:string;

const identityHomes=new AsyncLocalStorage<string>();
export function withWindowsIdentityHome<T>(home:string,work:()=>Promise<T>):Promise<T>{return identityHomes.run(home,work);}
const identityHome=()=>identityHomes.getStore()??(process.env.NXBRIDGE_TEST_SCOPE&&process.env.NXBRIDGE_PROCESS_REGISTRY?path.join(process.env.NXBRIDGE_PROCESS_REGISTRY,'native-identity'):bridgeHome());
export function windowsHelperStatus(home=identityHome()):{revision:string;binary:string;prepared:boolean}{
  const revision=typeof __NXBRIDGE_IDENTITY_REVISION__==='string'?__NXBRIDGE_IDENTITY_REVISION__:WINDOWS_IDENTITY_REVISION,binary=path.join(home,'run','hook-identity-'+revision,'identity.exe');
  return{revision,binary,prepared:existsSync(binary)};
}
export async function windowsHookIdentityHelper(home=identityHome(),timeout=10000,installing=false):Promise<string>{
  const deadline=performance.now()+timeout;
  const {binary}=windowsHelperStatus(home),directory=path.dirname(binary);
  if(existsSync(binary))return binary;
  const state=path.join(home,'setup-state.json');if(!installing&&existsSync(state)&&readJson<{disabled?:boolean}>(state).disabled)throw new BridgeError('uninstalled','nxbridge 已卸载，未创建原生身份助手。');
  const windows=process.env.SystemRoot??process.env.WINDIR??'C:\\Windows',compiler=['Framework64','Framework'].map(name=>path.join(windows,'Microsoft.NET',name,'v4.0.30319','csc.exe')).find(existsSync);
  if(!compiler)throw new BridgeError('native_identity_unavailable','系统 .NET 编译器不可用，无法准备 Windows 原生身份助手。');
  await mkdir(directory,{recursive:true});const staging=await mkdtemp(path.join(directory,'build-'));
  try{
    const {windowsHookIdentitySource}=await import('./native-sources.js');
    const source=path.join(staging,'identity.cs'),output=path.join(staging,'identity.exe');await writeFile(source,windowsHookIdentitySource,'utf8');
    await execute(compiler,['/nologo','/target:exe','/optimize+','/platform:anycpu','/out:'+output,source],{windowsHide:true,timeout:remainingTime(deadline,timeout)});
    try{await rename(output,binary);}catch(error){if(!existsSync(binary))throw error;}
    return binary;
  }catch(error){throw new BridgeError('native_identity_unavailable','Windows 原生助手恢复准备失败，请运行 nxbridge update 重新准备：'+describeError(error));
  }finally{await rm(staging,{recursive:true});}
}

const readComplete='\nNXBRIDGE_READ_COMPLETE\n';
export function parseWindowsProcessSnapshot<T>(contents:string):T{
  if(contents.endsWith(readComplete)){
    try{return JSON.parse(contents.slice(0,-readComplete.length)) as T;}catch{                       }
  }
  throw new BridgeError('process_census_incomplete','Windows 原生进程查询没有返回完整快照，未采用部分结果。');
}

async function readProcessSnapshot<T>(helper:string,args:string[],deadline:number):Promise<T>{
  try{
    const {stdout}=await execute(helper,['read-result',...args],{
      windowsHide:true,encoding:'utf8',timeout:remainingTime(deadline,Number.MAX_SAFE_INTEGER),maxBuffer:16*1024*1024,
    });
    return parseWindowsProcessSnapshot<T>(stdout);
  }catch(error){
    const detail=error as {code?:string;signal?:string;killed?:boolean};
    if(detail.code==='ERR_CHILD_PROCESS_STDIO_MAXBUFFER'||detail.code==='ETIMEDOUT'||detail.code==='timeout'||detail.killed||detail.signal)
      throw new BridgeError('process_census_incomplete','Windows 原生进程查询在完整输出前结束，未采用部分结果。');
    throw error;
  }
}

export async function readWindowsAncestors(pid:number,depth:number,timeout=10000,home=identityHome()):Promise<ProcessAncestry>{
  if(!Number.isSafeInteger(pid)||pid<=0||!Number.isSafeInteger(depth)||depth<=0)throw new BridgeError('process_identity_invalid','Windows 祖先查询参数无效。');
  const deadline=performance.now()+timeout;let helper:string;
  try{helper=await windowsHookIdentityHelper(home,remainingTime(deadline,timeout));}
  catch(error){
    if((error as BridgeError).code==='uninstalled'||(error as BridgeError).code==='timeout')throw error;
    throw new BridgeError('native_identity_unavailable','Windows 原生祖先助手准备未完成。');
  }
  let value:ProcessAncestry;
  try{value=await readProcessSnapshot(helper,['ancestors',String(pid),String(depth)],deadline);}
  catch(error){
    if((error as BridgeError).code==='process_census_incomplete')throw error;
    throw windowsAncestryFailure(error);
  }
  return{...value,source:'native'};
}

export function windowsAncestryFailure(error:unknown):BridgeError{
  const detail=error as {code?:string|number;stderr?:string;syscall?:string};
                                                    
  const spawnFailed=typeof detail.code==='string'&&detail.syscall?.startsWith('spawn ');
  if(spawnFailed||detail.code===1&&detail.stderr?.trim()==='invalid ancestor pid')return new BridgeError('native_identity_unavailable','Windows 原生祖先助手不可用或版本较旧。');
  return new BridgeError('process_identity_unknown','Windows 原生祖先查询未完成，未改用较弱来源。');
}

export async function readWindowsHookIdentity(pid:number,home=identityHome(),host:'codex'|'claude'='codex',timeout=12000,skipOrigin=false):Promise<{pid:number;parentPid:number;image:string;launchImage:string;procStart:string;procStartTicks:string;commandLine:string}>{
  return readWindowsIdentity(pid,home,skipOrigin?`${host}-hook`:host,timeout);
}

                                   
export async function readWindowsProcessArguments(identity:Pick<ProcessIdentity,'pid'|'startedAt'>,timeout=10000):Promise<string>{
  const native=await readWindowsIdentity(identity.pid,identityHome(),'process',timeout);
  if(native.pid!==identity.pid||native.procStart!==identity.startedAt)throw new BridgeError('process_identity_unknown','原生参数查询期间进程代次改变。');
  return native.commandLine;
}

async function readWindowsIdentity(pid:number,home:string,host:'codex'|'claude'|'codex-hook'|'claude-hook'|'process',timeout:number):Promise<{pid:number;parentPid:number;image:string;launchImage:string;procStart:string;procStartTicks:string;commandLine:string}>{
  const deadline=performance.now()+timeout,helper=await windowsHookIdentityHelper(home,remainingTime(deadline,10000)),began=performance.now();
  try{
    return await readProcessSnapshot(helper,[host,String(pid)],Math.min(deadline,began+2000));
  }
  catch(error){
    if((error as BridgeError).code==='process_census_incomplete')throw error;
    const detail=error as Error&{code?:string|number;signal?:string;killed?:boolean;stderr?:string};
    throw new BridgeError('process_identity_unknown',`Windows 原生身份查询未完成（耗时 ${Math.round(performance.now()-began)}ms，退出码 ${detail.code??'未知'}，信号 ${detail.signal??'无'}，终止 ${detail.killed===true}）：${detail.stderr?.trim()||detail.message}`);
  }
}

export async function readWindowsProcessGenerations(pids:number[],timeout:number):Promise<{pid:number;generation?:string;ticks?:string;missing?:boolean}[]>{
  const deadline=performance.now()+timeout,helper=await windowsHookIdentityHelper(identityHome(),timeout);
  return readProcessSnapshot(helper,['probe',...pids.map(String)],deadline);
}

                                     
export async function stopWindowsProcess(identity:Pick<ProcessIdentity,'pid'|'startedAt'>,timeout:number,options:{deadline?:number;beforeSignal?:()=>void}={}):Promise<void>{
  const helper=await windowsHookIdentityHelper(identityHome());
  options.beforeSignal?.();
  const remaining=options.deadline===undefined?timeout:options.deadline-Date.now();
  if(remaining<=0)throw new BridgeError('process_stop_pending','停止期限已到，保留自有进程和归属记录。');
  const absolute=options.deadline===undefined?[]:[String(BigInt(options.deadline)*10000n+116444736000000000n)];
  const {stdout}=await execute(helper,['stop',String(identity.pid),identity.startedAt,String(remaining),...absolute],{windowsHide:true,timeout:remaining+2000});
  if(JSON.parse(stdout)==='pending')throw new BridgeError('process_stop_pending','自有进程尚未退出，保留归属记录，稍后可重试。');
}

export async function readWindowsJobMembers(job:string,root:{pid:number;startedAt:string},timeout=10000):Promise<ProcessIdentity[]>{
  const deadline=performance.now()+timeout,helper=await windowsHookIdentityHelper(identityHome(),timeout);
  return readProcessSnapshot(helper,['job-members',job,String(root.pid),root.startedAt],deadline);
}

export async function stopWindowsJob(job:string,root:{pid:number;startedAt:string},timeout=5000,beforeSignal?:()=>void):Promise<void>{
  const helper=await windowsHookIdentityHelper(identityHome());
  beforeSignal?.();
  try{
    const {stdout}=await execute(helper,['job-stop',job,String(root.pid),root.startedAt,String(timeout)],{windowsHide:true,timeout:timeout+2000});
    if(JSON.parse(stdout)==='pending')throw new BridgeError('process_stop_pending','自有 Job 尚未退出，保留归属记录。');
  }catch(error){if((error as {stderr?:string}).stderr?.includes('job_contains_caller'))throw new BridgeError('process_is_caller','要停止的 Job 包含当前命令，未停止。请在外部终端重新执行。');throw error;}
}

                                        
export async function assignGatedWindowsJob(identity:Pick<ProcessIdentity,'pid'|'startedAt'>,completionFile:string):Promise<{name:string;holder:{pid:number;startedAt:string};completionFile:string}>{
  const helper=await windowsHookIdentityHelper(identityHome());
  const {stdout}=await execute(helper,['job-attach',String(identity.pid),identity.startedAt,completionFile],{windowsHide:true,timeout:10000});return JSON.parse(stdout);
}

export async function watchWindowsJobNative(job:string,identity:Pick<ProcessIdentity,'pid'|'startedAt'>):Promise<void>{
  const helper=await windowsHookIdentityHelper(identityHome());
  try{await execute(helper,['job-root',job,String(identity.pid),identity.startedAt],{windowsHide:true,timeout:12000});}
  catch(error){throw new BridgeError('launch_owner_unverified','无法交接同一次启动的原生宿主句柄：'+describeError(error));}
}
