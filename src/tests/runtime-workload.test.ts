import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { assertRuntimeWorkloadIdle, configuredMcpInfrastructure } from '../process/runtime-workload.js';
import { declareOwnedNative, releaseOwnedProcess, rememberOwnedChild } from '../process/owned-processes.js';
import { spawnPosixSupervisor } from '../process/posix/posix-supervisor.js';
import { processIdentity } from '../process/process.js';
import { ownedProcessState, stopOwnedProcess, type OwnedIdentity } from '../process/process-stop.js';
import { delay } from '../shared/time.js';
import { temporaryDirectory } from './support/temporary.js';

test('POSIX 隔离后台可保留已配置的 MCP，分离的工具仍在运行时不得回收',{skip:process.platform==='win32',timeout:30000},async t=>{
  const temp=temporaryDirectory(),home=temp.directory,ready=path.join(home,'ready.json');
  mkdirSync(path.join(home,'run'));
  const mcpArgs=['-e','setInterval(()=>{},1000)'];
  const source=`const{spawn}=require('node:child_process'),fs=require('node:fs');
    const options={stdio:'ignore',windowsHide:true};
    const mcp=spawn(process.execPath,${JSON.stringify(mcpArgs)},options);
    const tool=spawn(process.execPath,['-e','setInterval(()=>{},2000)'],{...options,detached:true});
    fs.writeFileSync(process.argv[1]+'.tmp',JSON.stringify({native:process.pid,mcp:mcp.pid,tool:tool.pid}));
    fs.renameSync(process.argv[1]+'.tmp',process.argv[1]);
    setInterval(()=>{},1000);
  `;
  const args=['-e',source,ready],launchId=randomUUID();
  const owner=await rememberOwnedChild(home,spawnPosixSupervisor(process.execPath,args,{stdio:'ignore',waitForNative:true,home}),'codex-runtime',{posixGroup:true},launchId);
  let tool:OwnedIdentity|undefined;
  t.after(async()=>{
                                                
    try{if(tool)await stopOwnedProcess(tool);if(owner)await releaseOwnedProcess(owner);}
    finally{await temp.cleanup();}
  });
  assert.ok(owner);
  for(let i=0;!existsSync(ready)&&i<150;i++)await delay(20);
  const pids=JSON.parse(readFileSync(ready,'utf8')) as {native:number;mcp:number;tool:number};
  tool=await processIdentity(pids.tool);
  const native=await processIdentity(pids.native),mcp=await processIdentity(pids.mcp);
  await declareOwnedNative(owner,native);
  const commands=[{command:process.execPath,args:mcpArgs}],infrastructure=(candidate:typeof native)=>configuredMcpInfrastructure(native,commands,candidate);
  const owners=[owner,{...owner,id:randomUUID(),native:undefined}];
  assert.equal(await infrastructure(mcp),true);
  await assert.rejects(assertRuntimeWorkloadIdle({native,owners,infrastructure}),
    {code:'runtime_workload_active',message:new RegExp('PID '+tool.pid+'）')});
  assert.equal(await ownedProcessState(native),'alive');assert.equal(await ownedProcessState(tool),'alive');
  await stopOwnedProcess(tool);
  await assertRuntimeWorkloadIdle({native,owners,infrastructure});
  assert.equal(await ownedProcessState(mcp),'alive');
                          
  let inspected=false;
  await assertRuntimeWorkloadIdle({native,owners,infrastructure:async candidate=>{
    assert.equal(candidate.pid,mcp.pid);
    inspected=true;
    await stopOwnedProcess(mcp);
    return false;
  }});
  assert.equal(inspected,true);
  assert.equal(await ownedProcessState(native),'alive');
});
