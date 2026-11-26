import test from 'node:test';
import assert from 'node:assert/strict';
import {existsSync,mkdirSync,readFileSync,readdirSync} from 'node:fs';
import path from 'node:path';
import {managedMarker} from '../install/installation.js';
import {pointerDetails,stageRuntime} from '../install/runtime-install.js';
import {updateBroker} from '../install/setup-broker.js';
import {ensureHome} from '../ipc/home.js';
import {brokerCall} from '../ipc/ipc.js';
import {probeProcesses} from '../process/process.js';
import {atomicJson} from '../shared/files.js';
import {ownedSocketDirectory} from '../shared/local-paths.js';
import {PACKAGE_ROOT} from '../shared/package-location.js';
import {delay} from '../shared/time.js';
import {temporaryDirectory} from '../tests/support/temporary.js';

test('发布协调器通过真实独立启动传送凭据，封存进程和私有地址及时退出',{timeout:120000},async t=>{
  const temp=temporaryDirectory(),home=temp.directory;
  t.after(async()=>{try{await brokerCall('shutdown',{reason:'test_cleanup'},{home,start:false});}catch{}await temp.cleanup();});
  await ensureHome(home);
  const candidate=await stageRuntime(home,path.join(PACKAGE_ROOT,'publish'));
  mkdirSync(path.join(home,'bin'),{recursive:true});
  atomicJson(path.join(home,'bin','runtime.json'),{owner:managedMarker(home),home,node:process.execPath,entry:candidate.entry,...pointerDetails(home,candidate)});
  const before=await brokerCall('ping',{}, {home});
  if(process.platform==='win32')assert.equal(before.runtime.windowsIndependent,true);
  assert.equal(before.runtime.buildId,candidate.buildId);
  const began=performance.now(),outcome=await updateBroker(home,candidate.manifest,brokerCall);
  assert.equal(outcome.restarted,true,JSON.stringify({...outcome,error:String(outcome.error)}));
  assert.deepEqual(outcome.handoff,{supported:true,used:true,transferred:0,held:0});
  assert.notEqual(outcome.servingGeneration?.pid,before.pid);
  const elapsedMs=Math.round(performance.now()-began);
  assert.ok(elapsedMs<45000,'启动与认领必须落在一次固定交接窗口内。');
  t.diagnostic(JSON.stringify({platform:process.platform,elapsedMs,independent:outcome.windowsIndependent,oldPid:before.pid,newPid:outcome.servingGeneration?.pid}));
  if(process.platform==='win32')assert.equal(outcome.windowsIndependent,true);
  const deadline=performance.now()+6500;
  for(;;){
    const [state]=await probeProcesses([{pid:before.pid,procStart:before.runtime.generation}]);
    if(state==='dead')break;
    if(performance.now()>=deadline)assert.fail('封存进程仍然存活。');await delay(100);
  }
  const sockets=ownedSocketDirectory(home);
  if(process.platform!=='win32'&&existsSync(sockets))assert.deepEqual(readdirSync(sockets).filter(name=>/^h-[a-f0-9]+\.sock$/.test(name)),[]);
  const launch=readFileSync(path.join(home,'run','launch-broker.json'),'utf8');
  assert.doesNotMatch(launch,/"ticket"|"startupToken"/);
});
