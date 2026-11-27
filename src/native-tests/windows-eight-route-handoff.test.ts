import assert from 'node:assert/strict';
import test from 'node:test';
import {multiRuntimeFixture} from './multi-runtime-fixture.js';
import {brokerCall} from '../ipc/ipc.js';
import {ipcBudgetMs,latency,probeIpcWave,waveReport} from './ipc-probe.js';
import {updateBroker} from '../install/setup-broker.js';
import {activeRuntime, runtimeManifest} from '../install/runtime-install.js';
import {ownedProcessState} from '../process/process-stop.js';
import {planCodexResume, readSessionProfile, sessionFile} from '../hosts/codex/codex-session-profile.js';
import {readCodexTurnEvidence} from '../hosts/codex/codex-turn-evidence.js';
import {delay} from '../shared/time.js';
import {readFile} from 'node:fs/promises';
import path from 'node:path';

interface ServingBroker {
  pid:number;
  runtime:{generation:string;buildId:string;windowsIndependent?:boolean};
  handoff?:{pending?:boolean;transferred:number;held:number};
}

const stressClients=24,stressPollMs=200,eventLoopP99BudgetMs=200;
const expectedCutoverCodes=new Set(['starting','handoff_retry','stopping','ENOENT','ECONNREFUSED','ECONNRESET','EPIPE','disconnected']);

function assertServingWave(wave:Awaited<ReturnType<typeof probeIpcWave>>,clients:number,pid?:number,generation?:string){
  assert.equal(wave.connected,clients,JSON.stringify(waveReport(wave)));
  for(const sample of wave.samples){
    assert.equal(sample.code,undefined,JSON.stringify(sample));
    assert.equal(sample.pid,pid);
    assert.equal(sample.generation,generation);
    assert.notEqual(sample.handoff?.pending,true,JSON.stringify(sample));
  }
}

interface StartupStage {stage:string;edge:string;pid:number;generation?:string;at:string;eventLoopP99Ms?:number}

test('Windows 交接八条实际专用路由，保留进行中的原生轮次并记录连接和就绪时间',
  {skip:process.platform!=='win32',timeout:170000},async t=>{
    const routeCount=8,fixture=await multiRuntimeFixture(t,true,routeCount);
    const pointer=activeRuntime(fixture.home),manifest=pointer&&runtimeManifest(pointer.directory);
    assert.ok(manifest);
    const before=await fixture.call<ServingBroker>('ping');
    assert.equal(before.runtime.windowsIndependent,true);
    assert.equal(before.runtime.buildId,manifest.buildId);

    const large=process.env.NXBRIDGE_TEST_WINDOWS_LARGE_ROLLOUT==='1';
    const primerSuffix='-history-primer';
    let rolloutBytes=0;
    if(large){
      await Promise.all(fixture.targets.map((_,index)=>fixture.busy(index,primerSuffix)));
                                              
      rolloutBytes=await fixture.extendHeldHistories(40);
      fixture.targets.forEach((_,index)=>fixture.release(index));
      await fixture.wait(async()=>{
        const status=await fixture.status();
        return fixture.targets.every((target,index)=>status.messages.some((message:any)=>
          message.pairId===fixture.pairs[index].id&&message.body==='MULTI_BUSY_'+index+primerSuffix&&message.state==='contextObserved')&&
          status.peers.some((peer:any)=>peer.id===target.id&&peer.state==='ready'));
      },'填充后 Codex 未完成原生历史预备轮次',20000);
    }
                                      
    const nativeTurns=large?await Promise.all(fixture.targets.map((_,index)=>fixture.busyNative(index))):undefined;
    if(!large)await Promise.all(fixture.targets.map((_,index)=>fixture.busy(index)));
    const snapshot=async()=>{
      const status=await fixture.status();
      return Promise.all(fixture.targets.map(async (target,index)=>{
        const pair=status.pairs.find((row:any)=>row.id===fixture.pairs[index].id);
        assert.equal(pair?.codex,target.id);
        assert.equal(pair?.claude,fixture.sources[index].id);
        const peer=status.peers.find((row:any)=>row.id===target.id);
        assert.equal(peer?.sessionId,target.sessionId);
        assert.equal(peer?.owner,'nxbridge');
        const endpoint=status.endpoints.find((row:any)=>row.peerId===target.id&&row.active&&row.mode==='app-server');
        assert.ok(endpoint?.runtimeId&&endpoint.pid&&endpoint.procStart);
        assert.equal(await ownedProcessState({pid:endpoint.pid,startedAt:endpoint.procStart}),'alive');
        const file=sessionFile(endpoint.configHome,target.sessionId);
        assert.ok(file);
        const [profile,firstTurn]=await Promise.all([readSessionProfile(file,target.sessionId),readCodexTurnEvidence(file,target.sessionId)]);
        assert.ok(profile.verified);
        let turn=firstTurn;
        if(large&&!turn.caughtUp)await fixture.wait(async()=>{
          turn=await readCodexTurnEvidence(file,target.sessionId);
          return turn.caughtUp;
        },'压力历史尚未读取到当前原生轮次',5000);
        assert.ok(turn.openTurnId);
        if(nativeTurns)assert.equal(turn.openTurnId,nativeTurns[index]);
        const original=status.messages.find((row:any)=>row.pairId===pair.id&&row.body==='MULTI_BUSY_'+index+(large?primerSuffix:''));
        assert.ok(original);
        return {sessionId:peer.sessionId,runtimeId:endpoint.runtimeId,pid:endpoint.pid,procStart:endpoint.procStart,
          address:endpoint.address,turnId:turn.openTurnId,messageId:original.id,attempts:original.attempts,
          profile:planCodexResume(target.sessionId,profile,target.cwd,{exact:true}).args,permission:profile.activePermission};
      }));
    };
    const input=await snapshot(),requests=fixture.requests.length,began=performance.now(),clients=large?stressClients:1;
    const baseline=await probeIpcWave(fixture.home,clients,began);
    assertServingWave(baseline,clients,before.pid,before.runtime.generation);
    const trace=[baseline];
    let finished=false;
    const observer=(async()=>{while(!finished&&!t.signal.aborted){
      trace.push(await probeIpcWave(fixture.home,clients,began));
      if(!finished)await delay(large?stressPollMs:500);
    }})();
    const progress:string[]=[];
    const outcome=await updateBroker(fixture.home,manifest,brokerCall,undefined,line=>progress.push(line)).finally(async()=>{finished=true;await observer;});
    const after=await probeIpcWave(fixture.home,clients,began);
    trace.push(after);
    const elapsedMs=Math.round(performance.now()-began);
    const readStartupTrace=()=>readFile(path.join(fixture.home,'run','broker-startup.log'),'utf8').catch(()=>undefined);
    let startupTrace=await readStartupTrace();
    let listenStage:StartupStage|undefined;
    let servingStage:StartupStage|undefined;
    let readyStage:StartupStage|undefined;
    const serving=outcome.servingGeneration;
    if(outcome.restarted&&serving)await fixture.wait(async()=>{
      startupTrace=await readStartupTrace();
      const complete=startupTrace?.slice(0,startupTrace.lastIndexOf('\n'))??'';
      const stages=complete.split('\n').filter(Boolean).map(line=>JSON.parse(line) as StartupStage)
        .filter(stage=>stage.edge==='end'&&stage.pid===serving.pid);
      listenStage=stages.find(stage=>stage.stage==='listen');
      servingStage=stages.find(stage=>stage.stage==='serving'&&stage.generation===serving.generation);
      readyStage=stages.find(stage=>stage.stage==='ready'&&stage.generation===serving.generation);
      return !!readyStage;
    },'新协调器未写出就绪阶段诊断',5000);
    const samples=trace.flatMap(wave=>wave.samples);
    const listeningAt=listenStage?Date.parse(listenStage.at):undefined,servingAt=servingStage?Date.parse(servingStage.at):undefined,
      readyAt=readyStage?Date.parse(readyStage.at):undefined;
    const wavesWithin=(start:number|undefined,end:number|undefined)=>start!==undefined&&end!==undefined?
      trace.filter(wave=>wave.startedAt>=start&&wave.finishedAt<end):[];
    const startingWaves=wavesWithin(listeningAt,servingAt),pendingWaves=wavesWithin(servingAt,readyAt);
    const ipc={clients,waves:trace.map(waveReport),startingWaves:startingWaves.map(wave=>wave.atMs),pendingWaves:pendingWaves.map(wave=>wave.atMs),
      connect:latency(samples.map(s=>s.connectMs)),
      ping:latency(samples.flatMap(s=>s.pingMs===undefined?[]:[s.pingMs]))};
    t.diagnostic(JSON.stringify({offered:outcome.handoff.transferred+outcome.handoff.held,elapsedMs,
      oldBroker:before.pid,servingGeneration:outcome.servingGeneration,handoff:outcome.handoff,ipc,progress,rolloutBytes,
      eventLoopP99Ms:readyStage?.eventLoopP99Ms,...(large?{limits:{ipcMs:ipcBudgetMs,eventLoopP99Ms:eventLoopP99BudgetMs}}:{}),realModelCalls:0}));
    if(startupTrace)t.diagnostic(startupTrace);
    assert.equal(outcome.restarted,true,JSON.stringify({...outcome,error:String(outcome.error)}));
    assert.deepEqual(outcome.handoff,{supported:true,used:true,transferred:routeCount,held:0});
    assert.ok(serving);
    assert.notEqual(serving.pid,before.pid);
    assertServingWave(after,clients,serving.pid,serving.generation);
    if(large){
      for(const wave of trace)for(const sample of wave.samples){
        const evidence=JSON.stringify({atMs:wave.atMs,...sample});
        assert.ok(!sample.code||expectedCutoverCodes.has(sample.code),evidence);
        assert.ok(sample.connectMs<=ipcBudgetMs&&(sample.pingMs??0)<=ipcBudgetMs,evidence);
      }
      const p99=readyStage?.eventLoopP99Ms;
      assert.ok(typeof p99==='number'&&p99>0&&p99<=eventLoopP99BudgetMs,`启动事件循环 p99=${p99} ms，预算 ${eventLoopP99BudgetMs} ms`);
      assert.ok(listeningAt!==undefined&&servingAt!==undefined&&readyAt!==undefined&&listeningAt<=servingAt&&servingAt<=readyAt,
        '缺少新协调器监听、开放服务和完成交接的阶段界限');
      if(servingAt-listeningAt>=2*stressPollMs)assert.ok(startingWaves.some(wave=>wave.connected>=20&&
        wave.samples.filter(sample=>sample.code==='starting'&&sample.pingMs!==undefined).length>=20),
      '监听后尚未开放服务时，至少应有一批 20 个并发客户端收到启动中的 RPC 回复');
      if(readyAt-servingAt>=2*stressPollMs)assert.ok(pendingWaves.some(wave=>wave.connected>=20&&
        wave.samples.filter(sample=>sample.pid===serving.pid&&sample.generation===serving.generation&&sample.handoff?.pending===true).length>=20),
      '开放服务后交接仍在进行时，至少应有一批 20 个并发客户端从新协调器收到待结算状态');
      for(const {pid,generation,handoff} of samples){
        if(!handoff?.pending)continue;
        assert.equal(pid,serving.pid);
        assert.equal(generation,serving.generation);
        assert.ok(Number.isInteger(handoff.transferred)&&Number.isInteger(handoff.held)&&handoff.transferred>=0&&handoff.held>=0);
        assert.equal(handoff.transferred+handoff.held,routeCount);
      }
    }
    assert.deepEqual(await snapshot(),input,'交接保持八个原生会话、进程、权限、原轮次和消息尝试次数');
    assert.equal(fixture.requests.length,requests,'接班不得额外提交模型输入');
    await fixture.wait(async()=>await ownedProcessState({pid:before.pid,startedAt:before.runtime.generation})==='dead',
      '已移交八条订阅的旧协调器未退出',7000);

    fixture.targets.forEach((_,index)=>fixture.release(index));
    await fixture.wait(async()=>{
      const status=await fixture.status();
      return input.every(row=>status.messages.some((message:any)=>message.id===row.messageId&&message.state==='contextObserved'))&&
        fixture.targets.every(target=>status.peers.some((peer:any)=>peer.id===target.id&&peer.state==='ready'));
    },'原轮次和已采样的消息应在新协调器上结算',15000);
    assert.equal(fixture.requests.length,requests,'完成原轮次时不得重放旧消息');
    assert.deepEqual(fixture.errors,[]);
  });
