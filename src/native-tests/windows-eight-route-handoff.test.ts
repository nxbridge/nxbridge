import assert from 'node:assert/strict';
import test from 'node:test';
import {multiRuntimeFixture} from './multi-runtime-fixture.js';
import {brokerCall, connectBroker} from '../ipc/ipc.js';
import {updateBroker} from '../install/setup-broker.js';
import {activeRuntime, runtimeManifest} from '../install/runtime-install.js';
import {ownedProcessState} from '../process/process-stop.js';
import {planCodexResume, readSessionProfile, sessionFile} from '../hosts/codex/codex-session-profile.js';
import {readCodexTurnEvidence} from '../hosts/codex/codex-turn-evidence.js';
import {delay} from '../shared/time.js';

interface ServingBroker {
  pid:number;
  runtime:{generation:string;buildId:string;windowsIndependent?:boolean};
}

test('Windows 交接八条实际专用路由，保留进行中的原生轮次并记录连接和就绪时间',
  {skip:process.platform!=='win32',timeout:170000},async t=>{
    const routeCount=8,fixture=await multiRuntimeFixture(t,true,routeCount);
    const pointer=activeRuntime(fixture.home),manifest=pointer&&runtimeManifest(pointer.directory);
    assert.ok(manifest);
    const before=await fixture.call<ServingBroker>('ping');
    assert.equal(before.runtime.windowsIndependent,true);
    assert.equal(before.runtime.buildId,manifest.buildId);

    await Promise.all(fixture.targets.map((_,index)=>fixture.busy(index)));
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
        const [profile,turn]=await Promise.all([readSessionProfile(file,target.sessionId),readCodexTurnEvidence(file,target.sessionId)]);
        assert.ok(profile.verified&&turn.openTurnId);
        const original=status.messages.find((row:any)=>row.pairId===pair.id&&row.body==='MULTI_BUSY_'+index);
        assert.ok(original);
        return {sessionId:peer.sessionId,runtimeId:endpoint.runtimeId,pid:endpoint.pid,procStart:endpoint.procStart,
          address:endpoint.address,turnId:turn.openTurnId,messageId:original.id,attempts:original.attempts,
          profile:planCodexResume(target.sessionId,profile,target.cwd,{exact:true}).args,permission:profile.activePermission};
      }));
    };
    const input=await snapshot(),requests=fixture.requests.length,began=performance.now();
    const trace:{atMs:number;connectMs:number;pingMs?:number;pid?:number;generation?:string;code?:string}[]=[];
    const measure=async()=>{
      const at=performance.now();let connected:number|undefined,rpc;
      try{
        rpc=await connectBroker(fixture.home,500);
        connected=performance.now();
        const result=await rpc.request<ServingBroker>('ping',{}, {timeout:500});
        trace.push({atMs:Math.round(at-began),connectMs:Math.round(connected-at),pingMs:Math.round(performance.now()-connected),
          pid:result.pid,generation:result.runtime.generation});
      }catch(error){
        const end=performance.now();
        trace.push({atMs:Math.round(at-began),connectMs:Math.round((connected??end)-at),
          ...(connected===undefined?{}:{pingMs:Math.round(end-connected)}),code:(error as {code?:string}).code??'unknown'});
      }finally{rpc?.close();}
    };
    let finished=false;
    const observer=(async()=>{while(!finished&&!t.signal.aborted){await measure();if(!finished)await delay(500);}})();
    const outcome=await updateBroker(fixture.home,manifest,brokerCall).finally(async()=>{finished=true;await observer;});
    await measure();
    t.diagnostic(JSON.stringify({offered:outcome.handoff.transferred+outcome.handoff.held,elapsedMs:Math.round(performance.now()-began),
      oldBroker:before.pid,servingGeneration:outcome.servingGeneration,handoff:outcome.handoff,ipc:trace,realModelCalls:0}));
    assert.equal(outcome.restarted,true,JSON.stringify({...outcome,error:String(outcome.error)}));
    assert.deepEqual(outcome.handoff,{supported:true,used:true,transferred:routeCount,held:0});
    assert.notEqual(outcome.servingGeneration?.pid,before.pid);
    const after=trace.at(-1);
    assert.equal(after?.pid,outcome.servingGeneration?.pid);
    assert.equal(after?.generation,outcome.servingGeneration?.generation);
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
