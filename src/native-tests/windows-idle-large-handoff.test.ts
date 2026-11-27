import assert from 'node:assert/strict';
import test from 'node:test';
import path from 'node:path';
import {multiRuntimeFixture} from './multi-runtime-fixture.js';
import {brokerCall,ensureBroker} from '../ipc/ipc.js';
import {updateBroker,type BrokerUpdate} from '../install/setup-broker.js';
import {activeRuntime,runtimeManifest} from '../install/runtime-install.js';
import {ownedProcessState} from '../process/process-stop.js';
import {planCodexResume,readSessionProfile,sessionFile} from '../hosts/codex/codex-session-profile.js';
import {formatGlobalStatus} from '../integration/mcp/presentation.js';
import {Store} from '../storage/store.js';
import {delay} from '../shared/time.js';
import {readFile} from 'node:fs/promises';
import {ipcBudgetMs,latency,probeIpcWave,waveReport} from './ipc-probe.js';

interface BrokerPing {
  pid:number;
  runtime:{generation:string;buildId:string;windowsIndependent?:boolean};
  handoff?:{pending?:boolean};
}

const routes=8,clearBudgetMs=10000;
const transient=new Set(['starting','stopping','handoff_retry','ENOENT','ECONNREFUSED','ECONNRESET','EPIPE','disconnected','timeout','broker_timeout']);

test('Windows 大历史空闲路由在热交接与普通重启后清除会话核对并继续投递',
  {skip:process.platform!=='win32'||process.env.NXBRIDGE_TEST_WINDOWS_LARGE_ROLLOUT!=='1',timeout:170000},async t=>{
    const fixture=await multiRuntimeFixture(t,true,routes);
    const pointer=activeRuntime(fixture.home),manifest=pointer&&runtimeManifest(pointer.directory);
    assert.ok(manifest);
    const read=<T=any>(method:string,params:unknown={})=>brokerCall<T>(method,params,{home:fixture.home,start:false,timeout:1500});
    const ping=()=>read<BrokerPing>('ping');
    const start=async()=>{(await ensureBroker(fixture.home)).close();};
    const waitForExit=async(old:BrokerPing)=>{
      const deadline=performance.now()+5000;
      while(await ownedProcessState({pid:old.pid,startedAt:old.runtime.generation})!=='dead'){
        assert.ok(performance.now()<deadline,`隔离旧协调器 ${old.pid} 未退出`);
        await delay(50);
      }
    };
    const stop=async(old:BrokerPing)=>{
      const reply=await brokerCall<{stopping:boolean;notes:string[]}>('shutdown',{
        releaseOwned:false,expectedBroker:{pid:old.pid,generation:old.runtime.generation},
      },{home:fixture.home,start:false,mutation:true,timeout:10000});
      assert.deepEqual(reply,{stopping:true,notes:[]});
      await waitForExit(old);
    };
    const histories=fixture.targets.map((target,index)=>{
      const file=sessionFile(fixture.endpoints[index].configHome,target.sessionId);
      assert.ok(file);return file;
    });

                                         
    const first=await ping();
    await stop(first);
    const unknownMarker='MULTI_KEEP_UNKNOWN_';
    const seed=new Store(path.join(fixture.home,'nxbridge.db'));
    let unknownIds:string[];
    try{unknownIds=fixture.pairs.map((pair,index)=>{
      const message=seed.accept(seed.pairById(pair.id)!,seed.peer(fixture.sources[index].id),unknownMarker+index,'idle-unknown-'+index);
      assert.equal(seed.claim(message.id),true);
      seed.transition(message.id,'unknown','隔离夹具保留的历史提交不确定记录');
      return message.id;
    });}finally{seed.close();}
    const assertUnknown=(status:any)=>{
      for(const id of unknownIds){
        const row=status.messages.find((message:any)=>message.id===id);
        assert.ok(row,id);assert.equal(row.state,'unknown',id);assert.equal(row.attempts,1,id);
      }
      assert.ok(!fixture.requests.some(request=>JSON.stringify(request.input).includes(unknownMarker)),'未知消息不得变成原生模型输入');
    };
    const assertNativeIdle=async(expected:string[])=>{
      const threads=await fixture.readNativeThreads();
      for(const [index,thread] of threads.entries()){
        assert.equal(thread.id,fixture.targets[index].sessionId);
        assert.equal(thread.status.type,'idle',thread.id);
        assert.equal(thread.turns.at(-1)?.id,expected[index],thread.id);
        assert.equal(thread.turns.at(-1)?.status,'completed',thread.id);
      }
    };
    const hasCompletion=(status:any,expected:string[])=>fixture.targets.every((target,index)=>status.peers.some((peer:any)=>
      peer.id===target.id&&peer.state==='ready'&&peer.completion_guard?.turn_id===expected[index]&&typeof peer.completion_guard.completed_at==='number'));
    const prepareIdleHistory=async(phase:string,mebibytes:number)=>{
      const turnIds=await Promise.all(fixture.targets.map((_,index)=>fixture.busyNative(index)));
      const bytes=await fixture.extendHeldHistories(mebibytes);
      fixture.targets.forEach((_,index)=>fixture.release(index));
      await fixture.wait(async()=>hasCompletion(await fixture.status(),turnIds),'真实长轮次未在隔离协调器登记为完成',15000);
      await assertNativeIdle(turnIds);

                                           
      const module=new URL('../hosts/codex/codex-turn-evidence.js',import.meta.url);
      module.searchParams.set('fixture',phase);
      const {readCodexTurnEvidence}=await import(module.href) as typeof import('../hosts/codex/codex-turn-evidence.js');
      const evidence=await Promise.all(histories.map(async(file,index)=>{
        const read=()=>readCodexTurnEvidence(file,fixture.targets[index].sessionId);
        let result=await read();
        const deadline=performance.now()+5000;
        while(!result.caughtUp&&performance.now()<deadline)result=await read();
        assert.equal(result.status,'supported',file);
        assert.equal(result.caughtUp,true,file);
        assert.equal(result.openTurnId,undefined,file);
        assert.equal(result.latestTurnId,turnIds[index],file);
        assert.equal(result.turns.length,0,file);
        return result.latestTurnId;
      }));
      assert.deepEqual(evidence,turnIds);
      return{turnIds,bytes};
    };
    const nativeSnapshot=async(status:any)=>Promise.all(fixture.targets.map(async(target,index)=>{
      const peer=status.peers.find((row:any)=>row.id===target.id),pair=status.pairs.find((row:any)=>row.id===fixture.pairs[index].id);
      const endpoint=status.endpoints.find((row:any)=>row.peerId===target.id&&row.active&&row.mode==='app-server');
      assert.equal(peer?.sessionId,target.sessionId);assert.equal(peer?.owner,'nxbridge');
      assert.equal(pair?.codex,target.id);assert.equal(pair?.claude,fixture.sources[index].id);
      assert.ok(endpoint?.pid&&endpoint.procStart&&endpoint.runtimeId);
      assert.equal(await ownedProcessState({pid:endpoint.pid,startedAt:endpoint.procStart}),'alive');
      const profile=await readSessionProfile(histories[index],target.sessionId);
      const plan=planCodexResume(target.sessionId,profile,target.cwd,{exact:true});
      return{pair:pair.id,sessionId:peer.sessionId,pid:endpoint.pid,generation:endpoint.procStart,runtimeId:endpoint.runtimeId,address:endpoint.address,
        permission:peer.permission,nativePermission:profile.activePermission,profile:plan.args};
    }));
    const successorReady=async(old:BrokerPing,started:number)=>{
      let firstServing:number|undefined,view='';
      while(performance.now()<(firstServing===undefined?started+20000:firstServing+clearBudgetMs)){
        try{
          const identity=await ping();
          if(identity.pid!==old.pid||identity.runtime.generation!==old.runtime.generation){
            firstServing??=performance.now();
            assert.equal(identity.runtime.buildId,manifest.buildId);
            assert.equal(identity.runtime.windowsIndependent,true);
            const status=await read('status');
            view=formatGlobalStatus(status);
            if(!identity.handoff?.pending&&!view.includes('会话核对：')){
              const now=performance.now();
              assert.ok(now-firstServing<=clearBudgetMs);
              return{identity,status,firstServingMs:Math.round(firstServing-started),clearMs:Math.round(now-firstServing)};
            }
          }
        }catch(error){if(!transient.has((error as {code?:string}).code??''))throw error;}
        await delay(100);
      }
      assert.fail('新协调器未在预算内清除会话核对：'+view.split('\n').filter(line=>line.includes('会话核对')).join('\n'));
    };
    const queuedDelivery=async(index:number,phase:string)=>{
      const pairId=fixture.pairs[index].id,body='MULTI_IDLE_DELIVERY_'+phase,before=fixture.requests.length;
      await fixture.call('pause',{pair_id:pairId});
      const {message_id}=await fixture.call<{message_id:string}>('send',{actor:fixture.actor(fixture.sources[index],'idle-'+phase),message:body});
      const queued=(await fixture.status()).messages.find((row:any)=>row.id===message_id);
      assert.equal(queued?.state,'accepted');assert.equal(queued?.attempts,0);
      assert.equal(fixture.requests.length,before);
      const began=performance.now();
      await fixture.call('resume',{pair_id:pairId});
      await fixture.wait(async()=>{
        const status=await fixture.status();
        return status.messages.some((row:any)=>row.id===message_id&&row.state==='contextObserved')&&
          status.peers.some((peer:any)=>peer.id===fixture.targets[index].id&&peer.state==='ready');
      },'新协调器未投递隔离搭档的待发消息',15000);
      assert.equal(fixture.requests.slice(before).filter(request=>JSON.stringify(request.input).includes(body)).length,1);
      assertUnknown(await fixture.status());
      return Math.round(performance.now()-began);
    };

    const initialRestart=performance.now();
    await start();
    await successorReady(first,initialRestart);
                                                        
    await Promise.all(fixture.targets.map((_,index)=>fixture.busy(index,'-idle-setup')));
    fixture.targets.forEach((_,index)=>fixture.release(index));
    await fixture.wait(async()=>{
      const status=await fixture.status();
      return fixture.targets.every((target,index)=>status.peers.some((peer:any)=>peer.id===target.id&&peer.state==='ready')&&
        status.messages.some((message:any)=>message.pairId===fixture.pairs[index].id&&message.body==='MULTI_BUSY_'+index+'-idle-setup'&&message.state==='contextObserved'));
    },'离线记账后的普通恢复尚未完成真实搭档投递');
    t.diagnostic(JSON.stringify({phase:'offline-ledger-setup',resumeAndDeliveryMs:Math.round(performance.now()-initialRestart),realModelCalls:0}));
    for(const [index,phase] of (['warm','ordinary'] as const).entries()){
      const {turnIds,bytes}=await prepareIdleHistory(phase,index===0?40:10);
      const original=await fixture.status();assertUnknown(original);
      const originals=await nativeSnapshot(original),requests=fixture.requests.length,old=await ping();
      assert.equal(old.handoff?.pending??false,false);
      if(phase==='ordinary')await stop(old);
      const began=performance.now();
      const waves:Awaited<ReturnType<typeof probeIpcWave>>[]=[];
      let finished=false;
      const observer=(async()=>{while(!finished&&!t.signal.aborted){
        waves.push(await probeIpcWave(fixture.home,24,began));if(!finished)await delay(200);
      }})();
      const recovering=(async()=>{
        const ready=await successorReady(old,began),cleared=performance.now();
        if(phase==='ordinary')await fixture.wait(async()=>{
          const status=await fixture.status();
          return hasCompletion(status,turnIds)&&(await fixture.readNativeThreads()).every(thread=>thread.status.type==='idle');
        },'普通重启后的原会话尚未完成常规冷恢复');
        return{...ready,status:phase==='ordinary'?await fixture.status():ready.status,
          nativeRecoveryAfterCheckMs:Math.round(performance.now()-cleared),
          nativeReadyAfterServingMs:Math.round(performance.now()-began-ready.firstServingMs)};
      })();
      const handoff:Promise<BrokerUpdate|void>=phase==='warm'?updateBroker(fixture.home,manifest,brokerCall):start();
      const [recovered,changed]=await Promise.allSettled([recovering,handoff] as const).finally(async()=>{finished=true;await observer;});
      waves.push(await probeIpcWave(fixture.home,24,began));
      const samples=waves.flatMap(wave=>wave.samples),ipc={clients:24,connect:latency(samples.map(sample=>sample.connectMs)),
        ping:latency(samples.flatMap(sample=>sample.pingMs===undefined?[]:[sample.pingMs])),waves:waves.map(waveReport)};
      let loop:{eventLoopP99Ms:number;eventLoopMaxMs:number}|undefined;
      if(phase==='warm'&&recovered.status==='fulfilled'){
        const log=await readFile(path.join(fixture.home,'run','broker-startup.log'),'utf8');
        const stages=log.slice(0,log.lastIndexOf('\n')).split('\n').filter(Boolean).map(line=>JSON.parse(line));
        loop=stages.findLast(stage=>stage.pid===recovered.value.identity.pid&&stage.stage==='ready'&&stage.edge==='end');
      }
      t.diagnostic(JSON.stringify({phase,ipc,...(loop?{eventLoopP99Ms:loop.eventLoopP99Ms,eventLoopMaxMs:loop.eventLoopMaxMs}:{})}));
      if(changed.status==='rejected')throw changed.reason;
      if(recovered.status==='rejected')throw recovered.reason;
      for(const sample of samples){
        assert.ok(!sample.code||transient.has(sample.code),JSON.stringify(sample));
        assert.ok(sample.connectMs<=ipcBudgetMs&&(sample.pingMs??0)<=ipcBudgetMs,JSON.stringify(sample));
      }
      if(phase==='warm')assert.ok(loop&&loop.eventLoopP99Ms>0&&loop.eventLoopP99Ms<=200,'空闲接班的事件循环 p99 必须在 200 毫秒预算内');
      const ready=recovered.value;
      const result:BrokerUpdate|void=changed.value;
      if(result){
        assert.equal(result.restarted,true,String(result.error));
        assert.deepEqual(result.handoff,{supported:true,used:true,transferred:routes,held:0});
        assert.equal(result.servingGeneration?.pid,ready.identity.pid);
        assert.equal(result.servingGeneration?.generation,ready.identity.runtime.generation);
        await waitForExit(old);
      }
      assert.notEqual(ready.identity.runtime.generation,old.runtime.generation);
      assert.equal(fixture.requests.length,requests,'所有路由空闲时清除核对状态');
      await assertNativeIdle(turnIds);
      assert.ok(hasCompletion(ready.status,turnIds),'新协调器必须保留同一原生轮次的完成事实');
      assert.deepEqual(await nativeSnapshot(ready.status),originals,'保留八对会话及原生进程和原设置');
      assertUnknown(ready.status);
      const queuedDeliveryMs=await queuedDelivery(index,phase);
      t.diagnostic(JSON.stringify({phase,routes,rolloutBytes:bytes,oldPid:old.pid,newPid:ready.identity.pid,
        firstServingMs:ready.firstServingMs,clearMs:ready.clearMs,nativeRecoveryAfterCheckMs:ready.nativeRecoveryAfterCheckMs,
        nativeReadyAfterServingMs:ready.nativeReadyAfterServingMs,queuedDeliveryMs,idleBeforeAndAfter:true,unknownAttempts:unknownIds.length,queuedDelivery:true,realModelCalls:0}));
    }
    assert.deepEqual(fixture.errors,[]);
  });
