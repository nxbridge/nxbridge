import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {readFileSync} from 'node:fs';
import {DatabaseSync} from 'node:sqlite';
import {Store} from '../storage/store.js';
import {probeProcesses} from '../process/process.js';
import {multiRuntimeFixture} from './multi-runtime-fixture.js';
import {publishEnvironment} from './publish-fixture.js';
import {PACKAGE_ROOT} from '../shared/package-location.js';

const execute=promisify(execFile),previousPublish=process.env.NXBRIDGE_TEST_PREDECESSOR_PUBLISH;
test('实际 2c 发布包以多条忙碌和空闲原会话接班，更新结果与最终交接计数一致',
  {skip:!previousPublish,timeout:165000},async t=>{
  assert.equal(JSON.parse(readFileSync(path.join(previousPublish!,'publish.json'),'utf8')).buildId,'0.1.1-2c874cd3ef76');
  const routeCount=9,f=await multiRuntimeFixture(t,true,routeCount,previousPublish);
  assert.equal((await f.call('ping')).runtime.buildId,'0.1.1-2c874cd3ef76');
  const snapshot=(threads:any[])=>threads.map(thread=>({id:thread.id,turnId:thread.turns.at(-1)?.id,status:thread.turns.at(-1)?.status}));
                                        
  const store=new Store(path.join(f.home,'nxbridge.db'));
  try{
    store.transaction(()=>{
      for(let index=0;index<2100;index++){
        const message=store.accept(f.pairs[index%routeCount],f.sources[index%routeCount],'历史消息 '+index+' '+'.'.repeat(4096),'state-size-'+index,undefined,undefined,'explicit',true);
        store.claim(message.id);store.transition(message.id,'contextObserved');
      }
    });
  }finally{store.close();}
  const busy=[0,2,4,6];
  for(const index of busy)await f.busy(index);
  const before=await f.readNativeThreads(),requests=f.requests.length,old=await f.call('ping');
  const ledger=new DatabaseSync(path.join(f.home,'nxbridge.db'),{readOnly:true});
  let stateSize:{messages:number;bodyBytes:number;databaseBytes:number};
  try{
    const rows=ledger.prepare('SELECT count(*) AS messages,sum(length(CAST(body AS BLOB))) AS bodyBytes FROM messages').get() as {messages:number;bodyBytes:number};
    const pages=ledger.prepare('PRAGMA page_count').get()!.page_count as number,pageSize=ledger.prepare('PRAGMA page_size').get()!.page_size as number;
    stateSize={...rows,databaseBytes:pages*pageSize};assert.ok(rows.messages>=2000);
  }finally{ledger.close();}
  assert.ok(busy.every(index=>before[index].status.type==='active'));
  const publish=process.env.NXBRIDGE_TEST_CANDIDATE_PUBLISH??path.join(PACKAGE_ROOT,'publish'),
    buildId=JSON.parse(readFileSync(path.join(publish,'publish.json'),'utf8')).buildId,bin=path.join(path.dirname(f.home),'fixture-bin');
  const began=performance.now();
  const {stdout,stderr}=await execute(process.execPath,['--import',new URL('./update-probe-delay.js',import.meta.url).href,
    path.join(publish,'nxbridge.mjs'),'update','--from',publish,'--bin-dir',bin],
    {cwd:PACKAGE_ROOT,env:publishEnvironment(f.home,bin),windowsHide:true,timeout:65000,maxBuffer:8*1024*1024});
  const result=JSON.parse(stdout),final=await f.call('ping');
  t.diagnostic(JSON.stringify({oldBuild:old.runtime.buildId,newBuild:buildId,routes:routeCount,stateSize,busy,elapsedMs:Math.round(performance.now()-began),
    reported:{brokerRestarted:result.brokerRestarted,handoff:result.handoff,notes:result.notes},final:{pid:final.pid,handoff:final.handoff},progress:stderr}));
  assert.match(stderr,/fixture\.readiness_probe_delayed/,'未注入实际状态查询的回执延迟，不能声称覆盖过短查询超时');
  assert.equal(result.installed,true);assert.equal(result.runtime.buildId,buildId);
  assert.equal(result.brokerRestarted,true,JSON.stringify({reported:result.handoff,actual:final.handoff,notes:result.notes}));
  assert.deepEqual(result.handoff,{supported:true,used:true,transferred:routeCount,held:0});
  assert.equal(final.handoff.transferred,routeCount);assert.equal(final.handoff.held,0);
  assert.equal(f.requests.length,requests,'更新不能再开启模型轮次');
  assert.deepEqual(await probeProcesses(f.endpoints.map(endpoint=>({pid:endpoint.pid,procStart:endpoint.procStart}))),f.endpoints.map(()=>'alive'));
  assert.deepEqual(snapshot(await f.readNativeThreads()),snapshot(before),'原线程及正在执行的轮次不能被替换或中断');
  for(const index of busy)f.release(index);
  await f.wait(async()=>(await f.readNativeThreads()).every(thread=>thread.status.type==='idle'),'接班后的原轮次没有正常完成');
  t.diagnostic(JSON.stringify({realModelCalls:0,oldBroker:old.pid,newBroker:final.pid,sameNativePids:f.endpoints.map(endpoint=>endpoint.pid),sameSessions:f.targets.map(target=>target.sessionId)}));
});
