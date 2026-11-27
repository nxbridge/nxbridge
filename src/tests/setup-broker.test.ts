import test from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import path from 'node:path';
import {CUSTODY_MANIFEST_ENTRY,CUSTODY_PROTOCOL} from '../broker/custody-protocol.js';
import {updateBroker} from '../install/setup-broker.js';
import type {brokerCall} from '../ipc/ipc.js';
import {BridgeError} from '../shared/errors.js';
import {temporary} from './support/temporary.js';

const candidate={owner:'nxbridge-publish-v1' as const,buildId:'0.1.1-123456789abc',version:'0.1.1',files:{[CUSTODY_MANIFEST_ENTRY]:'fixture'},dependencies:{}};
const old={pid:2147483646,runtime:{generation:'old',buildId:'0.1.1-2441d4ab28d8'}};
const {DatabaseSync}=createRequire(import.meta.url)('node:sqlite') as typeof import('node:sqlite');

for(const changed of [false,true])test(changed?'接班待结算期间换代不能采用另一协调器的结果':'IPC 已服务时只轮询同代接班结果，未结算的计数不能提前报告',async t=>{
  const home=temporary(t);let reads=0,launches=0;
  const next={pid:process.pid,runtime:{generation:'successor',buildId:candidate.buildId}};
  const broker=(async(method,_params,options)=>{
    assert.equal(method,'ping');
    if(options?.reserved){
      launches++;return{...next,handoff:{protocol:CUSTODY_PROTOCOL,used:true,transferred:0,held:2,pending:true}};
    }
    assert.equal(options?.start,false);
    if(reads++===0)throw Object.assign(Error('尚未运行'),{code:'ENOENT'});
    return{...next,runtime:{...next.runtime,generation:changed?'other-generation':'successor'},
      handoff:{protocol:CUSTODY_PROTOCOL,used:true,transferred:2,held:0}};
  }) as typeof brokerCall;
  const result=await updateBroker(home,candidate,broker);
  assert.equal(launches,1);assert.equal(result.restarted,!changed);
  if(changed)assert.equal((result.error as BridgeError).code,'broker_changed');
  else assert.deepEqual(result.handoff,{supported:true,used:true,transferred:2,held:0});
});

for(const code of ['timeout','broker_timeout'])test('接班查询的一次短超时不抢占整体截止，也不重发启动：'+code,async t=>{
  const home=temporary(t),next={pid:process.pid,runtime:{generation:'successor',buildId:candidate.buildId}};
  let initial=true,launches=0,polls=0;
  const broker=(async(method,_params,options)=>{
    assert.equal(method,'ping');
    if(options?.reserved){launches++;return{...next,handoff:{protocol:CUSTODY_PROTOCOL,used:true,transferred:0,held:9,pending:true}};}
    assert.equal(options?.start,false);
    if(initial){initial=false;throw Object.assign(Error('尚未运行'),{code:'ENOENT'});}
    if(++polls===1)throw new BridgeError(code,'初始化回补期间的短查询超时');
    return{...next,handoff:{protocol:CUSTODY_PROTOCOL,used:true,transferred:9,held:0}};
  }) as typeof brokerCall;
  const result=await updateBroker(home,candidate,broker);
  assert.equal(result.restarted,true);assert.equal(result.error,undefined);assert.equal(launches,1);
  assert.deepEqual(result.handoff,{supported:true,used:true,transferred:9,held:0});
});

for(const failure of ['换代','身份错误','总截止'] as const)test('接班短查询超时后的重查仍遵守原代次、身份和总截止：'+failure,async t=>{
  const home=temporary(t),clock=performance.now.bind(performance);let initial=true,launches=0,polls=0,offset=0;
  t.mock.method(performance,'now',()=>clock()+offset);
  const fault=new BridgeError('ipc_token_invalid','原 IPC 身份不再有效');
  const next={pid:process.pid,runtime:{generation:'successor',buildId:candidate.buildId}};
  const broker=(async(_method,_params,options)=>{
    if(options?.reserved){launches++;return{...next,handoff:{protocol:CUSTODY_PROTOCOL,used:true,transferred:0,held:9,pending:true}};}
    if(initial){initial=false;throw Object.assign(Error('尚未运行'),{code:'ENOENT'});}
    if(options?.timeout!==undefined){
      polls++;
      if(polls===1){if(failure==='总截止')offset+=60000;throw new BridgeError('timeout','单次回执未及时到达');}
      if(failure==='身份错误')throw fault;
    }
    return{...next,runtime:{...next.runtime,generation:failure==='换代'?'different-successor':'successor'},
      handoff:{protocol:CUSTODY_PROTOCOL,used:true,transferred:9,held:0}};
  }) as typeof brokerCall;
  const result=await updateBroker(home,candidate,broker);
  assert.equal(result.restarted,false);assert.equal(launches,1);
  assert.equal((result.error as BridgeError).code,failure==='换代'?'broker_changed':failure==='身份错误'?'ipc_token_invalid':'timeout');
  if(failure==='总截止')assert.equal(polls,1,'总截止耗尽后不能再发下一次查询');
});

test('已开始服务但接班结果一直未结算时沿用固定预算，不再次启动或无限等待',async t=>{
  const home=temporary(t),clock=performance.now.bind(performance);let calls=0,offset=0,launches=0;
  t.mock.method(performance,'now',()=>clock()+offset);
  const broker=(async(_method,_params,options)=>{
    if(options?.reserved)launches++;
    else if(calls++===0)throw Object.assign(Error('尚未运行'),{code:'ENOENT'});
                                
    offset+=60000;
    return{pid:process.pid,runtime:{generation:'successor',buildId:candidate.buildId},
      handoff:{protocol:CUSTODY_PROTOCOL,used:true,transferred:0,held:2,pending:true}};
  }) as typeof brokerCall;
  const result=await updateBroker(home,candidate,broker);
  assert.equal(launches,1);assert.equal(result.restarted,false);assert.equal((result.error as BridgeError).code,'timeout');
  assert.equal(result.handoff.transferred,0);
});

function oldQuestions(home:string,expiry:number){
  const db=new DatabaseSync(path.join(home,'nxbridge.db'));
  db.exec("CREATE TABLE questions(status TEXT, expires_at INTEGER); WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n<101) INSERT INTO questions SELECT 'expired',0 FROM rows");
  db.prepare("INSERT INTO questions VALUES('awaiting_user',?)").run(expiry);return db;
}

test('已知旧版有旧问题时延后更新，未知版不会停止或尝试交接',async t=>{
  for(const known of [false,true]){
    const home=temporary(t),steps:string[]=[];
    if(known)oldQuestions(home,Date.now()-1000).close();
    const serving=known?old:{...old,runtime:{...old.runtime,buildId:'0.1.1-111111111111'}};
    const broker=(async(method,params,options)=>{
      assert.equal(options?.start,false);steps.push(method);
      assert.equal(method,'ping');return serving;
    }) as typeof brokerCall;
    const result=await updateBroker(home,candidate,broker);
    assert.deepEqual(steps,['ping','ping']);
    assert.equal(result.restarted,false);assert.deepEqual(result.servingGeneration,{pid:serving.pid,...serving.runtime});
    assert.equal((result.error as BridgeError).code,known?'handoff_legacy_wait':'handoff_unsupported');
  }
});

test('已知旧版按原到期时间等待活动问题，完成后立即接班',async t=>{
  const home=temporary(t),db=oldQuestions(home,Date.now()+2500),steps:string[]=[],checkpoints:any[]=[];
  const broker=(async(method,_params,options)=>{
    steps.push(method);
    if(method==='shutdown')return{stopping:true};
    return options?.start===false?old:{pid:process.pid,runtime:{generation:'next',buildId:candidate.buildId},handoff:{protocol:CUSTODY_PROTOCOL,used:false,transferred:0,held:0}};
  }) as typeof brokerCall;
  try{
    const result=await updateBroker(home,candidate,broker,undefined,message=>{
      const prefix='协调器更新进度 ';
      if(message.startsWith(prefix)){checkpoints.push(JSON.parse(message.slice(prefix.length)));return;}
      steps.push('progress');assert.deepEqual(steps,['ping','progress']);
      setTimeout(()=>db.prepare("UPDATE questions SET status='answered' WHERE status='awaiting_user'").run(),50);
    });
    assert.equal(result.restarted,true);assert.deepEqual(steps,['ping','progress','shutdown','ping']);
    assert.deepEqual(checkpoints.map(value=>value.phase),['preflight','reserved','legacy_drain','wait_ready','ready']);
    assert.ok(checkpoints.every((value,index)=>value.buildId===candidate.buildId&&value.elapsedMs>=(checkpoints[index-1]?.elapsedMs??0)));
    assert.equal(checkpoints.at(-1).serving.generation,'next');
  }finally{db.close();}
});

test('不支持交接的候选包和已拒绝的协商都不会退回旧版停止流程',async t=>{
  for(const compatible of [false,true]){
    const home=temporary(t),steps:string[]=[];
    const broker=(async(method,params,options)=>{
      assert.equal(options?.start,false);steps.push(method);
      if(method==='broker.handoff.prepare')throw new BridgeError('handoff_rejected','模拟拒绝');
      assert.equal(method,'ping');return{...old,handoff:{protocol:CUSTODY_PROTOCOL}};
    }) as typeof brokerCall;
    const result=await updateBroker(home,compatible?candidate:{...candidate,files:{}},broker);
    assert.deepEqual(steps,compatible?['ping','broker.handoff.prepare','ping']:['ping','ping']);
    assert.equal(result.restarted,false);assert.equal(result.handoff.supported,true);
    assert.equal((result.error as BridgeError).code,compatible?'handoff_rejected':'handoff_candidate_unsupported');
  }
});
