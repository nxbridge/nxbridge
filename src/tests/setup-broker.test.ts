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
  const home=temporary(t),db=oldQuestions(home,Date.now()+2500),steps:string[]=[];
  const broker=(async(method,_params,options)=>{
    steps.push(method);
    if(method==='shutdown')return{stopping:true};
    return options?.start===false?old:{pid:process.pid,runtime:{generation:'next',buildId:candidate.buildId},handoff:{protocol:CUSTODY_PROTOCOL,used:false,transferred:0,held:0}};
  }) as typeof brokerCall;
  try{
    const result=await updateBroker(home,candidate,broker,undefined,()=>{
      steps.push('progress');assert.deepEqual(steps,['ping','progress']);
      setTimeout(()=>db.prepare("UPDATE questions SET status='answered' WHERE status='awaiting_user'").run(),50);
    });
    assert.equal(result.restarted,true);assert.deepEqual(steps,['ping','progress','shutdown','ping']);
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
