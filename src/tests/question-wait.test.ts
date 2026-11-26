import test from 'node:test';
import assert from 'node:assert/strict';
import {waitForQuestion} from '../integration/mcp/question-wait.js';
import {BridgeError} from '../shared/errors.js';
import {QUESTION_POLL_HOLD_MS} from '../shared/question-poll-budget.js';
import {brokerCall,listenBroker} from '../ipc/ipc.js';
import {ensureHome} from '../ipc/home.js';
import {temporary} from './support/temporary.js';
import {delay} from '../shared/time.js';

test('同一只读等待跨排空和断连重连，保留到期时间',async()=>{
  const expires_at=new Date(Date.now()+2000).toISOString();let calls=0,cutoff:number|undefined;
  const result=await waitForQuestion(async deadline=>{
    if(++calls===1)return{status:'pending',expires_at};
    if(calls===2)throw new BridgeError('handoff_retry','重连');
    if(calls===3){assert.ok(deadline&&deadline-performance.now()<2000);cutoff=deadline;throw new BridgeError('disconnected','断开');}
    assert.equal(deadline,cutoff);return{status:'answered',expires_at};
  });
  assert.equal(result.status,'answered');assert.equal(calls,4);
});

test('已到期或身份校验错误立即结束等待，不会再读',async()=>{
  let repeats=0;
  await assert.rejects(waitForQuestion(async()=>{
    if(++repeats===1)return{status:'pending',expires_at:new Date(0).toISOString()};
    throw new BridgeError('starting','尚未就绪');
  }),{code:'starting'});
  assert.equal(repeats,2);
  await assert.rejects(waitForQuestion(async()=>{throw new BridgeError('binding_changed','上下文已变更');}),{code:'binding_changed'});
});

test('重连后任一次成功的待回答回执都结束本次重连预算',async()=>{
  const expires_at=new Date(Date.now()+2000).toISOString();let calls=0;
  const value=await waitForQuestion(async deadline=>{
    if(++calls===1)throw new BridgeError('handoff_retry','重连');
    if(calls===2){assert.ok(deadline!==undefined);return{status:'pending',expires_at};}
    assert.equal(deadline,undefined);return{status:'answered',expires_at};
  });
  assert.equal(value.status,'answered');
});

test('断线三十八秒后首个长轮询仍有完整等待预算，问题原期限不延长',async t=>{
  const home=temporary(t);await ensureHome(home);
  let now=performance.now(),calls=0;
  t.mock.method(performance,'now',()=>now);
  const expires_at=new Date(Date.now()+60000).toISOString(),expiry=now+60000;
  const server=await listenBroker(home,async()=>{
    await delay(QUESTION_POLL_HOLD_MS);
    return{status:'pending',expires_at:new Date(Date.now()+1200000).toISOString()};
  });
  t.after(server.close);
  const value=await waitForQuestion(async connectionDeadline=>{
    calls++;
    if(calls===1)return{status:'pending',expires_at};
    if(calls===2)throw new BridgeError('handoff_retry','排空');
    if(calls===3){
      now+=38000;
      assert.ok(connectionDeadline!==undefined&&connectionDeadline-now<QUESTION_POLL_HOLD_MS);
      return brokerCall('question.wait',{}, {home,start:false,connectionDeadline});
    }
    if(calls===4){
      assert.equal(connectionDeadline,undefined,'成功回执结束旧的重连窗口');
      throw new BridgeError('disconnected','再次断连');
    }
    assert.ok(connectionDeadline!==undefined&&connectionDeadline<=expiry,'后续回执不能延长问题原期限');
    return{status:'answered',expires_at};
  });
  assert.equal(value.status,'answered');assert.equal(calls,5);
});
