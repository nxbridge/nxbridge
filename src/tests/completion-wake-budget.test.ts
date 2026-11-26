import test from 'node:test';
import assert from 'node:assert/strict';
import {CompletionWakeBudget,COMPLETION_WAKE_LIMIT} from '../broker/health/completion-wake-budget.js';
import type {HealthStorage} from '../broker/health/contracts.js';

function setup(){
  const settings=new Map<string,unknown>();
  const storage:HealthStorage={setting:<T>(key:string)=>structuredClone(settings.get(key)) as T|undefined,
    setSetting:(key,value)=>{settings.set(key,structuredClone(value));},transaction:work=>work(),event:()=>{}};
  return{storage,budget:new CompletionWakeBudget(storage)};
}
const peer='foreground',scope='native-scope';

test('确认、承诺和只读工具形成的连续回合不能补充自动完成提醒额度',()=>{
  for(const kind of ['确认消息','继续工作的承诺','只读工具']){
    const f=setup();
    for(let round=0;round<100;round++){
      const id=kind+round,allowed=f.budget.admit(peer,id,scope);
      assert.equal(!!allowed,round<COMPLETION_WAKE_LIMIT);
      if(allowed){assert.ok(f.budget.started(peer,id,scope,'turn-'+round));assert.equal(f.budget.completed(peer,id,scope,'turn-'+round,false),false);}
    }
    assert.equal(f.budget.status(peer).used,COMPLETION_WAKE_LIMIT);
  }
});

test('多次无动作通知回合结算后不消耗额度，之后真实停住仍能被提醒',()=>{
  const {budget}=setup();
  for(let round=0;round<100;round++){
    const id='report-'+round,turn='inert-'+round;
    assert.ok(budget.admit(peer,id,scope));assert.ok(budget.started(peer,id,scope,turn));
    assert.ok(budget.completed(peer,id,scope,turn,true));
    assert.equal(budget.completed(peer,id,scope,turn,true),false,'重复终态不能重复退费');
  }
  assert.deepEqual(budget.status(peer),{used:0,limit:COMPLETION_WAKE_LIMIT,remaining:COMPLETION_WAKE_LIMIT,awaitingProof:0});
  assert.ok(budget.admit(peer,'silent-stop',scope));
});

test('通知与原生轮次及会话范围必须精确关联，未知提交不会因重启重新获得额度',()=>{
  const f=setup();assert.ok(f.budget.admit(peer,'unknown',scope));
  const resumed=new CompletionWakeBudget(f.storage);
  assert.equal(resumed.admit(peer,'unknown',scope),undefined);
  assert.equal(resumed.completed(peer,'unknown',scope,'unproven-turn',true),false);
  assert.equal(resumed.started(peer,'unknown','new-scope','turn'),false);
  assert.ok(resumed.started(peer,'unknown',scope,'turn'));
  assert.equal(resumed.started(peer,'unknown',scope,'different-turn'),false);
  assert.equal(resumed.completed(peer,'unknown',scope,'different-turn',true),false);
  assert.ok(resumed.completed(peer,'unknown',scope,'turn',true));
  assert.equal(resumed.status(peer).used,0);
});

test('确定提交前失败可沿用预约重试，结果未知和已开始的轮次不能重投',()=>{
  const {budget}=setup();assert.ok(budget.admit(peer,'retry',scope));
  budget.beforeSubmitFailed(peer,'retry',scope);assert.ok(budget.admit(peer,'retry',scope));
  assert.equal(budget.status(peer).used,1);assert.equal(budget.admit(peer,'retry',scope),undefined);
  assert.ok(budget.started(peer,'retry',scope,'native-turn'));
  budget.beforeSubmitFailed(peer,'retry',scope);assert.equal(budget.admit(peer,'retry',scope),undefined);
});

test('已撤销的未提交提醒可用同一编号重新竞争额度，已进入原生轮次的退费不可重投',()=>{
  const f=setup(),budget=new CompletionWakeBudget(f.storage,1);
  assert.ok(budget.admit(peer,'same',scope));
  assert.ok(budget.beforeSubmitFailed(peer,'same',scope));
  assert.ok(budget.releaseUnsubmitted(peer,'same'));
  assert.equal(budget.status(peer).used,0);
  assert.equal(budget.canAdmit(peer,'same','other-scope'),false);
  assert.ok(budget.admit(peer,'unrelated',scope));
  assert.equal(budget.canAdmit(peer,'same',scope),false);
  assert.ok(budget.beforeSubmitFailed(peer,'unrelated',scope));
  assert.ok(budget.releaseUnsubmitted(peer,'unrelated'));
  const recovered=new CompletionWakeBudget(f.storage,1);
  assert.ok(recovered.canAdmit(peer,'same',scope));assert.ok(recovered.admit(peer,'same',scope));
  assert.equal(recovered.status(peer).used,1);
  assert.ok(recovered.started(peer,'same',scope,'already-entered'));
  assert.ok(recovered.completed(peer,'same',scope,'already-entered',true));
  assert.equal(recovered.canAdmit(peer,'same',scope),false);assert.equal(recovered.admit(peer,'same',scope),undefined);
});

test('只有新人工输入开启新额度，旧输入和旧轮次的迟到结算都不能影响新额度',t=>{
  t.mock.timers.enable({apis:['Date'],now:10});
  const {budget}=setup();assert.ok(budget.human(peer,{id:'human-1',at:10}));
  assert.ok(budget.admit(peer,'old-notice',scope));assert.ok(budget.started(peer,'old-notice',scope,'old-turn'));
  assert.equal(budget.human(peer,{id:'human-1',at:11}),false);
  assert.equal(budget.human(peer,{id:'older-human',at:9}),false);
  t.mock.timers.tick(10);
  assert.ok(budget.human(peer,{id:'human-2',at:20}));
  assert.equal(budget.completed(peer,'old-notice',scope,'old-turn',true),false);
  assert.equal(budget.status(peer).remaining,COMPLETION_WAKE_LIMIT);
});

test('人工输入的确认迟到时，输入之后发生的干预仍计入这一批额度',t=>{
  t.mock.timers.enable({apis:['Date'],now:100});
  const {budget}=setup();assert.ok(budget.admit(peer,'late-human-stop',scope));
  assert.ok(budget.human(peer,{id:'actually-started-earlier',at:90}));
  assert.equal(budget.status(peer).used,1);
  assert.equal(budget.human(peer,{id:'duplicate-with-new-time',at:90}),false);
});

test('运行中修改配置和重启不增加本批额度，下一次人工输入才采用新上限',()=>{
  const f=setup();assert.ok(f.budget.admit(peer,'first',scope));
  const changed=new CompletionWakeBudget(f.storage,8);
  assert.equal(changed.status(peer).limit,COMPLETION_WAKE_LIMIT);
  assert.ok(changed.human(peer,{id:'new-human',at:30}));
  assert.equal(changed.status(peer).limit,8);
});

test('调低上限或设为零立即阻止新干预，调高仍不能给当前批次补额',()=>{
  const f=setup();assert.ok(f.budget.admit(peer,'first',scope));
  const lowered=new CompletionWakeBudget(f.storage,1);assert.equal(lowered.status(peer).remaining,0);assert.equal(lowered.admit(peer,'second',scope),undefined);
  const disabled=new CompletionWakeBudget(f.storage,0);assert.equal(disabled.status(peer).limit,0);assert.equal(disabled.reservedAllowed(peer,'first',scope),false);
  disabled.beforeSubmitFailed(peer,'first',scope);assert.equal(disabled.admit(peer,'first',scope),undefined);
  assert.equal(new CompletionWakeBudget(f.storage,8).status(peer).limit,4);
});

test('来自其他版本的额度记录保持原样，功能不可用但不使停止或状态查询抛错',()=>{
  const f=setup();f.storage.setSetting('completionWakeBudget:'+peer,{version:2,used:7});
  assert.equal(f.budget.status(peer).available,false);assert.equal(f.budget.admit(peer,'unknown',scope),undefined);
  assert.equal(f.budget.human(peer,{id:'new-human',at:Date.now()}),false);
  assert.deepEqual(f.storage.setting('completionWakeBudget:'+peer),{version:2,used:7});
});
