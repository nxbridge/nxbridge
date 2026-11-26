import test,{type TestContext} from 'node:test';
import assert from 'node:assert/strict';
import {once} from 'node:events';
import {spawn} from 'node:child_process';
import {existsSync,mkdirSync,statSync} from 'node:fs';
import path from 'node:path';
import {reserveBrokerStartup,startupReservationDeadline} from '../broker/broker-startup.js';
import type {CustodyOffer,CustodySeal,ParkedCustody} from '../broker/broker-custody.js';
import {custodyCall,SuccessorCustody} from '../broker/custody-client.js';
import {CustodyHolder} from '../broker/custody-holder.js';
import {custodyAddress,custodyDeadlines,CUSTODY_MANIFEST_ENTRY,CUSTODY_PROTOCOL} from '../broker/custody-protocol.js';
import {managedMarker} from '../install/installation.js';
import {ensureHome} from '../ipc/home.js';
import {connectLocalRpc} from '../ipc/ipc.js';
import {processIdentity} from '../process/process.js';
import {atomicJson} from '../shared/files.js';
import {delay} from '../shared/time.js';
import {temporary} from './support/temporary.js';

const buildId='0.1.1-123456789abc';
const rows:CustodyOffer[]=['a','b','c'].map((id,index)=>({id,groupId:index===2?'second':'first',memberId:id,sessionId:id,address:'test',epoch:'epoch',runtimeId:'runtime',pid:42,procStart:'native'}));

async function stoppedChild(t:TestContext){
  const child=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore',windowsHide:true});
  const ended=once(child,'close');
  t.after(async()=>{if(child.exitCode===null&&child.signalCode===null)child.kill();await ended;});
  return{child,identity:await processIdentity(child.pid!),ended};
}

async function fixture(t:TestContext,seal:(cutover:CustodySeal)=>Promise<ParkedCustody>,updaterChild=false){
  const home=temporary(t),directory=path.join(home,'runtime',buildId),expected={pid:2147483646,startedAt:'predecessor'};
  await ensureHome(home);mkdirSync(directory,{recursive:true});mkdirSync(path.join(home,'bin'));
  const owner=managedMarker(home);
  atomicJson(path.join(home,'bin','runtime.json'),{owner,buildId,directory});
  atomicJson(path.join(directory,'.nxbridge-runtime.json'),{owner,buildId});
  atomicJson(path.join(directory,'publish.json'),{owner:'nxbridge-publish-v1',buildId,version:'0.1.1',files:{[CUSTODY_MANIFEST_ENTRY]:'fixture'}});
  const updaterRun=updaterChild?await stoppedChild(t):undefined,updater=updaterRun?.identity??await processIdentity(process.pid),reservation=await reserveBrokerStartup(home,updater);
  t.after(reservation);
  const endings:boolean[]=[];
  const holder=new CustodyHolder(home,expected,seal,()=>endings.push(true));
  t.after(()=>holder.close());
  const request={protocol:CUSTODY_PROTOCOL,expected,updater,buildId,startupToken:reservation.token};
  const descriptor=await holder.prepare(request),{deadline}=custodyDeadlines(descriptor.expiresAt);
  return{home,holder,descriptor,deadline,endings,reservation,request,updaterRun,call:<T>(method:string)=>custodyCall<T>(home,descriptor,method,deadline)};
}

const eventually=async(check:()=>boolean)=>{
  const deadline=performance.now()+3500;
  while(!check()){if(performance.now()>deadline)assert.fail('交接端点没有结束');await delay(25);}
};

test('交接凭据仅可认领一次，整组回执释放物理连接并支持同连接重复确认',async t=>{
  const release:string[]=[];let seals=0,closed=0;
  const f=await fixture(t,async({commit})=>{
    seals++;commit();return{offers:rows,live:id=>rows.some(row=>row.id===id)&&!release.includes(id),release:id=>{release.push(id);},close:()=>{closed++;}};
  });
  const address=custodyAddress(f.home,f.descriptor.updateId);
  if(process.platform!=='win32'){
    assert.ok(Buffer.byteLength(address)<100);assert.equal(statSync(address).mode&0o777,0o600);
  }
  assert.ok(f.descriptor.expiresAt<=await startupReservationDeadline(f.home,f.reservation.token,f.request.updater));
  assert.ok(f.descriptor.expiresAt-Date.now()<=45000);
  const bad=await connectLocalRpc(address,'0'.repeat(64),1000);try{await assert.rejects(bad.request('seal'),{code:'disconnected'});}finally{bad.close();}
  assert.equal(seals,0);
  assert.deepEqual(await f.call('seal'),{phase:'sealed',offered:3});
  await assert.rejects(f.call('seal'),{code:'handoff_rejected'});
  const {identity}=await stoppedChild(t);
  const acquisition=await SuccessorCustody.claim(f.home,f.descriptor,identity);
  assert.ok(acquisition);t.after(()=>acquisition.close());
  assert.equal(await SuccessorCustody.claim(f.home,f.descriptor,identity),undefined);
  await assert.rejects(f.call('abort'),{code:'handoff_rejected'});
  assert.equal(await acquisition.verify('a'),true);
  await assert.rejects(acquisition.resolve(['a'],'transferred'),{code:'handoff_rejected'});
  assert.deepEqual(release,[]);
  await acquisition.resolve(['a','b'],'transferred');await acquisition.resolve(['a','b'],'transferred');
  await acquisition.resolve(['c'],'held');
  assert.deepEqual(acquisition.result,{used:true,transferred:2,held:1});assert.deepEqual(release,['a','b','c']);
  assert.equal(await acquisition.verify('a'),false);
  await assert.rejects(acquisition.resolve(['a','b'],'held'),{code:'handoff_rejected'});
  acquisition.close();await eventually(()=>closed===1&&f.endings.length===1);
  if(process.platform!=='win32')assert.equal(existsSync(address),false);
});

test('排空连接意外断开时撤销排空，同一个服务可重新提供交接',async t=>{
  let attempts=0;let entered!:()=>void,returned!:()=>void;
  const started=new Promise<void>(resolve=>{entered=resolve;}),aborted=new Promise<void>(resolve=>{returned=resolve;});
  const f=await fixture(t,async({signal,commit})=>{
    if(++attempts===1){entered();await once(signal,'abort');returned();throw Error('撤销排空');}
    commit();return{offers:[],live:()=>false,release:()=>{},close:()=>{}};
  });
  const rpc=await connectLocalRpc(custodyAddress(f.home,f.descriptor.updateId),f.descriptor.ticket,1000);
  const pending=assert.rejects(rpc.request('seal'),{code:'disconnected'});
  await started;rpc.close();await pending;await aborted;
  await delay(100);
  assert.deepEqual(f.endings,[]);
  const next=await f.holder.prepare(f.request);assert.notEqual(next.ticket,f.descriptor.ticket);
  assert.deepEqual(await custodyCall(f.home,next,'seal',custodyDeadlines(next.expiresAt).deadline),{phase:'sealed',offered:0});
});

test('预约者在封存后退出时关闭未被认领的订阅',async t=>{
  let closed=false;
  const f=await fixture(t,async({commit})=>{
    commit();return{offers:rows,live:()=>true,release:()=>{},close:()=>{closed=true;}};
  },true);
  await f.call('seal');
  f.updaterRun!.child.kill();await f.updaterRun!.ended;
  await eventually(()=>closed&&f.endings.length===1);
  await assert.rejects(f.call('status'));
});

test('认领后的预约者退出不打断交接，接班进程退出才释放剩余订阅',async t=>{
  let closed=false;
  const f=await fixture(t,async({commit})=>{
    commit();return{offers:rows,live:()=>true,release:()=>{},close:()=>{closed=true;}};
  },true);
  await f.call('seal');
  const next=await stoppedChild(t),acquisition=await SuccessorCustody.claim(f.home,f.descriptor,next.identity);
  assert.ok(acquisition);t.after(()=>acquisition.close());
  f.updaterRun!.child.kill();await f.updaterRun!.ended;
  assert.equal(await acquisition.verify('a'),true);
                         
  const disconnected=once(acquisition.signal,'abort',{signal:AbortSignal.timeout(3500)});
  next.child.kill();
  await Promise.all([next.ended,eventually(()=>closed&&f.endings.length===1),disconnected]);
  assert.equal(acquisition.signal.aborted,true);
});
