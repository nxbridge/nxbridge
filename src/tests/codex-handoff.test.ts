import test from 'node:test';
import assert from 'node:assert/strict';
import {once} from 'node:events';
import {WebSocketServer} from 'ws';
import {acquireCodexGroups,parkCodexGroups} from '../hosts/codex/codex-handoff.js';
import {connectAppServer} from '../hosts/codex/adapter.js';
import {CodexNativeRpc} from '../hosts/codex/codex-native-rpc.js';
import {RpcClient} from '../ipc/rpc.js';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import type {CustodyOffer} from '../broker/broker-custody.js';

const offer=(id:string,groupId:string):CustodyOffer=>({id,groupId,memberId:id,sessionId:id,address:groupId,epoch:'route',runtimeId:'runtime',pid:1,procStart:'start',profile:{sessionId:id,offset:1,complete:true,verified:true,meta:{},settings:{}}});

test('同一物理旧连接等全部订阅结算后才关闭，失效只改变该行证据',()=>{
  let closed=0,wire=0;
  const rpc=new RpcClient(()=>{wire++;},()=>{closed++;});
  const parked=parkCodexGroups([{offer:offer('one','first'),rpc},{offer:offer('two','first'),rpc}]);
  rpc.receive({method:'thread/unloaded',params:{threadId:'two'}});
  assert.equal(parked.live('one'),true);assert.equal(parked.live('two'),false);
  parked.release('two');assert.equal(closed,0);
  parked.release('one');assert.equal(closed,1);assert.equal(wire,0);parked.close();assert.equal(closed,1);
});

test('一个端点组失败会先关闭全部临时连接，独立成功组仍晋升',async()=>{
  const trace:string[]=[];
  const result=await acquireCodexGroups({offers:[offer('a','first'),offer('b','first'),offer('c','second')],signal:new AbortController().signal,verify:async()=>true,resolve:async(ids,outcome)=>{trace.push(outcome+':'+ids.join(','));}},async rows=>({
    acquire:async row=>{trace.push('request:'+row.id);if(row.id==='b')throw Error('模拟会话确认失败');},validate:()=>{},promote:()=>{trace.push('promote:'+rows[0]!.groupId);},close:async()=>{trace.push('close:'+rows[0]!.groupId);},
  }));
  assert.deepEqual(result,{promoted:['c'],transferred:1,held:2});
  assert.ok(trace.indexOf('close:first')<trace.indexOf('held:a,b'));
  assert.ok(trace.indexOf('transferred:c')<trace.indexOf('promote:second'));
  assert.equal(trace.includes('promote:first'),false);
});

test('断开实际 WebSocket 后的恢复回执不能晋升，单次尝试不追加原生探针',async t=>{
  const server=new WebSocketServer({host:'127.0.0.1',port:0}),controller=new AbortController();
  t.after(async()=>{for(const socket of server.clients)socket.terminate();await new Promise<void>(resolve=>server.close(()=>resolve()));});
  await once(server,'listening');
  let sends=0,closed:Promise<unknown>|undefined,late:(()=>void)|undefined;
  server.on('connection',socket=>socket.on('message',bytes=>{
    const raw=JSON.parse(bytes.toString());
    if(raw.method==='initialize')socket.send(JSON.stringify({id:raw.id,result:{userAgent:'codex-tui/0.170.0 (test) (codex-tui; 0.170.0)'}}));
    if(raw.method==='thread/resume'){
      sends++;closed=once(socket,'close');late=()=>socket.send(JSON.stringify({id:raw.id,result:{thread:{id:'a'}}}));controller.abort();
    }
  }));
  const endpoint='ws://127.0.0.1:'+(server.address() as {port:number}).port;
  const trace:string[]=[];
  const result=await acquireCodexGroups({offers:[{...offer('a','first'),address:endpoint}],signal:controller.signal,verify:async()=>true,resolve:async(_ids,outcome)=>{trace.push(outcome);}},async()=>{
    const rpc=await connectAppServer(endpoint,DEFAULT_CONFIG,undefined,undefined,{signal:controller.signal});assert.ok(rpc instanceof CodexNativeRpc);
    return{acquire:async()=>{await rpc.resumeForHandoff({threadId:'a'},{timeout:1000,beforeSend:()=>{}});},validate:()=>{},promote:()=>{trace.push('promoted');},close:async()=>{await closed;late?.();trace.push('physical-close');}};
  });
  assert.equal(sends,1);assert.deepEqual(trace,['physical-close','held']);assert.deepEqual(result,{promoted:[],transferred:0,held:1});
});

test('交接确认回执在截止后返回时关闭临时物理连接，不接受迟到晋升',async()=>{
  const controller=new AbortController(),trace:string[]=[];
  const result=await acquireCodexGroups({offers:[offer('a','first')],signal:controller.signal,verify:async()=>true,resolve:async(_ids,outcome)=>{trace.push(outcome);if(outcome==='transferred')controller.abort();}},async()=>({acquire:async()=>{},validate:()=>{},promote:()=>{trace.push('promote');},close:async()=>{trace.push('close');}}));
  assert.deepEqual(trace,['transferred','close','held']);assert.equal(result.held,1);
});
