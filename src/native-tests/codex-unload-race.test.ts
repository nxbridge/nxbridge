import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import path from 'node:path';
import { mkdirSync, writeFileSync } from 'node:fs';
import { stringify } from 'smol-toml';
import { CodexAdapter, connectAppServer, threadRegistration } from '../hosts/codex/adapter.js';
import { runtimeFor } from '../hosts/managed-runtimes.js';
import { DEFAULT_CONFIG } from '../config/defaults.js';
import { Store } from '../storage/store.js';
import { ensureHome } from '../ipc/home.js';
import type { RpcClient, RpcMessage } from '../ipc/rpc.js';
import { delay } from '../shared/time.js';
import { releaseNativeTestLaunches } from '../tests/support/native-launch.js';
import { temporaryDirectory } from '../tests/support/temporary.js';
import { readModelBody } from './model-http-fixture.js';

async function waitFor(check:()=>boolean|Promise<boolean>,failure:string):Promise<void>{
  const deadline=performance.now()+12000;
  while(!await check()){
    if(performance.now()>=deadline)assert.fail(failure);
    await delay(25);
  }
}

const outcome=(pending:Promise<unknown>)=>pending.then(()=>'ok',error=>{
  if(error?.code==='-32600'&&typeof error.message==='string'&&error.message.startsWith('thread not loaded: '))return'notLoaded';
  return String(error?.code??'error');
});

test('最后订阅断开与原生零延迟卸载竞争时，完成唤醒没有本连接租约就不写入',{timeout:90000},async t=>{
  const temp=temporaryDirectory(),root=temp.directory,home=path.join(root,'bridge'),codexHome=path.join(root,'codex');
  const config={...DEFAULT_CONFIG,codexHome,...(process.env.NXBRIDGE_TEST_CODEX_BINARY?{codexBinary:process.env.NXBRIDGE_TEST_CODEX_BINARY}:{})};
  const adapter=new CodexAdapter(config,home),store=new Store(':memory:'),clients:RpcClient[]=[];
  let requests=0,held:Promise<void>|undefined,release=()=>{};
  const model=http.createServer(async(req,res)=>{
    try{
      await readModelBody(req);
      if(!req.url?.includes('/responses')){res.writeHead(200,{'content-type':'application/json'});res.end('{"data":[]}');return;}
      const id='unload-race-'+(++requests),item={id:'answer-'+id,type:'message',role:'assistant',status:'completed',content:[{type:'output_text',text:'隔离的原生轮次已完成'}]};
      await held;
      res.writeHead(200,{'content-type':'text/event-stream'});
      for(const event of [{type:'response.created',response:{id,status:'in_progress',output:[]}},{type:'response.output_item.added',output_index:0,item},{type:'response.output_item.done',output_index:0,item},{type:'response.completed',response:{id,status:'completed',output:[item],usage:{input_tokens:1,output_tokens:1,total_tokens:2}}}])res.write('data: '+JSON.stringify(event)+'\n\n');
      res.end();
    }catch(error){res.writeHead(500);res.end(String(error));}
  });
  t.after(async()=>{
    release();for(const client of clients)client.close();adapter.close();
    try{assert.deepEqual(await releaseNativeTestLaunches(home),[]);}
    finally{model.closeAllConnections();if(model.listening)await new Promise<void>(resolve=>model.close(()=>resolve()));store.close();await temp.cleanup();}
  });
  mkdirSync(codexHome);await ensureHome(home);
  await new Promise<void>(resolve=>model.listen(0,'127.0.0.1',resolve));
  writeFileSync(path.join(codexHome,'config.toml'),stringify({
    model:'gpt-5.1-codex',model_provider:'local_unload_race',approval_policy:'never',sandbox_mode:'read-only',web_search:'disabled',
    thread_unload_delay_secs:0,analytics:{enabled:false},features:{apps:false,remote_plugin:false,daemon_auto_start:false,code_mode:false},
    model_providers:{local_unload_race:{name:'隔离卸载验收',base_url:`http://127.0.0.1:${(model.address() as AddressInfo).port}/v1`,wire_api:'responses',requires_openai_auth:false,request_max_retries:0,stream_max_retries:0}},
  }),'utf8');
  const placeholder=store.placeholder('codex',root),address=await adapter.ensureEndpoint(root,placeholder.id),runtime=runtimeFor(home,{endpoint:address})!.value;
  assert.equal(runtime.threadUnloadDelaySecs,0);
  const connect=async()=>{const client=await connectAppServer(address,config,undefined,home);clients.push(client);return client;};
  const observer=await connect(),bridge=await adapter.connect(address);
  const loaded=async(id:string)=>(await observer.request('thread/loaded/list',{limit:100},{timeout:4000})).data.includes(id);
  const finish=()=>{release();held=undefined;};
  const startHeld=async()=>{
    const subscriber=await connect(),started=await subscriber.request('thread/start',{cwd:root}),threadId=started.thread.id;
    const expected=requests+1;held=new Promise<void>(resolve=>{release=resolve;});
    const turn=await subscriber.request('turn/start',{threadId,input:[{type:'text',text:'保持轮次到订阅断开后再结束'}]},{mutation:true});
    await waitFor(()=>requests===expected,'隔离模型没有收到被暂停的轮次');
    subscriber.close();
    assert.equal((await observer.request('thread/read',{threadId,includeTurns:false},{timeout:4000})).thread.status.type,'active');
    return{started,threadId,turnId:turn.turn.id};
  };
  const guarded=await startHeld();
  const input=threadRegistration(guarded.started.thread,address,guarded.started)!;
  input.endpoint={...input.endpoint,configHome:codexHome,runtimeId:runtime.id,pid:runtime.pid!,procStart:runtime.procStart!};
  const peer=store.adopt(placeholder.id,input),endpoint=store.endpoint(peer.id)!;
  assert.equal(peer.owner,'nxbridge');assert.equal(adapter.hasSubscription(peer,endpoint),false);
  const requestsOnBridge=t.mock.method(bridge,'request');let admissions=0;
  finish();
  assert.equal(await adapter.wakeSubagents(peer,endpoint,undefined,async()=>{admissions++;return true;}),undefined);
  assert.equal(admissions,0);assert.equal(requestsOnBridge.mock.callCount(),0,'无租约时不得向宿主提交唤醒或预读');
  await waitFor(async()=>!await loaded(guarded.threadId),'受保护线程在末轮结束后仍未卸载');
  const guardedState=(await observer.request('thread/read',{threadId:guarded.threadId,includeTurns:false},{timeout:4000})).thread.status.type;
  assert.equal(guardedState,'notLoaded');
  const turns=async(threadId:string)=>(await observer.request('thread/turns/list',{threadId,limit:1,sortDirection:'desc',itemsView:'summary'},{timeout:4000})).data;
  assert.deepEqual((await turns(guarded.threadId)).map((turn:any)=>[turn.id,turn.status]),[[guarded.turnId,'completed']]);
  assert.equal(requests,1,'无租约不得产生模型请求');

                               
  const raw=await startHeld();let boundary:string|undefined,race:Promise<string>|undefined;
  const raceStart=(at:string)=>{
    boundary=at;
    race=outcome(observer.request('turn/start',{threadId:raw.threadId,input:[{type:'text',text:'原生卸载边界的竞争请求'}]},{mutation:true,timeout:4000}));
  };
  const notification=(message:RpcMessage)=>{
    if(race||message.params?.threadId!==raw.threadId)return;
    const status=message.params?.status?.type;
    if(message.method==='thread/status/changed'&&['idle','notLoaded'].includes(status))raceStart(status);
    else if(message.method==='thread/closed')raceStart('closed');
  };
  observer.on('notification',notification);finish();
  try{
    await waitFor(async()=>!!race||!await loaded(raw.threadId),'没有观察到原生卸载边界');
    if(!race)raceStart('loaded-list-absent');
    const start=await race;
    const [read,unsubscribe,latest,listed,initialized]=await Promise.all([
      outcome(observer.request('thread/read',{threadId:raw.threadId,includeTurns:false},{timeout:4000})),
      outcome(observer.request('thread/unsubscribe',{threadId:raw.threadId},{timeout:4000})),
      outcome(turns(raw.threadId)),outcome(loaded(raw.threadId)),outcome(connect()),
    ]);
    assert.deepEqual([latest,listed,initialized],['ok','ok','ok'],'不进入该线程串行队列的检查应继续可用');
    t.diagnostic(JSON.stringify({guardedState,boundary,start,read,unsubscribe,latest,listed,initialized,queueStall:start==='timeout'&&read==='timeout'&&unsubscribe==='timeout',requests}));
  }finally{observer.off('notification',notification);}
});
