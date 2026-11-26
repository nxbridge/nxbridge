import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import path from 'node:path';
import {mkdirSync,writeFileSync} from 'node:fs';
import {stringify} from 'smol-toml';
import {DEFAULT_CONFIG} from '../config/defaults.js';
import {catalogCodexThreads} from '../hosts/codex/codex-evidence.js';
import {readCodexRecycleScope} from '../hosts/codex/codex-recycle.js';
import {configClient} from '../hosts/codex/stdio-codex.js';
import {sameDirectory} from '../shared/files.js';
import {delay} from '../shared/time.js';
import {overrideEnvironment} from '../tests/support/environment.js';
import {releaseNativeTestLaunches} from '../tests/support/native-launch.js';
import {temporaryDirectory} from '../tests/support/temporary.js';
import {readModelBody} from './model-http-fixture.js';

test('官方 Codex 独立状态目录完整核验加载范围，旧版兼容且查询不触发模型',{timeout:65000},async t=>{
  const temp=temporaryDirectory(),root=temp.directory,home=path.join(root,'bridge'),codexHome=root;
  mkdirSync(home);
  const restore=overrideEnvironment({NXBRIDGE_HOME:home,CODEX_HOME:codexHome,OPENAI_API_KEY:'catalog-local-only'});
  const config={...DEFAULT_CONFIG,codexHome,...(process.env.NXBRIDGE_TEST_CODEX_BINARY?{codexBinary:process.env.NXBRIDGE_TEST_CODEX_BINARY}:{})};
  let rpc:Awaited<ReturnType<typeof configClient>>|undefined,requests=0,providerError:unknown;
  const model=http.createServer(async(req,res)=>{
    try{
      const raw=await readModelBody(req);
      if(!req.url?.includes('/responses')){res.writeHead(200,{'content-type':'application/json'});res.end('{"data":[]}');return;}
      const body=JSON.parse(raw.toString('utf8'));assert.equal(body.model,'gpt-5.1-codex');requests++;
      const id='catalog-'+requests,item={type:'message',id:'msg-'+id,role:'assistant',phase:'final_answer',status:'completed',content:[{type:'output_text',text:'CATALOG_LOCAL_DONE'}]};
      res.writeHead(200,{'content-type':'text/event-stream'});
      for(const event of [{type:'response.created',response:{id,status:'in_progress',output:[]}},{type:'response.output_item.added',output_index:0,item},{type:'response.output_item.done',output_index:0,item},{type:'response.completed',response:{id,status:'completed',output:[item],usage:{input_tokens:1,output_tokens:1,total_tokens:2}}}])res.write('data: '+JSON.stringify(event)+'\n\n');
      res.end();
    }catch(error){providerError=error;res.writeHead(500);res.end(String(error));}
  });
  t.after(async()=>{
    try{await rpc?.close();assert.deepEqual(await releaseNativeTestLaunches(home),[]);}
    finally{model.closeAllConnections();await new Promise<void>(resolve=>model.close(()=>resolve()));restore();await temp.cleanup();}
  });
  await new Promise<void>(resolve=>model.listen(0,'127.0.0.1',resolve));
  writeFileSync(path.join(codexHome,'config.toml'),stringify({
    model:'gpt-5.1-codex',model_provider:'catalog_local',web_search:'disabled',approval_policy:'never',sandbox_mode:'read-only',
    check_for_update_on_startup:false,suppress_unstable_features_warning:true,analytics:{enabled:false},features:{apps:false,remote_plugin:false,daemon_auto_start:false},
    model_providers:{catalog_local:{name:'本地目录验收',base_url:'http://127.0.0.1:'+(model.address() as any).port+'/v1',wire_api:'responses',requires_openai_auth:false,request_max_retries:0,stream_max_retries:0}},
    projects:{[root]:{trust_level:'trusted'}},
  }),'utf8');
  rpc=await configClient(config,home);
  const completed=new Set<string>();rpc.on('notification',event=>{if(event.method==='turn/completed'&&event.params?.turn?.status==='completed')completed.add(event.params.turn.id);});
  const {thread}=await rpc.request('thread/start',{cwd:root,approvalPolicy:'never',sandbox:'read-only'});
  const {turn}=await rpc.request('turn/start',{threadId:thread.id,input:[{type:'text',text:'保存隔离的原生目录验收历史'}]});
  const deadline=Date.now()+30000;
  while(!completed.has(turn.id)){if(providerError)throw providerError;assert.ok(Date.now()<deadline,'隔离原生轮次未完成');await delay(50);}
  assert.equal(requests,1);
  let complete=false;
  const started=performance.now(),independent=await catalogCodexThreads(rpc,new Set([thread.id]),{independent:true,onComplete:()=>{complete=true;}});
  assert.equal(complete,true,'必须读完原生目录，不能把未读页当成缺席');
  assert.equal(independent.get(thread.id)?.id,thread.id);
  assert.ok(sameDirectory(independent.get(thread.id)!.cwd,root));
  const scope=await readCodexRecycleScope(rpc,thread.id);
  assert.deepEqual([...scope.loaded],[thread.id]);
  assert.equal(scope.threads.get(thread.id)?.id,thread.id);
  const ordinary=await catalogCodexThreads(rpc,new Set([thread.id]));
  assert.equal(ordinary.get(thread.id)?.id,thread.id);
  assert.equal(requests,1,'目录和范围核验均不创建额外模型轮次');
  t.diagnostic(JSON.stringify({catalogComplete:true,scopeComplete:true,catalogMs:performance.now()-started,localRequests:requests,realModelCalls:0}));
});
