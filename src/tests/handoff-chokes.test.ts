import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {Store} from '../storage/store.js';
import {CodexNativeRpc} from '../hosts/codex/codex-native-rpc.js';
import type {RpcMessage} from '../ipc/rpc.js';
import {temporary} from './support/temporary.js';

test('交接封存关闭真实数据库句柄，迟到的子组件和已捕获语句不能写入',t=>{
  const file=path.join(temporary(t),'state.db'),store=new Store(file);
  t.after(()=>store.close());
  store.setSetting('before','kept');
  const statement=(store as any).db.prepare('INSERT INTO settings(key,value,updated_at) VALUES(?,?,?)');
  store.seal();
  assert.throws(()=>store.setSetting('late','refused'));
  assert.throws(()=>statement.run('captured','false',Date.now()));
  assert.throws(()=>store.bindings.activate({host:'codex',configHome:'fixture',pid:123,procStart:'generation'},'member','session','late'));
  const reader=new DatabaseSync(file,{readOnly:true});
  try{
    assert.equal(reader.prepare("SELECT value FROM settings WHERE key='before'").get()?.value,'"kept"');
    assert.equal(reader.prepare("SELECT count(*) AS n FROM settings WHERE key IN ('late','captured')").get()?.n,0);
  }
  finally{reader.close();}
});

test('封存后的原生连接拒绝全局请求、通知和应答，保留物理连接',async t=>{
  const sent:RpcMessage[]=[],rpc=new CodexNativeRpc(message=>{sent.push(message);queueMicrotask(()=>rpc.receive({id:message.id,result:{ok:true}}));},()=>{},'fixture://handoff');
  t.after(()=>rpc.close());
  await rpc.request('config/read');
  rpc.seal();
  await assert.rejects(rpc.request('thread/loaded/list'),{code:'handoff_sealed',phase:'prepare'});
  assert.throws(()=>rpc.notify('initialized'),{code:'handoff_sealed'});
  assert.throws(()=>rpc.respond('native-request',{ok:true}),{code:'handoff_sealed'});
  assert.equal(sent.length,1);
  assert.equal(rpc.closed,false);
});

test('发送前回调中发生封存时，最后的实际发送门禁仍阻止出站',async t=>{
  const sent:RpcMessage[]=[],rpc=new CodexNativeRpc(message=>sent.push(message),()=>{},'fixture://late-seal');
  t.after(()=>rpc.close());
  await assert.rejects(rpc.request('config/read',{}, {beforeSend:()=>rpc.seal()}),{code:'handoff_sealed',phase:'prepare'});
  assert.deepEqual(sent,[]);
});
