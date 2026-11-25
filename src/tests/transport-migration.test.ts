import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fixture } from './support/broker-fixture.js';
import { TransportMigrations,ownedTransportPaths,persistentTransportPath } from '../broker/recovery/transport-migration.js';
import type { OwnedProcessRecord } from '../process/owned-processes.js';

test('迁移只在停止前消费自己的额度，协调器重建和健康旧连接不能返还额度',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),peer=f.store.updatePeer(f.c.id,{owner:'nxbridge'}),endpoint=f.store.endpoint(peer.id)!;
  let now=1000;const migrations=new TransportMigrations(f.store,()=>now),value=migrations.observe(peer,pair.id,endpoint,['old-inbox'])!;
  migrations.hold(value,'session_busy','等待原工作结束');assert.equal(value.attempts,0);assert.equal(migrations.due(value),false);
  now+=30000;assert.equal(migrations.due(value),true);
  migrations.claim(value,peer,endpoint,pair.id);assert.equal(value.attempts,1);
  assert.throws(()=>migrations.claim(value,peer,endpoint,pair.id),{code:'migration_uncertain'});
  const restarted=new TransportMigrations(f.store,()=>now),unknown=restarted.observe(peer,pair.id,endpoint,['old-inbox'])!;
  assert.equal(unknown.state,'unknown');assert.equal(unknown.attempts,1);assert.equal(restarted.due(unknown),false);
  assert.equal(f.broker.managedRecovery.get(peer.id),undefined);
  const fresh={...endpoint,address:'persistent-inbox',epoch:'new-epoch',pid:12345,procStart:'new-generation'};
  const ready=restarted.observe(peer,pair.id,fresh,[])!;assert.equal(ready.state,'ready');assert.equal(ready.attempts,1);
});

test('重启后仍发布临时地址只尝试一次，换配对或代次不能借用旧迁移预约',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),peer=f.store.updatePeer(f.c.id,{owner:'nxbridge'}),endpoint=f.store.endpoint(peer.id)!;
  const migrations=new TransportMigrations(f.store),value=migrations.observe(peer,pair.id,endpoint,['old'])!;
  assert.throws(()=>migrations.claim(value,peer,{...endpoint,procStart:'other'},pair.id),{code:'migration_changed'});
  assert.throws(()=>migrations.claim(value,peer,endpoint,'other-pair'),{code:'migration_changed'});
  migrations.claim(value,peer,endpoint,pair.id);migrations.launching(value);migrations.launchGuard(value);
  const result=migrations.observe(peer,pair.id,{...endpoint,address:'still-old',epoch:'replaced',procStart:'fresh'},['still-old'])!;
  assert.equal(result.state,'not_migratable');assert.equal(migrations.due(result),false);assert.equal(result.attempts,1);
  migrations.hold(result,'session_busy','不能覆盖终态');assert.equal(migrations.get(peer.id)?.state,'not_migratable');
});

test('未停止的迁移可在换配对或宿主能力更新后继续核验，已经提交的额度不会重置',t=>{
  const f=fixture(t),pair=f.store.pair(f.c.id,f.x.id),peer=f.store.updatePeer(f.c.id,{owner:'nxbridge'}),endpoint=f.store.endpoint(peer.id)!;
  let now=1000;const migrations=new TransportMigrations(f.store,()=>now),value=migrations.observe(peer,pair.id,endpoint,['old'])!;
  migrations.hold(value,'claude_inbox_migration_unsupported','宿主不支持',true);now+=30000;
  const changed=migrations.observe(peer,'new-pair',endpoint,['old'])!;assert.equal(changed.pairId,'new-pair');assert.equal(migrations.due(changed),true);
  migrations.claim(changed,peer,endpoint,'new-pair');assert.equal(changed.attempts,1);
  const again=migrations.observe(peer,'third-pair',endpoint,['old'])!;assert.equal(again.attempts,1);assert.equal(migrations.due(again),false);
});

test('迁移盘点只选择当前自有连接和控制端点，Windows 与原生 Codex 固定地址不会触发迁移',t=>{
  const f=fixture(t),home=f.c.cwd,peer={...f.c,owner:'nxbridge' as const},endpoint={...f.store.endpoint(peer.id)!,address:path.join(home,'legacy','inbox.sock')};
  assert.deepEqual(ownedTransportPaths(home,peer,endpoint,[],undefined,undefined,'win32'),[]);
  assert.deepEqual(ownedTransportPaths(home,{...peer,owner:'user'},endpoint,[],undefined,undefined,'linux'),[]);
  assert.deepEqual(ownedTransportPaths(home,peer,endpoint,[],undefined,undefined,'linux'),[endpoint.address]);
  assert.equal(persistentTransportPath(home,path.join(home,'run','ci-current.sock')),true);
  assert.equal(persistentTransportPath(home,path.join(home,'run-foreign','inbox.sock')),false);
  const codex={...f.x,owner:'nxbridge' as const},native={...f.store.endpoint(codex.id)!,address:'unix:///tmp/codex-daemon-1000/native'};
  assert.deepEqual(ownedTransportPaths(home,codex,native,[],undefined,undefined,'darwin'),[]);
  const record={peerId:codex.id,posixControl:path.join(home,'legacy','native.sock'),proof:{kind:'spawn',launchId:'current'},native:{pid:native.pid!,startedAt:native.procStart!}} as OwnedProcessRecord;
  assert.deepEqual(ownedTransportPaths(home,codex,native,[record],undefined,undefined,'darwin'),[record.posixControl]);
  assert.deepEqual(ownedTransportPaths(home,codex,native,[{...record,native:{pid:native.pid!,startedAt:'old'}}],undefined,undefined,'darwin'),[]);
});
