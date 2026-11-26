import assert from 'node:assert/strict';
import {mkdirSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {boundaryChanges, findForbiddenEdges} from './check-import-boundaries.mjs';

function workspace(t) {
  const root = mkdtempSync(path.join(tmpdir(), 'nxbridge-import-boundaries-'));
  t.after(() => rmSync(root, {recursive: true}));
  const write = (name, contents = '') => {
    const file = path.join(root, name);
    mkdirSync(path.dirname(file), {recursive: true});
    writeFileSync(file, contents, 'utf8');
  };
  write('package.json', '{"type":"module"}');
  return {root, write};
}

test('三条层级规则覆盖真实模块路径及各类静态依赖写法', t => {
  const {root, write} = workspace(t);
  write('src/broker/one.ts');
  write('src/broker/two.ts');
  write('src/hosts/nested/one.ts', "import Service = require('../../broker/one.js');");
  write('src/hosts/two.cts', "import type {T} from '../broker/two';");
  write('src/integration/tool.ts');
  write('src/storage/nested/source.ts', [
    "import type {T} from '../../broker/one.js';",
    "export type {T} from '../../hosts/nested/one.js';",
    "export * from '../../integration/tool.js';",
    "const loaded = import('../../broker/two.js');",
    "type Runtime = typeof import('../../broker/one.js');",
    "const message = \"import '../../hosts/not-a-module.js'\";",
    "// require('../../hosts/two.cjs');",
  ].join('\n'));
  write('src/process/loader.ts', "const load = require('../hosts/two.cjs');");
  write('src/process/type.ts', "type Result = import('../hosts/nested/one.js').Result;");
  write('src/broker/allowed.ts', "import '../hosts/two.cjs';");
  write('src/process/allowed.ts', "import '../broker/one.js';");
  write('src/hosts/allowed.ts', "export * from '../process/type.js';");
  write('src/tests/fixture.ts', "export * from '../broker/one.js';");
  write('src/native-tests/fixture.ts', "export * from '../hosts/two.cjs';");
  assert.deepEqual(findForbiddenEdges(root), [
    'src/hosts/nested/one.ts -> src/broker/one.ts',
    'src/hosts/two.cts -> src/broker/two.ts',
    'src/process/loader.ts -> src/hosts/two.cts',
    'src/process/type.ts -> src/hosts/nested/one.ts',
    'src/storage/nested/source.ts -> src/broker/one.ts',
    'src/storage/nested/source.ts -> src/broker/two.ts',
    'src/storage/nested/source.ts -> src/hosts/nested/one.ts',
    'src/storage/nested/source.ts -> src/integration/tool.ts',
  ]);
});

test('新增文件不能继承已允许的边，移除旧依赖后也必须删掉基线条目', t => {
  const {root, write} = workspace(t);
  write('src/hosts/adapter.ts');
  write('src/storage/store.ts', "import '../hosts/adapter.js';");
  write('src/process/no-edge.ts');
  const previous = findForbiddenEdges(root);
  write('src/storage/store.ts');
  write('src/storage/new.ts', "import('../hosts/adapter.js');");
  assert.deepEqual(boundaryChanges(findForbiddenEdges(root), previous), {
    added: ['src/storage/new.ts -> src/hosts/adapter.ts'],
    removed: ['src/storage/store.ts -> src/hosts/adapter.ts'],
  });
});
