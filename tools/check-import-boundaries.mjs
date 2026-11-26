import {readFileSync, readdirSync} from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import ts from 'typescript';

const projectRoot = fileURLToPath(new URL('../', import.meta.url));
const boundaries = {
  storage: ['src/broker/', 'src/hosts/', 'src/integration/'],
  hosts: ['src/broker/'],
  process: ['src/hosts/'],
};
const resolverOptions = {module: ts.ModuleKind.NodeNext, moduleResolution: ts.ModuleResolutionKind.NodeNext};
const relativePath = (root, filename) => path.relative(root, filename).split(path.sep).join('/');

function moduleUses(node, defaultMode) {
  if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) return [node.moduleSpecifier, defaultMode];
  if (ts.isImportTypeNode(node) && ts.isLiteralTypeNode(node.argument)) return [node.argument.literal, defaultMode];
  if (ts.isImportEqualsDeclaration(node) && ts.isExternalModuleReference(node.moduleReference)) {
    return [node.moduleReference.expression, ts.ModuleKind.CommonJS];
  }
  if (ts.isCallExpression(node)) {
    if (node.expression.kind === ts.SyntaxKind.ImportKeyword) return [node.arguments[0], ts.ModuleKind.ESNext];
    if (ts.isIdentifier(node.expression) && node.expression.text === 'require') return [node.arguments[0], ts.ModuleKind.CommonJS];
  }
  return [];
}

export function findForbiddenEdges(root = projectRoot) {
  const canonical = ts.sys.useCaseSensitiveFileNames ? filename => filename : filename => filename.toLowerCase();
  const cache = ts.createModuleResolutionCache(root, canonical, resolverOptions);
  const edges = new Set();
                                 
  for (const [directory, denied] of Object.entries(boundaries)) {
    const entries = readdirSync(path.join(root, 'src', directory), {recursive: true, withFileTypes: true});
    for (const entry of entries) {
      if (!entry.isFile() || !/\.(?:[cm]?[tj]s|[tj]sx)$/.test(entry.name)) continue;
      const filename = path.join(entry.parentPath, entry.name);
      const from = relativePath(root, filename);
      const source = ts.createSourceFile(filename, readFileSync(filename, 'utf8'), ts.ScriptTarget.Latest);
      const defaultMode = ts.getImpliedNodeFormatForFile(filename, cache.getPackageJsonInfoCache(), ts.sys, resolverOptions);
      function visit(node) {
        const [specifier, mode] = moduleUses(node, defaultMode);
        if (specifier && ts.isStringLiteralLike(specifier)) {
          const resolved = ts.resolveModuleName(specifier.text, filename, resolverOptions, ts.sys, cache, undefined, mode).resolvedModule;
          if (resolved) {
            const to = relativePath(root, resolved.resolvedFileName);
            if (denied.some(prefix => to.startsWith(prefix))) edges.add(from + ' -> ' + to);
          }
        }
        ts.forEachChild(node, visit);
      }
      visit(source);
    }
  }
  return [...edges].sort();
}

export function boundaryChanges(actual, baseline) {
  const permitted = new Set(baseline), observed = new Set(actual);
  return {
    added: actual.filter(edge => !permitted.has(edge)),
    removed: baseline.filter(edge => !observed.has(edge)),
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const baseline = JSON.parse(readFileSync(new URL('./import-boundaries-baseline.json', import.meta.url), 'utf8'));
  const {added, removed} = boundaryChanges(findForbiddenEdges(), baseline);
  for (const edge of added) process.stderr.write('新增禁止的依赖：' + edge + '\n');
  for (const edge of removed) process.stderr.write('依赖已移除，请删除基线条目：' + edge + '\n');
  if (added.length || removed.length) process.exitCode = 1;
}
