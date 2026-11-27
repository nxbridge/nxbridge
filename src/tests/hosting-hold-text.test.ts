import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { PACKAGE_ROOT } from '../shared/package-location.js';
import { codexDeliveryHoldText, TAKEOVER_VERIFY_STEPS, type CodexDeliveryHoldCode } from '../broker/delivery/codex-delivery-hold-text.js';
import { formatStatus } from '../integration/mcp/presentation.js';
import { readFileSync } from 'node:fs';
import { TAKEOVER_PERMISSION_TOOL_TEXT } from '../broker/recovery/takeover-permission-copy.js';

const hosting = async () => await import(pathToFileURL(path.join(PACKAGE_ROOT, 'plugins/claude/hooks/hosting.js')).href);
const hold = (code: CodexDeliveryHoldCode) => ({ code, ...codexDeliveryHoldText(code) });

test('后台 Codex 正在工作时模型看到暂缓原因，不再被说成只能等用户在窗口里输入', async () => {
  const { hostingModel, hostingText, hostingHoldText } = await hosting();
  const h = { preference: 'bg', phase: 'background', wake_capable: true, held_unsent: 1, delivery_hold: hold('session_busy') };
  const line = hostingModel(h);
  assert.match(line, /1 held: queued: partner busy/);
  assert.doesNotMatch(line, /no wake/);
                                           
  assert.equal(hostingText(h), undefined);
  assert.equal(hostingHoldText(h), undefined);
});

test('真没有唤醒通道时仍写 no wake', async () => {
  const { hostingModel } = await hosting();
  assert.match(hostingModel({ preference: 'user', phase: 'foreground', wake_capable: false, held_unsent: 2 }), /no wake \(picks up only on user input in that window\), 2 held/);
});

test('需要用户处理的暂缓原因有兜底说明，会话状态说明本身不变', async () => {
  const { hostingText, hostingHoldText } = await hosting();
  assert.equal(hostingHoldText({ preference: 'bg', phase: 'background', wake_capable: true, held_unsent: 2, delivery_hold: hold('pending_user') }),
    '2 条消息在等：对方在等你批准操作或回答问题，处理完再送');
  assert.equal(hostingHoldText({ preference: 'user', phase: 'foreground', held_unsent: 1, delivery_hold: hold('takeover_unverified') }),
    '1 条消息在等：这个会话正开在一个窗口里，但没法确认是 nxbridge 打开的，先不送；那个窗口关掉后会自动接回再送');
  assert.equal(hostingHoldText({ preference: 'bg', phase: 'background', held_unsent: 0, delivery_hold: hold('pending_user') }), undefined);
  assert.equal(hostingText({ preference: 'bg', phase: 'background', held_unsent: 2, delivery_hold: hold('pending_user') }), undefined);
});

test('nxbridge status 写出消息暂缓的原因，对方正在工作也写', () => {
  const peer = (name: string, extra: object = {}) => ({ id: name, name, host: name, state: 'busy', ...extra });
  const text = formatStatus({ paired: true, pending: 1, questions: [], self: peer('claude'),
    partner: peer('codex', { hosting: { phase: 'background', held_unsent: 1, delivery_hold: hold('session_busy') } }) });
  assert.match(text, /消息暂缓：对方正在工作，下次调用工具或这一轮结束时会取到/);
});

test('nxbridge 自己的后台会话没接上时，不再说成要等用户在窗口里输入', async () => {
  const { hostingModel } = await hosting();
  assert.match(hostingModel({ preference: 'bg', phase: 'background', wake_capable: false, held_unsent: 1 }), /no wake \(bg session not attached; delivers after nxbridge reattaches it\), 1 held/);
});

test('原会话记录暂时核对不全时只说证据不够，不请用户改设置', () => {
  const step = TAKEOVER_VERIFY_STEPS.restart_profile_unverified;
  assert.match(step, /暂时没法完整核对/);
  assert.doesNotMatch(step, /改回|--reset/);
});

const pendingText='权限改动待你决定：你在接手窗口里改了后台 Codex 的审批方式 on-request → never，现在按新设置运行；要改回原来的，跟 AI 说一声';
const change=(state:string,status_text?:string)=>({change_id:'c-1',state,changed_fields:[{field:'approval_policy',before:'on-request',after:'never'}],...(status_text?{status_text}:{})});

test('接手窗口权限改动：状态栏只在待决定、排队、正在改回时常驻一行，结果不常驻', async () => {
  const { permissionChangeText } = await hosting();
  assert.equal(permissionChangeText({ permission_change: change('pending', pendingText) }), pendingText);
  assert.equal(permissionChangeText({ permission_change: change('restore_pending', '权限改回已排队：后台 Codex 空闲时改回原设置') }), '权限改回已排队：后台 Codex 空闲时改回原设置');
  assert.equal(permissionChangeText({ permission_change: change('restoring', '正在把后台 Codex 的权限改回原设置并核对') }), '正在把后台 Codex 的权限改回原设置并核对');
  for (const state of ['restored', 'failed', 'kept', 'superseded']) assert.equal(permissionChangeText({ permission_change: change(state, '某个结果') }), undefined, state);
                                   
  assert.equal(permissionChangeText({ permission_change: { ...change('pending', '你在接手窗口里改了后台 Codex 的审批方式 on-request → never，现在按新设置运行'), restore_available: false } }), undefined);
  assert.equal(permissionChangeText({}), undefined);
});

test('nxbridge status 写出权限改动的各个状态；改回结果待核对时只留“消息暂缓”一行', () => {
  const peer = (name: string, extra: object = {}) => ({ id: name, name, host: name, state: 'ready', ...extra });
  const show = (partner: object) => formatStatus({ paired: true, pending: 0, questions: [], self: peer('claude'), partner: peer('codex', partner) });
  assert.ok(show({ permission_change: change('pending', pendingText) }).includes(pendingText));
  assert.match(show({ permission_change: change('failed', '后台 Codex 的权限没能改回：窗口关闭后权限又变了，没有改动') }), /没能改回：窗口关闭后权限又变了/);
  assert.match(show({ permission_change: change('restored', '后台 Codex 的权限已改回原设置') }), /已改回原设置/);
  const restoring = change('restoring', '正在把后台 Codex 的权限改回原设置并核对');
  assert.match(show({ permission_change: restoring }), /正在把后台 Codex 的权限改回原设置并核对/);
  const held = show({ permission_change: restoring, hosting: { phase: 'background', held_unsent: 1, delivery_hold: hold('permission_restore_unverified') } });
  assert.match(held, /消息暂缓：/);
  assert.doesNotMatch(held, /正在把后台 Codex 的权限改回/);
  assert.doesNotMatch(show({ permission_change: change('kept') }), /权限/);
});

test('上次启动一直没结果的后台 Claude：nxbridge status 说明没法自动恢复，不再叫人重试', () => {
  const peer = (name: string, extra: object = {}) => ({ id: name, name, host: name, state: 'offline', ...extra });
  const message = '上次启动后台 Claude 一直没有结果，也找不到能核对的进程';
  const text = formatStatus({ paired: true, pending: 0, questions: [], self: peer('codex', { state: 'ready' }),
    partner: peer('claude', { launchFailure: { code: 'claude_release_unresolved', message } }) });
  assert.match(text, /没法自动恢复：上次启动后台 Claude 一直没有结果/);
  assert.doesNotMatch(text, /修复后重试/);
  const other = formatStatus({ paired: true, pending: 0, questions: [], self: peer('codex', { state: 'ready' }),
    partner: peer('claude', { launchFailure: { code: 'workspace_untrusted', message: '项目目录还没被信任' } }) });
  assert.match(other, /启动失败：项目目录还没被信任；消息已保留。修复后重试：nxbridge open claude --member claude/);
});

test('Mod 的 partner_profile 说明里权限部分和 MCP 逐字相同', () => {
  const source = readFileSync(path.join(PACKAGE_ROOT, 'plugins/claude/hooks/pairing.js'), 'utf8');
  assert.ok(source.includes(TAKEOVER_PERMISSION_TOOL_TEXT.replaceAll("'", "\\'")));
});
