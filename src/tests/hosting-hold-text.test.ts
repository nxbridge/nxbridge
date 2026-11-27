import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { PACKAGE_ROOT } from '../shared/package-location.js';
import { codexDeliveryHoldText, type CodexDeliveryHoldCode } from '../broker/delivery/codex-delivery-hold-text.js';
import { formatStatus } from '../integration/mcp/presentation.js';

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
