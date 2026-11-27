import { test, expect, mock } from 'claude-code/testing';

const spawn = { tool_use_id: 'spawn', prompt: '任务 nxref:msg:source', description: '测试引用', subagentType: 'general-purpose', provider: { plugin: 'engine', tier: 'core' }, parentModel: 'opus', model: 'haiku' };

test('原生 Mod 引擎在 agent.spawn 前展开正文，保留模型及父代理信息', async ($, on) => {
  mock.env(on, { CLAUDE_CODE_MESSAGING_SOCKET: 'test-inbox' });
  on('session.id', () => ({ value: 'native-ref' }));on('session.cwd', () => ({ value: '/test' }));
  on('process.run', ($, e) => {
    const input = JSON.parse(e.argv.at(-1)!);
                                        
    if(input.operation==='control-open')return {value:{exitCode:0,stdout:'{}',stderr:''}};
    expect(input.operation).toBe('ref-expand');expect(input.text).toBe(spawn.prompt);
    return { value: { exitCode: 0, stdout: JSON.stringify({ text: '任务 完整中文正文' }), stderr: '' } };
  });
  on('agent.spawn', ($, e) => {
    expect(e.prompt).toBe('任务 完整中文正文');expect(e.model).toBe('haiku');expect(e.parentAgentId).toBe('parent');
    return { model: e.model!, agentId: 'native-child' };
  });
  const result = await $.agent.spawn({ ...spawn, parentAgentId: 'parent' });
  expect(result.agentId).toBe('native-child');
});

test('原生 Mod 引擎继续已有子代理时拒绝引用改写，不让原始引用进入子代理', async ($, on) => {
  mock.env(on, { CLAUDE_CODE_MESSAGING_SOCKET: 'test-inbox' });
  on('session.id', () => ({ value: 'native-ref-resume' }));on('session.cwd', () => ({ value: '/test' }));
  on('process.run', ($, e) => {
    const input=JSON.parse(e.argv.at(-1)!);
    if(input.operation==='control-open')return {value:{exitCode:0,stdout:'{}',stderr:''}};
    expect(input.operation).toBe('ref-expand');
    return {value:{exitCode:0,stdout:JSON.stringify({text:'任务 完整中文正文'}),stderr:''}};
  });
  let started = false;on('agent.spawn', () => { started = true;return { model: 'haiku' }; });
  const result = await $.agent.spawn({ ...spawn, resume: 'existing-agent' });
  expect(result.deny).toBe('nxref is not expanded when continuing an existing subagent; resend with the needed text inline');
  expect(started).toBe(false);
});

test('原生 Mod 引擎继续已有子代理且不含引用时保持原样', async ($, on) => {
  mock.env(on, { CLAUDE_CODE_MESSAGING_SOCKET: 'test-inbox' });
  on('session.id', () => ({ value: 'native-ref-resume-plain' }));on('session.cwd', () => ({ value: '/test' }));
  on('process.run', ($, e) => {
    const input=JSON.parse(e.argv.at(-1)!);
    if(input.operation==='control-open')return {value:{exitCode:0,stdout:'{}',stderr:''}};
    throw Error('不应请求引用展开');
  });
  on('agent.spawn', ($, e) => {
    expect(e.prompt).toBe('继续现有任务');expect(e.resume).toBe('existing-agent');
    return { model: e.model!, agentId: 'existing-agent' };
  });
  const result = await $.agent.spawn({ ...spawn, prompt: '继续现有任务', resume: 'existing-agent' });
  expect(result.agentId).toBe('existing-agent');
});

test('原生 Mod 引擎的 catch 将展开异常变为拒绝，不执行底层 spawn', async ($, on) => {
  mock.env(on, { CLAUDE_CODE_MESSAGING_SOCKET: 'test-inbox' });
  on('session.id', () => ({ value: 'native-ref-failure' }));on('session.cwd', () => ({ value: '/test' }));
  on('process.run', ($, e) => {
    const input=JSON.parse(e.argv.at(-1)!);
    if(input.operation==='control-open')return {value:{exitCode:0,stdout:'{}',stderr:''}};
    expect(input.operation).toBe('ref-expand');
    return {value:{exitCode:1,stdout:'',stderr:JSON.stringify({code:'ref_missing',message:'引用不存在',model_message:'nxref missing'})}};
  });
  let started = false;on('agent.spawn', () => { started = true;return { model: 'haiku' }; });
  const result = await $.agent.spawn(spawn);
  expect(typeof result.deny).toBe('string');expect(started).toBe(false);
});
