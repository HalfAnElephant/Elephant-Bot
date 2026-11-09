const test = require('node:test');
const assert = require('node:assert/strict');
const {equal, changes, resolve, defaultValue, errorPath} = require('../src/qqbot/admin/console.js');

test('draft equality ignores object key order but preserves null, empty and list order', () => {
  assert.equal(equal({model: {name: 'small', endpoint: null}, adapters: []}, {adapters: [], model: {endpoint: null, name: 'small'}}), true);
  assert.equal(equal({endpoint: null}, {endpoint: ''}), false);
  assert.equal(equal(['first', 'second'], ['second', 'first']), false);
});

test('changing a value back to the baseline removes the dirty diff', () => {
  const baseline = {execution: {max_active: 4}, tasks_enabled: false};
  const draft = structuredClone(baseline);
  draft.execution.max_active = 6;
  assert.deepEqual(changes(baseline, draft), [{path: 'execution.max_active', before: 4, after: 6}]);
  draft.execution.max_active = 4;
  assert.deepEqual(changes(baseline, draft), []);
});

test('diff shows array edits, deleted records, new keys and explicit null distinctly', () => {
  assert.deepEqual(changes({adapters: [{id: 'first'}, {id: 'second'}]}, {adapters: [{id: 'changed'}]}), [
    {path: 'adapters.0.id', before: 'first', after: 'changed'},
    {path: 'adapters.1', before: {id: 'second'}, after: undefined}
  ]);
  assert.deepEqual(changes({}, {endpoint: null}), [{path: 'endpoint', before: undefined, after: null}]);
  assert.deepEqual(changes({future_option: false}, {future_option: true}), [{path: 'future_option', before: false, after: true}]);
});

test('schema defaults retain per-reference overrides and do not share mutable arrays', () => {
  const root = {$defs: {Budget: {type: 'object', properties: {calls: {type: 'integer', default: 30}}}}};
  assert.deepEqual(defaultValue({$ref: '#/$defs/Budget'}, root), {calls: 30});
  assert.deepEqual(defaultValue({$ref: '#/$defs/Budget', default: {calls: 6}}, root), {calls: 6});
  const schema = {type: 'array', default: ['first']};
  const draft = defaultValue(schema, {}); draft.push('second');
  assert.deepEqual(defaultValue(schema, {}), ['first']);
});

test('nullable unions preserve null defaults while supporting the concrete field type', () => {
  const schema = {anyOf: [{type: 'integer', minimum: 1}, {type: 'null'}], default: null};
  assert.equal(defaultValue(schema, {}), null);
  assert.equal(resolve(schema, {}).nullable, true);
  assert.equal(resolve(schema, {}).minimum, 1);
  assert.equal(defaultValue({type: 'boolean', default: false}, {}), false);
  assert.equal(defaultValue({type: 'integer', default: 0}, {}), 0);
});

test('new rule and free-form object defaults are representable without a dependency', () => {
  assert.deepEqual(defaultValue({type: 'object', properties: {enabled: {type: 'boolean', default: true}, effect: {type: 'string', enum: ['allow', 'deny']}, resources: {type: 'array', default: ['*']}}}, {}), {enabled: true, effect: 'allow', resources: ['*']});
  assert.deepEqual(defaultValue({type: 'object', additionalProperties: true}, {}), {});
  assert.deepEqual(defaultValue({type: 'array', items: {type: 'string'}}, {}), []);
});

test('backend errors resolve to form fields across config, persona and policy payloads', () => {
  assert.equal(errorPath(['body', 'config', 'adapters', 0, 'app_id']), 'adapters.0.app_id');
  assert.equal(errorPath(['body', 'persona', 'traits', 1]), 'traits.1');
  assert.equal(errorPath(['body', 'policy', 'rules', 0, 'subject']), 'rules.0.subject');
  assert.equal(errorPath(['body', 'config']), '');
  assert.equal(errorPath(['expected_version']), 'expected_version');
});

test('invalid JSON structure is rejected before replacing any editor draft', () => {
  const {assertShape} = require('../src/qqbot/admin/console.js');
  const schema = {type: 'object', properties: {
    adapters: {type: 'array', items: {type: 'object', properties: {id: {type: 'string'}, kind: {type: 'string', enum: ['onebot', 'official']}}}},
    endpoint: {anyOf: [{type: 'string'}, {type: 'null'}]}, style: {type: 'object', additionalProperties: true}
  }};
  assert.throws(() => assertShape({adapters: 'oops'}, schema), error => error.path === 'adapters');
  assert.throws(() => assertShape({adapters: [{id: 3}]}, schema), error => error.path === 'adapters.0.id');
  assert.throws(() => assertShape({adapters: [{kind: 'unknown'}]}, schema), error => error.path === 'adapters.0.kind');
  assert.throws(() => assertShape({style: []}, schema), error => error.path === 'style');
  const draft = {adapters: [{id: 'first', kind: 'onebot'}], endpoint: null, style: {custom: ['a']}};
  const before = structuredClone(draft);
  assert.doesNotThrow(() => assertShape(draft, schema));
  assert.deepEqual(draft, before);
});

test('permission actions retain unknown patterns and selecting a specific action narrows wildcard deliberately', () => {
  const {updateAction} = require('../src/qqbot/admin/console.js');
  const original = ['*', 'future.tool', 'github.*'];
  const updated = updateAction(original, 'conversation.receive', true);
  assert.deepEqual(updated, ['future.tool', 'github.*', 'conversation.receive']);
  assert.deepEqual(original, ['*', 'future.tool', 'github.*']);
  assert.deepEqual(updateAction(updated, '*', true), [...updated, '*']);
  assert.deepEqual(updateAction(updated, 'conversation.receive', false), ['future.tool', 'github.*']);
});

test('new permission rules target a named user and do not deny every operation globally', () => {
  const {newPermissionRule, validatePolicy} = require('../src/qqbot/admin/console.js');
  const rule = newPermissionRule('example', {adapter_instance_id: null, user_id: null, identity_namespace: null});
  assert.equal(rule.subject.kind, 'person');
  assert.deepEqual(rule.actions, ['conversation.receive']);
  assert.equal(rule.effect, 'deny');
  assert.throws(() => validatePolicy({rules: [rule]}), error => error.path === 'rules.0.subject.adapter_instance_id');
});

test('namespace inference uses known adapter identities and preserves explicit custom namespaces', () => {
  const {subjectNamespace} = require('../src/qqbot/admin/console.js');
  const adapters = [{instance_id: 'nap', kind: 'onebot'}, {instance_id: 'official', kind: 'official', app_id: '123'}, {instance_id: 'unknown', kind: 'official'}];
  assert.equal(subjectNamespace({kind: 'person', adapter_instance_id: 'nap', identity_namespace: null}, adapters), 'qq');
  assert.equal(subjectNamespace({kind: 'person', adapter_instance_id: 'official', identity_namespace: 'qq'}, adapters, 'qq'), 'qq_app:123');
  assert.equal(subjectNamespace({kind: 'person', adapter_instance_id: 'official', identity_namespace: 'custom'}, adapters, 'qq'), 'custom');
  assert.equal(subjectNamespace({kind: 'person', adapter_instance_id: 'unknown', identity_namespace: 'qq'}, adapters, 'qq'), null);
  assert.equal(subjectNamespace({kind: 'group', adapter_instance_id: 'nap', identity_namespace: null}, adapters), null);
});

test('policy validation preserves preset, unknown actions and additional selector constraints', () => {
  const {validatePolicy, ruleTarget} = require('../src/qqbot/admin/console.js');
  const policy = {preset: 'allow_all', default_effect: 'deny', rules: [{id: 'future', effect: 'allow', actions: ['future.operation'], resources: ['*'], subject: {kind: 'global', user_id: '123', identity_namespace: 'special'}}]};
  const before = structuredClone(policy);
  assert.equal(validatePolicy(policy), policy);
  assert.deepEqual(policy, before);
  assert.match(ruleTarget(policy.rules[0].subject), /用户 123/);
});

test('permission validation locates missing member identities, empty action lists and duplicate rule IDs', () => {
  const {validatePolicy} = require('../src/qqbot/admin/console.js');
  const rule = {id: 'a', subject: {kind: 'group_member', adapter_instance_id: 'nap', identity_namespace: 'qq', group_id: '456', user_id: null}, actions: ['*'], resources: ['*']};
  assert.throws(() => validatePolicy({rules: [rule]}), error => error.path === 'rules.0.subject.user_id');
  rule.subject.user_id = '123'; rule.actions = [];
  assert.throws(() => validatePolicy({rules: [rule]}), error => error.path === 'rules.0.actions');
  rule.actions = ['future.action'];
  assert.throws(() => validatePolicy({rules: [rule, rule]}), error => error.path === 'rules.1.id');
});

test('style type helpers distinguish null from objects and preserve false and zero defaults', () => {
  const {valueType, emptyValue} = require('../src/qqbot/admin/console.js');
  assert.equal(valueType(null), 'null'); assert.equal(valueType([]), 'array');
  assert.equal(valueType(false), 'boolean'); assert.equal(valueType(0), 'number');
  assert.equal(emptyValue('boolean'), false); assert.equal(emptyValue('number'), 0);
  assert.equal(defaultValue({type: 'null'}, {}), null);
});

test('JSON imports reject missing required fields before replacing a populated form', () => {
  const {assertShape} = require('../src/qqbot/admin/console.js');
  const schema = {type: 'object', required: ['name', 'style'], properties: {name: {type: 'string'}, style: {type: 'object', additionalProperties: true}}};
  assert.throws(() => assertShape({style: {}}, schema), error => error.path === 'name');
  const untouched = {name: '小象', style: {tone: '自然', unknown: {enabled: false, value: null}}};
  assert.doesNotThrow(() => assertShape(untouched, schema));
  assert.deepEqual(untouched.style.unknown, {enabled: false, value: null});
});


test('permission explanation identifies matching rule or fallback without requiring JSON', () => {
  const {permissionExplanation} = require('../src/qqbot/admin/console.js');
  assert.match(permissionExplanation({effect: 'allow', matched_rule: null, policy_version: 2}), /允许此操作。没有规则匹配，使用默认处理方式/);
  const text = permissionExplanation({effect: 'deny', matched_rule: {priority: 3, subject: {kind: 'person', user_id: '123'}}, policy_version: 4});
  assert.match(text, /禁止此操作。命中规则：指定用户 · 用户 123；优先级 3/);
  assert.match(text, /策略版本 4/);
});

test('decision logs distinguish skipped model calls from model abstention', () => {
  const {decisionJudgeStatus} = require('../src/qqbot/admin/console.js');
  assert.equal(decisionJudgeStatus({decision: {signals: null}}), '未调用模型');
  assert.equal(decisionJudgeStatus({decision: {signals: {abstain: true}}}), '模型弃权');
  assert.equal(decisionJudgeStatus({decision: {signals: {abstain: false}}}), '模型正常返回');
  assert.equal(decisionJudgeStatus({decision: {signals: null}, context: {judge_status: 'abstained'}}), '模型弃权');
  assert.equal(decisionJudgeStatus({decision: {signals: {abstain: true}}, context: {judge_status: 'rule_only'}}), '规则判别器弃权（未调用模型）');
});

test('decision score display preserves zero, precise scores and historical threshold gaps', () => {
  const {decisionScoreRows} = require('../src/qqbot/admin/console.js');
  const rows = decisionScoreRows({signals: {directed_to_bot: 0, useful_contribution: 0.74999, private_exchange: 0.1}}, {useful_contribution: 0.75, private_exchange: 0});
  assert.deepEqual(rows[0], {label: '面向机器人', score: 0, threshold: '仅记录'});
  assert.deepEqual(rows[3], {label: '有用贡献', score: 0.74999, threshold: '≥ 0.75'});
  assert.deepEqual(rows[4], {label: '他人私人交流', score: 0.1, threshold: '≤ 0'});
  assert.equal(rows[1].threshold, '历史未记录');
  assert.equal(decisionScoreRows({signals: null})[0].score, '—');
  assert.equal(decisionScoreRows({signals: {abstain: true, useful_contribution: 0}})[3].score, '—');
});

test('conversation search handles names, remote IDs, case, whitespace and empty lists', () => {
  const {filterConversations, conversationModes} = require('../src/qqbot/admin/console.js');
  const items = [{id: 'one:123', display_label: '学习群（123）', remote_id: '123'}, {id: 'two:456', display_label: 'Alice（456）', remote_id: '456'}];
  assert.deepEqual(filterConversations(items, ' 学习 '), [items[0]]);
  assert.deepEqual(filterConversations(items, 'ALICE'), [items[1]]);
  assert.deepEqual(filterConversations(items, '456'), [items[1]]);
  assert.deepEqual(filterConversations(items, 'one:'), [items[0]]);
  assert.deepEqual(filterConversations(items, 'missing'), []);
  assert.deepEqual(filterConversations(items, ''), items);
  assert.deepEqual(filterConversations([], ''), []);
  for (const mode of ['addressed_only', 'silent', 'shadow', 'social']) assert.equal(conversationModes[mode].length, 2);
});
