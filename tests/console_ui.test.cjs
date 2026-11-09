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
