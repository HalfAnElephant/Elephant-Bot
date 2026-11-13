'use strict';

const ConsoleModel = (() => {
  function canonical(value) {
    if (Array.isArray(value)) return value.map(canonical);
    if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
    return value;
  }
  const equal = (left, right) => JSON.stringify(canonical(left)) === JSON.stringify(canonical(right));
  function changes(before, after, path = '') {
    if (equal(before, after)) return [];
    if (before && after && typeof before === 'object' && typeof after === 'object' && Array.isArray(before) === Array.isArray(after)) {
      return [...new Set([...Object.keys(before), ...Object.keys(after)])].flatMap(key => changes(before[key], after[key], path ? path + '.' + key : key));
    }
    return [{path, before, after}];
  }
  function resolve(schema, root) {
    if (schema.$ref) return {...resolve(root.$defs[schema.$ref.split('/').pop()], root), ...Object.fromEntries(Object.entries(schema).filter(([key]) => key !== '$ref'))};
    if (schema.anyOf) return {...resolve(schema.anyOf.find(item => item.type !== 'null') || {}, root), nullable: schema.anyOf.some(item => item.type === 'null'), ...('default' in schema ? {default: schema.default} : {})};
    return schema;
  }
  function defaultValue(raw, root) {
    const schema = resolve(raw, root);
    if ('default' in schema) return structuredClone(schema.default);
    if (schema.nullable) return null;
    if (schema.enum) return schema.enum[0];
    if (schema.type === 'object') return Object.fromEntries(Object.entries(schema.properties || {}).map(([key, child]) => [key, defaultValue(child, root)]));
    if (schema.type === 'array') return [];
    if (schema.type === 'boolean') return false;
    if (schema.type === 'null') return null;
    if (schema.type === 'integer' || schema.type === 'number') return schema.minimum ?? 0;
    return '';
  }
  function errorPath(location = []) {
    const parts = [...location];
    if (parts[0] === 'body') parts.shift();
    if (['config', 'policy'].includes(parts[0])) parts.shift();
    return parts.join('.');
  }
  function assertShape(value, raw, root = raw, path = '') {
    const schema = resolve(raw, root);
    if (value === null && (schema.nullable || schema.type === 'null')) return;
    const valid = schema.type === 'object' ? value !== null && typeof value === 'object' && !Array.isArray(value)
      : schema.type === 'array' ? Array.isArray(value)
      : schema.type === 'integer' ? Number.isInteger(value)
      : schema.type ? typeof value === schema.type : true;
    if (!valid || (schema.enum && !schema.enum.some(item => equal(item, value)))) {
      throw Object.assign(new Error('内容类型与字段要求不符，请检查 JSON。'), {path});
    }
    if (schema.type === 'object' && schema.properties) {
      for (const key of schema.required || []) if (!Object.hasOwn(value, key)) throw Object.assign(new Error('缺少必填字段，请保留完整内容。'), {path: path ? path + '.' + key : key});
      for (const [key, child] of Object.entries(schema.properties)) if (Object.hasOwn(value, key)) assertShape(value[key], child, root, path ? path + '.' + key : key);
    }
    if (schema.type === 'array' && schema.items) value.forEach((item, index) => assertShape(item, schema.items, root, path + '.' + index));
  }
  const subjectFields = {
    global: [], adapter: ['adapter_instance_id'], bot: ['adapter_instance_id', 'bot_identity_id'],
    group: ['adapter_instance_id', 'group_id'], person: ['adapter_instance_id', 'identity_namespace', 'user_id'],
    group_member: ['adapter_instance_id', 'identity_namespace', 'group_id', 'user_id']
  };
  const scopeLabels = {global: '所有人', person: '指定用户', group: '指定群', group_member: '群内成员', adapter: '整个接入', bot: '指定机器人'};
  const actionLabels = {
    '*': '全部操作', 'conversation.receive': '接收消息', 'notification.send': '发送回复与通知',
    'model.request': '调用模型', 'conversation.policy.set': '调整发言模式',
    'task.create': '创建任务', 'task.read': '查看任务', 'task.cancel': '取消任务', 'task.*': '全部任务操作',
    'memory.read': '读取记忆', 'memory.*': '记忆操作', 'history.*': '读取历史与来源'
  };
  function updateAction(actions, action, enabled) {
    if (!enabled) return actions.filter(value => value !== action);
    const kept = action === '*' ? [...actions] : actions.filter(value => value !== '*');
    return kept.includes(action) ? kept : [...kept, action];
  }
  function subjectNamespace(subject, adapters, automaticValue = null) {
    if ((subject.identity_namespace && subject.identity_namespace !== automaticValue) || !subjectFields[subject.kind]?.includes('identity_namespace')) return subject.identity_namespace;
    const adapter = adapters.find(item => item.instance_id === subject.adapter_instance_id);
    if (adapter?.kind === 'official' && adapter.app_id) return 'qq_app:' + adapter.app_id;
    return automaticValue ? null : subject.identity_namespace;
  }
  function newPermissionRule(id, subject = {}) {
    return {id, enabled: true, priority: 0, effect: 'deny', actions: ['conversation.receive'], resources: ['*'], subject: {...subject, kind: 'person'}};
  }
  function validatePolicy(policy) {
    const ids = new Set();
    (policy.rules || []).forEach((rule, index) => {
      const fail = (path, message) => { throw Object.assign(new Error(message), {path: 'rules.' + index + '.' + path}); };
      if (!rule.id || ids.has(rule.id)) fail('id', '每条规则需要唯一的标识。'); ids.add(rule.id);
      for (const key of subjectFields[rule.subject.kind] || []) if (!rule.subject[key]) fail('subject.' + key, '请填写此对象所需的' + ({adapter_instance_id: '接入', identity_namespace: '身份来源', user_id: '用户账号', group_id: '群号', bot_identity_id: '机器人标识'}[key]) + '。');
      if (!rule.actions?.length) fail('actions', '请至少选择一种操作。');
      if (!rule.resources?.length) fail('resources', '请填写资源范围；全部资源使用 *。');
    });
    return policy;
  }
  function valueType(value) { return value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value; }
  function emptyValue(type) { return {string: '', number: 0, boolean: false, object: {}, array: [], null: null}[type]; }
  function ruleTarget(subject) {
    const title = scopeLabels[subject.kind] || subject.kind;
    const pieces = [];
    if (subject.user_id) pieces.push('用户 ' + subject.user_id);
    if (subject.group_id) pieces.push('群 ' + subject.group_id);
    if (subject.bot_identity_id) pieces.push('机器人 ' + subject.bot_identity_id);
    if (subject.adapter_instance_id) pieces.push('接入 ' + subject.adapter_instance_id);
    return title + (pieces.length ? ' · ' + pieces.join(' / ') : '');
  }
  function permissionExplanation(result) {
    const outcome = result.effect === 'allow' ? '允许此操作' : '禁止此操作';
    return outcome + '。' + (result.matched_rule ? '命中规则：' + ruleTarget(result.matched_rule.subject) + '；优先级 ' + result.matched_rule.priority + '。' : '没有规则匹配，使用默认处理方式。') + '（策略版本 ' + result.policy_version + '）';
  }
  return {equal, changes, resolve, defaultValue, errorPath, assertShape, subjectFields, scopeLabels, actionLabels, updateAction, subjectNamespace, validatePolicy, valueType, emptyValue, ruleTarget, newPermissionRule, permissionExplanation};
})();
if (typeof module !== 'undefined') module.exports = ConsoleModel;
if (typeof document !== 'undefined') (() => {
  const $ = id => document.getElementById(id), encode = encodeURIComponent;
  const {equal, changes, resolve, defaultValue, errorPath, assertShape, subjectFields, scopeLabels, actionLabels, updateAction, subjectNamespace, validatePolicy, valueType, emptyValue, ruleTarget, newPermissionRule, permissionExplanation} = ConsoleModel;
  const editors = [], replaceButtons = ['load-config', 'load-permissions', 'load-versions', 'load-version', 'config-use-json', 'permissions-use-json'];
  let token = '', authEpoch = 0, saving = false, dialogPending = false, activePanel = 'configuration', noticeTimer;
  let configVersion = null, savedConfigVersion = null, permissionVersion = null;
  let configView = null, eventController = null, streamTask = '', eventCursor = '0', fieldCounter = 0;
  const uuid = () => Array.from(crypto.getRandomValues(new Uint8Array(16)), value => value.toString(16).padStart(2, '0')).join('');
  function element(tag, text, className) {
    const node = document.createElement(tag);
    if (text !== undefined) node.textContent = text;
    if (className) node.className = className;
    return node;
  }
  function show(id, value) {
    const node = $(id); if (!node) return;
    node.textContent = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
    if (node.classList.contains('error-summary')) node.hidden = !node.textContent;
  }
  function notice(message, error = false) {
    show('notice', message); $('notice').classList.toggle('error', error); $('notice').hidden = !message;
    clearTimeout(noticeTimer);
    if (message && !error) noticeTimer = setTimeout(() => { $('notice').hidden = true; }, 5000);
  }
  function loginVisible(visible) {
    $('login-screen').hidden = !visible; $('app-shell').hidden = visible;
    show('connection', visible ? '未连接' : '已连接');
  }
  function required(id, label) {
    const value = $(id).value.trim();
    if (!value) throw new Error('请填写' + label);
    return value;
  }
  const taskID = () => required('task-id', '任务 ID');
  function headers(extra = {}) {
    if (!token) throw new Error('请先输入管理员令牌连接服务。');
    return {Authorization: 'Bearer ' + token, ...extra};
  }
  async function request(path, options = {}) {
    const epoch = authEpoch;
    let response, raw;
    try {
      response = await fetch(path, {method: options.method || 'GET', cache: 'no-store', signal: AbortSignal.timeout(30000), headers: headers({'Content-Type': 'application/json', ...options.headers}), body: options.body === undefined ? undefined : JSON.stringify(options.body)});
      raw = await response.text();
    } catch (error) {
      if (error.name === 'TimeoutError') throw new Error('请求超时，草稿已保留。如果正在保存，请重新读取以确认结果，再决定是否重试。');
      throw new Error(error instanceof TypeError ? '无法连接服务。请检查服务是否运行和网络连接，再重试；草稿已保留。' : error.message);
    }
    if (epoch !== authEpoch) throw Object.assign(new Error('连接已切换'), {stale: true});
    let value;
    try { value = raw ? JSON.parse(raw) : null; } catch { value = raw; }
    if (!response.ok) {
      const messages = {401: '令牌无效或已失效，请重新连接。当前草稿已保留。', 403: '当前权限不允许此操作。请检查权限策略。', 409: '配置已变化或当前状态不允许操作。草稿已保留，请重新读取并核对差异。', 422: '请修正标出的设置后重试。'};
      if (response.status === 401) { token = ''; authEpoch++; stopEvents(); loginVisible(true); show('auth-error', messages[401]); }
      throw Object.assign(new Error(messages[response.status] || '服务暂时无法完成操作，请稍后重试。'), {status: response.status, details: value?.detail ?? value});
    }
    return value;
  }
  function report(error, state) {
    if (error.stale) return;
    notice(error.message, true);
    if (state) state.errors(error);
  }
  async function runAction(control, operation) {
    if (control.disabled) return;
    const editorAtStart = editors.find(item => item.panel === activePanel);
    const text = control.textContent; control.disabled = true; control.textContent = '正在处理…'; control.setAttribute('aria-busy', 'true');
    try { await operation(); } catch (error) { report(error, editorAtStart); }
    finally { control.textContent = text; control.disabled = false; control.removeAttribute('aria-busy'); updateSaveButtons(); }
  }
  function action(id, operation) { $(id)?.addEventListener('click', () => runAction($(id), operation)); }
  function button(parent, title, operation, className) {
    const control = element('button', title, className); control.type = 'button';
    control.addEventListener('click', () => runAction(control, operation)); parent.append(control); return control;
  }
  function cell(row, value) { const td = element('td', String(value ?? '')); row.append(td); return td; }
  function empty(parent, message, retry) {
    parent.replaceChildren(element('p', message, 'empty-state'));
    if (retry) button(parent, '重试', retry);
  }
  async function confirmDialog(title, description, diff = [], confirmLabel = '确认') {
    if (dialogPending) return false;
    dialogPending = true;
    const dialog = $('confirm-dialog'), previous = document.activeElement;
    show('dialog-title', title); show('dialog-description', description); show('dialog-confirm', confirmLabel);
    $('dialog-changes').replaceChildren();
    for (const change of diff) {
      const row = element('div', undefined, 'diff-row');
      row.append(
        element('strong', readablePath(change.path)),
        element('small', '原值'),
        element('pre', change.before === undefined ? '未设置' : readableValue(change.path, change.before), 'diff-before'),
        element('small', '新值'),
        element('pre', change.after === undefined ? '移除' : readableValue(change.path, change.after), 'diff-after')
      );
      $('dialog-changes').append(row);
    }
    return new Promise(resolveResult => {
      function finish(value) {
        dialog.close(); dialogPending = false;
        $('dialog-cancel').removeEventListener('click', cancel); $('dialog-confirm').removeEventListener('click', accept); dialog.removeEventListener('cancel', cancel);
        if (previous?.isConnected) previous.focus(); resolveResult(value);
      }
      function cancel(event) { event.preventDefault(); finish(false); }
      function accept(event) { event.preventDefault(); finish(true); }
      $('dialog-cancel').addEventListener('click', cancel); $('dialog-confirm').addEventListener('click', accept); dialog.addEventListener('cancel', cancel);
      dialog.showModal(); $('dialog-cancel').focus();
    });
  }
  const labels = {
    deployment_id: '部署标识', database: '数据库路径', adapters: 'QQ 官方接入', admin_token_env: '管理员令牌变量', internal_token_env: '内部令牌变量', model: '模型', source_url: '源码地址', send_budget_per_minute: '每分钟发送预算', timezone: '时区', tasks_enabled: '启用后台任务', interactive_execution_enabled: '启用交互执行', permissions: '初始权限策略', execution: '执行与资源预算', orchestration: 'Leader 与 Worker 调度', owner: '个人助手', context: '上下文', internal_socket: '内部通信 Socket', memory: 'OpenViking 记忆', skills: 'Hermes 技能',
    instance_id: '接入实例标识', kind: '类型', bot_id: '机器人账号标识', secret_env: '接入密钥变量', credential_ref: '凭据引用', credential_store_dir: '凭据目录', app_id: '应用 ID', allowed_groups: '兼容群列表', allowed_users: '兼容用户列表', admin_users: '管理员角色列表', full_mode_enabled: '启用全量消息权限', full_mode_verified: '已验证全量消息', proactive_verified: '已验证主动消息', reply_window_seconds: '回复窗口（秒）', max_replies: '最多回复数', signature_max_age_seconds: '签名最大时效（秒）', api_base: 'API 地址', token_url: '令牌地址',
    endpoint: '请求地址', api_key_env: 'API 密钥变量', timeout_seconds: '超时（秒）', context_window: '模型上下文窗口', max_active: '最大活跃执行数', sustained_limit: '旧兼容值（不参与调度）', foreground_wait_ms: '前台等待（毫秒）', notify_before_tools: '操作前告知用户', interactive_budget: '交互任务预算', budget: '后台任务预算', warm_max: '常驻进程上限', warm_idle_max: '空闲进程上限', warm_ttl_seconds: '空闲保留（秒）',
    max_request_bytes: '最大请求字节数', max_tokens: '上下文软上限', keep_recent_tokens: '近期消息保留 Token', compaction_output_tokens: '压缩输出预算', recent_messages: '近期消息条数', topic_idle_seconds: '话题空闲超时（秒）', version: '版本',
    id: '标识', preferences: '个人偏好', qq_bindings: 'QQ 官方身份绑定', user_openid: '用户 OpenID', verified_at: '验证时间',
    preset: '预设', default_effect: '默认行为', rules: '权限规则', enabled: '启用', priority: '优先级', subject: '主体范围', actions: '操作匹配', resources: '资源匹配', effect: '行为', adapter_instance_id: '接入实例 ID', bot_identity_id: '机器人身份 ID', identity_namespace: '身份命名空间', group_id: '群 ID', user_id: '用户 ID',
    model_calls: '模型调用次数', tool_calls: '工具调用次数', active_seconds: '活跃执行（秒）', total_wall_seconds: '总时长（秒）', output_tokens_per_call: '每次输出 Token 上限', input_tokens: '输入 Token 预算', output_tokens: '输出 Token 预算', completion_reserve: '完成阶段预留模型调用',
    pending_inputs_per_owner: '每个 owner 待处理输入上限', pending_root_tasks_per_owner: '每个 owner 根任务上限', child_parallelism: '子任务并行度', max_task_depth: '最大任务深度', max_nodes_per_root: '每个根任务节点上限', leader_inbox_batch_limit: 'Leader 收件批次上限', leader_inbox_coalesce_seconds: 'Leader 收件合并时间（秒）', model_request_limit: '模型请求并发上限', pi_executable: 'Pi 可执行文件', github_host: 'GitHub 主机', workspace_bindings: '工作区绑定',
    provider: '技能提供方', owner_storage_root: 'owner 技能存储根目录', hermes: 'Hermes 设置', background_review_enabled: '启用后台复盘', create_dir: '技能创建目录', write_approval: '写入审批', inline_shell: '内联 Shell', ledger: '记录技能变更', ledger_max_bytes: '变更记录上限（字节）', creation_nudge_interval: '技能创建提醒间隔',
    namespace: '命名空间', bindings: '用户凭据绑定', expected_version: '预期 OpenViking 版本', private_enabled: '启用私聊记忆', group_enabled: '启用群记忆', operation_budget_seconds: '操作预算（秒）', max_query_chars: '最大查询字符数', recall_max_chars: '召回内容上限（字符）', owner_id: 'Owner ID', workspace_id: '工作区 ID', account_id: 'OpenViking 账户 ID',
    surface: '消息场景', max_request_bytes: '最大请求字节数', recent_messages: '近期消息条数'
  };
  const enumLabels = {
    kind: {official: 'QQ 官方机器人', global: '全局', adapter: '接入实例', bot: '机器人', group: '群组', person: '个人', group_member: '群内成员'},
    provider: {hermes: 'Hermes'}, mode: {leader_workers: 'Leader + Workers'},
    preset: {allow_all: '默认开放', custom: '自定义'},
    effect: {allow: '允许', deny: '拒绝'}, default_effect: {allow: '允许', deny: '拒绝'}
  };
  const helps = {
    notify_before_tools: '旧执行记录的兼容设置。Leader 根据实际过程自主决定接收确认和进度通知。',
    output_tokens_per_call: '勾选“暂不设置”时使用模型服务默认输出长度。填写数字可设置单次请求的输出 Token 上限。',
    adapters: '添加 QQ 官方机器人接入。账号、群号与用户 ID 均使用字符串。',
    permissions: '仅用作初始策略；数据库已有权限策略时，以“权限策略”页为准。',
    model: 'Core 模型元信息；实际请求使用 Worker 的模型、地址和凭据，请保持一致。',
    database: '更改路径会切换数据库，不会迁移已有数据。需保证服务账号能访问目标路径。',
    allowed_groups: '兼容字段；实际访问控制由统一权限策略决定。', allowed_users: '兼容字段；实际访问控制由统一权限策略决定。',
    context: '保留近期 Token < 软上限 < 硬上限；输入与输出总预算不能超过模型窗口。',
    execution: '交互与后台工作共享资源准入；容量用尽时新任务排队。',
    owner: '稳定的个人助手身份与显式绑定。偏好内容用于个性化表达，不授予权限。',
    preferences: '最多 4000 个字符。填写称呼、语言和沟通风格等明确偏好。',
    orchestration: 'Leader 负责理解、委派和核验；Worker 按任务目标执行。'
  };
  const groups = [
    ['接入', ['adapters', 'deployment_id']], ['模型', ['model']],
    ['工作流', ['tasks_enabled', 'interactive_execution_enabled', 'execution', 'orchestration']], ['个人助手', ['owner']], ['上下文', ['context']],
    ['记忆与技能', ['memory', 'skills']], ['系统', ['database', 'internal_socket', 'timezone', 'source_url', 'send_budget_per_minute', 'admin_token_env', 'internal_token_env']], ['初始权限', ['permissions']]
  ];
  let category = 0;
  function field(raw, initial, key, root, state, path) {
    const schema = resolve(raw, root), wrap = element('div', undefined, 'field');
    const title = raw.uiLabel || labels[key] || key, name = 'setting-' + (++fieldCounter);
    wrap.dataset.path = path;
    const label = element('label', title);
    label.append(element('small', path, 'field-key')); wrap.append(label);
    let read, control;
    if (schema.type === 'object' && schema.properties) {
      wrap.classList.add('wide'); const content = element('div', undefined, 'nested-fields'); wrap.append(content);
      const reads = [], advanced = element('details', undefined, 'advanced-fields'); advanced.append(element('summary', '兼容与高级设置'));
      for (const [childKey, child] of Object.entries(schema.properties)) {
        const item = field(child, Object.hasOwn(initial || {}, childKey) ? initial[childKey] : defaultValue(child, root), childKey, root, state, path + '.' + childKey);
        const parent = ['allowed_groups', 'allowed_users', 'admin_users', 'signature_max_age_seconds', 'api_base', 'token_url'].includes(childKey) ? advanced : content;
        parent.append(item.node); reads.push([childKey, item.read]);
      }
      if (advanced.children.length > 1) content.append(advanced);
      read = () => ({...initial, ...Object.fromEntries(reads.map(([childKey, getter]) => [childKey, getter()]))});
    } else if (schema.type === 'array' && resolve(schema.items || {}, root).type === 'object') {
      wrap.classList.add('wide'); const items = element('div', undefined, 'array-items'); wrap.append(items); let rows = [];
      function render(values) {
        for (const registered of [...state.fields.keys()]) if (registered.startsWith(path + '.')) state.fields.delete(registered);
        items.replaceChildren(); rows = [];
        if (!values.length) items.append(element('p', '还没有' + title + '，点击下方按钮添加。', 'empty-state'));
        values.forEach((value, index) => {
          const box = element('details', undefined, 'array-item'), summary = element('summary', value.instance_id || value.name || value.id || title + ' ' + (index + 1)); box.open = !value.instance_id && !value.id;
          box.append(summary);
          const item = field(schema.items, value, String(index + 1), root, state, path + '.' + index); box.append(item.node);
          button(box, '移除这一项', () => { const current = read(); current.splice(index, 1); render(current); state.sync(); }, 'text-danger');
          item.node.addEventListener('input', () => { try { const current = item.read(); summary.textContent = current.instance_id || current.name || current.id || title + ' ' + (index + 1); } catch {} });
          rows.push(item.read); items.append(box);
        });
      }
      read = () => rows.map(getter => getter()); render(initial || []);
      button(wrap, '添加' + title, () => { render([...read(), defaultValue(schema.items, root)]); state.sync(); items.lastElementChild?.querySelector('input')?.focus(); }, 'add-item');
    } else if (schema.type === 'array' && resolve(schema.items || {}, root).type === 'string') {
      control = element('textarea'); control.rows = 3; control.value = (initial || []).join('\n');
      read = () => control.value === (initial || []).join('\n') ? structuredClone(initial || []) : control.value === '' ? [] : control.value.split('\n');
      wrap.append(element('small', '每行一项；留空表示空列表。', 'field-help'));
    } else if (schema.type === 'boolean') {
      control = element('input'); control.type = 'checkbox'; control.checked = Boolean(initial); control.setAttribute('role', 'switch'); control.className = 'switch-control';
      read = () => control.checked;
    } else if (schema.enum) {
      control = element('select');
      for (const value of schema.enum) { const option = element('option', raw.uiOptions?.[value] || enumLabels[key]?.[value] || String(value)); option.value = String(value); control.append(option); }
      control.value = String(initial ?? defaultValue(raw, root)); read = () => control.value;
    } else if (schema.type === 'integer' || schema.type === 'number') {
      control = element('input'); control.type = 'number'; control.step = schema.type === 'integer' ? '1' : 'any'; control.value = initial ?? ''; control.required = true;
      if (schema.minimum !== undefined) control.min = schema.minimum;
      if (schema.maximum !== undefined) control.max = schema.maximum;
      read = () => { if (!control.checkValidity() || !Number.isFinite(control.valueAsNumber)) throw new Error('请填写允许范围内的数值。'); return control.valueAsNumber; };
      wrap.append(element('small', '范围：' + (schema.minimum ?? '不限') + ' – ' + (schema.maximum ?? '不限') + (schema.type === 'integer' ? '，整数' : ''), 'field-help'));
    } else if (schema.type === 'string') {
      control = element(key === 'preferences' || raw.uiMultiline ? 'textarea' : 'input'); control.value = initial ?? '';
      if (schema.minLength !== undefined) { control.minLength = schema.minLength; control.required = schema.minLength > 0; }
      if (schema.maxLength !== undefined) control.maxLength = schema.maxLength;
      read = () => { if (!control.checkValidity()) throw new Error('请检查必填内容和长度限制。'); return control.value; };
    } else {
      control = element('textarea'); control.rows = 4; control.value = JSON.stringify(initial ?? defaultValue(raw, root), null, 2);
      read = () => { try { return JSON.parse(control.value); } catch { throw new Error('JSON 格式不正确，请检查引号和逗号。'); } };
      wrap.append(element('small', '自由结构，使用 JSON 编辑。', 'field-help'));
    }
    if (control) { label.htmlFor = name; control.id = name; control.spellcheck = false; wrap.append(control); }
    else { const heading = element('div', undefined, 'field-label'); heading.append(...label.childNodes); label.replaceWith(heading); }
    if (schema.nullable && control) {
      const toggle = element('label', undefined, 'nullable'), nullBox = element('input'); nullBox.type = 'checkbox'; nullBox.checked = initial === null;
      toggle.append(nullBox, document.createTextNode('暂不设置')); wrap.append(toggle); control.disabled = nullBox.checked;
      nullBox.addEventListener('change', () => { control.disabled = nullBox.checked; });
      const getter = read; read = () => nullBox.checked ? null : getter();
    }
    const hint = raw.uiHelp || helps[key] || (key.endsWith('_env') ? '填写环境变量名称；密钥在服务环境文件中管理。' : schema.description);
    if (hint) wrap.append(element('small', hint, 'field-help'));
    const error = element('p', '', 'field-error'); error.id = name + '-error'; error.hidden = true; wrap.append(error);
    control?.setAttribute('aria-describedby', error.id);
    state.fields.set(path, {node: wrap, error, control});
    return {node: wrap, read: () => { try { return read(); } catch (error) { if (!error.path) error.path = path; throw error; } }};
  }
  function readablePath(path) {
    if (!path) return '完整内容';
    return path.split('.').map((part, index, parts) => /^\d+$/.test(part) ? (parts[index - 1] === 'rules' ? '规则 ' : '第 ') + (Number(part) + 1) + (parts[index - 1] === 'rules' ? '' : ' 项') : labels[part] || part).join(' › ');
  }
  function readableValue(path, value) {
    const key = path.split('.').pop();
    if (value === null) return '未设置';
    if (typeof value === 'boolean') return value ? '开启' : '关闭';
    if (key === 'preset') return String(value) + '（配置标签）';
    if (value && typeof value === 'object' && value.subject && value.actions && value.effect) return (value.effect === 'allow' ? '允许 ' : '禁止 ') + ruleTarget(value.subject) + '\n操作：' + value.actions.map(action => actionLabels[action] || action).join('、') + '\n资源：' + value.resources.join('、') + '\n优先级：' + value.priority + (value.enabled ? ' · 已启用' : ' · 未启用');
    if (value && typeof value === 'object' && value.kind && key === 'subject') return ruleTarget(value);
    if (typeof value === 'string') return (key === 'kind' ? scopeLabels[value] : enumLabels[key]?.[value]) || (path.includes('actions') ? actionLabels[value] : null) || value;
    if (Array.isArray(value) && value.every(item => typeof item === 'string')) return value.map(item => path.includes('actions') ? actionLabels[item] || item : item).join('、') || '空列表';
    return typeof value === 'object' ? JSON.stringify(value, null, 2) : String(value);
  }
  function containerField(state, path, node, title) {
    const error = element('p', '', 'field-error'); error.hidden = true; node.append(error); node.dataset.path = path;
    state.fields.set(path, {node, error, control: null});
    if (title) node.setAttribute('aria-label', title);
  }
  function appendField(parent, schema, value, key, root, state, path, options = {}) {
    const item = field({...schema, ...options}, value, key, root, state, path); parent.append(item.node); return item;
  }
  function adapterHints(input) {
    if (!input) return;
    let options = $('console-adapter-options');
    if (!options) { options = element('datalist'); options.id = 'console-adapter-options'; document.body.append(options); }
    options.replaceChildren();
    for (const adapter of configView?.config?.adapters || []) { const option = element('option', 'QQ 官方机器人'); option.value = adapter.instance_id; options.append(option); }
    input.setAttribute('list', options.id); input.placeholder = '选择已接入的机器人';
  }
  function subjectForm(initial, schema, root, state, path) {
    const node = element('div', undefined, 'subject-fields nested-fields'); let source = structuredClone(initial), reads = [], automaticNamespace = null;
    const kind = appendField(node, schema.properties.kind, source.kind || 'global', 'kind', root, state, path + '.kind', {uiLabel: '这条规则针对谁', uiOptions: scopeLabels});
    const fields = element('div', undefined, 'subject-conditions nested-fields');
    const more = element('details', undefined, 'subject-advanced'); more.append(element('summary', '更多身份条件'));
    node.append(fields, more);
    function renderConditions() {
      fields.replaceChildren(); more.replaceChildren(element('summary', '更多身份条件')); reads = [];
      const requiredFields = subjectFields[kind.read()] || [];
      for (const [key, raw] of Object.entries(schema.properties)) {
        if (key === 'kind') continue;
        const target = requiredFields.includes(key) && key !== 'identity_namespace' ? fields : more;
        const item = appendField(target, raw, Object.hasOwn(source, key) ? source[key] : defaultValue(raw, root), key, root, state, path + '.' + key, {
          uiLabel: {adapter_instance_id: 'QQ 接入', user_id: '用户账号 / OpenID', group_id: '群号 / 群 OpenID', identity_namespace: '身份来源', bot_identity_id: '机器人标识'}[key] || labels[key],
          uiHelp: key === 'identity_namespace' ? '官方机器人会根据应用 ID 自动识别身份来源。其他来源请填写真实身份命名空间。' : requiredFields.includes(key) ? '此对象类型需要填写。' : '未设置时不增加限制；已填写的额外条件仍参与匹配。'
        });
        reads.push([key, item.read]);
        const control = state.fields.get(path + '.' + key)?.control;
        if (key === 'adapter_instance_id') adapterHints(control);
        if (key === 'identity_namespace') item.node.addEventListener('input', () => { automaticNamespace = null; });
        if (requiredFields.includes(key) && key !== 'identity_namespace') {
          const nullable = item.node.querySelector('.nullable'); if (nullable) nullable.hidden = true;
          const box = nullable?.querySelector('input'); if (box) box.checked = false;
          if (control) control.disabled = false;
        }
      }
      const extra = Object.keys(schema.properties).filter(key => key !== 'kind' && !requiredFields.includes(key) && source[key] != null && source[key] !== '');
      more.querySelector('summary').textContent = extra.length ? '更多身份条件 · ' + extra.length + ' 项额外限定' : '更多身份条件';
      more.open = extra.length > 0;
    }
    function read() {
      const value = {...source, kind: kind.read(), ...Object.fromEntries(reads.map(([key, getter]) => [key, getter()]))};
      const namespace = subjectNamespace(value, configView?.config?.adapters || [], automaticNamespace);
      if (namespace !== value.identity_namespace) {
        automaticNamespace = namespace || null;
        value.identity_namespace = namespace;
        const item = state.fields.get(path + '.identity_namespace'), checkbox = item.node.querySelector('.nullable input');
        if (checkbox) checkbox.checked = !namespace; item.control.disabled = !namespace; item.control.value = namespace || '';
      }
      return value;
    }
    renderConditions();
    state.fields.get(path + '.kind').control.addEventListener('change', () => { source = read(); renderConditions(); state.sync(); });
    containerField(state, path, node, '规则对象');
    return {node, read};
  }
  function actionPicker(initial, state, path) {
    const node = element('div', undefined, 'action-picker field wide'), options = element('div', undefined, 'action-options');
    node.append(element('label', '涉及哪些操作'), options); let values = [...initial]; const controls = new Map();
    const custom = element('details', undefined, 'custom-actions'); custom.append(element('summary', '其他操作名称'));
    const text = element('textarea'); text.rows = 2; text.setAttribute('aria-label', '其他操作名称，每行一项'); text.value = values.filter(value => !Object.hasOwn(actionLabels, value)).join('\n');
    custom.append(element('p', '未列出的操作会原样保留。每行填写一个准确名称或命名空间通配符，例如 github.*。', 'field-help'), text); node.append(custom);
    for (const [value, title] of Object.entries(actionLabels)) {
      const label = element('label', undefined, 'action-option'), input = element('input'); input.type = 'checkbox'; input.checked = values.includes(value); input.value = value;
      label.append(input, document.createTextNode(title)); options.append(label); controls.set(value, input);
      input.addEventListener('change', () => { values = updateAction(values, value, input.checked); for (const [key, item] of controls) item.checked = values.includes(key); state.sync(); });
    }
    text.addEventListener('input', () => { values = [...values.filter(value => Object.hasOwn(actionLabels, value)), ...text.value.split('\n').map(value => value.trim()).filter(Boolean)]; });
    node.append(element('p', '选择具体操作会取消“全部操作”；完整工作流程可能需要多种操作权限。', 'field-help'));
    containerField(state, path, node, '操作选择'); return {node, read: () => [...values]};
  }
  function permissionForm(value, schema, state, form) {
    form.classList.add('permission-editor'); const policy = {...defaultValue(schema, schema), ...structuredClone(value)};
    const defaults = element('div', undefined, 'policy-default'); form.append(defaults);
    const fallback = appendField(defaults, schema.properties.default_effect, policy.default_effect, 'default_effect', schema, state, 'default_effect', {uiLabel: '没有规则匹配时', uiOptions: {allow: '默认允许', deny: '默认拒绝'}});
    const guidance = element('p', '', 'policy-guidance'); defaults.append(guidance);
    function defaultHelp() { guidance.textContent = fallback.read() === 'allow' ? '只想屏蔽某人或某群？保持默认允许，再添加一条禁止规则即可。' : '默认拒绝会限制未被明确允许的操作。先保留管理员访问规则，再为需要使用的人配置允许规则。'; }
    defaultHelp(); state.fields.get('default_effect').control.addEventListener('change', defaultHelp);
    const list = element('div', undefined, 'permission-rule-list'); let rows = [];
    const ruleSchema = resolve(schema.properties.rules.items, schema), selectorSchema = resolve(ruleSchema.properties.subject, schema);
    function renderRules(values, expanded = null) {
      for (const key of [...state.fields.keys()]) if (key.startsWith('rules.')) state.fields.delete(key);
      list.replaceChildren(); rows = [];
      if (!values.length) list.append(element('p', '还没有单独规则，所有操作使用上面的默认行为。', 'empty-state'));
      values.forEach((originalRule, index) => {
        const rule = {...defaultValue(schema.properties.rules.items, schema), ...originalRule};
        rule.subject = {...defaultValue(ruleSchema.properties.subject, schema), ...rule.subject};
        const path = 'rules.' + index, card = element('details', undefined, 'permission-rule'); card.open = rule.id === expanded;
        const summary = element('summary', undefined, 'rule-summary');
        const effectTag = element('span', '', 'rule-effect'), target = element('strong', '', 'rule-target'), actionsText = element('span', '', 'rule-actions'), status = element('small', '', 'rule-state');
        summary.append(effectTag, target, actionsText, status); card.append(summary);
        const body = element('div', undefined, 'permission-rule-body'), basics = element('div', undefined, 'rule-basics nested-fields'); body.append(basics); card.append(body);
        const effect = appendField(basics, ruleSchema.properties.effect, rule.effect, 'effect', schema, state, path + '.effect', {uiLabel: '处理方式'});
        const enabled = appendField(basics, ruleSchema.properties.enabled, rule.enabled ?? true, 'enabled', schema, state, path + '.enabled', {uiLabel: '启用这条规则'});
        const subject = subjectForm(rule.subject, selectorSchema, schema, state, path + '.subject'); body.append(subject.node);
        const actions = actionPicker(rule.actions, state, path + '.actions'); body.append(actions.node);
        const advanced = element('details', undefined, 'rule-advanced'); advanced.append(element('summary', '高级设置')); body.append(advanced);
        const advancedGrid = element('div', undefined, 'nested-fields'); advanced.append(advancedGrid);
        const resources = appendField(advancedGrid, ruleSchema.properties.resources, rule.resources, 'resources', schema, state, path + '.resources', {uiLabel: '资源范围', uiHelp: '通常保留 *，表示全部资源。需要限制特定资源时再填写精确标识或前缀。'});
        const priority = appendField(advancedGrid, ruleSchema.properties.priority, rule.priority ?? 0, 'priority', schema, state, path + '.priority', {uiHelp: '数值越大越优先；同优先级按对象、资源与操作的具体程度匹配，同级时拒绝优先。列表顺序不影响结果。'});
        const id = appendField(advancedGrid, ruleSchema.properties.id, rule.id, 'id', schema, state, path + '.id', {uiLabel: '规则标识'});
        const read = () => ({...rule, effect: effect.read(), enabled: enabled.read(), subject: subject.read(), actions: actions.read(), resources: resources.read(), priority: priority.read(), id: id.read()});
        function summarize() {
          try { const current = read(); effectTag.textContent = current.effect === 'allow' ? '允许' : '禁止'; effectTag.className = 'rule-effect ' + current.effect; target.textContent = ruleTarget(current.subject); actionsText.textContent = current.actions.map(action => actionLabels[action] || action).join('、') || '尚未选择操作'; status.textContent = current.enabled ? '已启用' : '未启用'; }
          catch { status.textContent = '请完成设置'; }
        }
        summarize(); body.addEventListener('input', summarize); body.addEventListener('change', summarize);
        button(body, '移除规则', () => { const remaining = rows.filter((_, rowIndex) => rowIndex !== index).map(row => row.read()); renderRules(remaining); state.sync(); }, 'text-danger');
        containerField(state, path, card, '权限规则'); rows.push({read}); list.append(card);
      });
    }
    function addRule(rule) { renderRules([...rows.map(row => row.read()), rule], rule.id); state.sync(); }
    button(defaults, '添加管理员保留规则', () => {
      addRule({id: 'console-admin-' + uuid().slice(0, 8), enabled: true, priority: 1000, effect: 'allow', actions: ['*'], resources: ['*'], subject: {...defaultValue(ruleSchema.properties.subject, schema), kind: 'person', adapter_instance_id: 'admin_api', identity_namespace: 'admin_api', user_id: 'owner'}});
      notice('管理员保留规则已加入草稿，保存后生效。');
    });
    form.append(list); renderRules(policy.rules || []); containerField(state, 'rules', list, '权限规则列表');
    button(form, '添加规则', () => addRule(newPermissionRule('rule-' + uuid().slice(0, 8), defaultValue(ruleSchema.properties.subject, schema))), 'add-item');
    const advanced = element('details', undefined, 'policy-advanced'); advanced.append(element('summary', '兼容配置标签')); form.append(advanced);
    const preset = appendField(advanced, schema.properties.preset, policy.preset, 'preset', schema, state, 'preset', {uiLabel: '预设标签', uiOptions: {allow_all: 'allow_all（兼容标签）', custom: 'custom（兼容标签）'}, uiHelp: '这是保存的配置标签，不决定是否放行，也不会清空规则。实际默认行为以上方选择为准。'});
    return () => ({...policy, default_effect: fallback.read(), preset: preset.read(), rules: rows.map(row => row.read())});
  }
  let permissionCheckRead;
  function setupPermissionChecker(schema) {
    const hidden = $('permission-subject'), detail = hidden.closest('details'), original = {adapter_instance_id: 'admin_api', identity_namespace: 'admin_api', user_id: 'owner'};
    const action = $('permission-action'), resource = $('permission-resource'), submit = $('check-permission');
    detail.replaceChildren(element('summary', '检查当前生效权限'));
    detail.append(element('p', '选择对象与操作，查看已保存策略的判断。尚未保存的草稿不参与检查。', 'field-help'));
    const form = element('div', undefined, 'permission-check-form'), targetLabel = element('label', '检查对象'), target = element('select'); target.id = 'permission-check-target'; targetLabel.htmlFor = target.id;
    for (const [value, title] of [['admin', '当前管理员'], ['custom', '指定 QQ 用户或群']]) { const option = element('option', title); option.value = value; target.append(option); }
    targetLabel.append(target); form.append(targetLabel); detail.append(form);
    const ruleSchema = resolve(schema.properties.rules.items, schema), selectorSchema = resolve(ruleSchema.properties.subject, schema);
    const state = {fields: new Map(), sync: () => { hidden.value = JSON.stringify(readSubject()); }};
    const subject = subjectForm({...defaultValue(ruleSchema.properties.subject, schema), kind: 'person'}, selectorSchema, schema, state, 'check-subject'); subject.node.hidden = true; form.append(subject.node);
    function readSubject() { if (target.value === 'admin') return {...original}; const {kind, ...value} = subject.read(); return value; }
    target.addEventListener('change', () => { subject.node.hidden = target.value === 'admin'; state.sync(); });
    subject.node.addEventListener('input', state.sync); subject.node.addEventListener('change', state.sync);
    const actionLabel = element('label', '检查哪种操作'), choices = element('select'); choices.id = 'permission-check-action'; actionLabel.htmlFor = choices.id;
    const catalog = {'admin.get': '访问控制台', ...Object.fromEntries(Object.entries(actionLabels).filter(([key]) => !key.includes('*')))};
    if (!Object.hasOwn(catalog, action.value)) catalog[action.value] = action.value;
    for (const [value, title] of Object.entries(catalog)) { const option = element('option', title); option.value = value; choices.append(option); }
    const custom = element('option', '自定义操作…'); custom.value = '__custom__'; choices.append(custom); choices.value = action.value;
    actionLabel.append(choices); form.append(actionLabel);
    const advanced = element('details', undefined, 'permission-check-advanced'); advanced.append(element('summary', '指定资源或自定义操作'));
    const actionCustom = element('label', '操作名称'); actionCustom.htmlFor = action.id; actionCustom.append(action);
    const resourceLabel = element('label', '要检查的实际资源'); resourceLabel.htmlFor = resource.id; resourceLabel.append(resource);
    advanced.append(actionCustom, resourceLabel, element('p', '结果只针对填写的资源；有资源范围规则时，请填写真实资源标识，例如 conversation:会话ID。', 'field-help')); form.append(advanced);
    choices.addEventListener('change', () => { if (choices.value === '__custom__') { advanced.open = true; action.focus(); } else action.value = choices.value; });
    action.addEventListener('input', () => { choices.value = Object.hasOwn(catalog, action.value) ? action.value : '__custom__'; });
    hidden.hidden = true; form.append(hidden, submit);
    const result = element('p', '', 'source-note'); result.id = 'permission-check-result'; result.hidden = true; result.setAttribute('role', 'status'); detail.append(result);
    form.addEventListener('input', () => { result.hidden = true; }); form.addEventListener('change', () => { result.hidden = true; });
    permissionCheckRead = () => {
      if (target.value === 'custom') {
        try { validatePolicy({rules: [{id: 'check', subject: subject.read(), actions: [action.value], resources: [resource.value]}]}); }
        catch (error) { delete error.path; throw error; }
      }
      return readSubject();
    };
  }
  function editor(prefix, panel) {
    const form = $(prefix + '-form'), json = $(prefix + '-json');
    const state = {prefix, panel, schema: null, getter: null, baseline: null, dirty: false, jsonDirty: false, revision: 0, loading: false, fields: new Map(), groupNodes: []};
    state.clearErrors = () => {
      show(prefix + '-errors', ''); json.removeAttribute('aria-invalid');
      for (const item of state.fields.values()) { item.error.hidden = true; item.node.classList.remove('is-invalid'); item.control?.removeAttribute('aria-invalid'); }
    };
    state.errors = (error, focus = true) => {
      state.clearErrors();
      const details = Array.isArray(error.details) ? error.details.map(item => ({path: errorPath(item.loc), message: item.msg})) : [{path: error.path || '', message: typeof error.details === 'string' ? /lock.*out|console access/i.test(error.details) ? '此策略会导致管理员无法进入控制台，请保留配置读取和权限管理权限。' : error.message + ' ' + error.details : error.message}];
      show(prefix + '-errors', details.map(item => (item.path ? item.path + '：' : '') + item.message).join('\n'));
      let first;
      for (const detail of details) {
        let path = detail.path;
        while (path && !state.fields.has(path)) path = path.split('.').slice(0, -1).join('.');
        const item = state.fields.get(path);
        if (!item) continue;
        item.error.textContent = detail.message; item.error.hidden = false; item.node.classList.add('is-invalid'); item.control?.setAttribute('aria-invalid', 'true'); first ||= item;
      }
      if (focus && state.jsonDirty) {
        json.closest('details')?.setAttribute('open', ''); json.setAttribute('aria-invalid', 'true'); json.focus(); return;
      }
      if (first && focus) {
        if (prefix === 'config') { $('config-search').value = ''; category = state.groupNodes.findIndex(group => group.contains(first.node)); filterConfig(); }
        let parent = first.node.parentElement;
        while (parent && parent !== form) { if (parent.tagName === 'DETAILS') parent.open = true; parent = parent.parentElement; }
        first.node.scrollIntoView({block: 'center', behavior: 'smooth'}); (first.control || first.node.querySelector('input, select, textarea'))?.focus();
      }
    };
    state.read = () => {
      if (!state.getter) throw new Error('配置尚未读取完成，请先读取。');
      try { return state.jsonDirty ? JSON.parse(json.value) : state.getter(); }
      catch (error) { if (state.jsonDirty) error.message = '高级 JSON 格式不正确，请检查引号和逗号。'; throw error; }
    };
    state.sync = () => {
      state.revision++; state.clearErrors();
      let count = 0;
      try {
        const candidate = state.read(); count = changes(state.baseline, candidate).length; state.dirty = count > 0;
        if (!state.jsonDirty) json.value = JSON.stringify(candidate, null, 2);
      } catch (error) { state.dirty = true; state.errors(error, false); }
      show(prefix + '-state', state.dirty ? (state.jsonDirty ? 'JSON 草稿待保存' : '有未保存修改') : '已与保存版本同步');
      $(prefix + '-state').classList.toggle('dirty', state.dirty);
      if (prefix === 'config') { show('config-change-count', count ? count + ' 项变更' : state.dirty ? '请修正输入' : '没有未保存修改'); show('config-save-status', state.dirty ? '草稿仅保存在本页' : configView?.restart_required ? '已保存 · 等待重启' : '全部更改已保存'); }
      updateSaveButtons();
    };
    state.load = (value, schema = state.schema, baseline = value) => {
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('配置必须是 JSON 对象。');
      if (!schema) throw new Error('请先读取配置。');
      assertShape(value, schema);
      state.schema = schema; state.baseline = structuredClone(baseline); state.jsonDirty = false; form.inert = false; form.replaceChildren(); state.fields.clear(); state.groupNodes = [];
      if (prefix === 'permissions') {
        state.getter = permissionForm(value, schema, state, form);
        json.value = JSON.stringify(value, null, 2); state.sync(); return;
      }
      const reads = [], seen = new Set();
      function add(parent, key) {
        const raw = schema.properties[key]; if (!raw) return; seen.add(key);
        const item = field(raw, Object.hasOwn(value, key) ? value[key] : defaultValue(raw, schema), key, schema, state, key);
        parent.append(item.node); reads.push([key, item.read]);
      }
      if (prefix === 'config') {
        const categories = [...groups];
        const unknown = Object.keys(schema.properties).filter(key => !groups.some(([, keys]) => keys.includes(key)));
        if (unknown.length) categories.push(['其他设置', unknown]);
        $('config-categories').replaceChildren();
        categories.forEach(([title, keys], index) => {
          const group = element('div', undefined, 'config-group nested-fields'); group.dataset.category = String(index); group.id = 'config-category-' + index;
          if (keys.length === 1 && keys[0] === 'permissions') {
            const intro = element('div', undefined, 'initial-permission-note'); intro.append(element('p', '这里仅是新数据库的初始值。管理当前机器人权限，请打开实时权限策略。', 'field-help'));
            button(intro, '管理当前生效权限', () => navigate('permissions')); group.append(intro);
            const advanced = element('details'); advanced.append(element('summary', '查看初始权限配置')); group.append(advanced); add(advanced, 'permissions');
          } else keys.forEach(key => add(group, key));
          form.append(group); state.groupNodes.push(group);
          const tab = button($('config-categories'), title, () => { category = index; $('config-search').value = ''; filterConfig(); });
          tab.dataset.category = String(index); tab.setAttribute('aria-controls', group.id); tab.setAttribute('aria-pressed', 'false');
        });
      }
      for (const key of Object.keys(schema.properties)) if (!seen.has(key)) add(form, key);
      if (prefix === 'config') category = Math.min(category, Math.max(0, state.groupNodes.length - 1));
      state.getter = () => ({...value, ...Object.fromEntries(reads.map(([key, getter]) => [key, getter()]))}); json.value = JSON.stringify(value, null, 2);
      state.sync(); if (prefix === 'config') filterConfig();
    };
    state.canReplace = async () => !state.dirty || confirmDialog('替换当前草稿？', '未保存的修改将被替换。取消后可继续编辑或先保存。', [], '继续读取');
    state.clear = () => { state.getter = state.schema = state.baseline = null; state.dirty = state.jsonDirty = false; state.revision++; form.replaceChildren(); state.fields.clear(); json.value = '{}'; show(prefix + '-state', '尚未读取'); state.clearErrors(); };
    form.addEventListener('input', () => state.sync()); form.addEventListener('change', () => state.sync());
    json.addEventListener('input', () => { state.jsonDirty = true; form.inert = true; state.sync(); });
    action(prefix + '-use-json', () => { if (!state.schema) throw new Error('请先读取配置。'); state.load(JSON.parse(json.value), state.schema, state.baseline); notice('已同步到表单，尚未保存。'); });
    action('discard-' + prefix, async () => { if (state.dirty && await confirmDialog('撤销未保存修改？', '恢复到上次读取或保存的版本。', [], '撤销修改')) state.load(state.baseline); });
    editors.push(state); return state;
  }
  const configEditor = editor('config', 'configuration'), permissionEditor = editor('permissions', 'permissions');
  function filterConfig() {
    const query = $('config-search').value.trim().toLowerCase(); let count = 0;
    const entries = [...configEditor.fields.entries()].sort((a, b) => b[0].split('.').length - a[0].split('.').length);
    for (const [path, item] of entries) {
      const children = entries.filter(([candidate]) => candidate.startsWith(path + '.'));
      const match = !query || (path + ' ' + path.split('.').map(key => labels[key] || '').join(' ')).toLowerCase().includes(query);
      item.node.hidden = !(match || children.some(([, child]) => !child.node.hidden));
      if (!children.length && !item.node.hidden && (query || configEditor.groupNodes[category]?.contains(item.node))) count++;
      if (query && !item.node.hidden) { let parent = item.node.parentElement; while (parent && parent !== $('config-form')) { if (parent.tagName === 'DETAILS') parent.open = true; parent = parent.parentElement; } }
    }
    configEditor.groupNodes.forEach((group, index) => { group.hidden = query ? ![...group.children].some(item => !item.hidden) : index !== category; });
    for (const tab of $('config-categories').children) { const active = Number(tab.dataset.category) === category && !query; tab.setAttribute('aria-pressed', String(active)); tab.classList.toggle('active', active); }
    show('config-search-count', query ? count ? '找到 ' + count + ' 项设置' : '没有匹配设置，试试中文名称或字段名。' : count + ' 项设置');
  }
  function updateSaveButtons() {
    const bindings = [['save-config', configEditor], ['apply-config', configEditor], ['save-permissions', permissionEditor]];
    for (const [id, state] of bindings) $(id).disabled = saving || !state.dirty || !state.getter;
    for (const state of editors) { $('discard-' + state.prefix).disabled = saving || !state.dirty; }
    let cold = true;
    try { cold = changes(configView?.config, configEditor.read()).some(item => !configView.hot_fields.includes(item.path.split('.')[0])); } catch {}
    const hotReason = configView?.restart_required ? '存在等待重启的配置，请先完成重启或恢复。' : cold ? '草稿包含需重启的设置，请使用保存配置。' : '';
    $('apply-config').disabled ||= Boolean(hotReason); $('apply-config').title = hotReason;
    for (const id of replaceButtons) $(id).disabled = saving;
    $('preview-config').disabled = !configEditor.dirty || saving;
    $('validate-config').disabled = !configEditor.getter || saving;
  }
  async function mutate(operation) {
    if (saving) return;
    saving = true; updateSaveButtons();
    for (const state of editors) { $(state.prefix + '-form').inert = true; $(state.prefix + '-json').disabled = true; }
    try { await operation(); }
    finally {
      saving = false;
      for (const state of editors) { $(state.prefix + '-form').inert = state.jsonDirty; $(state.prefix + '-json').disabled = false; }
      updateSaveButtons();
    }
  }
  async function overview() {
    empty($('overview-metrics'), '正在读取服务状态…');
    try {
      const [status, adapters] = await Promise.all([request('/api/v1/status'), request('/api/v1/adapters')]);
      show('service-status', status); show('adapter-status', adapters); $('overview-metrics').replaceChildren(); $('overview-adapters').replaceChildren();
      const metrics = [['待处理消息', status.inbox_pending ?? 0], ['QQ 接入', adapters.length], ['交互执行', status.interactive_execution_enabled ? '已开启' : '已关闭'], ['后台任务', status.tasks_enabled ? '已开启' : '已关闭']];
      for (const [label, value] of metrics) { const card = element('article', undefined, 'metric-card'); card.append(element('span', label, 'metric-label'), element('strong', String(value), 'metric-value')); $('overview-metrics').append(card); }
      if (!adapters.length) empty($('overview-adapters'), '尚未配置 QQ 接入。在“全部配置 → 接入”添加机器人后，重启服务连接。');
      for (const adapter of adapters) {
        const card = element('article', undefined, 'adapter-card');
        card.append(element('h3', adapter.id), element('span', 'QQ 官方', 'badge'));
        card.append(element('p', adapter.credentials_configured ? '接入凭据已配置' : '接入凭据未配置', 'hint'));
        $('overview-adapters').append(card);
      }
    } catch (error) { empty($('overview-metrics'), '暂时无法读取运行状态。', overview); throw error; }
  }
  async function loadTasks() {
    const rows = await request('/api/v1/tasks'); $('task-rows').replaceChildren();
    for (const item of rows) { const row = document.createElement('tr'); cell(row, item.public_id); cell(row, item.state); cell(row, item.created_at); button(cell(row, ''), '查看', async () => { selectTask(item.public_id); await loadTask(); }); $('task-rows').append(row); }
    if (!rows.length) { const row = element('tr'); const td = cell(row, '暂无后台任务。'); td.colSpan = 4; $('task-rows').append(row); }
    notice('已读取 ' + rows.length + ' 个任务');
  }
  function selectTask(id) { if ($('task-id').value !== id) { stopEvents(); eventCursor = '0'; streamTask = ''; show('event-output', '未连接'); } $('task-id').value = id; }
  $('task-id').addEventListener('change', () => { stopEvents(); eventCursor = '0'; streamTask = ''; show('event-output', '未连接'); });
  async function loadTask() { show('task-output', await request('/api/v1/tasks/' + encode(taskID()))); $('task-output').closest('details')?.setAttribute('open', ''); }
  action('load-tasks', loadTasks); action('load-task', loadTask);
  action('cancel-task', async () => { if (!await confirmDialog('取消此任务？', '取消请求发送后，执行器会停止当前任务。', [], '取消任务')) return; show('task-output', await request('/api/v1/tasks/' + encode(taskID()) + '/cancel', {method: 'POST'})); notice('取消请求已处理'); });
  $('create-key').value = uuid();
  action('new-create-key', () => { $('create-key').value = uuid(); });
  action('create-task', async () => {
    const result = await request('/api/v1/tasks', {method: 'POST', headers: {'Idempotency-Key': required('create-key', '新任务幂等键')}, body: {goal: required('task-goal', '任务目标')}});
    selectTask(result.public_id); show('task-output', result); notice('任务已提交');
  });
  function stopEvents() { if (eventController) eventController.abort(); eventController = null; }
  $('stop-events').addEventListener('click', () => { stopEvents(); notice('事件读取已停止'); });
  action('start-events', async () => {
    stopEvents(); const id = taskID(); if (streamTask !== id) { streamTask = id; eventCursor = '0'; show('event-output', ''); }
    const controller = new AbortController(); eventController = controller;
    try {
      const response = await fetch('/api/v1/tasks/' + encode(id) + '/events', {headers: headers({'Last-Event-ID': eventCursor}), signal: controller.signal, cache: 'no-store'});
      if (!response.ok) { if (response.status === 401) { token = ''; authEpoch++; loginVisible(true); show('auth-error', '令牌已失效，请重新连接。'); } throw new Error('事件读取失败，请检查权限和连接后重试。'); }
      if (!response.body) throw new Error('浏览器无法读取事件流');
      notice('正在读取任务事件'); const reader = response.body.getReader(), decoder = new TextDecoder(); let buffer = '';
      while (true) {
        const chunk = await reader.read(); if (chunk.done || controller.signal.aborted) break; buffer += decoder.decode(chunk.value, {stream: true}); buffer = buffer.replace(/\r\n/g, '\n');
        let end;
        while ((end = buffer.indexOf('\n\n')) !== -1) {
          const frame = buffer.slice(0, end); buffer = buffer.slice(end + 2); const data = [];
          for (const line of frame.split('\n')) { if (line.startsWith('id:')) eventCursor = line.slice(3).trim(); if (line.startsWith('data:')) data.push(line.slice(5).trimStart()); }
          if (data.length) $('event-output').textContent = ($('event-output').textContent + data.join('\n') + '\n').slice(-24000);
        }
        if (buffer.length > 1000000) throw new Error('事件帧超过读取上限');
      }
      notice('事件流已结束');
    } catch (error) { if (error.name !== 'AbortError') throw error; }
    finally { if (eventController === controller) eventController = null; }
  });
  function configMetadata(result) {
    configView = result; configVersion = result.version; savedConfigVersion = result.saved_version;
    show('config-version', result.version); show('saved-config-version', result.saved_version);
    show('hot-fields', '即时应用支持：' + (result.hot_fields || []).map(key => labels[key] || key).join('、'));
    $('credentials').replaceChildren();
    for (const credential of result.credentials || []) $('credentials').append(element('span', credential.name + ' · ' + (credential.configured ? '已配置' : '未配置'), 'badge'));
  }
  async function loadEditor(state, operation, force = false) {
    if (saving || state.loading || (!force && !await state.canReplace())) return;
    const revision = state.revision; state.loading = true;
    show(state.prefix + '-state', '正在读取…');
    if (!state.getter) empty($(state.prefix + '-form'), '正在读取设置…');
    try {
      const result = await operation();
      if (revision !== state.revision) { notice('读取期间草稿已修改，已保留当前编辑。'); return; }
      result();
    } catch (error) {
      if (!state.getter) empty($(state.prefix + '-form'), '读取失败。请检查连接后重试。', () => loadEditor(state, operation, force));
      show(state.prefix + '-state', '读取失败，当前草稿已保留'); throw error;
    } finally { state.loading = false; updateSaveButtons(); }
  }
  async function loadConfig(force = false, initial) {
    return loadEditor(configEditor, async () => {
      const [result, schema] = await Promise.all([initial || request('/api/v1/config'), request('/api/v1/config/schema')]);
      return () => {
        configMetadata(result); configEditor.load(result.saved_config, schema);
        show('config-state', result.restart_required ? '已保存，等待重启后生效' : '当前运行与保存配置一致');
        show('config-output', {restart_required: result.restart_required, changed_fields: result.changed_fields});
      };
    }, force);
  }
  const loadPermissions = () => loadEditor(permissionEditor, async () => {
    const [result, schema] = await Promise.all([request('/api/v1/permissions'), request('/api/v1/permissions/schema')]);
    return () => { const {policy_version, ...policy} = result; permissionVersion = policy_version; permissionEditor.load(policy, schema); setupPermissionChecker(schema); show('permissions-output', {policy_version}); };
  });
  action('load-config', () => loadConfig()); action('load-permissions', () => loadPermissions());
  action('validate-config', async () => {
    const result = await request('/api/v1/config/validate', {method: 'POST', body: {expected_version: configVersion, config: configEditor.read()}});
    configEditor.clearErrors(); show('config-output', result); notice(result.restart_required ? '校验通过。包含需重启生效的设置，可保存到下次启动。' : '校验通过。这些更改支持即时应用。');
  });
  async function saveConfig(forceHot = false) {
    await mutate(async () => {
      const candidate = configEditor.read(), diff = changes(configEditor.baseline, candidate);
      if (!diff.length) return;
      const validation = await request('/api/v1/config/validate', {method: 'POST', body: {expected_version: configVersion, config: candidate}});
      configEditor.clearErrors();
      const hot = !validation.restart_required && !configView.restart_required;
      if (forceHot && !hot) throw new Error('这些更改不能即时应用，请使用保存配置。');
      if (!await confirmDialog(hot ? '即时应用这些更改？' : '保存这些更改？', hot ? '配置已校验。下列更改可立即生效，无需重启。' : '配置已校验。保存后不自动重启；需重启的字段在下次启动时生效。模型改动还需同步 Worker。', diff, hot ? '立即应用' : '保存配置')) return;
      const result = await request('/api/v1/config/' + (hot ? 'apply' : 'save'), {method: 'POST', body: {expected_version: hot ? configVersion : savedConfigVersion, config: candidate}});
      configMetadata({...configView, ...(hot ? {config: candidate, version: result.version} : {}), saved_config: candidate, saved_version: result.version, restart_required: hot ? false : result.restart_required, changed_fields: result.changed_fields || []});
      let refreshed = false;
      try { const latest = await request('/api/v1/config'); configMetadata(latest); configEditor.load(latest.saved_config); refreshed = true; }
      catch (error) {
        configEditor.load(candidate); configEditor.baseline = structuredClone(configEditor.read()); configEditor.sync();
        if (error.status === 401) throw error;
      }
      show('config-output', result);
      if (!refreshed) { notice('配置已保存，但未能刷新服务返回的最终配置。请稍后重新读取。', true); return; }
      show('config-state', hot ? '更改已应用，正在使用新配置' : result.restart_required ? '已保存，等待重启后生效' : '已保存，与当前运行一致');
      notice(hot ? '更改已即时应用。' : result.restart_required ? '配置已保存。请重启 Core 使待生效设置启用。' : '配置已保存。');
    });
  }
  action('apply-config', () => saveConfig(true)); action('save-config', () => saveConfig());
  action('preview-config', async () => { await confirmDialog('未保存的更改', '对比已保存的原值与当前草稿的新值。关闭预览不会保存。', changes(configEditor.baseline, configEditor.read()), '返回编辑'); });
  action('save-permissions', () => mutate(async () => {
    const policy = validatePolicy(permissionEditor.read()), diff = changes(permissionEditor.baseline, policy);
    if (!diff.length || !await confirmDialog('更新生效中的权限策略？', '更改会立即影响所有操作。服务会检查管理员入口是否仍可访问。', diff, '保存权限策略')) return;
    const result = await request('/api/v1/permissions', {method: 'PUT', body: {expected_version: permissionVersion, policy}});
    const {policy_version, ...updated} = result; permissionVersion = policy_version; permissionEditor.load(updated); show('permissions-output', result); notice('权限策略已保存并生效。');
  }));
  action('load-versions', async () => {
    const versions = await request('/api/v1/config/versions?include_metadata=true'); $('versions').replaceChildren();
    if (!versions.length) { const option = element('option', '暂无历史版本'); option.value = ''; $('versions').append(option); }
    for (const item of versions) { const option = element('option', new Date(item.saved_at).toLocaleString('zh-CN') + ' · ' + item.version.slice(0, 12)); option.value = item.version; $('versions').append(option); }
    notice(versions.length ? '已读取 ' + versions.length + ' 个历史版本。' : '尚无历史版本。首次保存后会记录版本。');
  });
  action('load-version', async () => {
    if (!configEditor.getter) throw new Error('请先读取配置。');
    if (!await configEditor.canReplace()) return;
    const revision = configEditor.revision, candidate = await request('/api/v1/config/versions/' + encode(required('versions', '历史版本')));
    if (revision !== configEditor.revision) throw new Error('读取期间草稿已修改，历史版本未覆盖当前编辑。');
    configEditor.load(candidate, configEditor.schema, configEditor.baseline); notice('历史版本已载入草稿。校验并保存后才会恢复。');
  });
  action('check-permission', async () => {
    const result = await request('/api/v1/permissions/explain', {method: 'POST', body: {subject: permissionCheckRead ? permissionCheckRead() : JSON.parse($('permission-subject').value), action: required('permission-action', '操作'), resource: required('permission-resource', '资源')}});
    show('permissions-output', result); show('permission-check-result', permissionExplanation(result)); $('permission-check-result').hidden = false;
  });
  $('config-search').addEventListener('input', filterConfig);
  const panels = {
    configuration: ['全部配置', '连接、个人偏好与运行参数。', () => configEditor.getter || loadConfig()],
    overview: ['运行总览', '了解服务状态与 QQ 接入情况。', overview],
    tasks: ['后台任务', '查看 Leader 与 Workers 的持久任务，或提交新的工作目标。', loadTasks],
    permissions: ['权限策略', '决定哪些用户、群聊可以使用哪些功能。', () => permissionEditor.getter || loadPermissions()]
  };
  async function navigate(panel, updateHash = true) {
    if (!panels[panel]) panel = 'configuration'; activePanel = panel;
    for (const control of document.querySelectorAll('[data-panel]')) {
      const active = control.dataset.panel === panel; control.setAttribute('aria-current', active ? 'page' : 'false'); control.setAttribute('aria-pressed', String(active)); $(control.dataset.panel).hidden = !active;
    }
    show('page-title', panels[panel][0]); show('page-subtitle', panels[panel][1]);
    if (updateHash) history.replaceState(null, '', '#' + panel);
    if (token) { try { await panels[panel][2](); } catch (error) { report(error, editors.find(item => item.panel === panel)); } }
  }
  for (const control of document.querySelectorAll('[data-panel]')) control.addEventListener('click', () => navigate(control.dataset.panel));
  window.addEventListener('hashchange', () => { if (panels[location.hash.slice(1)]) navigate(location.hash.slice(1), false); });
  $('auth').addEventListener('submit', async event => {
    event.preventDefault(); const control = $('auth-submit');
    await runAction(control, async () => {
      stopEvents(); authEpoch++; token = $('token').value.trim(); show('auth-error', '');
      try {
        const initial = await request('/api/v1/config'); $('token').value = ''; loginVisible(false);
        if (!configEditor.getter) await loadConfig(true, initial);
        await navigate(location.hash.slice(1) || 'configuration');
        notice(editors.some(state => state.dirty) ? '已重新连接，未保存草稿已保留。' : '已连接，设置已就绪。');
      } catch (error) { if (!configEditor.getter) loginVisible(true); show('auth-error', error.message); throw error; }
    });
  });
  $('login-toggle-token').addEventListener('click', () => {
    const visible = $('token').type === 'password'; $('token').type = visible ? 'text' : 'password';
    $('login-toggle-token').setAttribute('aria-pressed', String(visible)); $('login-toggle-token').setAttribute('aria-label', visible ? '隐藏令牌' : '显示令牌'); show('login-toggle-token', visible ? '隐藏' : '显示');
  });
  action('logout', async () => {
    if (editors.some(state => state.dirty) && !await confirmDialog('断开并清除草稿？', '管理员令牌和本页未保存的内容会被清除。', [], '断开连接')) return;
    stopEvents(); authEpoch++; token = ''; configVersion = savedConfigVersion = permissionVersion = null;
    editors.forEach(state => state.clear()); configView = null; $('token').value = ''; $('token').type = 'password';
    document.querySelectorAll('pre').forEach(item => { item.textContent = ''; });
    for (const id of ['task-rows', 'versions', 'credentials', 'overview-metrics', 'overview-adapters']) $(id).replaceChildren();
    for (const id of ['task-id', 'task-goal']) $(id).value = '';
    show('auth-error', '');
    $('login-toggle-token').setAttribute('aria-pressed', 'false');
    $('login-toggle-token').setAttribute('aria-label', '显示令牌'); show('login-toggle-token', '显示');
    loginVisible(true); $('token').focus();
  });
  window.addEventListener('beforeunload', event => { if (editors.some(state => state.dirty)) { event.preventDefault(); event.returnValue = ''; } });
  window.addEventListener('pagehide', () => { stopEvents(); authEpoch++; token = ''; });
  updateSaveButtons(); loginVisible(true); navigate(location.hash.slice(1) || 'configuration', false);
})();
