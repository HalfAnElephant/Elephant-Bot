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
    if (schema.type === 'integer' || schema.type === 'number') return schema.minimum ?? 0;
    return '';
  }
  function errorPath(location = []) {
    const parts = [...location];
    if (parts[0] === 'body') parts.shift();
    if (['config', 'persona', 'policy'].includes(parts[0])) parts.shift();
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
      for (const [key, child] of Object.entries(schema.properties)) if (Object.hasOwn(value, key)) assertShape(value[key], child, root, path ? path + '.' + key : key);
    }
    if (schema.type === 'array' && schema.items) value.forEach((item, index) => assertShape(item, schema.items, root, path + '.' + index));
  }
  return {equal, changes, resolve, defaultValue, errorPath, assertShape};
})();
if (typeof module !== 'undefined') module.exports = ConsoleModel;
if (typeof document !== 'undefined') (() => {
  const $ = id => document.getElementById(id), encode = encodeURIComponent;
  const {equal, changes, resolve, defaultValue, errorPath, assertShape} = ConsoleModel;
  const editors = [], replaceButtons = ['load-config', 'load-global-persona', 'load-permissions', 'load-versions', 'load-version', 'config-use-json', 'global-persona-use-json', 'permissions-use-json'];
  let token = '', authEpoch = 0, saving = false, dialogPending = false, activePanel = 'configuration', noticeTimer;
  let conversations = [], configVersion = null, savedConfigVersion = null, personaVersion = null, permissionVersion = null;
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
  const cid = () => required('cid', '会话 ID'), taskID = () => required('task-id', '任务 ID');
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
        element('strong', change.path || '完整内容'),
        element('small', '原值'),
        element('pre', change.before === undefined ? '未设置' : JSON.stringify(change.before, null, 2), 'diff-before'),
        element('small', '新值'),
        element('pre', change.after === undefined ? '移除' : JSON.stringify(change.after, null, 2), 'diff-after')
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
    deployment_id: '部署标识', database: '数据库路径', adapters: 'QQ 接入', admin_token_env: '管理员令牌变量', internal_token_env: '内部令牌变量', model: '模型', source_url: '源码地址', send_budget_per_minute: '每分钟发送预算', timezone: '时区', tasks_enabled: '启用后台任务', interactive_execution_enabled: '启用交互执行', permissions: '初始权限策略', execution: '执行与资源预算', context: '上下文', internal_socket: '内部通信 Socket', persona_path: '人格文件路径', judge: '发言判断', memory: '记忆', social_evaluated_conversations: '已完成社交评估的会话',
    instance_id: '接入实例标识', kind: '类型', bot_id: '机器人账号标识', secret_env: '接入密钥变量', app_id: '应用 ID', websocket_url: 'WebSocket 地址', allowed_groups: '兼容群列表', allowed_users: '兼容用户列表', admin_users: '管理员角色列表', known_bot_ids: '已知机器人账号', full_mode_enabled: '启用全量消息权限', full_mode_verified: '已验证全量消息', proactive_verified: '已验证主动消息', reply_window_seconds: '回复窗口（秒）', max_replies: '最多回复数', signature_max_age_seconds: '签名最大时效（秒）', api_base: 'API 地址', token_url: '令牌地址',
    endpoint: '请求地址', api_key_env: 'API 密钥变量', timeout_seconds: '超时（秒）', context_window: '模型上下文窗口', max_active: '最大活跃执行数', sustained_limit: '持续任务上限', foreground_wait_ms: '前台等待（毫秒）', interactive_budget: '交互任务预算', budget: '后台任务预算', warm_max: '常驻进程上限', warm_idle_max: '空闲进程上限', warm_ttl_seconds: '空闲保留（秒）',
    max_request_bytes: '最大请求字节数', max_tokens: '上下文软上限', hard_input_tokens: '输入硬上限', keep_recent_tokens: '近期消息保留 Token', compaction_input_tokens: '压缩输入预算', compaction_output_tokens: '压缩输出预算', recent_messages: '近期消息条数', topic_idle_seconds: '话题空闲超时（秒）', version: '版本',
    maintenance_enabled: '启用记忆维护', auto_extract: '自动提取记忆', extractor_endpoint: '提取服务地址', extractor_model: '提取模型', extractor_key_env: '提取密钥变量', embedding_endpoint: '向量服务地址', embedding_model: '向量模型', embedding_version: '向量版本', embedding_dimensions: '向量维度', embedding_key_env: '向量密钥变量', raw_retention_days: '原始记录保留（天）',
    preset: '预设', default_effect: '默认行为', rules: '权限规则', id: '规则 ID', enabled: '启用', priority: '优先级', subject: '主体范围', actions: '操作匹配', resources: '资源匹配', effect: '行为', adapter_instance_id: '接入实例 ID', bot_identity_id: '机器人身份 ID', identity_namespace: '身份命名空间', group_id: '群 ID', user_id: '用户 ID',
    persona_id: '人格标识', name: '名称', identity: '身份说明', traits: '性格特征', style: '表达风格', boundaries: '行为边界', model_calls: '模型调用次数', tool_calls: '工具调用次数', active_seconds: '活跃执行（秒）', total_wall_seconds: '总时长（秒）', output_tokens_per_call: '每次输出 Token', input_tokens: '输入 Token 预算', output_tokens: '输出 Token 预算', completion_reserve: '完成阶段预留模型调用'
  };
  const enumLabels = {
    kind: {onebot: 'OneBot / NapCat', official: 'QQ 官方机器人', rule: '规则判断', jev: 'Jev', structured: '结构化模型', global: '全局', adapter: '接入实例', bot: '机器人', group: '群组', person: '个人', group_member: '群内成员'},
    preset: {allow_all: '默认开放', custom: '自定义'},
    effect: {allow: '允许', deny: '拒绝'}, default_effect: {allow: '允许', deny: '拒绝'}
  };
  const helps = {
    adapters: '添加 OneBot 或 QQ 官方接入。账号、群号与用户 ID 均使用字符串。',
    permissions: '仅用作初始策略；数据库已有权限策略时，以“权限策略”页为准。',
    model: 'Core 模型元信息；实际请求使用 Worker 的模型、地址和凭据，请保持一致。',
    database: '更改路径会切换数据库，不会迁移已有数据。需保证服务账号能访问目标路径。',
    persona_path: '首次读取的人格文件。全局人格内容请在“全局人格”页编辑。',
    allowed_groups: '兼容字段；实际访问控制由统一权限策略决定。', allowed_users: '兼容字段；实际访问控制由统一权限策略决定。',
    context: '保留近期 Token < 软上限 < 硬上限；输入与输出总预算不能超过模型窗口。',
    execution: '持续任务上限应小于最大活跃执行数，给新交互保留容量。',
    social_evaluated_conversations: '每行一个会话 ID；启用 social 模式前应完成真实回放评估。'
  };
  const groups = [
    ['接入', ['adapters', 'deployment_id']], ['模型', ['model']],
    ['执行', ['tasks_enabled', 'interactive_execution_enabled', 'execution']], ['上下文', ['context']],
    ['发言', ['send_budget_per_minute', 'judge', 'social_evaluated_conversations']], ['记忆', ['memory']],
    ['系统', ['database', 'internal_socket', 'timezone', 'source_url', 'persona_path', 'admin_token_env', 'internal_token_env']], ['初始权限', ['permissions']]
  ];
  let category = 0;
  function field(raw, initial, key, root, state, path) {
    const schema = resolve(raw, root), wrap = element('div', undefined, 'field');
    const title = labels[key] || key, name = 'setting-' + (++fieldCounter);
    wrap.dataset.path = path;
    const label = element('label', title); label.htmlFor = name;
    label.append(element('small', path, 'field-key')); wrap.append(label);
    let read, control;
    if (schema.type === 'object' && schema.properties) {
      wrap.classList.add('wide'); const content = element('div', undefined, 'nested-fields'); wrap.append(content);
      const reads = [], advanced = element('details', undefined, 'advanced-fields'); advanced.append(element('summary', '兼容与高级设置'));
      for (const [childKey, child] of Object.entries(schema.properties)) {
        const item = field(child, Object.hasOwn(initial || {}, childKey) ? initial[childKey] : defaultValue(child, root), childKey, root, state, path + '.' + childKey);
        const parent = ['allowed_groups', 'allowed_users', 'admin_users', 'known_bot_ids', 'signature_max_age_seconds', 'api_base', 'token_url'].includes(childKey) ? advanced : content;
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
      read = () => control.value === '' ? [] : control.value.split('\n');
      wrap.append(element('small', '每行一项；留空表示空列表。', 'field-help'));
    } else if (schema.type === 'boolean') {
      control = element('input'); control.type = 'checkbox'; control.checked = Boolean(initial); control.setAttribute('role', 'switch'); control.className = 'switch-control';
      read = () => control.checked;
    } else if (schema.enum) {
      control = element('select');
      for (const value of schema.enum) { const option = element('option', enumLabels[key]?.[value] || String(value)); option.value = String(value); control.append(option); }
      control.value = String(initial ?? defaultValue(raw, root)); read = () => control.value;
    } else if (schema.type === 'integer' || schema.type === 'number') {
      control = element('input'); control.type = 'number'; control.step = schema.type === 'integer' ? '1' : 'any'; control.value = initial ?? ''; control.required = true;
      if (schema.minimum !== undefined) control.min = schema.minimum;
      if (schema.maximum !== undefined) control.max = schema.maximum;
      read = () => { if (!control.checkValidity() || !Number.isFinite(control.valueAsNumber)) throw new Error('请填写允许范围内的数值。'); return control.valueAsNumber; };
      wrap.append(element('small', '范围：' + (schema.minimum ?? '不限') + ' – ' + (schema.maximum ?? '不限') + (schema.type === 'integer' ? '，整数' : ''), 'field-help'));
    } else if (schema.type === 'string') {
      control = element(key === 'identity' ? 'textarea' : 'input'); control.value = initial ?? '';
      if (schema.minLength !== undefined) { control.minLength = schema.minLength; control.required = schema.minLength > 0; }
      if (schema.maxLength !== undefined) control.maxLength = schema.maxLength;
      read = () => { if (!control.checkValidity()) throw new Error('请检查必填内容和长度限制。'); return control.value; };
    } else {
      control = element('textarea'); control.rows = 4; control.value = JSON.stringify(initial ?? defaultValue(raw, root), null, 2);
      read = () => { try { return JSON.parse(control.value); } catch { throw new Error('JSON 格式不正确，请检查引号和逗号。'); } };
      wrap.append(element('small', '自由结构，使用 JSON 编辑。', 'field-help'));
    }
    if (control) { control.id = name; control.spellcheck = false; wrap.append(control); }
    if (schema.nullable && control) {
      const toggle = element('label', undefined, 'nullable'), nullBox = element('input'); nullBox.type = 'checkbox'; nullBox.checked = initial === null;
      toggle.append(nullBox, document.createTextNode('暂不设置')); wrap.append(toggle); control.disabled = nullBox.checked;
      nullBox.addEventListener('change', () => { control.disabled = nullBox.checked; });
      const getter = read; read = () => nullBox.checked ? null : getter();
    }
    const hint = helps[key] || (key.endsWith('_env') ? '填写环境变量名称；密钥在服务环境文件中管理。' : schema.description);
    if (hint) wrap.append(element('small', hint, 'field-help'));
    const error = element('p', '', 'field-error'); error.id = name + '-error'; error.hidden = true; wrap.append(error);
    control?.setAttribute('aria-describedby', error.id);
    state.fields.set(path, {node: wrap, error, control});
    return {node: wrap, read: () => { try { return read(); } catch (error) { if (!error.path) error.path = path; throw error; } }};
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
          keys.forEach(key => add(group, key)); form.append(group); state.groupNodes.push(group);
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
  const configEditor = editor('config', 'configuration'), personaEditor = editor('global-persona', 'global-persona'), permissionEditor = editor('permissions', 'permissions');
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
    const bindings = [['save-config', configEditor], ['apply-config', configEditor], ['save-global-persona', personaEditor], ['save-permissions', permissionEditor]];
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
        card.append(element('h3', adapter.id), element('span', adapter.kind === 'onebot' ? 'OneBot' : 'QQ 官方', 'badge'));
        card.append(element('p', adapter.online === true ? '在线' : adapter.online === false ? '离线' : '状态待确认', 'adapter-status'));
        card.append(element('p', adapter.credentials_configured ? '接入凭据已配置' : '接入凭据未配置', 'hint'));
        if (adapter.context_gap) card.append(element('p', '存在上下文缺口，请检查接入连接。', 'error'));
        $('overview-adapters').append(card);
      }
    } catch (error) { empty($('overview-metrics'), '暂时无法读取运行状态。', overview); throw error; }
  }
  function selectConversation(id) { $('cid').value = id; const row = conversations.find(item => item.id === id); $('policy-version').value = row?.policy?.policy_version ?? ''; if (row?.policy?.mode) $('mode').value = row.policy.mode; }
  $('cid').addEventListener('change', () => selectConversation($('cid').value.trim()));
  async function loadConversations() {
    conversations = await request('/api/v1/conversations');
    $('conversation-options').replaceChildren(); $('conversation-rows').replaceChildren();
    for (const item of conversations) {
      const option = document.createElement('option'); option.value = item.id; $('conversation-options').append(option);
      const row = document.createElement('tr'); cell(row, item.id); cell(row, item.policy?.mode); cell(row, item.policy?.policy_version); button(cell(row, ''), '选择', () => selectConversation(item.id)); $('conversation-rows').append(row);
    }
    if (!$('cid').value && conversations.length) selectConversation(conversations[0].id); else selectConversation($('cid').value);
    if (!conversations.length) { const row = element('tr'); const td = cell(row, '暂无会话。接入 QQ 后，收到的会话会出现在这里。'); td.colSpan = 4; $('conversation-rows').append(row); }
    notice('已读取 ' + conversations.length + ' 个会话');
  }
  action('refresh-overview', overview); action('refresh-conversations', loadConversations);
  async function patchPolicy(change) {
    const expected = Number(required('policy-version', '预期策略版本')); if (!Number.isInteger(expected) || expected < 1) throw new Error('预期策略版本应为正整数');
    const id = cid(); const result = await request('/api/v1/conversations/' + encode(id) + '/policy', {method: 'PATCH', body: {expected_version: expected, ...change}});
    await loadConversations(); selectConversation(id); show('decision-output', result); notice('会话设置已保存');
  }
  action('save-mode', () => patchPolicy({mode: $('mode').value}));
  action('save-persona', () => patchPolicy({persona: JSON.parse($('persona').value)}));
  action('load-decisions', async () => { show('decision-output', await request('/api/v1/conversations/' + encode(cid()) + '/decisions')); $('decision-output').closest('details')?.setAttribute('open', ''); notice('已读取最近判别结果。'); });
  async function loadTasks() {
    const rows = await request('/api/v1/tasks?conversation_id=' + encode(cid())); $('task-rows').replaceChildren();
    for (const item of rows) { const row = document.createElement('tr'); cell(row, item.public_id); cell(row, item.state); cell(row, item.created_at); button(cell(row, ''), '查看', async () => { selectTask(item.public_id); await loadTask(); }); $('task-rows').append(row); }
    if (!rows.length) { const row = element('tr'); const td = cell(row, '此会话暂无后台任务。'); td.colSpan = 4; $('task-rows').append(row); }
    notice('已读取 ' + rows.length + ' 个任务');
  }
  function selectTask(id) { if ($('task-id').value !== id) { stopEvents(); eventCursor = '0'; streamTask = ''; show('event-output', '未连接'); } $('task-id').value = id; }
  $('task-id').addEventListener('change', () => { stopEvents(); eventCursor = '0'; streamTask = ''; show('event-output', '未连接'); });
  async function loadTask() { show('task-output', await request('/api/v1/tasks/' + encode(taskID()))); $('task-output').closest('details')?.setAttribute('open', ''); }
  action('load-tasks', loadTasks); action('load-task', loadTask);
  action('cancel-task', async () => { if (!await confirmDialog('取消此任务？', '取消请求发送后，执行器会停止当前任务。', [], '取消任务')) return; show('task-output', await request('/api/v1/tasks/' + encode(taskID()) + '/cancel', {method: 'POST'})); notice('取消请求已处理'); });
  $('retry-key').value = uuid(); $('create-key').value = uuid();
  action('new-retry-key', () => { $('retry-key').value = uuid(); }); action('new-create-key', () => { $('create-key').value = uuid(); });
  action('retry-task', async () => {
    const result = await request('/api/v1/tasks/' + encode(taskID()) + '/retry', {method: 'POST', headers: {'Idempotency-Key': required('retry-key', '重试幂等键')}});
    selectTask(result.public_id); show('task-output', result); notice('重试请求已处理');
  });
  action('create-task', async () => {
    const result = await request('/api/v1/tasks', {method: 'POST', headers: {'Idempotency-Key': required('create-key', '新任务幂等键')}, body: {conversation_id: cid(), goal: required('task-goal', '任务目标')}});
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
  let memoryRows = [];
  async function loadMemories(jargon = false) {
    let path = '/api/v1/' + (jargon ? 'jargon' : 'memories') + '?conversation_id=' + encode(cid());
    if (!jargon && $('memory-status').value) path += '&status=' + encode($('memory-status').value);
    memoryRows = await request(path); $('memory-rows').replaceChildren();
    for (const record of memoryRows) {
      const row = document.createElement('tr'); cell(row, record.memory_id + '\n' + record.scope_type); cell(row, record.status); cell(row, record.text);
      const controls = cell(row, ''); button(controls, '选择', () => { $('memory-id').value = record.memory_id; $('memory-text').value = record.text; show('memory-output', record); });
      if (record.predicate.startsWith('jargon:') && record.status === 'candidate') {
        button(controls, '审核通过', async () => { show('memory-output', await request('/api/v1/jargon/' + encode(record.memory_id) + '?publish=true', {method: 'PATCH'})); await loadMemories(true); });
        button(controls, '拒绝', async () => { show('memory-output', await request('/api/v1/jargon/' + encode(record.memory_id) + '?publish=false', {method: 'PATCH'})); await loadMemories(true); });
      }
      $('memory-rows').append(row);
    }
    if (!memoryRows.length) { const row = element('tr'); const td = cell(row, '没有符合条件的记录。'); td.colSpan = 4; $('memory-rows').append(row); }
    notice('已读取 ' + memoryRows.length + ' 条记录');
  }
  action('load-memories', () => loadMemories()); action('load-jargon', () => loadMemories(true));
  action('correct-memory', async () => {
    const result = await request('/api/v1/memories/' + encode(required('memory-id', '记忆 ID')), {method: 'PATCH', body: {source_event_id: required('source-event', '新的来源事件 ID'), text: required('memory-text', '纠正内容')}});
    show('memory-output', result); $('memory-output').closest('details')?.setAttribute('open', ''); $('memory-id').value = result.memory_id; notice('记忆已纠正');
  });
  action('delete-memory', async () => { if (!await confirmDialog('删除这条记忆？', '此操作会撤销这条已保存的记忆。', [], '删除记忆')) return; const result = await request('/api/v1/memories/' + encode(required('memory-id', '记忆 ID')), {method: 'DELETE'}); show('memory-output', result); $('memory-text').value = ''; await loadMemories(); notice('记忆已删除'); });
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
  const loadPersona = () => loadEditor(personaEditor, async () => {
    const result = await request('/api/v1/persona');
    return () => { personaVersion = result.version; personaEditor.load(result.persona, result.schema); show('global-persona-output', {version: result.version}); };
  });
  const loadPermissions = () => loadEditor(permissionEditor, async () => {
    const [result, schema] = await Promise.all([request('/api/v1/permissions'), request('/api/v1/permissions/schema')]);
    return () => { const {policy_version, ...policy} = result; permissionVersion = policy_version; permissionEditor.load(policy, schema); show('permissions-output', {policy_version}); };
  });
  action('load-config', () => loadConfig()); action('load-global-persona', loadPersona); action('load-permissions', loadPermissions);
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
  action('save-global-persona', () => mutate(async () => {
    const persona = personaEditor.read(), diff = changes(personaEditor.baseline, persona);
    if (!diff.length || !await confirmDialog('保存全局人格？', '更改会立即用于后续回复，会话人格覆盖仍然保留。', diff, '保存并生效')) return;
    const result = await request('/api/v1/persona', {method: 'PUT', body: {expected_version: personaVersion, persona}});
    personaVersion = result.version; personaEditor.load(result.persona); show('global-persona-output', result); notice('全局人格已保存并生效。');
  }));
  action('save-permissions', () => mutate(async () => {
    const policy = permissionEditor.read(), diff = changes(permissionEditor.baseline, policy);
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
    const result = await request('/api/v1/permissions/explain', {method: 'POST', body: {subject: JSON.parse($('permission-subject').value), action: required('permission-action', '操作'), resource: required('permission-resource', '资源')}});
    show('permissions-output', result); $('permissions-output').closest('details')?.setAttribute('open', ''); notice('当前策略检查结果：' + (result.effect === 'allow' ? '允许此操作' : '拒绝此操作') + '。');
  });
  $('config-search').addEventListener('input', filterConfig);
  const panels = {
    configuration: ['全部配置', '连接与行为设置，统一管理。', () => configEditor.getter || loadConfig()],
    overview: ['运行总览', '了解服务状态与 QQ 接入情况。', overview],
    conversations: ['会话管理', '管理会话的发言模式与人格覆盖。', loadConversations],
    tasks: ['后台任务', '查询任务进展，处理执行与交付。', async () => { if (!conversations.length) await loadConversations(); if ($('cid').value) await loadTasks(); }],
    memories: ['记忆与词典', '审阅记忆、纠正内容与管理词典。', async () => { if (!conversations.length) await loadConversations(); if ($('cid').value) await loadMemories(); }],
    'global-persona': ['全局人格', '设定小象的身份、表达与行为边界。', () => personaEditor.getter || loadPersona()],
    permissions: ['权限策略', '为主体、操作与资源配置访问范围。', () => permissionEditor.getter || loadPermissions()]
  };
  async function navigate(panel, updateHash = true) {
    if (!panels[panel]) panel = 'configuration'; activePanel = panel;
    for (const control of document.querySelectorAll('[data-panel]')) {
      const active = control.dataset.panel === panel; control.setAttribute('aria-current', active ? 'page' : 'false'); control.setAttribute('aria-pressed', String(active)); $(control.dataset.panel).hidden = !active;
    }
    show('page-title', panels[panel][0]); show('page-subtitle', panels[panel][1]);
    $('conversation-picker').hidden = !['conversations', 'tasks', 'memories'].includes(panel);
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
    stopEvents(); authEpoch++; token = ''; conversations = []; configVersion = savedConfigVersion = personaVersion = permissionVersion = null;
    editors.forEach(state => state.clear()); configView = null; $('token').value = ''; $('token').type = 'password';
    document.querySelectorAll('pre').forEach(item => { item.textContent = ''; });
    for (const id of ['conversation-rows', 'task-rows', 'memory-rows', 'conversation-options', 'versions', 'credentials', 'overview-metrics', 'overview-adapters']) $(id).replaceChildren();
    for (const id of ['cid', 'policy-version', 'task-id', 'task-goal', 'memory-id', 'source-event', 'memory-text']) $(id).value = '';
    $('persona').value = '{}'; show('auth-error', '');
    $('login-toggle-token').setAttribute('aria-pressed', 'false');
    $('login-toggle-token').setAttribute('aria-label', '显示令牌'); show('login-toggle-token', '显示');
    loginVisible(true); $('token').focus();
  });
  window.addEventListener('beforeunload', event => { if (editors.some(state => state.dirty)) { event.preventDefault(); event.returnValue = ''; } });
  window.addEventListener('pagehide', () => { stopEvents(); authEpoch++; token = ''; });
  updateSaveButtons(); loginVisible(true); navigate(location.hash.slice(1) || 'configuration', false);
})();
