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
    'model.request': '调用模型', 'conversation.policy.set': '调整发言模式', 'conversation.persona.set': '调整会话人格',
    'task.create': '创建任务', 'task.read': '查看任务', 'task.cancel': '取消任务', 'task.*': '全部任务操作',
    'memory.read': '读取记忆', 'memory.*': '全部记忆与词典操作', 'history.*': '读取历史与来源',
    'expression.read': '读取表达素材', 'expression.learn': '学习表达', 'expression.manage': '管理表达素材', 'expression.*': '全部表达素材操作'
  };
  function updateAction(actions, action, enabled) {
    if (!enabled) return actions.filter(value => value !== action);
    const kept = action === '*' ? [...actions] : actions.filter(value => value !== '*');
    return kept.includes(action) ? kept : [...kept, action];
  }
  function subjectNamespace(subject, adapters, automaticValue = null) {
    if ((subject.identity_namespace && subject.identity_namespace !== automaticValue) || !subjectFields[subject.kind]?.includes('identity_namespace')) return subject.identity_namespace;
    const adapter = adapters.find(item => item.instance_id === subject.adapter_instance_id);
    if (adapter?.kind === 'onebot') return 'qq';
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
  const conversationModes = {
    addressed_only: ['仅被提及时回复', '私聊、被提及或收到有效引用时回复，其余消息不主动接话。'],
    silent: ['静默', '暂停会话自动回复。需要恢复时，可切换到其他模式。'],
    shadow: ['观察模式', '记录发言判断，但不实际发送回复，适合检查参与策略。'],
    social: ['主动参与', '结合上下文判断是否接话，仍受权限、预算和发言阈值限制。']
  };
  function filterConversations(items, query) {
    const term = query.trim().toLocaleLowerCase();
    return items.filter(item => [item.display_label, item.id, item.remote_id].some(value => String(value ?? '').toLocaleLowerCase().includes(term)));
  }
  function decisionJudgeStatus(entry) {
    const status = entry.context?.judge_status || (entry.decision.signals ? (entry.decision.signals.abstain ? 'abstained' : 'success') : 'not_called');
    return {not_called: '未调用模型', rule_only: '规则判别器弃权（未调用模型）', success: '模型正常返回', abstained: '模型弃权'}[status] || status;
  }
  function decisionScoreRows(decision, thresholds = {}) {
    thresholds = {proactive_allowed: 0.5, ...thresholds};
    const signals = decision.signals?.abstain ? null : decision.signals;
    const specs = [
      ['directed_to_bot', '面向机器人', '仅记录'],
      ['continues_bot_topic', '延续机器人话题', '≥'],
      ['invites_participation', '邀请或适合参与', '≥'],
      ['useful_contribution', '有用贡献', '≥'],
      ['private_exchange', '他人私人交流', '≤'],
      ['proactive_allowed', '主动参与偏好允许', '≥']
    ];
    return specs.map(([key, label, comparison]) => ({
      label, score: signals?.[key] ?? '—',
      threshold: comparison === '仅记录' ? comparison : thresholds[key] == null ? '历史未记录' : comparison + ' ' + thresholds[key]
    }));
  }
  return {conversationModes, filterConversations, equal, changes, resolve, defaultValue, errorPath, assertShape, subjectFields, scopeLabels, actionLabels, updateAction, subjectNamespace, validatePolicy, valueType, emptyValue, ruleTarget, newPermissionRule, permissionExplanation, decisionJudgeStatus, decisionScoreRows};
})();
if (typeof module !== 'undefined') module.exports = ConsoleModel;
if (typeof document !== 'undefined') (() => {
  const $ = id => document.getElementById(id), encode = encodeURIComponent;
  const {conversationModes, filterConversations, equal, changes, resolve, defaultValue, errorPath, assertShape, subjectFields, scopeLabels, actionLabels, updateAction, subjectNamespace, validatePolicy, valueType, emptyValue, ruleTarget, newPermissionRule, permissionExplanation, decisionJudgeStatus, decisionScoreRows} = ConsoleModel;
  const editors = [], replaceButtons = ['load-config', 'load-global-persona', 'load-permissions', 'load-versions', 'load-version', 'config-use-json', 'global-persona-use-json', 'permissions-use-json'];
  let token = '', authEpoch = 0, saving = false, dialogPending = false, activePanel = 'configuration', noticeTimer;
  let conversations = [], configVersion = null, savedConfigVersion = null, personaVersion = null, permissionVersion = null;
  let configView = null, eventController = null, streamTask = '', eventCursor = '0', fieldCounter = 0;
  let decisionConversation = '', decisionBefore = null, decisionEpoch = 0;
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
    const editorAtStart = control.id === 'save-default-policy' ? configEditor : editors.find(item => item.panel === activePanel);
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
    deployment_id: '部署标识', database: '数据库路径', adapters: 'QQ 接入', admin_token_env: '管理员令牌变量', internal_token_env: '内部令牌变量', model: '模型', source_url: '源码地址', send_budget_per_minute: '每分钟发送预算', timezone: '时区', tasks_enabled: '启用后台任务', interactive_execution_enabled: '启用交互执行', permissions: '初始权限策略', execution: '执行与资源预算', context: '上下文', internal_socket: '内部通信 Socket', persona_path: '人格文件路径', judge: '发言判断', memory: '记忆', social_evaluated_conversations: '回放评估记录（兼容旧配置）',
    instance_id: '接入实例标识', kind: '类型', bot_id: '机器人账号标识', secret_env: '接入密钥变量', app_id: '应用 ID', websocket_url: 'WebSocket 地址', allowed_groups: '兼容群列表', allowed_users: '兼容用户列表', admin_users: '管理员角色列表', known_bot_ids: '已知机器人账号', full_mode_enabled: '启用全量消息权限', full_mode_verified: '已验证全量消息', proactive_verified: '已验证主动消息', reply_window_seconds: '回复窗口（秒）', max_replies: '最多回复数', signature_max_age_seconds: '签名最大时效（秒）', api_base: 'API 地址', token_url: '令牌地址',
    endpoint: '请求地址', api_key_env: 'API 密钥变量', timeout_seconds: '超时（秒）', context_window: '模型上下文窗口', max_active: '最大活跃执行数', sustained_limit: '持续任务上限', foreground_wait_ms: '前台等待（毫秒）', notify_before_tools: '操作前告知用户', interactive_budget: '交互任务预算', budget: '后台任务预算', warm_max: '常驻进程上限', warm_idle_max: '空闲进程上限', warm_ttl_seconds: '空闲保留（秒）',
    max_request_bytes: '最大请求字节数', max_tokens: '上下文软上限', keep_recent_tokens: '近期消息保留 Token', compaction_output_tokens: '压缩输出预算', recent_messages: '近期消息条数', topic_idle_seconds: '话题空闲超时（秒）', version: '版本',
    maintenance_enabled: '启用记忆维护', auto_extract: '自动提取记忆', extractor_endpoint: '提取服务地址', extractor_model: '提取模型', extractor_key_env: '提取密钥变量', embedding_endpoint: '向量服务地址', embedding_model: '向量模型', embedding_version: '向量版本', embedding_dimensions: '向量维度', embedding_key_env: '向量密钥变量', raw_retention_days: '原始记录保留（天）',
    preset: '预设', default_effect: '默认行为', rules: '权限规则', id: '规则 ID', enabled: '启用', priority: '优先级', subject: '主体范围', actions: '操作匹配', resources: '资源匹配', effect: '行为', adapter_instance_id: '接入实例 ID', bot_identity_id: '机器人身份 ID', identity_namespace: '身份命名空间', group_id: '群 ID', user_id: '用户 ID',
    persona_id: '人格标识', name: '名称', identity: '人格与身份', traits: '性格特征', style: '表达风格', boundaries: '行为边界', model_calls: '模型调用次数', tool_calls: '工具调用次数', active_seconds: '活跃执行（秒）', total_wall_seconds: '总时长（秒）', output_tokens_per_call: '每次输出 Token', input_tokens: '输入 Token 预算', output_tokens: '输出 Token 预算', completion_reserve: '完成阶段预留模型调用',
    behavior_style: '参与聊天的方式', reply_style: '说话习惯', group_chat_prompt: '群聊提示', private_chat_prompts: '私聊提示', multiple_reply_style: '备用语气', multiple_probability: '备用语气概率',
    expression: '表达素材', expression_checked_only: '仅使用人工精选', expression_self_reflect: '学习后自动审核', learning_list: '学习与使用范围', expression_groups: '表达共享组', learning_blacklist: '学习黑名单',
    learner_endpoint: '表达学习服务地址', learner_model: '表达学习模型', learner_key_env: '表达学习密钥变量', min_messages: '每批最少消息', max_messages: '每批最多消息', learn_interval_seconds: '学习间隔（秒）', max_expression_learner: '最多同时学习数', timeout_seconds: '请求超时（秒）', use: '使用表达', learn: '学习表达', conversation_id: '会话 ID', type: '会话类型'
  };
  const enumLabels = {
    kind: {onebot: 'OneBot / NapCat', official: 'QQ 官方机器人', rule: '规则判断', jev: 'Jev', structured: '通用大模型（OpenAI 兼容）', global: '全局', adapter: '接入实例', bot: '机器人', group: '群组', person: '个人', group_member: '群内成员'},
    preset: {allow_all: '默认开放', custom: '自定义'},
    effect: {allow: '允许', deny: '拒绝'}, default_effect: {allow: '允许', deny: '拒绝'}
  };
  Object.assign(labels, {
    thresholds: '发言评分阈值', invites_participation: '邀请参与最低分', continues_bot_topic: '延续话题最低分',
    useful_contribution: '有用贡献最低分', private_exchange: '私人交流最高分', wait_useful_contribution: '等待重评估最低分'
  });
  const helps = {
    thresholds: '分数范围为 0 到 1。邀请参与或延续话题达标，并且有用贡献、私人交流均达标时，才会主动发言。保存后重启生效。',
    invites_participation: '与延续话题满足任意一项即可。',
    continues_bot_topic: '与邀请参与满足任意一项即可。',
    useful_contribution: '主动发言所需的最低有用贡献评分。',
    private_exchange: '超过此分数时不主动发言。',
    wait_useful_contribution: '未满足发言条件时，达到此分数会先等待再评估。',
    judge: '可选择 Jev 或通用大模型。通用大模型使用 OpenAI 兼容的 Chat Completions 接口，填写完整请求地址、模型名称和密钥环境变量；可与聊天模型共用。保存后重启生效。',
    notify_before_tools: '默认开启。需要调用工具时先简短告知用户，再开始操作；直接能回答的聊天照常回复。保存后重启生效。',
    adapters: '添加 OneBot 或 QQ 官方接入。账号、群号与用户 ID 均使用字符串。',
    permissions: '仅用作初始策略；数据库已有权限策略时，以“权限策略”页为准。',
    model: 'Core 模型元信息；实际请求使用 Worker 的模型、地址和凭据，请保持一致。',
    database: '更改路径会切换数据库，不会迁移已有数据。需保证服务账号能访问目标路径。',
    persona_path: '首次读取的人格文件。全局人格内容请在“全局人格”页编辑。',
    allowed_groups: '兼容字段；实际访问控制由统一权限策略决定。', allowed_users: '兼容字段；实际访问控制由统一权限策略决定。',
    context: '保留近期 Token < 软上限 < 硬上限；输入与输出总预算不能超过模型窗口。',
    execution: '持续任务上限应小于最大活跃执行数，给新交互保留容量。',
    expression: '采用 MaiBot 快速表达选择。积累至少 10 条可用素材后开始抽取；群聊默认学习和使用，私聊需添加规则。',
    learner_endpoint: '留空使用主模型地址；使用不同地址时请配置独立的表达学习密钥。',
    learner_model: '留空使用主模型。学习在后台执行，不增加每次回复的模型调用。',
    multiple_probability: '0 表示始终使用基础语气；触发时仅影响本轮。',
    expression_groups: '仅在显式加入的会话间共享表达；私聊来源不会流入群聊。',
    social_evaluated_conversations: '每行一个会话 ID；仅保留旧评估记录，不再作为主动参与的开启条件。'
  };
  const groups = [
    ['接入', ['adapters', 'deployment_id']], ['模型', ['model']],
    ['执行', ['tasks_enabled', 'interactive_execution_enabled', 'execution']], ['上下文', ['context']],
    ['主动搭话', ['speak_up']], ['发言', ['send_budget_per_minute', 'judge', 'social_evaluated_conversations']], ['记忆与表达', ['memory', 'expression']],
    ['系统', ['database', 'internal_socket', 'timezone', 'source_url', 'persona_path', 'admin_token_env', 'internal_token_env']], ['初始权限', ['permissions']]
  ];
  Object.assign(labels, {speak_up: '私聊主动搭话', idle_minutes: '空闲多久后搭话（分钟）', min_interval_minutes: '最短搭话间隔（分钟）', max_interval_minutes: '最长搭话间隔（分钟）', max_per_day: '每人每日搭话次数', awake_from: '清醒开始时间（小时）', awake_to: '清醒结束时间（小时）', nudge: '未回复时轻轻追问', allow: '额外搭话会话'});
  Object.assign(helps, {speak_up: '默认主动找各 QQ 接入中管理员的已知私聊；额外对象填写会话管理里的完整会话 ID。只在有主动发送能力、权限允许且未静默时搭话。保存后重启生效。', idle_minutes: '从双方最近一次私聊开始计算；空闲时间和随机间隔都满足才触发。', min_interval_minutes: '每轮在最短与最长间隔之间随机安排下一次。', max_interval_minutes: '不能小于最短间隔。', max_per_day: '每人单独计数；生成沉默或失败也计一次，避免频繁重试。', awake_from: '使用系统设置的时区，包含开始小时。', awake_to: '不包含结束小时；24 表示到午夜。', nudge: '最后一句是问句、对方 15～40 分钟没回时，有 50% 概率轻问一次；每句只判断一次，不占每日次数。', allow: '每行一个完整私聊会话 ID；仍需该会话的接收、模型和发送权限。'});
  let category = 0;
  function field(raw, initial, key, root, state, path) {
    const schema = resolve(raw, root), wrap = element('div', undefined, 'field');
    const title = raw.uiLabel || (path === 'judge.participation' ? '主动参与条件与预算' : labels[key] || key), name = 'setting-' + (++fieldCounter);
    wrap.dataset.path = path;
    const label = element('label', title);
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
      control = element(['identity', 'behavior_style', 'reply_style', 'group_chat_prompt', 'private_chat_prompts'].includes(key) || raw.uiMultiline ? 'textarea' : 'input'); control.value = initial ?? '';
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
  Object.assign(labels, {default_language: '默认语言', tone: '交流语气', wording: '措辞偏好', formality: '正式程度', reply_length: '回复长度', participation: '参与方式'});
  Object.assign(labels, {cooldown_seconds: '发言冷却时间（秒）', min_human_messages: '新增人类消息最低条数', require_member_response: '主动接话后等待成员回应', max_recent_bot_ratio: '机器人消息占比上限', max_rechecks: '最多重评估次数', activity_window_seconds: '活动统计时间窗（秒）', activity_max_events: '占比统计最多活动条数', participation_budget_ratio: '主动参与预算比例'});
  Object.assign(helps, {
    cooldown_seconds: '距上次成功发送消息的最短间隔；0 表示不限制间隔。',
    min_human_messages: '上次聊天回复之后，统计时间窗内至少新增的人类消息数。',
    require_member_response: '开启后，上次主动接话若未获得成员 @ 或有效引用回应，将暂停继续主动接话。',
    max_recent_bot_ratio: '近期机器人消息占比必须低于此值，0.2 表示 20%。',
    max_rechecks: '等待话题发展后最多重新判断的次数，0 表示不重评估。',
    activity_window_seconds: '人类消息、机器人消息占比和参与预算共用的回看时间窗，默认 60 秒。',
    activity_max_events: '在时间窗内，只取最近这些条活动计算机器人占比，默认 20 条。',
    participation_budget_ratio: '以时间窗内的人类消息数乘此比例作为额度；受邀消息、已发送回复和待发送预占均占用额度。0 表示不允许主动接话。'
  });
  const specialTypes = {string: '文本', number: '数字', boolean: '开关'};
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
    for (const adapter of configView?.config?.adapters || []) { const option = element('option', adapter.kind === 'onebot' ? 'OneBot / NapCat' : 'QQ 官方机器人'); option.value = adapter.instance_id; options.append(option); }
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
          uiHelp: key === 'identity_namespace' ? '已配置的 OneBot 自动使用 qq；官方机器人在已知应用 ID 时自动识别。其他来源请填写真实身份命名空间。' : requiredFields.includes(key) ? '此对象类型需要填写。' : '未设置时不增加限制；已填写的额外条件仍参与匹配。'
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
  function schemaForStyle(value) {
    const type = valueType(value);
    if (type === 'object') return {type, properties: Object.fromEntries(Object.entries(value).map(([key, item]) => [key, schemaForStyle(item)]))};
    if (type === 'array') return {type, items: value.every(item => typeof item === 'string') ? {type: 'string'} : {}};
    return {type};
  }
  function styleForm(value, state, root) {
    const node = element('div', undefined, 'style-fields'), fields = element('div', undefined, 'nested-fields'); node.append(fields); let rows = [];
    function render(values) {
      for (const key of [...state.fields.keys()]) if (key.startsWith('style.')) state.fields.delete(key);
      fields.replaceChildren(); rows = [];
      for (const [key, original] of Object.entries(values)) {
        const row = element('div', undefined, 'style-entry'), schema = schemaForStyle(original), complex = ['object', 'array', 'null'].includes(valueType(original)) && !(Array.isArray(original) && original.every(item => typeof item === 'string'));
        let parent = row;
        if (complex) { parent = element('details', undefined, 'style-complex'); parent.append(element('summary', (labels[key] || key) + ' · 结构化内容')); row.append(parent); }
        const item = appendField(parent, schema, original, key, root, state, 'style.' + key, {uiMultiline: ['tone', 'wording'].includes(key), uiHelp: key === 'default_language' ? '例如 zh-CN（简体中文）、en（英语）。' : ''});
        const tools = element('div', undefined, 'style-entry-tools'); row.append(tools);
        button(tools, '移除', () => { render(Object.fromEntries(rows.filter(entry => entry.key !== key).map(entry => [entry.key, entry.read()]))); state.sync(); }, 'quiet');
        fields.append(row); rows.push({key, read: item.read});
      }
    }
    render(value);
    const add = element('details', undefined, 'style-add'); add.append(element('summary', '添加表达偏好')); node.append(add);
    const name = element('input'); name.placeholder = '偏好名称，例如 humor'; name.setAttribute('aria-label', '新的表达偏好名称');
    const type = element('select'); type.setAttribute('aria-label', '表达偏好内容类型');
    for (const [key, label] of Object.entries(specialTypes)) { const option = element('option', label); option.value = key; type.append(option); }
    add.append(name, type);
    button(add, '添加', () => {
      const key = name.value.trim(); if (!key) throw new Error('请填写表达偏好名称。');
      if (rows.some(item => item.key === key)) throw new Error('这个表达偏好已经存在。');
      render({...Object.fromEntries(rows.map(item => [item.key, item.read()])), [key]: emptyValue(type.value)}); name.value = ''; state.sync();
    });
    containerField(state, 'style', node, '表达方式'); return {node, read: () => Object.fromEntries(rows.map(item => [item.key, item.read()]))};
  }
  function personaForm(value, schema, state, form) {
    form.classList.add('persona-editor'); const reads = [];
    function section(title, keys) {
      const section = element('div', undefined, 'persona-section'), fields = element('div', undefined, 'nested-fields'); section.append(element('h3', title), fields); form.append(section);
      for (const key of keys) { const item = appendField(fields, schema.properties[key], value[key], key, schema, state, key); reads.push([key, item.read]); }
      return section;
    }
    section('基本信息', ['name', 'identity']);
    section('聊天方式', ['behavior_style', 'reply_style', 'group_chat_prompt', 'private_chat_prompts', 'multiple_reply_style', 'multiple_probability'].filter(key => schema.properties[key]));
    section('性格与边界', ['traits', 'boundaries']);
    const expression = section('表达方式', []), style = styleForm(value.style || {}, state, schema); expression.append(style.node); reads.push(['style', style.read]);
    const advanced = element('details', undefined, 'persona-advanced'); advanced.append(element('summary', '人格标识与版本')); const content = element('div', undefined, 'nested-fields'); advanced.append(content); form.append(advanced);
    for (const key of Object.keys(schema.properties).filter(key => !reads.some(([name]) => name === key))) {
      const item = appendField(content, schema.properties[key], value[key], key, schema, state, key); reads.push([key, item.read]);
    }
    return () => ({...value, ...Object.fromEntries(reads.map(([key, getter]) => [key, getter()]))});
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
      if (prefix === 'config') show('default-policy-state', state.dirty ? '默认设置与配置中心有未保存草稿；保存前将统一预览。' : configView?.restart_required ? '默认设置已保存，等待重启后生效。群内显示的默认值仍为当前运行值。' : '默认设置已与保存版本同步。');
      updateSaveButtons();
    };
    state.load = (value, schema = state.schema, baseline = value) => {
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('配置必须是 JSON 对象。');
      if (!schema) throw new Error('请先读取配置。');
      assertShape(value, schema);
      state.schema = schema; state.baseline = structuredClone(baseline); state.jsonDirty = false; form.inert = false; form.replaceChildren(); state.fields.clear(); state.groupNodes = [];
      if (prefix === 'permissions' || prefix === 'global-persona') {
        state.getter = prefix === 'permissions' ? permissionForm(value, schema, state, form) : personaForm(value, schema, state, form);
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
      if (prefix === 'config') {
        $('default-policy-fields').replaceChildren();
        for (const path of ['judge.thresholds', 'judge.participation']) {
          const node = state.fields.get(path)?.node;
          if (!node) continue;
          const link = element('div', undefined, 'field wide');
          button(link, (path.endsWith('thresholds') ? '发言评分阈值' : '主动参与条件') + ' → 会话管理', () => { navigate('conversations'); $('default-policy-settings').open = true; });
          node.replaceWith(link); $('default-policy-fields').append(node);
        }
      }
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
    $('save-default-policy').disabled = saving || !configEditor.dirty || !configEditor.getter;
    $('default-policy-fields').inert = saving || configEditor.jsonDirty;
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
  let selectedConversation = '', conversationSaving = false;
  const conversationPolicyEditors = new Map();
  function renderConversationPolicy(row, onlyKey) {
    if (!onlyKey) { conversationPolicyEditors.clear(); $('conversation-policy-form').replaceChildren(); }
    const group = row && JSON.parse(row.ref_json || '{}').surface === 'group';
    $('conversation-policy-card').hidden = !group;
    if (!group) return;
    for (const [key, configKey, title] of [['speech_thresholds', 'thresholds', '发言评分阈值'], ['participation', 'participation', '主动参与条件与预算']]) {
      if (onlyKey && key !== onlyKey) continue;
      const defaults = row.policy?.['default_' + key];
      if (!defaults) continue;
      const baseline = row.policy[key] ?? null;
      const section = element('div', undefined, 'conversation-policy-section'); section.append(element('h4', title));
      const sourceLabel = element('label', '设置来源'), source = element('select');
      source.setAttribute('aria-label', title + '设置来源');
      for (const [value, text] of [['default', '使用默认设置'], ['custom', '为这个群单独设置']]) { const option = element('option', text); option.value = value; source.append(option); }
      source.value = baseline === null ? 'default' : 'custom'; sourceLabel.append(source); section.append(sourceLabel);
      const fields = element('div', undefined, 'nested-fields'), controls = new Map();
      const judgeSchema = resolve(configEditor.schema?.properties?.judge || {}, configEditor.schema || {});
      const schema = resolve(judgeSchema.properties?.[configKey] || {}, configEditor.schema || {});
      for (const [name, value] of Object.entries(defaults)) {
        const label = element('label', labels[name] || name), input = element('input'); input.dataset.policyKey = name;
        if (typeof value === 'boolean') { input.type = 'checkbox'; input.checked = (baseline || defaults)[name]; }
        else { const spec = schema.properties?.[name] || {}; input.type = 'number'; input.required = true; input.step = spec.type === 'integer' ? '1' : 'any'; input.min = spec.minimum ?? 0; if (spec.maximum != null) input.max = spec.maximum; input.value = (baseline || defaults)[name]; }
        label.append(input); if (helps[name]) label.append(element('small', helps[name], 'hint'));
        fields.append(label); controls.set(name, input);
      }
      const state = element('p', '', 'hint'); state.setAttribute('role', 'status');
      const read = (validate = true) => source.value === 'default' ? null : Object.fromEntries([...controls].map(([name, input]) => {
        if (!input.checkValidity()) { if (validate) input.reportValidity(); throw new Error('请修正「' + (labels[name] || name) + '」的数值。'); }
        return [name, input.type === 'checkbox' ? input.checked : input.valueAsNumber];
      }));
      const dirty = () => { try { return !equal(read(false), baseline); } catch { return true; } };
      const save = button(section, '保存' + title, () => patchPolicy({[key]: read()}), 'primary');
      const update = () => {
        for (const [name, input] of controls) {
          input.disabled = source.value === 'default';
          if (source.value === 'default') { if (input.type === 'checkbox') input.checked = defaults[name]; else input.value = defaults[name]; }
        }
        save.disabled = !dirty();
        state.textContent = dirty() ? '有未保存修改' : source.value === 'default' ? '正在使用默认设置，默认值更新后自动跟随。' : '这个群使用独立设置，不随默认值变化。';
      };
      section.insertBefore(fields, save); section.insertBefore(state, save);
      source.addEventListener('change', update); fields.addEventListener('input', update); update();
      const previous = conversationPolicyEditors.get(key);
      if (previous) previous.node.replaceWith(section); else $('conversation-policy-form').append(section);
      conversationPolicyEditors.set(key, {dirty, read, node: section});
    }
  }

  function policyDraftSnapshot() { return [...$('conversation-policy-form').querySelectorAll('input, select')].map(input => input.type === 'checkbox' ? input.checked : input.value); }
  function conversationDirty() {
    const row = conversations.find(item => item.id === selectedConversation);
    return Boolean(row && ($('mode').value !== row.policy?.mode || $('persona').value.trim() !== '{}' || [...conversationPolicyEditors.values()].some(item => item.dirty())));
  }
  async function canReplaceConversation() {
    return !conversationSaving && (!conversationDirty() || await confirmDialog('放弃当前会话的草稿？', '未保存的发言模式、群级设置和人格覆盖将被清除。', [], '放弃并继续'));
  }
  function renderConversationList() {
    const focusedID = document.activeElement?.dataset.conversationId;
    const items = filterConversations(conversations, $('conversation-search').value);
    $('conversation-rows').replaceChildren();
    show('conversation-count', '显示 ' + items.length + ' / ' + conversations.length + ' 个会话');
    for (const item of items) {
      const control = element('button', undefined, 'conversation-item'); control.type = 'button';
      control.addEventListener('click', async () => {
        try {
          if (conversationSaving) return;
          if (item.id !== selectedConversation) {
            if (!await canReplaceConversation()) return;
            selectConversation(item.id);
          }
        } catch (error) { report(error); }
      });
      $('conversation-rows').append(control);
      control.dataset.conversationId = item.id; control.disabled = conversationSaving;
      control.setAttribute('aria-pressed', String(item.id === selectedConversation));
      control.append(element('strong', item.display_label || item.id), element('span', conversationModes[item.policy?.mode]?.[0] || '未知模式', 'badge'));
      if (item.id === focusedID) control.focus();
    }
    if (!items.length) empty($('conversation-rows'), conversations.length ? '没有匹配的会话，试试其他名称或 ID。' : '暂无会话。接入 QQ 后，收到的会话会出现在这里。');
  }
  function selectConversation(id) {
    const changed = selectedConversation !== id;
    if (decisionConversation !== id) {
      decisionConversation = id; decisionBefore = null; decisionEpoch++;
      empty($('decision-logs'), '点击刷新判别日志，查看当前会话的发言选择。'); show('decision-log-state', ''); $('older-decisions').hidden = true;
    }
    selectedConversation = id; $('cid').value = id;
    const row = conversations.find(item => item.id === id);
    $('policy-version').value = row?.policy?.policy_version ?? '';
    $('mode').value = row?.policy?.mode || 'addressed_only';
    if (changed) { $('persona').value = '{}'; show('decision-output', '尚未读取'); }
    $('conversation-controls').disabled = !row || conversationSaving;
    $('load-decisions').disabled = !row;
    show('selected-conversation', row?.display_label || '请选择会话');
    show('selected-conversation-id', row?.id || '接入 QQ 并收到消息后，即可管理会话。');
    show('mode-description', conversationModes[$('mode').value]?.[1] || '');
    renderConversationPolicy(row); renderConversationList();
  }
  $('conversation-search').addEventListener('input', renderConversationList);
  $('mode').addEventListener('change', () => show('mode-description', conversationModes[$('mode').value]?.[1] || ''));
  $('cid').addEventListener('change', async () => {
    const id = $('cid').value; $('cid').value = selectedConversation;
    if (id !== selectedConversation && !await canReplaceConversation()) return;
    selectConversation(id);
    if (activePanel === 'decision-history' && id) runAction($('load-decisions'), () => loadDecisionLogs());
  });
  async function loadConversations() {
    if (!await canReplaceConversation()) return;
    const draft = [selectedConversation, $('mode').value, $('persona').value, $('policy-version').value, policyDraftSnapshot()];
    const result = await request('/api/v1/conversations');
    if (conversationSaving || !equal(draft, [selectedConversation, $('mode').value, $('persona').value, $('policy-version').value, policyDraftSnapshot()])) {
      notice('读取期间会话或草稿已变化，已保留当前编辑。'); return;
    }
    conversations = result;
    const selected = selectedConversation; $('cid').replaceChildren();
    for (const item of conversations) {
      const option = document.createElement('option'); option.value = item.id; option.textContent = item.display_label; $('cid').append(option);
    }
    $('persona').value = '{}';
    selectConversation(conversations.some(item => item.id === selected) ? selected : conversations[0]?.id || '');
    if (activePanel === 'decision-history' && selectedConversation) await loadDecisionLogs();
  }
  action('refresh-overview', overview); action('refresh-conversations', loadConversations); action('reload-conversations', loadConversations);
  async function patchPolicy(change) {
    if (conversationSaving) return;
    const expected = Number(required('policy-version', '预期策略版本')); if (!Number.isInteger(expected) || expected < 1) throw new Error('预期策略版本应为正整数');
    const id = cid();
    if (change.persona && !await confirmDialog('替换这个会话的人格覆盖？', '这会替换「' + $('selected-conversation').textContent + '」已保存的覆盖；空对象会恢复全局设置。', [], '保存覆盖')) return;
    conversationSaving = true; $('conversation-controls').disabled = true; $('cid').disabled = true; renderConversationList();
    try {
      const result = await request('/api/v1/conversations/' + encode(id) + '/policy', {method: 'PATCH', body: {expected_version: expected, ...change}});
      const row = conversations.find(item => item.id === id); if (row) row.policy = result;
      $('policy-version').value = result.policy_version;
      if (change.persona) $('persona').value = '{}';
      for (const key of ['speech_thresholds', 'participation']) if (key in change) renderConversationPolicy(row, key);
      show('decision-output', result); notice('会话设置已保存');
    } finally {
      conversationSaving = false; $('conversation-controls').disabled = !selectedConversation; $('cid').disabled = false; renderConversationList();
    }
  }
  action('open-decision-history', () => navigate('decision-history'));
  action('save-mode', () => patchPolicy({mode: $('mode').value}));
  action('save-persona', () => patchPolicy({persona: JSON.parse($('persona').value)}));
  const decisionActions = {IGNORE: '跳过', WAIT: '等待', SPEAK: '发言'};
  const decisionReasons = {
    HARD_GATE: '身份、权限或禁言限制', ECHO_OR_DUPLICATE: '机器人回声、重复消息或非聊天事件',
    PAUSED_OR_BUDGET: '暂停、静默或发送预算限制', NO_SEND_CAPABILITY: '接入当前无法发送',
    INVITED: '私聊、被提及或收到有效引用', UNADDRESSED: '仅回应被提及的消息',
    SOCIAL_NOT_READY: '缺少全量消息能力', PARTICIPATION_LIMIT: '参与限制',
    JUDGE_ABSTAIN: '判别器弃权', USEFUL_INVITATION: '邀请或话题相关，且能够提供有用贡献',
    PARTICIPATION_PREFERENCE: '主动参与偏好禁止在此场景接话',
    OTHERS_ARE_FINISHING_TOPIC: '等待话题发展后重新评估', LOW_RELEVANCE: '未满足发言条件，且未进入等待或重评估次数已用尽',
    SOFT_PARTICIPATION_BUDGET: '主动参与比例预算已用尽'
  };
  const blockedRuleLabels = {
    SUPPRESSED: '处于临时禁止插话期', COOLDOWN: '距离上次发言未满冷却时间',
    MIN_HUMAN_MESSAGES: '近期人类消息不足', UNANSWERED_PROACTIVE: '上次主动发言尚未得到回应',
    RECENT_BOT_RATIO: '近期机器人发言比例达到上限'
  };
  function decisionLogCard(entry, open) {
    const decision = entry.decision, context = entry.context || {}, card = element('details', undefined, 'decision-log');
    card.open = open;
    const summary = element('summary'), header = element('span', undefined, 'decision-log-header');
    const at = new Date(entry.created_at), time = element('time', Number.isNaN(at.getTime()) ? entry.created_at : at.toLocaleString()); time.dateTime = entry.created_at;
    header.append(time, element('strong', decisionActions[decision.action] || decision.action, 'badge'));
    summary.append(header, element('span', entry.message_preview || '无可用文本，来源已撤回、清理或消息不含文本', 'decision-log-message'));
    card.append(summary);
    const content = element('div', undefined, 'decision-log-content');
    content.append(element('p', (decision.reason_codes || []).map(code => decisionReasons[code] || code).join(' · '), 'decision-reason'));
    if (decision.blocked_rules?.length) content.append(element('p', decision.blocked_rules.map(code => blockedRuleLabels[code] || code).join(' · ')));
    if (decision.shadow_action) content.append(element('p', '观察模式下的建议结果为' + (decisionActions[decision.shadow_action] || decision.shadow_action)));
    content.append(element('p', decisionJudgeStatus(entry) + ' · ' + decision.judge_model + (context.judge_duration_ms == null ? '' : ' · ' + context.judge_duration_ms + ' ms'), 'hint'));
    const inboxState = {PENDING: '待处理', WAITING: '等待', DONE: '已处理', FAILED: '失败'}[entry.inbox_state] || entry.inbox_state || '来源已清理';
    content.append(element('p', '消息当前状态为' + inboxState + ' · 会话模式 ' + (context.mode || '历史未记录') + ' · 策略版本 ' + (context.policy_version ?? '历史未记录'), 'hint'));
    const table = element('table', undefined, 'decision-scores'), head = element('tr');
    for (const title of ['判断维度', '评分', '当时的发言阈值']) { const th = element('th', title); th.scope = 'col'; head.append(th); }
    const thead = element('thead'); thead.append(head); table.append(thead);
    const body = element('tbody');
    for (const score of decisionScoreRows(decision, context.thresholds)) { const row = element('tr'); cell(row, score.label); cell(row, score.score); cell(row, score.threshold); body.append(row); }
    table.append(body); content.append(table);
    content.append(element('p', context.thresholds?.wait_useful_contribution == null ? '历史记录未保存评分阈值快照。' : '未满足发言条件时，有用贡献 ≥ ' + context.thresholds.wait_useful_contribution + ' 且重评估次数未达上限，会进入等待。', 'hint'));
    if (context.policy_state) {
      const state = context.policy_state, rules = context.participation_rules || {}, lines = [
        '近期人类消息 ' + state.new_human_messages + ' / 最少 ' + (rules.min_human_messages ?? '未记录'),
        '未获回应的主动发言 ' + state.unanswered_proactive + (rules.require_member_response == null ? ' / 上限 ' + (rules.max_unanswered_proactive ?? '未记录') : rules.require_member_response ? ' / 需成员回应' : ' / 不要求成员回应'),
        '近期机器人发言比例 ' + state.recent_bot_ratio + ' / 上限 ' + (rules.max_recent_bot_ratio ?? '未记录'),
        '已重评估 ' + state.rechecks + ' 次 / 上限 ' + (rules.max_rechecks ?? '未记录'),
        '发言冷却 ' + (rules.cooldown_seconds ?? '未记录') + ' 秒',
        '统计窗口 ' + (rules.activity_window_seconds ?? '未记录') + ' 秒 / 最多 ' + (rules.activity_max_events ?? '未记录') + ' 条活动',
        '参与预算比例 ' + (rules.participation_budget_ratio ?? '未记录')
      ];
      content.append(element('p', lines.join(' · '), 'hint'));
    }
    const raw = element('details', undefined, 'technical-details'); raw.append(element('summary', '原始判别与规则输入快照'), element('pre', JSON.stringify(entry, null, 2))); content.append(raw);
    card.append(content); return card;
  }
  async function loadDecisionLogs(older = false) {
    const id = cid(), before = older ? decisionBefore : null;
    if (older && before == null) return;
    const epoch = older ? decisionEpoch : ++decisionEpoch;
    show('decision-log-state', older ? '正在读取更早记录…' : '正在读取判别日志…');
    let result;
    try { result = await request('/api/v1/conversations/' + encode(id) + '/decision-logs?limit=50' + (before == null ? '' : '&before=' + before)); }
    catch (error) { if ($('cid').value === id && epoch === decisionEpoch) show('decision-log-state', '读取失败。已有记录已保留，可点击刷新重试。'); throw error; }
    if ($('cid').value !== id || epoch !== decisionEpoch) return;
    if (!older) $('decision-logs').replaceChildren();
    result.items.forEach((entry, index) => $('decision-logs').append(decisionLogCard(entry, !older && index === 0)));
    if (!older && !result.items.length) empty($('decision-logs'), '当前会话暂无发言判别日志。');
    decisionConversation = id; decisionBefore = result.next_before; $('older-decisions').hidden = decisionBefore == null;
    show('decision-log-state', '已显示 ' + $('decision-logs').querySelectorAll('.decision-log').length + ' 条记录 · 最新记录在前');
  }
  action('load-decisions', () => loadDecisionLogs());
  action('older-decisions', () => loadDecisionLogs(true));
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
  async function loadExpressions() {
    const result = await request('/api/v1/expressions?conversation_id=' + encode(cid()));
    $('expression-rows').replaceChildren();
    const states = {PENDING: '等待积累消息', RUNNING: '正在学习', DONE: '上批学习完成', FAILED: '上批失败，稍后重试', SKIPPED: '暂未执行'};
    const status = result.learning.learn ? states[result.learning.state] || '等待积累消息' : '未启用学习';
    show('expression-status', (result.learner_configured ? '' : '尚未配置学习模型或密钥。') + '当前有 ' + result.records.length + ' 条素材，积累了 ' + result.learning.pending_count + ' 条消息。' + status + '；' + (result.learning.use ? '允许使用素材。' : '暂不使用素材。'));
    for (const record of result.records) {
      const row = document.createElement('tr');
      cell(row, record.situation); cell(row, record.style); cell(row, record.count + ' / ' + (record.checked ? '已精选' : '未精选'));
      const controls = cell(row, '');
      button(controls, '编辑', () => {
        $('expression-id').value = record.id; $('expression-revision').value = record.revision;
        $('expression-situation').value = record.situation; $('expression-style').value = record.style;
        $('expression-checked').checked = record.checked; $('expression-id').closest('details').open = true;
      });
      button(controls, record.checked ? '取消精选' : '精选', async () => {
        await request('/api/v1/expressions/' + encode(record.id) + '?conversation_id=' + encode(cid()), {method: 'PATCH', body: {expected_revision: record.revision, checked: !record.checked}});
        await loadExpressions();
      });
      button(controls, '删除', async () => {
        if (!await confirmDialog('删除这条表达？', '同一条表达不会被重新学习或从旧备份恢复。', [], '删除表达')) return;
        await request('/api/v1/expressions/' + encode(record.id) + '?conversation_id=' + encode(cid()) + '&expected_revision=' + record.revision, {method: 'DELETE'});
        await loadExpressions();
      });
      $('expression-rows').append(row);
    }
    if (!result.records.length) { const row = document.createElement('tr'), message = cell(row, '尚无表达素材。积累足够的有效聊天后会自动学习。'); message.colSpan = 4; $('expression-rows').append(row); }
  }
  action('load-expressions', loadExpressions);
  action('save-expression', async () => {
    await request('/api/v1/expressions/' + encode(required('expression-id', '请先选择表达')) + '?conversation_id=' + encode(cid()), {method: 'PATCH', body: {
      expected_revision: Number($('expression-revision').value), checked: $('expression-checked').checked,
      situation: required('expression-situation', '使用情境'), style: required('expression-style', '表达方式')
    }});
    $('expression-id').value = ''; $('expression-id').closest('details').open = false;
    await loadExpressions(); notice('表达已保存');
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
  const loadPersona = () => loadEditor(personaEditor, async () => {
    const result = await request('/api/v1/persona');
    return () => { personaVersion = result.version; personaEditor.load(result.persona, result.schema); show('global-persona-output', {version: result.version}); };
  });
  const loadPermissions = () => loadEditor(permissionEditor, async () => {
    const [result, schema] = await Promise.all([request('/api/v1/permissions'), request('/api/v1/permissions/schema')]);
    return () => { const {policy_version, ...policy} = result; permissionVersion = policy_version; permissionEditor.load(policy, schema); setupPermissionChecker(schema); show('permissions-output', {policy_version}); };
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
  $('default-policy-fields').addEventListener('input', () => configEditor.sync());
  $('default-policy-fields').addEventListener('change', () => configEditor.sync());
  action('save-default-policy', async () => { await saveConfig(); });
  action('apply-config', () => saveConfig(true)); action('save-config', () => saveConfig());
  action('preview-config', async () => { await confirmDialog('未保存的更改', '对比已保存的原值与当前草稿的新值。关闭预览不会保存。', changes(configEditor.baseline, configEditor.read()), '返回编辑'); });
  action('save-global-persona', () => mutate(async () => {
    const persona = personaEditor.read(), diff = changes(personaEditor.baseline, persona);
    if (!diff.length || !await confirmDialog('保存全局人格？', '更改会立即用于后续回复，会话人格覆盖仍然保留。', diff, '保存并生效')) return;
    const result = await request('/api/v1/persona', {method: 'PUT', body: {expected_version: personaVersion, persona}});
    personaVersion = result.version; personaEditor.load(result.persona); show('global-persona-output', result); notice('全局人格已保存并生效。');
  }));
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
    configuration: ['全部配置', '连接与行为设置，统一管理。', () => configEditor.getter || loadConfig()],
    overview: ['运行总览', '了解服务状态与 QQ 接入情况。', overview],
    conversations: ['会话管理', '管理发言模式、群级设置与默认参与策略。', () => conversations.length ? undefined : loadConversations()],
    'decision-history': ['发言判别日志', '查看每次发言、等待和跳过的原因与当时使用的设置。', async () => { if (!conversations.length) await loadConversations(); else if ($('cid').value) await loadDecisionLogs(); }],
    tasks: ['后台任务', '查询任务进展，处理执行与交付。', async () => { if (!conversations.length) await loadConversations(); if ($('cid').value) await loadTasks(); }],
    memories: ['记忆与词典', '审阅记忆、纠正内容与管理词典。', async () => { if (!conversations.length) await loadConversations(); if ($('cid').value) await loadMemories(); }],
    'global-persona': ['全局人格', '设定小象的身份、表达与行为边界。', () => personaEditor.getter || loadPersona()],
    permissions: ['权限策略', '决定哪些用户、群聊可以使用哪些功能。', () => permissionEditor.getter || loadPermissions()]
  };
  async function navigate(panel, updateHash = true) {
    if (!panels[panel]) panel = 'configuration'; activePanel = panel;
    for (const control of document.querySelectorAll('[data-panel]')) {
      const active = control.dataset.panel === panel; control.setAttribute('aria-current', active ? 'page' : 'false'); control.setAttribute('aria-pressed', String(active)); $(control.dataset.panel).hidden = !active;
    }
    show('page-title', panels[panel][0]); show('page-subtitle', panels[panel][1]);
    $('conversation-picker').hidden = !['decision-history', 'tasks', 'memories'].includes(panel);
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
    if ((conversationDirty() || editors.some(state => state.dirty)) && !await confirmDialog('断开并清除草稿？', '管理员令牌和本页未保存的内容会被清除。', [], '断开连接')) return;
    stopEvents(); authEpoch++; decisionEpoch++; decisionConversation = ''; decisionBefore = null; token = ''; conversations = []; configVersion = savedConfigVersion = personaVersion = permissionVersion = null;
    editors.forEach(state => state.clear()); configView = null; $('token').value = ''; $('token').type = 'password';
    document.querySelectorAll('pre').forEach(item => { item.textContent = ''; });
    for (const id of ['conversation-rows', 'decision-logs', 'task-rows', 'memory-rows', 'cid', 'versions', 'credentials', 'overview-metrics', 'overview-adapters']) $(id).replaceChildren();
    $('older-decisions').hidden = true;
    for (const id of ['cid', 'policy-version', 'task-id', 'task-goal', 'memory-id', 'source-event', 'memory-text']) $(id).value = '';
    $('default-policy-fields').replaceChildren(); selectedConversation = ''; $('conversation-search').value = ''; selectConversation(''); $('persona').value = '{}'; show('auth-error', '');
    $('login-toggle-token').setAttribute('aria-pressed', 'false');
    $('login-toggle-token').setAttribute('aria-label', '显示令牌'); show('login-toggle-token', '显示');
    loginVisible(true); $('token').focus();
  });
  window.addEventListener('beforeunload', event => { if (conversationDirty() || editors.some(state => state.dirty)) { event.preventDefault(); event.returnValue = ''; } });
  window.addEventListener('pagehide', () => { stopEvents(); authEpoch++; token = ''; });
  updateSaveButtons(); loginVisible(true); navigate(location.hash.slice(1) || 'configuration', false);
})();
