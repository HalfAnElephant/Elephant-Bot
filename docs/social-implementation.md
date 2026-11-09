# P3/P4 接入与离线验证

## 数据库

迁移 0002 依赖 0001。初始化共享 metadata 前导入 `qqbot.storage.social_schema`。表为 `social_records`、`social_tombstones`、`social_versions`、`social_index_jobs`。无需新依赖。由 Core 持有 `SocialService(db, clock)`，事务使用现有 `Database.transaction()`。

## 发言决策

```python
from qqbot.policy import SpeechPolicy, PolicyState
from qqbot.policy.judge import RuleOnlyJudge, JevJudge, StructuredLLMJudge

policy = SpeechPolicy(clock)
decision = policy.decide(event, capabilities, state, signals=None)
# state 是 PolicyState，明确受邀无需模型。
# 仅软判别路径调用 judge，传入有界、不含凭据的可见上下文。
signals = await judge.judge(context)
decision = policy.decide(event, capabilities, state, signals)
```

PolicyState 字段包含 mode、authenticated、allowed、muted、paused、duplicate、valid_bot_reply、remaining_budget、last_bot_at、new_human_messages、unanswered_proactive、recent_bot_ratio、rechecks、social_evaluated、suppressed_until。默认 addressed_only。Core 负责身份、白名单、有效引用、权限、平台发送条件和预算的真实值，不能以模型输出替代。显式命令在此之前处理。

JevJudge(api_key, model='jev-1.14.0', endpoint='https://api.typesafe.ai/v1/systemone', client=None) 使用固定五项 Noul 问题。StructuredLLMJudge(api_key, model, endpoint, client=None) 使用 Chat Completions JSON。二者返回 JudgmentSignals，数值错误、缺字段、HTTP 错误或超时均 ABSTAIN。缺 key 不发请求。未做真实账户调用或供应商成本测量。

ParticipationController 为每会话独立实例。observe(event_id, invited=False, now=None)、prune(now, window_seconds=60)、available(ratio=.2)、reserve(delivery_id, now=None)、feedback(delivery_id, state)。传入 now 才推进滑动窗口，Core 应始终提供。UNKNOWN 保留预占直至人工或发送恢复流程对账，只有 SENT 更新成功集合。该辅助控制器的状态恢复由 Core 提供，与硬预算并行应用。

## 会话等待与发送前检查

ConversationWindow(clock, max_messages=40) 提供 accept(event, invited=False)、ready()、snapshot(persona_version, policy_version, revocation_version)、finish()、wait(decision)、wake()、dump_wait()、restore_wait(value)。普通 debounce 为 800ms，最大 2500ms，受邀为 200ms。持有不可变快照期间窗口继续接收消息，每会话仅一份生成。WAIT 最多重评估两次，wake 消耗一次等待，Core 需要把 dump_wait 结果与持久定时器一起保存，在新消息或一次定时器触发时调用。

validate_draft(snapshot, invited=..., current_revocation=..., paused=False, recalled=False, already_replied=False, cancelled=False, resolved=False) 返回是否保留草稿。Core 还需检查当前平台凭据、发送许可、有效期和策略/人格版本。主动草稿的 snapshot seq 变化会失效。受邀请求保留原锚点，后续普通消息不会导致永远重生成。模型文本使用 ReplyDraft DTO，不能直接转成工具调用。

## 人格与提示词

PersonaStore(path, overrides_path=None) 从 YAML 读取核心人格。reload() 原子验证后切换。set_override(conversation_id, values, authorized=False) 需要已鉴权管理员授权，只允许 name、formality、reply_length、participation 字段。默认覆盖文件为核心 YAML 同目录同名 `.overrides.json`。可将 overrides_path 指向运行时数据目录。

snapshot(conversation_id) 返回 `(persona_json, version_digest)`，两者保存在回合中。assemble_prompt(persona_json, boundary=..., platform=..., request=..., recent_messages=..., memories=(), jargon=(), summary='', max_chars=32000) 返回 system/user 消息数组。记忆、摘要、原始消息均在 user 数据区。当前请求与权限上下文不可截断，超预算报错，优先去掉表达素材、低优先级记忆及摘要。字符预算是保守初值，未声称精确 token 计数。

## 记忆操作

MemoryRecord 在 `qqbot.memory` 导出，所有时间必须带时区。source_event_ids 必须指向当前会话 inbox 中真实、未撤回、非机器人原消息。principal_in_conversation 的 scope_id 为 conversation.key，subject_ref 为 principal.key。owner_private 的 scope_id 与 subject_ref 为 owner principal.key，仅 owner 的私有场景可见。

- candidate(access, record, auto_enabled=False, explicit=False) 验证候选，不会自动提交。自动提取默认关闭。稳定 memory_id 下相同来源和内容重放返回现状，内容冲突报错。
- commit(access, memory_id, authorized=False) 只有 Core 显式授权才可激活。相同事实去重，显式事实优先于旧 inference，其他冲突双方 disputed。
- retrieve(access, query='', limit=8, embedding=None, model_id=None, version=None) 先按 SQL 访问控制筛选，再做关键词与可选向量各 top20 的倒数名次融合，最多八条。短汉字用字符 n-gram。
- correct(access, memory_id, replacement) 需要显式用户证据，不能扩大作用域或改变主语和 predicate，旧记录 superseded。replacement 使用稳定 ID 可安全重放。
- forget(access, memory_id) 同步检索失效、清向量与索引任务、撤销证据及依赖记录，重复调用安全。
- revoke_source(access, source_id) 用于原消息撤回，权限须为原发送人或本群管理角色。同步失效依赖记忆并持久记录来源撤销。
- version(access) 返回共享撤销 epoch，生成开始保存，入 Outbox 前比较。任意记忆变更会使所有会话的既有快照失效。
- store_embedding(access, memory_id, vector, model_id, version) 在异步 HTTP 完成后再次检查 active 和证据，可防删除与索引任务竞态。

HTTPEmbedding(endpoint, model, version, api_key=None, dimensions=None, client=None).embed(text) 是真实 HTTP 接口，无 key、失败或非法响应返回 None。模型/维度切换不会混用旧向量。未连接真实 embedding 账户。

`jargon(...)` 验证 term/sense 的独立上下文和成员证据。`learn_jargon(access, term, sense_id, definition, evidence_ids, authorized=False, allow_output=False)` 经授权保存候选，至少三种不同消息正文、两位成员才 active。`jargon_entries(access, term, for_output=False)` 作用域隔离，输出素材最多一条。相同正文复读不累计，同词多义以 sense_id 分开。此实现的独立上下文判据是不同完整消息正文，不能替代人工语义审核。

普通用户只管理本人会话记录，群管理角色只管理本群公共事实，无法读取其他人的局部个人记忆或私聊。全局事实只接受 owner 显式人工审定。用户命令的解析与鉴权、恢复时删除记录重放由 Core 接入。

## 定点验证与评估边界

`uv run pytest -q tests/test_social_policy.py tests/test_social_memory.py` 覆盖受邀元信息、伪 mention、shadow、社交冷却、WAIT 上限、窗口快照、撤回检查、Judge 异常与熔断、人格失败回滚/重启、跨群个人隔离、显式纠正、删除来源、短词与复读、缺 embedding key、维度过滤和稳定 ID 重放。HTTP 测试使用 MockTransport 验证协议及降级，不构成真实模型验收。

`uv run python evals/social_replay.py DATASET.jsonl` 回放真实提供的标注片段。每行需要 group_id、fragment_id、split、label、event、capabilities，可选 state 和 signals。DTO 的 event 时间使用 ISO 字符串，工具会按 JSON 解析。按群及连续片段检查 split 隔离，仅 test 集输出召回、误触发、precision 的分母与 Wilson 区间。未提供 300 条真实去标识片段，未比较供应商模型、未报告延迟/成本或宣称 social 达标。多位标注者争议和采样概率须保留在源数据中。

候选提取、增量摘要和向量维护接口见下文。自动语义黑话审核仍需单独接入。Core 显式接入上述事务 API。删除后旧来源不可再提交，但历史备份物理清除由部署保留策略负责。密钥拦截包含常见格式规则，生产自动提取仍需额外数据审查。未运行真实 QQ 客户端、真实 Jev/embedding、云 CI 或全局测试。

## 增量维护接口

迁移 0005 依赖 0004，新增增量检查点、会话摘要、无正文删除台账及原消息保留处理记录。仍只需导入 `qqbot.storage.social_schema` 注册所有 metadata，0002 的表集合与迁移未变化。

```python
from qqbot.memory.extraction import HTTPCandidateExtractor
from qqbot.memory.embedding import HTTPEmbedding
from qqbot.memory.maintenance import MemoryMaintenance

extractor = HTTPCandidateExtractor(endpoint, model, api_key, enabled=False)
embedding = HTTPEmbedding(embedding_endpoint, embedding_model, embedding_version,
                          embedding_key, dimensions=embedding_dimensions)
maintenance = MemoryMaintenance(social_service, extractor, embedding,
                                enabled=False, raw_retention_days=30, max_messages=40)
report = await maintenance.maintenance_once(access, auto_extract=False)
summary = maintenance.summary(access)
```

配置约定由 Core 接入。`memory_maintenance_enabled`、`memory_auto_extract`、`memory_extractor_enabled` 默认 false，分别对应 maintenance.enabled、auto_extract 和 extractor.enabled。模型参数为提取 endpoint/model/API key 与 embedding endpoint/model/version/API key/dimensions。密钥不进入 prompt、数据库或日志。Core 在低优先级、成本预算允许且没有前台请求待处理时调用，每会话避免重叠维护任务。网络供应商对象不接触数据库，所有持久化发生在 Core 持有的 maintenance/service 内。

候选提取器调用真实 Chat Completions HTTP，最多三秒、无隐式重试，每批最多八条结构化建议。没有 key、禁用或无有效人类消息返回 SKIPPED。错误字段、超时和网络失败返回 FAILED。建议不能指定任意 scope_id、owner_private、global_curated、persona 或 source_kind。作用域由 Core 的 access 决定，只能引用该有界片段的原事件，并复用服务对归属、证据、秘密及管理权限的检查。模型建议全部为 inference/candidate，`should_persist` 不授予发布权限，后续仍须人工授权 commit。成员只能创建本会话本人候选，公共会话候选需要管理员身份，不能以管理员身份自动创建他人的个人记录。

摘要按 inbox seq 增量推进。程序抽取有来源、时间和说话人的最多 40 条、每条最多 300 字符原文片段，不产生新的模型事实。摘要与检查点在同一事务写入成功后推进。提取检查点独立，失败保留原 seq 以允许下次调度重试，关闭/无 key 则记录 SKIPPED 并推进。此种确定性摘录用于有界上下文压缩，未声称语义总结能力。

向量索引每次最多处理八个有管理权限的作业。缺 key、禁用或记录失效时作业显式 SKIPPED，供应商失败为 FAILED。后续配置好供应商或解决故障后，调用 `maintenance.requeue_embeddings(access)` 重排本作用域可管理的 SKIPPED/FAILED 作业。HTTP 返回后 `store_embedding` 再验证 active 和来源，删除不会被晚到的向量结果复活。

删除现在同步把 MemoryRecord 的 text 改为 `[deleted]`、value_json 改为 `null`、embedding 清空，并记录仅包含 memory ID、会话 ID、来源 ID 和时间的删除台账。有关摘要立即清空并 INVALID，来源 tombstone 禁止旧摘要重新提取。维护会清理升级前遗留的 deleted 正文与索引作业。当前保留策略按 raw_retention_days 清理原消息，长期 active/candidate 事实仍需要的来源仅保留最多 300 字符证据摘录，其余来源清空并撤销。原消息保留由会话维护权限约束，普通用户仅清理本人来源。

SQLite 在线字段清空不等于磁盘扇区、WAL、外部备份的物理抹除，部署备份与数据库压缩策略需单独处理。中文关键词层目前按授权候选集扫描并在查询时生成 n-gram，达到约五万条有效记录或实测延迟不达标时应转向持久关键词索引及相应迁移。
