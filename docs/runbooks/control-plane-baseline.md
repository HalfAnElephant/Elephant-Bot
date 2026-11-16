# 本地控制面与恢复基线

本页跟踪路线图 #18 的可复跑检查。测量使用临时 SQLite、QQ 官方事件结构、锁定 Pi 与本地受控 HTTP 夹具。报告保存构建 SHA、环境、固定条件、样本数和分位数。

## 控制面短跑

```sh
uv sync --locked
uv run --locked python scripts/benchmark_control_plane.py --seconds 10 --rate 50 --seed 18
```

默认输出到 stdout。若要保存结果，可用 `--output` 指定一个**不存在**的 JSON 路径。已有报告不会被覆盖。默认负载 600 秒、每秒 50 条，与旧报告只在时长和投放率上相同。旧报告 `docs/control-plane-benchmark.json` 使用历史适配器和计时方式，不作为新旧性能对比。

脚本使用现有事件规范化入口和真实 Core/SQLite。每十条插入一个重复事件作为单独样本，每秒提交、读取并取消一项合成任务。阶段包括规范化、接纳、去重、任务操作和收件箱轮询。`DONE` 表示收件箱处理结束。该场景的模型、Pi Worker、平台签名与投递在下面的混合场景和外部验收中核对。

`sqlite_begin_wait` 测量实际 `BEGIN IMMEDIATE` 的等待与进入开销，`sqlite_transaction_body` 测量事务体，`sqlite_commit` 包含提交和连接归还。`event_loop_lag` 使用 20 ms 定时器，进程树 RSS 每 500 ms 采样一次，单位为 KiB。`cpu_self_seconds` 为 Python 自身 CPU 时间。原有 `rss_samples` 继续保留本进程高水位及对应单位。末尾记录实际 WAL 模式和一次被动 checkpoint 的 busy/log/checkpointed 页数。

混合场景另外记录 `cpu_children_seconds`，在实际 Pi 全部停止并收割后取子进程 CPU 差值，包含 Pi 与资源采样的 `ps` 开销。`workload_elapsed_seconds` 截止任务排空，`elapsed_seconds` 包含已验证停止的收尾时间。`process_tree_rss_kib` 包含基准 Python 与其活动子进程。

## 混合负载、写竞争与原生历史

```sh
mkdir -p test-results
for repeat in 1 2 3; do
  uv run python scripts/benchmark_control_plane.py --seconds 60 --rate 50 --seed 18 \
    --output "test-results/control-$repeat.json"
  uv run python scripts/benchmark_control_plane.py --seconds 10 --rate 50 --seed 18 \
    --writer-hold-ms 20 --output "test-results/contention-$repeat.json"
done
uv run python scripts/benchmark_runtime.py --output test-results/runtime-baseline.json
uv run python scripts/benchmark_memory.py --output test-results/memory-baseline.json
```

输出路径须尚未存在。持续负载和注入竞争各重复三次。历史 600 秒负载可将 `--seconds` 改为 `600`。`--writer-hold-ms 20` 让独立 SQLite 连接在每次间隔 100 ms 的循环中持有 20 ms 写锁，用来测量同步写等待如何影响入站与事件循环。

| 场景 | 实际路径与控制变量 | 核对的结果 |
| --- | --- | --- |
| 0、1、10 个后台任务与三轮前台对话，各三次 | Core → Scheduler → 实际 Pi → ProviderAdapter → 文件读取 → 观察记录 → 结果结算 → Outbox。模型并发 4，后台可用 3，空闲缓存 16，TTL 300 秒。 | 每个 Worker 成功读取夹具文件并保存观察，目标与 Worker 均完成，三条前台回报得到受控 QQ 确认。记录前台回报时仍活动的后台 run 数。 |
| Pi 冷启动和空闲复用 | 冷启动包含实际 `get_state` 就绪握手，空闲复用走真实 SessionPool 与绑定。 | 分别记录冷启动、冷/热 acquire、首段文字、模型容量等待与命中次数。 |
| 短/长原生日志 | 带 Pi v3 header 的 100/10,000 条合成消息，导出最后 10 条，各测三次。对同一日志实际重启 Pi 三次并核对 messageCount。 | 记录完整扫描条目和文件字节、导出时间、Pi 重新就绪时间及运行时进程树 RSS。返回分页上限为 10，原始读取量仍由整份日志决定。 |
| Provider 与 QQ 分段 | 模型首帧延迟固定为 Leader 20 ms、Worker 150 ms，尾帧 30 ms。QQ 接收确认固定延迟 30 ms。 | 从持久 usage 读取 TTFT、总耗时和估算输入 token，并测工具读取与前台总往返。TTFT 包含容量等待。完整往返还包含 Outbox 的平台节流。 |
| OpenViking 同步与召回 | 实际 adapter 经受控 HTTP 接纳来源、提交提炼、轮询索引，再 search → read。每个请求固定延迟 5 ms，三次独立库各十次召回。 | 索引前不可召回，索引后内容须对应原始来源。分别报告接纳、提炼、索引就绪、检索、读取及完整召回，重试保留同一远端操作。 |

Pi 和本地文件工具实际运行，模型输出与远端提炼、索引、关键词检索及 QQ 接收响应由程序生成。其测量范围为协议与宿主开销。真实服务延迟、模型效果、检索质量、平台送达和计费分别由对应环境验收。新脚本的断言只检查状态与证据完整性，延迟以测量分布记录。

## 故障边界与验证范围

| 中断阶段 | 对应夹具及当前结论边界 |
| --- | --- |
| HOST 任务数据库提交前 | `test_control_plane_rollback.py` 在来源、会话和任务树全部写入后抛错，另建 Core 打开同一数据库，确认未留半成品。同一幂等键重试只得到一棵树。这里模拟异常回滚，不等同于操作系统强杀。 |
| 工具已发起、原执行尝试停止 | `test_runtime_recovery.py` 验证缺少配对证据的操作保持 `UNKNOWN`，无证据时不自动重跑。另测旧执行者的停止证明与容量边界。 |
| 外部动作完成但原尝试未取得结果 | `test_leader_crash_recovery.py` 与 `test_runtime_recovery.py` 的合成回执覆盖恢复和只读核对。不能从本地进程退出推断外部影响成功。 |
| Outbox 发出、接收确认尚未本地落盘 | `test_outbox_crash_recovery.py` 子进程通过实际 OfficialAdapter 发出请求，受控平台持久化接收证据后立即 `os._exit(73)`。父进程重开原库或备份恢复副本，核对 `SENDING → UNKNOWN`、发送次数和证据保留，并验证另一独立消息仍可发送。 |
| 记忆远端请求已受理、回执丢失 | `test_openviking.py` 的假服务分别模拟追加/提交回执丢失，验证原操作只发生一次。原生会话更正/遗忘恢复及删除屏障另见 `test_committed_memory_recovery.py` 和 `test_memory_restore.py`。不替代真实远端恢复演练。 |

```sh
npm ci --ignore-scripts
uv run --locked pytest -q \
  tests/test_benchmark_control_plane.py tests/test_control_plane_rollback.py tests/test_outbox_crash_recovery.py \
  tests/test_runtime_recovery.py tests/test_leader_crash_recovery.py \
  tests/test_process_residency_recovery.py tests/test_memory_mutation_recovery.py \
  tests/test_committed_memory_recovery.py tests/test_memory_restore.py tests/test_openviking.py
```

## 结果与后续决策

CI 的 `Controlled runtime baseline` 在同一 Ubuntu job 中顺序执行上述重复场景，原始 JSON 和环境记录归档为 `controlled-baseline-results`。恢复矩阵使用 `Correctness` 的逐项 pytest 结果。安装与跨平台首任务使用同一份通过 SHA-256 校验的 `release-assets`，详见[发布手册](releases.md)。

比较同一场景三次结果的 P50/P95/P99 范围、队列等待、CPU 和进程树 RSS，再定位等待或本地计算的主要来源。当前原生日志导出会完整读取文件，长日志是明确的测量候选。模型与 Outbox 的固定延迟和平台节流已在条件中列出。语言迁移决策需要真实目标负载下、本地计算占主要时间且现有实现优化后仍有瓶颈的证据。
