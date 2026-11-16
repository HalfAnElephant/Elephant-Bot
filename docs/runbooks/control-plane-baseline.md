# 本地控制面与恢复基线

本页跟踪路线图 #18 的第一批可复跑检查。测试环境用临时 SQLite、合成 QQ 事件和本地受控夹具。不需要真实模型、QQ 或 OpenViking 凭据，不修改部署数据。

## 控制面短跑

```sh
uv sync --locked
uv run --locked python scripts/benchmark_control_plane.py --seconds 10 --rate 50 --seed 18
```

默认输出到 stdout。若要保存结果，可用 `--output` 指定一个**不存在**的 JSON 路径；已有报告不会被覆盖。默认负载 600 秒、每秒 50 条，与旧报告只在时长和投放率上相同。旧报告 `docs/control-plane-benchmark.json` 使用历史适配器和计时方式，不作为新旧性能对比。

脚本使用 QQ 官方事件结构、现有事件规范化入口和真实 Core/SQLite。每十条插入一个重复事件作为单独样本，每秒提交、读取并取消一项合成任务。脚本独立报告规范化、接纳、去重、任务操作和收件箱轮询，不启动实际模型、Pi Worker 或 QQ 投递；也不测 HTTP 签名校验、真实 QQ 送达或任务端到端完成。`DONE` 表示收件箱处理结束，不代表回复送达。单进程峰值 RSS 不是进程树用量。短跑只核对功能结果和报告形状，不设通用延迟门槛。

## 故障边界与验证范围

| 中断阶段 | 对应夹具及当前结论边界 |
| --- | --- |
| HOST 任务数据库提交前 | `test_control_plane_rollback.py` 在来源、会话和任务树全部写入后抛错，另建 Core 打开同一数据库，确认未留半成品；同一幂等键重试只得到一棵树。这里模拟异常回滚，不等同于操作系统强杀。 |
| 工具已发起、原执行尝试停止 | `test_runtime_recovery.py` 验证缺少配对证据的操作保持 `UNKNOWN`，无证据时不自动重跑；另测旧执行者的停止证明与容量边界。 |
| 外部动作完成但原尝试未取得结果 | `test_leader_crash_recovery.py` 与 `test_runtime_recovery.py` 的合成回执覆盖恢复和只读核对；不能从本地进程退出推断外部影响成功。 |
| Outbox 状态未知 | `test_runtime_recovery.py` 中构造 `UNKNOWN` 并验证只承认已排队、不承认已交付或自动重发；还未模拟真实平台收包后丢失确认或执行系统级强杀。 |
| 记忆远端请求已受理、回执丢失 | `test_openviking.py` 的假服务分别模拟追加/提交回执丢失，验证原操作只发生一次。原生会话更正/遗忘恢复及删除屏障另见 `test_committed_memory_recovery.py` 和 `test_memory_restore.py`；不替代真实远端恢复演练。 |

```sh
npm ci --ignore-scripts
uv run --locked pytest -q \
  tests/test_benchmark_control_plane.py tests/test_control_plane_rollback.py \
  tests/test_runtime_recovery.py tests/test_leader_crash_recovery.py \
  tests/test_process_residency_recovery.py tests/test_memory_mutation_recovery.py \
  tests/test_committed_memory_recovery.py tests/test_memory_restore.py tests/test_openviking.py
```

仍需单独建立长时混合负载、Pi 冷/热、模型首字和 SQLite 写竞争等分段基线，再据此判断本地热点。真实 QQ、模型或 OpenViking 的外部验收与本页分开。第一批短跑和现有恢复回归不能代替 #18 完整验收，也不作为更换语言的依据。
