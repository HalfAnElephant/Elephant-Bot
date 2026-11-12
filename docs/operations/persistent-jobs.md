# 独立监督的后台 Job

短命令仍使用原 `ProcessService`，随 run 清理。`job.start` 创建的 Job 由 systemd user manager 独立监督。Leader/Worker 轮次结束或 daemon 重启不会关闭 Job。这里只承诺 daemon 重启续存，不承诺主机重启后自动重放命令。

daemon 的 Scheduler 装配 `JobRuntime` 和 `JobService`，RunGateway 通过同一个 RuntimeClient 注册并调用 `job.start`、`job.inspect`、`job.cancel`。身份和目录由持久 Worker Attempt、验证后的 owner、Task/root 与冻结的 execution world 解析。工具调用使用以下参数。

```json
{"job_key":"build-once","argv":["/usr/bin/make","test"],"cwd":".","timeout_seconds":3600,"restart_survival":true}
```

`job_key` 在同 task 内幂等，参数和作用域冻结。不同参数复用同 key 明确失败。dispatch intent 先落 SQLite `execution_jobs`（迁移 0019），再调用唯一 unit 名的 `systemd-run --user`。回执丢失或重启恢复只查询 unit，不重新启动已登记工作。不能确认的记录保持 UNKNOWN 并占容量，防止重复效果。Scheduler 在首次恢复 Job 状态后才准入新的模型任务，此后独立维护任务处理单位状态、过期、权限撤销、任务树取消和用户修订。它不占用模型轮次，也不等待原 run 的租约。

Job 只使用同一个 local execution world。bubblewrap 映射该 workspace 到 `/workspace`，`cwd` 必须在其中。系统二进制只读，`/tmp`、`/proc`、`/dev` 隔离，网络关闭。环境清空后仅设置固定 PATH/HOME/LANG，可额外传 LANG、LC_ALL、TZ。不转交 daemon、Pi 或用户的凭据。仍活跃或 UNKNOWN 的 Job 保留工作区写资格，TaskService.claim 会阻止其他 task 占用相同规范目录。普通任务恢复自己的 Job 时保持同一 task 和目录。

systemd 单位使用 `ExitType=cgroup`、`KillMode=control-group`、`RemainAfterExit=yes`、有限 `RuntimeMaxSec` 和停止宽限。回读包含 unit、InvocationID、cgroup 和启动标识。不凭裸 PID 回收。成功退出还要确认 cgroup 已空，父进程退出但孩子仍运行不会报告完成。取消请求先持久保存，回执不明时重启仍继续核对。发现 InvocationID 改变会保留未核对状态。Task 及其后代的 Pi 与 Job 均确认停止后，取消状态才收敛为 CANCELLED。

`job.inspect` 只允许当前 owner、task、root 和 workspace，`include_logs:true` 有界读取原 invocation 的 journal。不能查看其他任务日志。原 run 已取消、完成或释放执行槽后，可信 Job 回执仍可回读并核对原动作账本。宿主用冻结参数哈希和原单位身份补记事实。执行退出码、取消和日志可读性分别记录。journal 保留期由主机 journald 配置管理，不承诺无限日志保存。成功完成的 transient unit 保留退出状态，确认回执已持久后可由运维 `systemctl --user stop elephant-job-...service` 释放，勿在尚未核对时批量 reset/stop。

## 主机准备与真实验收

使用实际运行 daemon 的普通账号，并安装 Ubuntu 的 systemd（支持 `ExitType=cgroup`，systemd 250+）和 bubblewrap。管理员运行 `deploy/elephant-job-user-setup.sh ACCOUNT` 开启 linger 与 user manager。它不授予 daemon sudo 权限，不创建 root Job。控制命令使用该账号的 `/run/user/UID/bus`，daemon 账号必须与 user manager 账号一致。没有 user manager、bubblewrap 或 Linux 时，能力握手明确拒绝持久 Job，不退化成声称可跨重启的子进程。

配置脚本同时写入 daemon drop-in，依赖指定 UID 的 user manager，并把它的运行目录挂入 `ProtectHome=tmpfs` 沙箱。`ProtectHome=true` 下即使 user manager 已运行也可能无法连接，不能只在服务外以同账号执行命令判断可用性。

正式启用前，在真实 daemon 内验证一个写测试文件并等待的有期限 Job：记录 job_id/InvocationID，重启 daemon，确认同一单位和进程仍执行、没有第二次写入。等待完成并回读退出码与日志。再测试带子进程的 Job 取消、期限到达、错误退出、任务隔离和 user-manager/bubblewrap 不可用。文件检查应使用相同 workspace。离线 fake-systemd 契约测试不替代这些主机验收。
