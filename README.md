# sandbox-job-runtime

面向不可信提交内容的隔离执行服务。提交方通过 HTTP 接口投递任务，服务负责
排队调度、隔离执行、资源约束与结果回传。默认路径完全在本地单机复现，
不依赖任何外部服务（隔离能力来自 Linux 自带的 `unshare` / `setpriv` /
`prlimit` 与独立进程组）。

## 快速开始

```bash
uv sync
uv run python main.py          # 监听 127.0.0.1:8000
uv run python -m pytest tests/ # 运行测试
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/jobs` | 提交任务，返回 `job_id`（永远入队，不因负载拒绝） |
| GET | `/jobs/{job_id}` | 查询状态与结果 |
| POST | `/jobs/{job_id}/cancel` | 取消排队中或运行中的任务 |
| GET | `/capabilities` | 隔离能力、降级说明与并发策略（可观测性） |
| GET | `/healthz` | 健康检查 |

提交示例：

```json
{
  "submitter": "alice",
  "command": ["python3", "-c", "print('hi')"],
  "env": {"MY_VAR": "1"},
  "limits": {
    "wall_timeout_s": 30,
    "cpu_time_s": 20,
    "memory_bytes": 268435456,
    "max_output_bytes": 1048576
  },
  "allow_network": false
}
```

## 生命周期与状态语义

```
queued -> running -> succeeded | failed | cancelled
queued -> cancelled
```

- 状态流转**单调**：终态（succeeded/failed/cancelled）不可再变更；存储层
  用显式迁移表在 SQL 层面强制（`store.py` 的 `_ALLOWED`）。
- 任意时刻查询到的都是某个完整状态：状态与结果在同一行内原子更新。
- 失败原因可区分（`failure_kind`）：`non_zero_exit`、`signalled`、
  `wall_timeout`、`cpu_limit`、`memory_limit`、`output_limit`、
  `spawn_error`、`internal_error`。

## 调度

- 全局并发上限 `global_concurrency` 与单提交方并发上限
  `per_submitter_concurrency`；超限的提交进入队列而非被拒绝。
- 先到先服务：按持久化的提交序号派发；当队首提交方达到其个人上限时，
  调度器会跳过它选取队中第一个可运行的任务，避免单个提交方堵死全队
  （在可运行集合内仍保持 FCFS，长任务不会饿死其他提交方）。

## 隔离边界

每个任务在**包装链**中执行：

```
[unshare --net --] [prlimit ... --] [setpriv ... --] <command>
```

- **工作目录**：每个任务独立目录（`<data_dir>/work/<job_id>`），结束后删除。
- **环境变量**：仅保留最小 `PATH`/`HOME`/`TMPDIR`/`LANG`；提交方可注入
  自定义变量，但 `PATH`、`HOME`、`TMPDIR`、`LD_PRELOAD`、`LD_LIBRARY_PATH`
  被强制过滤。
- **进程组**：任务在独立进程组中运行，终止时整组 `SIGKILL`，不残留进程。
- **网络**：默认关闭。提交方需显式 `allow_network: true` 声明，且服务端
  策略 `SANDBOX_NETWORK_POLICY_ALLOW=1` 同时允许才会放行；未放行时任务在
  独立网络命名空间（无网络）中运行。
- **资源约束**（四维，任一越界即终止并给出可区分原因）：
  - 墙钟超时：看门狗线程按 deadline 终止；
  - 处理器时间：`prlimit --cpu`（SIGXCPU，归类 `cpu_limit`）；
  - 内存：进程组 RSS 轮询（归类 `memory_limit`），`RLIMIT_AS` 仅作兜底；
  - 输出体积：stdout/stderr 读取线程计数，超限即杀（归类 `output_limit`），
    文件写入由 `RLIMIT_FSIZE` 兜底。

## 降权策略

- 服务以 root 运行且配置了降权目标（默认 `nobody`，uid/gid 65534）时，
  任务通过 `setpriv --reuid --regid --clear-groups` 以低权限身份执行，
  隔离模式为 `full`。
- 条件不具备（非 root、未配置 uid/gid、缺少 `setpriv`）时**显式降级**为
  `degraded`：任务仍以服务自身身份运行，但降级原因会出现在
  `GET /capabilities` 的 `notes` 与每个任务结果的 `notes` 字段中，
  绝不静默放行。
- 网络命名空间不可用时会同样以 `notes` 形式显式暴露。

## 取消与异常隔离

- 取消排队中的任务立即生效；取消运行中的任务通过取消事件触发整组
  `SIGKILL`，工作目录随即回收，不影响其他任务。
- 非零退出、信号终止、超时、资源越界、启动失败与未知异常分别归类回传；
  执行器与调度工作线程均不向外抛异常，单个任务的任何异常都不会拖垮
  调度器。

## 重启恢复

- 所有状态持久化在 SQLite（`<data_dir>/jobs.db`，WAL + 同步提交）。
- 重启后：处于 `queued`/`running` 的未完成任务被重新入队执行（running
  先回退为 queued）；已终态的任务原样保留，**不会**重复执行已成功任务，
  也**不会**把未完成任务当作成功返回。

## 配置

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `SANDBOX_DATA_DIR` | `./sandbox_data` | 数据目录（SQLite + 工作目录根） |
| `SANDBOX_GLOBAL_CONCURRENCY` | `4` | 全局并发上限 |
| `SANDBOX_PER_SUBMITTER_CONCURRENCY` | `2` | 单提交方并发上限 |
| `SANDBOX_NETWORK_POLICY_ALLOW` | `0` | 服务侧网络放行总开关 |

任务级资源上限还会被服务端天花板钳制（`Settings.max_*`），防止提交方
申请无限资源。

## 已知边界

- 隔离强度依赖本机能力：无 `unshare`/`setpriv`/`prlimit` 时对应维度降级
  并显式可观测；磁盘用量无独立配额（仅 `RLIMIT_FSIZE` 限制单文件）。
- 内存限制以进程组 RSS 轮询（50ms 间隔）执行，瞬时尖峰可能略微超限才
  被终止；`RLIMIT_AS` 作为兜底。
