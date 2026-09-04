# Handoff — 长程任务韧性：模型重试、read_only 命令、Supervisor、worktree 播种（2026-09-04 晚）

> 接着 [2026-09-04-swebench-e2e-handoff.md](2026-09-04-swebench-e2e-handoff.md) §6 往下做的结果。
> 本文档记录做了什么、证据在哪、以及**还没做**的两件事（其中一件需要用户决定是否花钱）。

## 0. TL;DR

上一份 handoff §6 的 1、2、3、5（step 消耗部分）、6、7 已完成，五个 commit：

| commit | 内容 |
| --- | --- |
| `af03f91` | 瞬时模型错误在同一 execution 内有界重试；重试耗尽 → `MODEL_UNAVAILABLE`，**不写 `run.completed`**，run 保持可 Resume |
| `5d47491` | `Tool(call_resume_policy=...)` 逐调用决定 resume 策略；`run_command(read_only=true)` 崩溃后自动重试 |
| `e58070e` | `RunSupervisor` + `journal.list_orphaned_runs()`；Web `REACT_AGENT_SUPERVISOR=true`；`examples/chaos_resume.py --supervisor` |
| `04f7e29` | `model_abandoned` 不再吃 step（原因：Resume 先写 abandoned 再建 resume state，pending 已被弹出）；`migrate()` 跳过 `._*.sql` |
| `3d2477c` | `GitWorktreeWorkspace(seed_paths=, seed_command=)`：新 worktree 复制被忽略的生成物并执行一次 setup |

验证：本地 `python -m pytest` **471 passed, 3 skipped**（Node.js / PG）；server3（PostgreSQL 16 容器）
**488 passed, 3 skipped**，含三个真实 `kill -9` 的 chaos 测试。`ruff` / `mypy --strict` 干净。

## 1. 语义变化（读代码前先知道这些）

### 1.1 `model_failed` 不再总是终态
- `ModelInvocationError` 新增 `status_code / error_code / error_param / retryable`。provider 分类：连接失败、
  超时、408/409/429、5xx、Responses `server_error` / `rate_limit_exceeded` → retryable；其他 4xx、解析错误 → 否。
  错误**文本**永不进 message（只有结构化 code/param），`error_param` 与 `error` 只进 private payload。
- `AgentConfig.model_retry_limit=3 / model_retry_backoff_s=2 / model_retry_max_backoff_s=30`（Web 仓库任务 profile：5 / 60）。
  重试 = 同一 step 的新 attempt，journal 里是 `model_failed(terminal_decision=false, retry_in_ms)` → `model_started`。
  attempt 1 保留旧 operation id，attempt ≥2 用 `model:s{N}:a{k}:started:{exec}`，否则 journal 的幂等 append 会把它们折叠掉。
- 重试耗尽 → `AgentResult(status=FAILED, stop_reason=MODEL_UNAVAILABLE)`，**agent 不调用 `finish()`**（无 `run.completed` 事实；
  内存事件流仍以 `RUN_COMPLETED` 收尾）。Runtime `_finalize` 见到它只释放 lease。`_recover_pre_result_terminal` 把
  `terminal_decision is False` 视为"非决定"；`_build_resume_state` 从同一 step 重试；`resume_reason="model_retry"`。
- 重试预算是**每 execution** 的，跨进程 Resume 从 0 开始。Supervisor 用 `max_executions_per_run` 在 run 级别封顶。

### 1.2 逐调用 resume 策略
- `Tool.resume_policy_for(call)`：有 `call_resume_policy` hook 时用校验后的参数决定，hook 出错/参数无效/返回类型错 → 回落静态策略。
  agent 在 `tool_planned` / `tool_started` 里记录**决定后**的策略，Runtime 恢复逻辑不变。manifest hash 含 `call_resume_policy` 布尔。
- `run_command` 多了必填 `read_only: bool`；`REPOSITORY_TOOLS_VERSION` 升到 `repo-tools-v2`（→ 旧 run 不能 Resume，符合设计）。
  注意 pydantic lax 模式会把 `"yes"` 当 `True`——hook 看到的就是工具会执行的参数，一致即可。

### 1.3 Supervisor
- `RunJournal.list_orphaned_runs(limit, agent_revision)`：非终态、lease 缺失或过期，**最新更新在前**，按 agent binding revision 过滤。
  共享数据库里有别的部署/测试留下的大量孤儿（包括故意损坏的 journal），这个过滤和"unreadable 只是一种 outcome"都是被它逼出来的。
- 上报的 outcome（`needs_reconciliation / resume_rejected / resume_budget_exhausted / unreadable`）每 (run, outcome) 只经 `on_attention` 一次；
  同一 run 连续 Resume 之间进程内指数退避。`GET /api/supervisor`、`POST /api/supervisor/sweep`。

### 1.4 worktree 播种
- `seed_paths` 必须是被 Git 忽略的路径（跟踪文件、符号链接、敏感文件都拒绝），复制后仍被忽略，不进 checkpoint/patch；
  `seed_command` 失败 → 整个 worktree 丢弃。`.venv` 不能这样播种（含逃逸符号链接），工具链走 `CommandRunner` 镜像。

## 2. server3 现状

- `~/witness-swebench/witness/` 已同步到 `3d2477c` 的 src/tests/examples；`harness/swe_harness.py` 已更新：指令里教模型
  `read_only=true`、`WITNESS_MAX_CONTEXT_CHARS` 环境变量覆盖 `max_context_chars`、新增 `supervise` 子命令
  （worker B 不知道 run id，靠 Supervisor 找孤儿）。`harness/run.sh status` 对旧 run 正常；`supervise` 对旧 run 正确地什么都不做（旧 revision 被过滤）。
- `examples/chaos_resume.py --supervisor` 在 server3 上跑通，输出见本次会话（17 个事件、hash chain 校验通过、executions=2）。
- 本仓库 `analysis_outputs/swebench_e2e_20260904/harness/` 是 server3 harness 的镜像副本，已同步更新。

## 2.5 下午补记：OpenTelemetry 通电 + Tier 2 压测已做（commit `e1b805d` + 证据 commit）

用户点头后跑了三条真实 run，证据在 `analysis_outputs/swebench_e2e_20260904/README.md` 下半部分和三个
`run_*_otel/` 目录（每个含 `otel/jaeger_traces.json`、`collector_metrics.prom`、`prometheus_queries.json`）。结论：

- **Tier 2 在 60k 下是病态的**：44 次压缩（gpt-5.5，每次 ~104 s）吃掉 93 分钟里的 64 分钟，其中 38 次压完仍超预算 → hard fallback；
  step 40 时被操作员 `CancelRun` 终止（`run_aborted`，未 resolved）。**120k 下 0 次压缩、0 次 hard fallback，3.5–6 分钟 resolved**，
  126k 峰值靠 Tier 1 的 73 次确定性淘汰就够。决定：不降默认预算，不再给压缩质量投入。
- **§1.1 的机制在真实世界触发了两次**：relay 两次 503 风暴（`server_is_overloaded`）→ 4 次 retryable attempt 退避 → `retry_exhausted`
  → lease 释放 → `run_resumed(resume_reason=model_retry)` 从同一 step 续跑。顺带发现 harness 的 `supervise` 接管一次就不再扫描
  （run 空等 18 分钟），已改成持续扫描到终态。
- **OTel**：真实 Collector + Jaeger + Prometheus（server3，1panel 镜像拉的 `jaegertracing/all-in-one:1.65.0`、
  `otel/opentelemetry-collector-contrib:0.120.0`、`prom/prometheus:v3.1.0`）。崩溃 run 在 Jaeger 里是两条 trace：被 kill 的 execution
  只有子 span（根 span 没机会结束），恢复的 execution 根 span 带 `execution.kind=resume` 和指向前者的 link。
  修了两处：tail sampling 在第一个 span 到达 5 s 后就决定，根 span 独有的属性永远赶不上 → 子 span 现在都带 `execution.kind`；
  成功 trace 抽样率改成 `WITNESS_OTEL_SUCCESS_SAMPLE_PERCENT`（默认 10，证据 run 用 100）。model span 多了
  `react_agent.model.{status_code,error_code,retryable,retry_exhausted,execution_attempt}`。
  没修：httpx 自动插桩的 `POST /v1/responses` span 是独立 trace，没挂在 `chat` 下（适配器 `start_span` 后没激活 context）。
  Prometheus 的 series 在 worker 进程退出几分钟后过期，事后总量以 journal 为准。
- server3 上 `~/witness-swebench/harness/run.sh` 多了 `WITNESS_OTEL=1` 开关；观测栈用 `--restart unless-stopped` 常驻。
  WSL keepalive 的 ssh 中途断过一次（exit 255）——用 `-o ServerAliveCountMax=6` 重开后没再断，但它仍是单点，长任务前先看它活着。

## 2.6 傍晚补记：Tier 2 重做成填表式滚动工作状态（commit `0fca6cd`）

用户看了 60k 数据后指出两点：压缩每步从头重压整段历史不对；这个场景要的是填表式摘要，不是散文。重做后：

- `working_state.py`：**goal**（首条用户消息原样）+ **ledger**（读/改/跑 + 结果，框架按 `ToolContextPolicy` 从
  transcript 机械生成，零模型调用）+ **notes**（findings/hypothesis/next_steps/open_questions，每格硬上限，模型只做
  "旧表 + 新增几轮 → 新表"的增量更新，工具输出先截成预览）。
- 状态沿 turn-group 边界对 canonical transcript 做链式哈希；任何进程从 store 找最新一份 notes 接着折，同一前缀不重压。
  `ContextCompressor.compress(request)` seam 不变，request 多了 `previous_summary` / `ledger`。没配 compressor 或
  压缩失败时机械表单照常替换前缀。hard fallback 先把旧轮次工具输出降为预览再抹。`CONTEXT_ALGORITHM_VERSION=working-state-v5`。
- 离线回放证据（同一 60k run 的 40 个 step transcript，`analysis_outputs/.../replay_tier2_working_state/`）：
  每次压缩输入 5.9k 字符（旧 35k→202k）、`gpt-5.4-mini` 平均 **7.3 s**（旧 gpt-5.5 104 s）、hard fallback **0**（旧 38）。
  harness 新增 `WITNESS_COMPRESSION_MODEL`（推荐 `gpt-5.4-mini`）和 `run.sh replay-context`。
- 还没做：用新 Tier 2 **真跑**一次 60k（现在便宜了，一条命令：`WITNESS_COMPRESSION_MODEL=gpt-5.4-mini
  WITNESS_MAX_CONTEXT_CHARS=60000 harness/run.sh start ...`）；`search_text` 的 `ToolContextPolicy` 仍是 OPAQUE，ledger 里
  只显示次数不显示 pattern。

## 3. 没做的（按优先级）

1. ~~Tier 2 压测~~ 已做，见 §2.5。
2. **`workspace_tools` / `repo_tools` 统一（上一份 §0.5）。** 刻意没做：要改 `demo.py` 的 scripted model、`evals.py`、
   `static/assets/js/projection.js` 标签、`tests/test_workspace_tools*.py`、`tests/test_demo_fixture.py`、README 崩溃演示里的事件清单和
   DESIGN.md，收益是整洁而不是可验证的能力。建议路径：让 `repo_tools` 的读写工具接受 `workspace_tools` 的返回约定
   （`status: ok/denied/not_found` 结构化而非抛 `ToolError`）之后再删 `workspace_tools.py`，一次 commit 全换。
3. **预算语义（上一份 §6.5 剩余）**：pricing catalog 示例 + 按 token/成本/墙钟设预算。两次 run 的 42 条 `cost_recorded` 全是
   `price_not_found`，`PricingCatalog("unconfigured")`。
4. 小：`RunSupervisor` 的退避状态是进程内的；如果以后有多个 supervisor 进程，把"上次 Resume 时间"落到 journal 或 store。
5. 小：telemetry 适配器在模型/工具调用期间激活 span context，让 httpx 自动插桩的 provider HTTP span 成为 `chat` 的子 span。

## 4. 验证命令

```bash
uv run --extra debug --extra dev ruff check src tests examples
uv run --extra debug --extra dev mypy src/react_agent
uv run --extra debug --extra dev python -m pytest -q           # 471 passed, 3 skipped
# server3（WSL 内）：TEST_POSTGRES_DSN=postgresql://witness:witness@127.0.0.1:55990/witness → 488 passed
```
