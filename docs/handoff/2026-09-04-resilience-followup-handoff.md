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

## 3. 没做的（按优先级）

1. **Tier 2 压测（上一份 §6.4）——需要用户点头再花钱。** 约 100 万 token 的真实 `gpt-5.5` run。准备工作已完成，一条命令：
   ```bash
   cd ~/witness-swebench && WITNESS_MAX_CONTEXT_CHARS=60000 WITNESS_WORKER_ID=worker-A \
     nohup harness/run.sh start --instance $HOME/witness-swebench/instance.json \
     --session pytest-7490-tier2 --key tier2-1 > logs/tier2.log 2>&1 &
   ```
   看 `report` 里的 `compression_calls / hard_fallbacks` 和 `evaluate` 是否仍 resolved。顺便可以验证 `read_only=true` 的使用率
   （`tool_started.resume_policy` 里 `idempotent_retry` 的占比）和有没有触发 `model_failed(retryable=true)`。
   记得先开 WSL keepalive（见上一份 §5）。
2. **`workspace_tools` / `repo_tools` 统一（上一份 §0.5）。** 刻意没做：要改 `demo.py` 的 scripted model、`evals.py`、
   `static/assets/js/projection.js` 标签、`tests/test_workspace_tools*.py`、`tests/test_demo_fixture.py`、README 崩溃演示里的事件清单和
   DESIGN.md，收益是整洁而不是可验证的能力。建议路径：让 `repo_tools` 的读写工具接受 `workspace_tools` 的返回约定
   （`status: ok/denied/not_found` 结构化而非抛 `ToolError`）之后再删 `workspace_tools.py`，一次 commit 全换。
3. **预算语义（上一份 §6.5 剩余）**：pricing catalog 示例 + 按 token/成本/墙钟设预算。两次 run 的 42 条 `cost_recorded` 全是
   `price_not_found`，`PricingCatalog("unconfigured")`。
4. 小：`RunSupervisor` 的退避状态是进程内的；如果以后有多个 supervisor 进程，把"上次 Resume 时间"落到 journal 或 store。

## 4. 验证命令

```bash
uv run --extra debug --extra dev ruff check src tests examples
uv run --extra debug --extra dev mypy src/react_agent
uv run --extra debug --extra dev python -m pytest -q           # 471 passed, 3 skipped
# server3（WSL 内）：TEST_POSTGRES_DSN=postgresql://witness:witness@127.0.0.1:55990/witness → 488 passed
```
