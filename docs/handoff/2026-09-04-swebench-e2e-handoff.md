# Handoff — Witness SWE-bench 端到端验证与仓库工具（2026-09-04）

> 给下一个接手的人/会话：本文档描述当前工作树里**未提交**的改动、两次真实 run 的证据、
> server3 的操作手册，以及按性价比排好序的下一步。读完这一页应该能直接继续干活。

## 0. TL;DR

- 用 SWE-bench Verified 的 `pytest-dev__pytest-7490`（"15 min – 1 hour"）在 server3 上跑通了
  Witness 的完整链路：PostgreSQL journal → 隔离 worktree → `gpt-5.5`（Responses API）→
  真实 `kill -9` → 跨进程 Resume → 完成。两次 run 都 **resolved**（FAIL_TO_PASS 2/2，PASS_TO_PASS 78/78）。
- 修了一个默认配置下必现的 bug（Responses 流式路径回放 SDK 私有字段 `parsed_arguments`）。
- 新增 runtime 内置仓库工具 `react_agent.repo_tools`（7 个工具 + `CommandRunner` seam）和框架层
  `ToolError`；Web 工作台配置了仓库时自动注册。
- 本地验证：`uv run --extra debug --extra dev python -m pytest -q` → **358 passed, 18 skipped**；
  `ruff check`、`mypy src/react_agent`（strict）干净。
- 所有改动**尚未提交**。建议拆三个 commit（见 §2）。

## 1. 本次改动（未提交）

| 文件 | 变更 |
| --- | --- |
| `src/react_agent/provider.py` | 新增 `_wire_output_item()`：从流式 `ParsedResponse` 的输出项中剥掉 SDK 本地字段 `parsed_arguments` / `parsed` 再存入 `raw_items`（**bug 修复**） |
| `tests/test_openai_contract.py` | `test_responses_stream_emits_text_and_tool_deltas_from_typed_events` 增加 `raw_items` 回归断言（修前失败） |
| `src/react_agent/repo_tools.py`（新） | `create_repository_tools()`、`LocalCommandRunner`、`ContainerCommandRunner`、`RepositoryToolError` |
| `src/react_agent/tools.py` | 新增 `ToolError`：工具作者显式声明"这条错误消息给模型看"，`ToolRegistry.execute` 透传其 `code/message/retryable`；其他异常仍为不透明的 `TOOL_EXCEPTION` |
| `src/react_agent/web.py` | `_tools_from_env()` / `_agent_config_from_env()`：设置 `REACT_AGENT_REPOSITORY` 时注册仓库工具并切换到仓库任务预算（60 步 / 200 调用 / 1 h）；新增 `REACT_AGENT_COMMAND_APPROVAL` |
| `src/react_agent/__init__.py` | 导出 `ToolError`、`create_repository_tools`、`LocalCommandRunner`、`ContainerCommandRunner`、`CommandRunner`、`CommandResult`、`RepositoryToolError` |
| `tests/test_repo_tools.py`（新，20 例）、`tests/test_tool_contract.py`（+1）、`tests/test_runtime_web.py`（+1） | 覆盖路径逃逸/符号链接/`.git`/敏感文件、读范围与二进制守卫、`edit_file` 唯一性与 `already_applied` 幂等、环境变量清洗（断言 `OPENAI_API_KEY` 不进命令环境）、超时杀进程组、`run_tests` 参数 shlex 引用、manifest 稳定性、`ToolError` 透传 vs 普通异常不透明、Web 注册 |
| `README.md`、`.env.example` | 新增「内置仓库工具」一节、能力表一行、`REACT_AGENT_COMMAND_APPROVAL` |
| `analysis_outputs/swebench_e2e_20260904/` | 两次 run 的工件、harness、日志（已确认不含密钥），见其 `README.md` |

注意：工作树里还有大量**更早**的未提交/未跟踪文件（context governance、runtime debugging、project PR
等，`git status` 里 30+ 个 `??`）。它们不是本次的产物，但也需要人来提交。

## 2. 建议的提交拆分

1. `fix(provider): strip SDK-local parsed fields from replayed Responses items` — `provider.py` + `test_openai_contract.py`
2. `feat(tools): built-in repository tools and model-facing ToolError` — `repo_tools.py`、`tools.py`、`web.py`、`__init__.py`、三个测试文件、README、`.env.example`
3. `docs(evidence): SWE-bench pytest-7490 end-to-end runs with crash/resume` — `analysis_outputs/swebench_e2e_20260904/` + 本文档

## 3. 发现与根因（按重要性）

### 3.1 已修复：Responses 流式路径在第 2 步必挂
- 现象：run 在 `model_calls=2` 以 `model_error` 终止；provider 返回 `400 Unknown parameter: 'input[2].parsed_arguments'`。
- 根因：`AgentRuntime` 总是走 `provider.py` 的 `_complete_responses_stream`（为了 live delta），
  `stream.get_final_response()` 返回 SDK 的 `ParsedResponse`；对 **strict** function tool，SDK 在客户端给
  `function_call` item 加 `parsed_arguments` dict。原来的 `model_dump(exclude_none=True)` 把它存进
  `raw_items` 并在下一轮原样回放，provider 拒绝。
- 为什么之前没发现：非流式路径不受影响；README/`.env.example` 的验收全用 `chat_completions` + compat；
  单元测试从未断言 `raw_items`。

### 3.2 未修复，但已量化
- **瞬时模型错误 = 终态。** `agent.py:1157` 的 `except ModelInvocationError` 分支直接写
  `terminal_decision: True` 并 `finish(FAILED, MODEL_ERROR)`；`runtime.py:3378` 的恢复判定也只认终态。
  Resume 对终态 run 是 no-op，唯一出路是 Fork。SDK `max_retries=2` 用完后的一次 429/5xx 就能永久杀死长任务。
- **Resume 不等待旧 lease。** SIGKILL 后 `submit(ResumeRun)` 抛 `RuntimeConflict("the run already has a live writer lease")`，
  调用方要自己轮询 ≥ `lease_ttl_s`（默认 30 s）；两次 run 分别用了 2 次和 5–7 次尝试。README 的 Resume 矩阵没写这条。
- **`run_command` 非幂等会让长任务停在 reconciliation。** run 4 里 41 次工具调用有 7 次是 `run_command`（17%），
  crash 落在这个窗口就要操作员介入。
- **worktree 只含 Git 跟踪文件。** `src/_pytest/_version.py`（setuptools_scm 生成、gitignore）不在隔离 worktree 里；
  run 3 里模型自己把它写出来，run 4 里模型花了 5 次 `run_command` 做 `sitecustomize` hack。评测副本也因此先报
  `resolved: false`，补文件后通过。server3 的主仓库已把它 `git add -f` 提交（commit `ec4933a6f`）。
- **Tier 2 生成式压缩从未触发。** 两次 run 峰值 projected context 约 12 万字符，84 次确定性淘汰（去掉 39 万字符）就够了，
  `compression_calls=0`。压缩路径在真实任务上是否损害解题**没有数据**。
- **成本账本全 unknown。** 42 条 `cost_recorded`：41 条 `price_not_found`（`PricingCatalog("unconfigured")`），
  1 条 `provider_completion_not_committed`。行为正确，但成本预算无从谈起。
- **`model_abandoned` 消耗一个 step**（被中断的 step 28 → 新 attempt 是 step 29）。
- **错误脱敏过头。** journal 里只有 `Model request failed (status=400): BadRequestError`，`error.code/param/request_id` 全丢；
  3.1 是靠 harness 外挂 httpx hook 才定位的。
- 小问题：`migrate()` 的 `migrations/*.sql` glob 会吃掉 macOS 的 `._*.sql`；`uv run pytest` 下
  `tests/test_context_ab_benchmark.py` 因 `benchmarks` 不在 `sys.path` 报 collection error（`python -m pytest` 正常）。

## 4. 两次 run 的关键数字

| | run_13016e96（harness 自带工具） | run_16393f75（内置 `repo_tools`） |
| --- | --- | --- |
| durable events | 400，hash chain 通过，删 snapshot 重建一致 | 392 |
| executions（进程数） | 3（kill 于 `run_tests` 中 → `tool_retry`；kill 于 model call 中 → `model_abandoned`） | 2（kill 于 `run_tests` 中 → `tool_retry`） |
| model calls / tool executions | 42 / 40 | 41 / 41 |
| input tokens（cached） | 1,072,292（745,472） | 类似量级 |
| public projection 泄漏 | 0 | 0 |
| 主仓库 | HEAD 与 clean 状态未变，88 个 checkpoint ref | 同 |
| SWE-bench | 2/2 + 78/78 | 2/2 + 78/78（评测副本需补 `_version.py`） |

补丁内容不同（前者在 `pytest_runtest_call` 后重算 xfail 计数，后者在 `makereport` 里重评估），都通过隐藏测试。

## 5. server3 操作手册

连接方式（详见 `~/.codex/AGENTS.md` 与记忆 `witness-swebench-e2e-on-server3`）：

```bash
# Linux 命令一律通过 stdin 传脚本，绕开 cmd.exe 的引号解析
ssh server3 'wsl.exe -d Ubuntu-24.04 --cd ~ -- bash -ls' < script.sh
```

**必须先开 keepalive**，否则 SSH 一断 WSL 就在 ~8 s 后关机，后台 worker 和无 restart 策略的容器一起死：

```bash
ssh -o ServerAliveInterval=15 server3 'wsl.exe -d Ubuntu-24.04 -- bash -c "exec sleep infinity"' &
```

server3 上的布局（用户 `zhanghongtao`，`~/witness-swebench/`）：

| 路径 | 内容 |
| --- | --- |
| `witness/` | 源码副本（含本次改动），`uv sync --extra dev --extra debug` 已完成 |
| `harness/swe_harness.py`、`harness/run.sh` | 驱动脚本；`run.sh` 固定 DSN / 镜像 / 模型，从 `.secrets.env`（0600）读 `OPENAI_API_KEY` |
| `repo/` | 从 SWE-bench 镜像抽出的 `/testbed`，已额外提交 `_version.py` |
| `worktrees/<session>/` | 各 session 的隔离 worktree（`pytest-7490-r3`、`-r4` 保留着） |
| `artifacts/<run_id>/` | report / evaluation / patch / events |
| `logs/` | worker 日志、`provider_http.ndjson`（含错误 body，无密钥） |
| 容器 `witness-swe-pg` | PostgreSQL 16，`127.0.0.1:55990`，`--restart unless-stopped`，库 `witness/witness` |
| 镜像 | `docker.1panel.live/swebench/sweb.eval.x86_64.pytest-dev_1776_pytest-7490:latest`（Docker Hub 直连不通，走 1panel 镜像） |

常用命令（都在 WSL 内）：

```bash
cd ~/witness-swebench
WITNESS_WORKER_ID=worker-A nohup harness/run.sh start --instance $HOME/witness-swebench/instance.json \
  --session <new-session> --key <new-key> > logs/x.log 2>&1 &
harness/run.sh status  --run-id $(cat current_run_id)
harness/run.sh resume  --run-id <run_id>            # 会自动轮询 lease，最多 90 s
harness/run.sh report  --run-id <run_id> --out artifacts/<run_id>
harness/run.sh evaluate --run-id <run_id> --instance $HOME/witness-swebench/instance.json --out artifacts/<run_id>
```

模拟 crash 时要杀 **`.venv/bin/python3`** 进程，不是 `uv run` 启动器：

```bash
pgrep -f "^/home/zhanghongtao/witness-swebench/witness/.venv/bin/python3 .*swe_harness.py"
```

同步本地改动到 server3：`tar czf - <files> | ssh server3 'wsl.exe -d Ubuntu-24.04 --cd ~ -- bash -c "cd ~/witness-swebench/witness && tar xzf - --no-same-owner"'`，
之后 `find . -name "._*" -delete`（macOS tar 会带 AppleDouble 文件，migration 会被它绊倒）。

换实例：从 `/tmp/s3/verified.parquet`（Mac 上，来自 hf-mirror 的 `princeton-nlp/SWE-bench_Verified`）抽一行写成
`instance.json`，镜像名规则 `swebench/sweb.eval.x86_64.<repo>_1776_<repo>-<id>`，`harness/run.sh` 里改 `WITNESS_SWE_IMAGE`，
`repo/` 重新 `docker cp` 抽取，记得检查 gitignore 的生成物。

## 6. 下一步（按性价比，面向长程任务）

1. **瞬时模型错误不应终态**（半天–一天）。改 `agent.py:1157` 的 `ModelInvocationError` 分支：按 provider 错误分类
   （429/5xx/timeout/连接 → 可重试，有界退避后发新 attempt，落 `model_failed(terminal_decision=false)`；4xx 语义错误 → 终态）。
   `provider.py` 需要把 status/`error.code`/`error.param`/`request_id` 带到 `ModelInvocationError` 上并写入 private payload。
   `runtime.py:3378` 的恢复判定要认识非终态的 `model_failed`。
2. **`run_command` 允许声明 `read_only`**（`repo_tools.py`，几小时）：`read_only=True` 走 `IDEMPOTENT_RETRY`，否则 `REQUIRE_OPERATOR`。
3. **Supervisor**（一天）：定期 `store.list_runs` 找非终态且 lease 过期的 run，用同一 agent 构造 `AgentRuntime` 提交 `ResumeRun`；
   `NEEDS_RECONCILIATION` 按策略处理或告警。它把"kill -9 后 30 s 自愈"变成一键 demo。
4. **压一次 Tier 2**（一次 run，约 100 万 token）：`CONFIG.max_context_chars` 降到 60,000 再跑同一实例，看
   `compression_calls`、`hard_fallbacks` 和是否仍 resolved。用数据决定上下文治理是否继续投入。
5. **预算语义**：pricing catalog 示例 + 按 token/成本/墙钟设预算；`model_abandoned` 不吃 step。
6. **worktree 环境保真**：`GitWorktreeWorkspace.create()`（`workspace.py:372`）后执行一次 seed 命令或复制指定 ignored 路径。
7. 文档：README Resume 矩阵补"旧 worker 崩溃：lease 未过期前拒绝，TTL 默认 30 s"；`migrate()` glob 排除点文件；
   `pyproject` 加 `pythonpath = ["."]`。

## 7. 验证命令

```bash
uv run --extra debug --extra dev ruff check src tests
uv run --extra debug --extra dev mypy src/react_agent
uv run --extra debug --extra dev python -m pytest -q      # 358 passed, 18 skipped（PG 集成测试需 server2/server3 的 DSN）
```

PostgreSQL 集成测试可用 server3 的容器：`TEST_POSTGRES_DSN=postgresql://witness:witness@127.0.0.1:55990/witness`（在 WSL 内）。

## 8. 密钥与边界

- 模型 endpoint：`https://su.kelaode.sbs:8443/v1`，可用模型 `gpt-5.5` / `gpt-5.6-terra` 等；key 只存在 server3 的
  `~/witness-swebench/.secrets.env`（0600）和用户手里，**不在仓库、工件或日志中**。
- 该 relay 偶尔不返回 `reasoning` item；Witness 能容忍（`include=["reasoning.encrypted_content"]` 缺失时正常）。
- 两次 run 的 `run_command` 在容器内无网络、以宿主用户身份运行；主仓库从未被 Agent 修改。
