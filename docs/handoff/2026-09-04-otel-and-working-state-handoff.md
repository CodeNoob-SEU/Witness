# Handoff — OpenTelemetry 落地证据 + Tier 2 改成填表式滚动工作状态（2026-09-04 晚）

> 接着 [2026-09-04-resilience-followup-handoff.md](2026-09-04-resilience-followup-handoff.md) 往下做的结果。
> 数字与工件都在 [`analysis_outputs/swebench_e2e_20260904/README.md`](../../analysis_outputs/swebench_e2e_20260904/README.md)，
> 这里只记"为什么"、"改了哪里"、"还没做什么"。

## 0. TL;DR

| commit | 内容 |
| --- | --- |
| `e1b805d` | OTel 适配器：子 span 也带 `react_agent.execution.kind`（否则 Collector 的 tail sampling 永远匹配不到长 run 的 resume）；模型 span 带 `react_agent.model.{status_code,error_code,retryable,retry_exhausted,execution_attempt}`；成功 trace 采样率改成 `WITNESS_OTEL_SUCCESS_SAMPLE_PERCENT`（默认 10，证据 run 用 100） |
| `9e5fd4d` | server3 上三次真实 run 的证据：60k 压测（病态）、120k（3.5 min resolved）、120k + `kill -9` + supervisor 接管（6 min resolved）；每个 run 目录下有 Jaeger trace 导出、Collector 指标快照 |
| `0fca6cd` | **Tier 2 重做**：散文摘要 → 填表式滚动工作状态（goal / 机械 ledger / 模型 notes），链式哈希缓存，hard fallback 先降级预览再抹 |
| `8e994a5` | 用 60k run 的 40 份逐步 transcript 离线回放新 Tier 2：每次压缩 5.9k 字符（旧 35k→202k），7.3 s（旧 104 s），0 次 hard fallback（旧 38） |
| `3b41bbb` | `FileContextSummaryStore` 在构造时就拒绝非私有目录（之前只在第一次 put 时静默失败，导致今天所有 run 都没持久化过摘要）；harness 用 0700 建目录 |
| 证据 | **新 Tier 2 的真实 60k run**：`run_9c94f307_60k_form_otel` **resolved**（2/2 + 78/78），每次压缩 5.3k 字符 / 7.9 s，压缩 token 110k（旧 ~1.5M），总输入 549k（旧 1.76M），0 次 hard fallback；对照 `run_d9b1a4dc_60k_form_ledger_only`（notes 没到模型手里）0/2 |

验证：`python -m pytest` 本地全绿；server3（PostgreSQL 16）全绿。`ruff` / `mypy --strict` 干净。

## 1. 为什么重做 Tier 2（读代码前先知道）

60k 压测 run（`run_bd04f7b3_tier2_60k_otel`）89 分钟里 64 分钟在做压缩，解题本身 4.3 分钟。根因不是"压缩质量"，是设计：

1. 缓存键 = 前缀哈希，而前缀 = 全部历史减最近 3 轮，每步都变 → 40 步 0 次命中，每步把整段原始历史（step 40 时 20 万字符）重新喂给 gpt-5.5 写一篇 1.2 万字散文。
2. 散文不可核对、不可增量更新；模型下一步还得从散文里重新找结构。
3. 表里一半以上的字段（读过什么、改过什么、跑过什么、结果如何）根本不需要模型——工具返回的是结构化 JSON。
4. `ModelContextCompressor(model)` 直接复用主模型（reasoning effort high）；summary + 最近 3 轮仍超预算时 hard fallback 把**最新**的工具输出抹掉，模型于是把同一个文件反复读了 26 次。

## 2. 新 Tier 2 的形状（`src/react_agent/working_state.py` + `context.py`）

替换被压缩前缀的那条 `UserMessage` 现在是一张表：

```
[working state; replaces transcript items 0..N; state_sha256=…]
## Goal            ← 原始请求逐字保留（上限 max_goal_chars，截断有显式标记）
## Ledger          ← 机械生成：Read / Edited / Executed / Other calls，带次数与最后结果（exit code、timed out、already applied、error code）
## Notes           ← 模型写：findings / hypothesis / next_steps / open_questions，每格有硬上限
```

- **ledger 零模型调用**。来源是 transcript 里配对的 tool call/result 和工具声明的 `ToolContextPolicy`（effect + identity_fields），所以是工具无关的：任何按规范声明了 policy 的工具都会被正确归类；没声明的落到 "Other calls"。
- **notes 增量更新**。`ContextCompressionRequest` 多了 `previous_summary` 和 `ledger`；`ModelContextCompressor` 把"上一份 notes + 从那之后的几轮（工具输出降级为预览）"交给模型，要求返回 JSON，解析→约束→渲染。超过 `max_source_chars` 时按块顺序折叠而不是 map/reduce。
- **链式哈希**。`chain_hashes()` 在规范 transcript 的每个 turn-group 边界上算 `h_k = H(h_{k-1}, group_k)`；`summary_key` 用的是这个链上的哈希。任何进程都能沿链往回找到最近一份已持久化的 notes，只压缩新增的切片；同一前缀永不重压。Resume 到另一台机器时只要 `FileContextSummaryStore` 目录共享就能接上。
- **没有 compressor 或压缩失败时**仍然用机械表替换前缀（旧实现是直接 hard fallback）。`test_corrupt_persisted_summary_fails_closed_to_hard_fallback` 等测试的期望相应改了：损坏的 summary 不用，但 goal + ledger 照常。
- **hard fallback** 现在先把 tail 里较旧轮次的工具输出降成 head/tail 预览（`preview_chars`），还不够才抹成标记。
- `CONTEXT_ALGORITHM_VERSION = "working-state-v5"`：旧的持久化摘要文件会被拒绝（设计如此）。
- 压缩模型可以单独指定：`ModelContextCompressor(cheap_model)`；harness 用 `WITNESS_COMPRESSION_MODEL=gpt-5.4-mini`。

`ContextCompressor.compress(request) -> ContextCompression` 这个 seam、`FileContextSummaryStore` 的内容寻址/权限校验、journal 里的 compression 生命周期事件和成本核算都没变。`benchmarks/context_ab.py` 的离线 A/B 重新生成过，聚合数字不变。

## 3. OTel 现在的状态

- 代码 + `deploy/otel/` + `docker-compose.observability.yml` 在 server3 上真实跑通：Collector → Jaeger / Prometheus，`opentelemetry-instrument` 包着 harness。三个 run 的 `otel/jaeger_traces.json` 里能看到：崩溃的 execution（只有子 span，根 span 永远没结束）和 supervisor 接管的 execution（根 span `execution.kind=resume`，link 到前者）；模型 span 上能看到 503 重试的分类字段。
- 两个发现已修（`e1b805d`）：tail sampling 在**第一个** span 到达 5 s 后就做决定，根 span 要 run 结束才到，所以只挂在根上的属性永远匹配不到——现在子 span 也带；采样率可用环境变量覆盖。
- 未修：`httpx` 自动插桩的 `POST …/v1/responses` span 是独立的单 span trace，没挂在 `chat` 下面（适配器 `start_span` 后没把 span 设为当前 context）。只有 method/URL/status，没有密钥，导出时已排除。

## 4. server3 现状

- `~/witness-swebench/witness/` = HEAD 的 src/tests；`harness/swe_harness.py`、`run.sh` 与仓库里 `analysis_outputs/swebench_e2e_20260904/harness/` 一致（sha256 核对过）。
- 常驻容器：`witness-swe-pg`（PostgreSQL，55990）、`react-agent-observability-{otel-collector,jaeger,prometheus}`（Collector 以 `WITNESS_OTEL_SUCCESS_SAMPLE_PERCENT=100` 启动）。镜像走 `docker.1panel.live` 拉再 `docker tag` 成 compose 里的名字。
- `harness/run.sh` 新开关：`WITNESS_OTEL=1`、`WITNESS_COMPRESSION_MODEL=…`、`replay-context` 子命令（离线回放 Tier 2）。
- 记得 WSL keepalive（上一份 §5）。

## 5. 没做的（按优先级）

1. **60k 下仍然跑满 60 步。** 两次 60k run 都以 `max_steps` 结束（补丁已正确，模型在反复跑测试而不是作答）；120k 只用 22–33 步。原因是 `max_tool_output_chars=30k × keep_recent_turns=3` 在 60k 里放不下几轮原始输出，模型被迫重读（`skipping.py` x14）。可做的：tail 里较旧轮次的工具输出按 `preview_chars` 降级（现在只在 hard fallback 里做），或让 `read_file` 的 ledger 条目带一行"上次读到的关键内容"。半天，需要一次 60k run 验证。
2. **httpx span 挂到 `chat` 下面**：`OTelTelemetry._start_span` 用 `trace.use_span(span, end_on_exit=False)` 或在模型调用期间 attach context；几小时。
3. **ledger 里加失败用例名**：`run_tests` 的 stdout 里有 `FAILED …` 行，机械提取几条进 Executed 一节，模型就不用在 notes 里重复它们；半天。
4. `workspace_tools` / `repo_tools` 统一、预算语义、Supervisor 退避状态落库——同上一份 §3。

## 6. 验证命令

```bash
uv run --extra debug --extra dev ruff check src tests examples
uv run --extra debug --extra dev mypy src/react_agent
uv run --extra debug --extra dev python -m pytest -q
# server3（WSL 内）：TEST_POSTGRES_DSN=postgresql://witness:witness@127.0.0.1:55990/witness
# 离线回放：cd ~/witness-swebench && harness/run.sh replay-context --run-id <run> --out artifacts/<run>/replay
```
