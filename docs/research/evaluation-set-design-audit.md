# 上下文治理与 Runtime Debugging 测评集设计审计

> 审计日期：2026-08-21  
> 审计对象：当前 7 个离线上下文场景、20 对 live trace-QA、1 个 Runtime Debugging golden demo  
> 证据规则：外部事实只引用论文、官方 benchmark、协议规范或固定版本官方源码；“建议数量”与“文献已经证明的数量”严格分开。

## 结论先行

1. **当前没有一套预先注册、均衡的“简单 / 中等 / 困难”测评集。** 当前上下文数据实际上隐含了三种不同的**机制压力**：确定性淘汰后即可入预算、确定性淘汰后仍需生成式压缩、完全没有确定性淘汰机会；但构造代码只标了 `replacement_heavy` / `non_redundant` 或 `replacement_heavy` / `append_only`，并没有在运行前声明难度层级。Runtime Debugging 只有一个单程序、单异常、单次 caller-frame 取证路径，也没有难度分层。
2. **分级必须拆成两个正交维度。** 一是仓库任务本身的认知难度，采用 SWE-bench Verified 的人工预计修复时间；二是待测机制承受的压力，依据运行前即可计算的轨迹结构或 debugger oracle 路径。不能用“模型最后答错了”反推该例是困难例，否则会形成循环定义。
3. **数量是否足够取决于要声称什么。** 7 个离线场景足以做固定机制回归；20 对 live trace-QA 足以报告“这个模型端点在这 20 条固定轨迹上的配对结果”；1 个 golden demo 足以证明一条 MCP → DAP → debugpy → evidence replay 链路贯通。三者都不足以证明仓库级 solve-rate、跨仓库泛化、按难度收益或 debugger 相对静态分析的因果提升。
4. **`10,000` 次 bootstrap 不是 `10,000` 个样本。** 现有离线 replacement-heavy 推断单位只有 5 个固定场景；live 推断单位名义上是 20 条轨迹，而且它们来自同一仓库、同一构造器和重叠文件。增加 bootstrap 重采样次数只减少数值积分噪声，不能制造新的独立信息。
5. **下一阶段不应先拍一个“足够的 n”。** 应先冻结主指标、可接受的 95% CI 半宽和配对单位，再用 pilot 得到的“两个 arm 结果不一致的比例”与仓库聚类程度确定正式样本量。若要与《The Complexity Trap》做同等级的仓库任务结论，最清晰的参照仍是同一 500 个 SWE-bench Verified 实例上的完整 paired run；若要评估 debugging utility，公开的一手研究使用了 400 个 Python 仓库问题，而不是一个 demo。

## 1. 外部设计依据

### 1.1 仓库任务难度：采用人工预计修复时间，不采用模型结果

SWE-bench Verified 是目前最直接的一手分级依据。OpenAI 与 SWE-bench 作者的官方说明记录了以下流程：

- 93 名有 Python 经验的软件开发者标注了 1,699 个随机 SWE-bench 样本；每个样本由 3 名不同标注者独立标注，质量筛选采用三人中的最高严重度。
- 难度问题的原文是：在已有数小时熟悉代码库的前提下，一名有经验的软件工程师理解问题、确定方案并完成代码需要多久。
- 原始四档为 `<15 min`、`15 min–1 h`、`1–4 h`、`>4 h`；最终 Verified 500 中，官方还给出了 196 个 `<15 min` easy 样本和 45 个 `>1 h` hard 样本。
- Verified 并不是简单删除困难题：它先过滤描述含糊、测试不公平或环境有重大问题的样本，并尽可能保留长时任务，再随机补齐到 500。

来源：[OpenAI, *Introducing SWE-bench Verified*](https://openai.com/index/introducing-swe-bench-verified/)；[官方标注说明 PDF，Difficulty / Question 3.1](https://cdn.openai.com/introducing-swe-bench-verified/swe-b-annotation-instructions.pdf)；[SWE-bench 官方仓库](https://github.com/SWE-bench/SWE-bench)。

因此，本项目若需要三档任务难度，建议保留原始四档元数据，同时聚合成：

| 三档展示 | 原始标注 | 含义 |
| --- | --- | --- |
| 简单 | `<15 min` | 资深工程师的短修复 |
| 中等 | `15 min–1 h` | 需要一定定位或推理的小型修复 |
| 困难 | `1–4 h` 与 `>4 h` | 多文件、重写、研究型或长时修复 |

这套分级回答的是**任务本身有多难**。它不回答一条既有工具轨迹对 context governor 有多大压力，也不回答一次运行时诊断需要多少次 DAP 控制；后二者需要独立分层。

### 1.2 上下文治理：轨迹长度和可淘汰性才是机制压力

《The Complexity Trap》的主实验不是小型合成集：它在 SWE-bench Verified 的同一 `n=500` 实例上，对 5 个模型配置分别比较 Raw、Observation Masking 和 LLM-Summary；每个模型/策略与 Raw 的差值采用 paired nonparametric bootstrap，`B=10,000`，并明确保持 instance-level correlation。论文也明确指出轨迹 turn 数会影响管理策略：Masking 在 `M=10` 后开始生效，而其 Summary 要到 `N+M=31` 轮才首次触发，因此作者把主实验 turn limit 设为 250。

来源：[Lindenbauer et al., *The Complexity Trap*, 实验设置 pp. 5–7](https://arxiv.org/pdf/2508.21433v3#page=5)、[统计方法 pp. 15–16](https://arxiv.org/pdf/2508.21433v3#page=15)、[固定官方仓库 `bf15b5f`](https://github.com/JetBrains-Research/the-complexity-trap/tree/bf15b5fb7d279679035a007ac9a81084d6b9a89a)。

该论文没有给任务打 easy/medium/hard 标签，也没有测试“按资源身份淘汰过期观察”。它给本项目的可靠依据是：

1. 真实效用结论应在同一批仓库任务上 paired 比较；
2. 轨迹长度/预算压力必须进入实验设计；
3. recency masking 是必须保留的强通用 baseline；
4. `B=10,000` 只描述 bootstrap 算法精度，论文的经验基础仍是 500 个共同实例。

确定性淘汰的分层依据来自公开生产源码，而不是主观命名：

- `oh-my-pi@72000ac` 的 [`pruneSupersededToolResults()`](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/packages/agent/src/compaction/pruning.ts#L242-L309) 按 supersession key 从后向前识别较新的同资源读取；配置同时显式区分 protected tail、minimum savings、约 8K token 的 cache-warm suffix 与 idle flush。这说明“可淘汰字符比例”“淘汰后剩余预算压力”“资源身份”和“cache fence”都是机制变量。
- `Comis@56c4759` 的 [`dead-content-evictor.ts`](https://github.com/comisai/comis/blob/56c47590d3e25e148a28a4b0009d09ef96e0195f/packages/agent/src/context-engine/dead-content-evictor.ts#L1-L23) 明确把 superseded file/exec/web、旧 image 与 stale error 分成类别，并用 forward index 做 O(n) detection；其 [`context-management.mdx`](https://github.com/comisai/comis/blob/56c47590d3e25e148a28a4b0009d09ef96e0195f/docs/agents/context-management.mdx#L40-L55) 也明确采用“dead-content eviction → observation masking → LLM compaction”的顺序。

这些实现可以证明“按 supersession 与预算残余分层”有外部工程依据，但不能证明任意相同命令都可安全淘汰。特别是 Comis 把相同 command 当作 exec supersession key；本项目继续采用显式 `ToolContextPolicy` 是更保守的选择，测评必须包含“旧成功后新失败仍保留旧成功”等反例。

### 1.3 Runtime Debugging：按运行时问题类型与所需控制路径分层

Microsoft DAP 官方规范给出了客观的控制复杂度：程序可能因 entry、breakpoint、exception 或 pause 停止；停止后标准观测瀑布为 `threads → stackTrace → scopes → variables`；`continue`、`next`、`stepIn`、`stepOut`、`pause` 是不同控制动作；frame/scope/variable references 仅在当前 suspended state 有效，恢复执行后必须失效。来源：[Microsoft DAP 固定规范 `bf8a5d2`, Stopping and accessing debuggee state](https://github.com/microsoft/debug-adapter-protocol/blob/bf8a5d27e8040044b84b863f90916e08925ee811/overview.md#stopping-and-accessing-debuggee-state)。

这使 debugger 难度可以在运行前由 oracle 路径定义，例如“一个 stop + 一个 frame”与“多个 stop + step + 多 frame”客观不同，而不需要看模型是否答对。

Debug2Fix 是更接近本项目 utility 目标的一手仓库级研究。SWE-Bench-Live 从 93 个仓库收集问题，该研究的 Python 实验使用其 frozen Verified split 中 400 个样本；Java 实验使用来自 55 个仓库的 186 个 GitBug-Java 样本。作者还在 50 个轨迹样本中归纳出 7 类 debugger 问题：exception diagnosis、root-cause analysis、local-variable inspection、attribute-value inspection、assertion failure、code reachability、post-fix verification。失败分析将 debugger session/build/attach 失败、正确诊断但修复错误、高复杂度多会话、API 错误分别统计，而没有把它们混成一个 solve-rate 数字。

来源：[Garg & Huang, *Debug2Fix*, v2，实验数据集与 ablation pp. 5–8](https://arxiv.org/pdf/2602.18571v2#page=5)、[问题类型和失败模式 pp. 8–10](https://arxiv.org/pdf/2602.18571v2#page=8)。这篇论文不提供实现源码，因此只能作为测评设计与规模参照，不能作为代码复用来源。

MCP 官方规范只要求 server 声明 tools capability，并通过 `tools/list` 暴露 schema、通过 `tools/call` 调用工具。[MCP 2025-11-25 Tools 规范](https://modelcontextprotocol.io/specification/2025-11-25/server/tools) 可以支撑“7 个工具可发现、可调用、schema 一致”这一**协议合规**验收，但协议合规本身不是“debugger 提升诊断成功率”的证据。

同样，[debugpy `v1.8.21` 官方源码](https://github.com/microsoft/debugpy/tree/858b05c08555cfc54efa7cf90e70184c7495b38e) 可作为真实 Python adapter、launch 与异常观测的实现依赖依据，却没有规定 easy/medium/hard benchmark，也不能把一次成功会话转化成 utility 统计结论。

## 2. 当前测评集到底是什么

### 2.1 7 个离线上下文场景

[`benchmarks/context_ab.py`](../../benchmarks/context_ab.py) 构造了 7 条固定合成轨迹：

| 隐含机制层 | 当前场景 | 数量 | 设计意图 |
| --- | --- | ---: | --- |
| C1：确定性淘汰后即可入预算 | `same_file_rereads`、`test_reruns`、`successful_retry`、`success_then_failed_rerun` | 4 | Tier 1 应零 compressor call；同时覆盖旧成功→新失败保留 |
| C2：部分可淘汰，仍需压缩 | `edit_read_churn` | 1 | mutation boundary 后仍有必须保留的写入事实 |
| C3：没有可证明过期内容 | `unrelated_reads`、`opaque_append_only` | 2 | 必须走 summary / fallback，避免误淘汰 |

这是一个合理的**机制覆盖表**，而不是有抽样含义的 benchmark population：每个场景只有一个固定长度、一个固定 payload 模板和一个固定事实布局；5 个 arm 产生 35 行结果，仍然只有 7 个场景单位。replacement-heavy compressor ratio 的 bootstrap 更只有 5 个场景单位。

能支持的结论：

- 这些明确的 supersession / safety 夹具上，Tier 1、Tier 2 与 hard fallback 是否按设计触发；
- 固定输入上的字符数、事实保留与字节稳定性；
- 作为 CI 回归门禁，未来提交是否破坏已列语义。

不能支持的结论：

- “真实仓库任务平均节省 70.5%”或“真实任务 compressor calls 平均下降 80%”；
- 模型 solve-rate、跨 provider 泛化或生产成本；
- 任何 easy/medium/hard 的总体差异。

### 2.2 20 对 Terra live trace-QA

[`benchmarks/context_live_ab.py`](../../benchmarks/context_live_ab.py) 的 20 个 instance 是：8 个 reread、4 个 edit+reread、4 个 retry、4 个 append-only；每个 instance 都跑 Generic 与 Tiered 两个 arm，所以是 20 个 paired units、40 个 model runs，不是 40 个样本。

按运行前机制路径，可描述为：

| 隐含机制层 | 模板 | paired units | 当前单例对层内比例的最小改变量 |
| --- | --- | ---: | ---: |
| C1：确定性淘汰可满足预算 | reread 8 + retry 4 | 12 | 8.3 pp |
| C2：淘汰后仍需 summary | edit+reread | 4 | 25 pp |
| C3：append-only、无确定性机会 | append-only | 4 | 25 pp |
| 总计 | 4 类模板 | 20 | 5 pp |

这里的主要限制不是只有“20 很小”，而是**独立性与覆盖范围**：

- 20 个实例全部从当前一个仓库的 10 个文件循环选取；每例使用 4 个文件，实例之间大量重叠。
- 同类实例共享完全相同的事件模板、每条 source excerpt 固定 1,250 字符、每题固定查询 4 个 16 字符 hash facts。
- 没有真实 issue、真实 agent 决策、patch 或测试通过判定；这是 repository-derived trace-QA，而不是 repository task。
- hard/medium 层各只有 4 例；单例即可令该层准确率变化 25 个百分点，无法评估小到中等效应。

因此，当前 `5% → 70%`、recall 与 compressor-call 差异是这 20 条固定轨迹和该次第三方端点响应的合法描述；它们不能转写成“真实仓库任务提升 65 pp”。现有 paired bootstrap 保留了同一 trace 的两臂相关性，这是对的，但若把 20 条同仓模板视作 20 个独立仓库任务，CI 会遗漏 repository/template clustering。

### 2.3 1 个 Runtime Debugging golden demo

[`examples/runtime_debug_demo/buggy_pricing.py`](../../examples/runtime_debug_demo/buggy_pricing.py) 只有一个确定性 `ZeroDivisionError`：在 caller frame 中 `item_count=0`，golden runner 执行 verified breakpoint、一次 `continue` 到异常、stack、select caller frame、locals、stop，再把 23 个事件重放为 PR Evidence。

按 DAP 控制需求，它属于 **D1：单会话、单关键 stop、单 caller-frame 观测**。它没有在 golden workload 中要求：

- `next` / `stepIn` / `stepOut` 才能到达根因；
- 多 breakpoint / 多 stop 的 stop-id 生命周期；
- assertion-only、silent wrong output、reachability、attribute/nested-state 等不同问题类型；
- async / multi-thread、被包装异常、外部依赖或多次 debug session；
- static-only 对照 arm 或最终 patch solve。

测试套件对 stepping、arguments、截断、取消和进程回收的覆盖是重要的**功能与安全证据**，但测试数量不能当作独立缺陷样本数。当前 demo 足以证明端到端 plumbing、固定路径 root-cause evidence 和零模型回放；不能证明 runtime debugging 比静态检索更有效，也不能量化 Reviewer 认知负担的下降。

## 3. 建议采用的正式分层

### 3.1 两轴而不是单一“难度”标签

每个仓库任务保存两类独立标签：

1. **E（engineering effort）**：沿用 SWE-bench Verified 的人工时间档；这是任务难度。
2. **C 或 D（mechanism demand）**：从冻结的 canonical trace 或人工 oracle debug plan 在运行前计算；这是待测机制压力。

任何分层标签都必须在查看 arm 输出之前冻结。`agent failed`、`用了很多 tokens`、`Tiered 答错` 都是 outcome，不能拿来定义困难层。

### 3.2 Context mechanism demand

对同一条冻结 canonical trace 定义：

- `B`：provider-neutral serialized-envelope hard budget；
- `H0`：治理前字符/token 数；
- `H1`：只运行确定性 Tier 1 后的字符/token 数；
- `R`：可证明过期、可无损淘汰的 observation 字符占比；
- 连续协变量：必要事实数量、最老必要事实距末尾的 turn 数、distinct resources、read/mutate/execute/opaque 比例、失败→成功和成功→失败 transition 数。

据此预注册：

| 层级 | 判定（arm 运行前，对冻结 trace） | 机制含义 |
| --- | --- | --- |
| C0 负对照 | `H0 <= B` | 不需要任何治理；检查无谓压缩/回归 |
| C1 简单 | `H0 > B` 且 `H1 <= B` | 确定性淘汰足够 |
| C2 中等 | `H1 > B` 且 `R > 0` | 先淘汰再压缩，三级链路都可能参与 |
| C3 困难 | `H1 > B` 且 `R = 0`（或所有候选均受保护） | append-only / non-redundant，主要考验 summary 与 budget fallback |

summary 失败、超长、store 损坏、crash/resume 属于 **F（fault robustness）** 维度，不应伪装成“更难的自然任务”。这类用故障注入逐项验收，不做 population solve-rate。

### 3.3 Debug mechanism demand

先让人工或 deterministic oracle 写出最短合法 DAP 观测路径，再冻结层级：

| 层级 | 最短 oracle 路径 | 例子 |
| --- | --- | --- |
| D0 负对照 | 静态源码/traceback 已含完整根因，不需要 runtime state | 检查 debugger 是否徒增步骤/成本 |
| D1 简单 | 一个 session、一个关键 stop、stack + 一个 frame/scope | 当前 divide-by-zero golden demo |
| D2 中等 | 至少两个 stop，或必须 `next/stepIn/stepOut`、跨 caller/callee 才能观察关键状态 | 分支 reachability、状态在调用链中被改写 |
| D3 困难 | 多 breakpoint / 多 stop / 多 frame，且涉及 nested attributes、被包装异常或多次 session 才能组成完整证据 | silent wrong output、复杂状态传播、修复后验证 |

在每个 D 层内再按 Debug2Fix 的 7 类问题做 coverage：exception、root cause、local variable、attribute、assertion、reachability、post-fix verification。async/multi-thread/attach 等当前 MVP 不支持的能力应列为 external-validity backlog 或“明确拒绝”用例，而不是暗中计为普通失败。

## 4. 数量如何决定，而不是如何拍脑袋

### 4.1 先固定推断单位和主指标

- 上下文仓库任务：单位是一个独立 issue/repository revision；Generic 与 Tiered 在同一单位上 paired。
- Debugging utility：单位是一个可执行、带 ground truth 的缺陷；Static-only 与 Static+DAP 在同一缺陷上 paired。
- 同一 base program 生成 20 个 mutation 可以增加机制覆盖，但不能自动当成 20 个独立仓库。正式 CI 应按 repository cluster 重采样或使用 cluster-aware 模型。
- 一个任务的多个随机 seed 是该任务内重复，不是新的任务；主结果应先聚合到任务，或使用明确的层级模型。

主任务指标建议是 binary solve/test pass；次要指标包括必要事实 recall、root-cause localization exact match、evidence coverage、compressor/debugger 调用率、tokens、wall time 和基础设施失败率。debug session 无法启动与启动后诊断错误必须分开报告。

### 4.2 paired binary 指标的样本量规则

对每个任务定义 `d_i = Y_tiered - Y_generic`（或 `Y_DAP - Y_static`），所以 `d_i ∈ {-1, 0, 1}`。令：

- `p10`：只有新 arm 成功的比例；
- `p01`：只有 baseline 成功的比例；
- `q = p10 + p01`：discordant-pair 比例；
- `Δ = p10 - p01`：配对成功率差。

则单任务差值方差为 `q - Δ²`，均值差标准误近似为 `sqrt((q - Δ²) / n)`。这是为什么 paired design 的样本量取决于 discordant pairs，而不是仅看两个 arm 各自的成功率；McNemar 的原始 paired-proportion 方法同样只依赖 discordant cells。来源：[McNemar, 1947, *Note on the sampling error of the difference between correlated proportions or percentages*](https://doi.org/10.1007/BF02295996)。

正式流程应是：

1. 预注册希望 95% CI 达到的半宽 `h`，例如是 ±5 pp 还是 ±10 pp；
2. 用不进入 confirmatory analysis 的多仓 pilot 估计每个 E×C 或 E×D 层的 `q`、`Δ` 与 repo clustering；
3. 用 paired bootstrap / exact or simulation coverage 在候选 n 上模拟，直到目标半宽在预定比例的模拟中满足；
4. 对每个主要分层分别满足精度要求，不能只让总体 n 足够；
5. 冻结 n、arm order randomization、模型 revision、temperature/seed、turn/tool/time limit 后再跑 confirmatory set。

在没有 pilot 时只能给保守上界，不能给“保证 power”的精确数字。因为 `q - Δ² <= 1`，普通正态近似下：

- 若要 95% CI 半宽约 `10 pp`，最坏情形约需 `1.96² / 0.10² ≈ 385` 个独立 pairs；
- 若要半宽约 `5 pp`，最坏情形约需 `1.96² / 0.05² ≈ 1,537` 个独立 pairs。

这些只是未计 cluster、模型随机性和有限样本 coverage 的保守 planning envelope，不是对本系统效应大小的 power claim。pilot 若显示 discordance 很低，所需 n 会下降；仓库内相关性或按难度分层会使所需任务数上升。

连续 cost/token 指标则对任务内 paired difference `z_i` 做规划：pilot 得到标准差 `s_d` 后，用 `n ≈ (1.96 s_d / h)²` 作为起点，再用 cluster-aware bootstrap 验证 CI coverage。不能用每次模型调用当作独立样本。

Bootstrap 的原始定义就是从观测样本的 empirical distribution 重采样；因此 `B` 增大只让该经验分布上的近似更稳定，不会扩大原始信息。来源：[Efron, 1979, *Bootstrap Methods: Another Look at the Jackknife*](https://doi.org/10.1214/aos/1176344552)。

### 4.3 可执行的下一阶段规模

以下分成“覆盖集”“pilot”“正式集”，避免把三种 n 混为一谈：

| 阶段 | 上下文治理 | Runtime Debugging | 可以声称什么 |
| --- | --- | --- | --- |
| deterministic coverage | 扩成 C0–C3 × reread/mutate/rerun/retry/opaque 的适用单元格；每条语义至少有正例和反例 | 7 类 runtime 问题 × D1–D3，适用单元格各一个 golden；D0 另做负对照 | 协议、状态机与机制覆盖；不做总体统计 |
| 多仓 pilot | **30–50 个独立 issue**，覆盖多个 repo，并尽量铺开 E×C | **30–50 个可执行独立缺陷**，覆盖多个 repo、7 类问题和 D 层 | 估计 eligibility、discordance、cluster/seed variance、基础设施失败率；不作为 confirmatory 结论 |
| confirmatory paired run | 若要与《The Complexity Trap》同口径，首选完整 **500 个 SWE-bench Verified**；总体 ITT 与 E×C 分层同时报告 | 按 pilot 和目标 CI 半宽计算 n；公开 Python 研究的可比规模是 **400 个 SWE-Bench-Live 样本**，可作为资源规划参照而非法定阈值 | 仓库级效用、成本与分层差异；仍受 benchmark/domain/model 边界约束 |

表中的 `30–50` 是为了估计方差和失败构成的**项目 pilot 配额**，不是文献宣布的“统计充分线”；Debug2Fix 的 50 例也只是 qualitative taxonomy/failure analysis，而其效用主实验用的是 186/400 例。若 pilot 后算出的正式 n 大于可用 hard pool（SWE-bench Verified 官方只有 45 个 `>1 h` 样本），应诚实报告 hard stratum CI 很宽，不能用总体结果替代该层结论。

## 5. 对当前结果应采用的表述

### 可以保留

- “在 7 个预定义合成机制场景中，Tiered 通过了确定性淘汰、summary 和 hard-budget 的固定验收。”
- “在同一模型端点的 20 对 repository-derived trace-QA 中，Tiered 的固定集 exact/recall 与压缩调用优于 Generic；改善集中在 reread/retry，append-only 没有改善。”
- “一个真实 MCP → DAP → debugpy golden target 上，7 个工具链路、异常 frame/locals 取证和五次字节稳定 replay 通过。”

### 必须避免

- “已经做了完整简单/中等/困难测评。”
- “20 个样本足以证明真实仓库提升 65 pp。”
- “bootstrap 10,000 次说明 n 很大或 CI 已覆盖跨仓库不确定性。”
- “一个 debug demo 证明 debugging 比静态分析更强，或已经减轻 Reviewer 认知负担。”
- “测试套件 300+ PASS 等于 300+ 个真实任务样本。”

## 6. 最终审计判定

| 用户问题 | 审计回答 |
| --- | --- |
| 有没有分级？ | **没有正式预注册的三级集。** 有可追溯的隐含机制层：offline 4/1/2、live 12/4/4；debug demo 只覆盖 D1。 |
| 分级依据是什么？ | 正式设计应采用 SWE-bench Verified 的人工修复时间作为任务难度，并采用 `H0/B`、`H1/B`、supersession ratio、oracle DAP stop/control path 作为机制压力；依据分别来自 Verified、The Complexity Trap、oh-my-pi/Comis 与 DAP/Debug2Fix。 |
| 数量够不够？ | **够做机制/端到端 acceptance，不够做总体效用和难度外推。** 7、20、1 不能替代 500 个 paired repository tasks 或按 paired-discordance/目标 CI 规划的 debugging benchmark。 |

本审计的核心整改不是简单地“再加几个 case”，而是先把**推断单位、双轴分层、目标 CI 精度和独立仓库来源**固定下来，再决定正式 n。这样新增样本才是在增加证据，而不是重复同一模板。
