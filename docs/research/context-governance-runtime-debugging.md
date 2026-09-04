# 确定性上下文治理与 Runtime Debugging / PR Evidence：第一方研究基线

> 调研快照：2026-08-20。本文只采用论文原文、官方仓库、协议规范和本仓库源码。所有外部代码链接均固定到 commit；“验收阈值”是本项目的工程门槛，不冒充论文结论。

## 0. 结论先行

证据强度在本文中统一标为：

- **[确认]**：可由固定版本的第一方论文、源码或规范直接验证。
- **[高度可能]**：证据高度吻合，但用户原话存在命名歧义。
- **[本项目设计]**：由前述证据推导出的 Witness Core 落地方案，尚需本项目实验验证。
- **[未确认]**：第一方来源不足，不应写成事实。

核心结论如下。

1. **[确认] 没有找到一篇论文或一个仓库完整等同于“事件驱动淘汰 → 可持久化生成式压缩 → 硬预算兜底”三级机制。** 最接近的公开证据由三部分组成：JetBrains 的受控实验说明简单 Observation Masking 已能显著降本；`oh-my-pi` 实现了 cache-aware 的过期读取淘汰和一等持久化 compaction；Comis 实现了更宽的 O(n) dead-content eviction。三级机制应被表述为本项目对这些机制的保守综合，而非复现某一篇论文。
2. **[未确认] “Py Agent” 的准确归属不能由公开一手资料唯一确定。** 至少有两条强候选线索：一是 “Pi Agent” 家族中的 [`oh-my-pi`](https://github.com/can1357/oh-my-pi)，固定版本 [`v17.4.0` / `72000ac`](https://github.com/can1357/oh-my-pi/tree/72000acfeb902e21816252699482887f34d1a5a4) 同时具备 DAP `debug` 工具和 superseded-read pruning；二是 JetBrains 2026.2 的 Agentic Debugging，它通过 MCP 向 Codex 等外部 Agent 暴露断点、执行控制、栈和变量。但目前只有 IntelliJ IDEA 文档明确确认完整 debugger toolset，不能据此声称 PyCharm 已提供相同能力。实现参考应拆开表述：`oh-my-pi` 提供可审计的 DAP/session 源码，JetBrains 提供第一方 MCP debugger 工具面和 Agent UX 佐证。
3. **[确认] 上下文研究并不支持“LLM summary 天然优于简单遮蔽”。** 《The Complexity Trap》的 5 组 SWE-bench Verified 主实验中，Observation Masking 在 4/5 组成本最低；论文只检验了各策略相对 Raw 的显著性，没有给出 Masking 对 Summary 的直接配对检验。
4. **[本项目设计] Witness Core 应永久保留 canonical transcript / journal，只治理发送给模型的 active projection。** 被淘汰的原始 observation 仍由私有、hash-chained 事件保存；投影中只留下带原因、替代者和内容摘要的最小占位符。这同时满足上下文节省、协议配对、崩溃恢复和审计要求。
5. **[确认] Runtime Debugging 最稳妥的可移植栈是 `MCP facade → 有状态 DAP client → debugpy adapter`。** MCP 负责 Agent 工具契约，DAP 负责调试状态机，debugpy 负责 Python 运行时。三者职责不可混为一个文本命令解析器。
6. **[本项目设计] PR Evidence 不应再次调用模型。** 它应验证 journal 的 sequence/hash 内部一致性，按 `call_key` 配对调试工具事件，校验 observation digest，再以稳定排序和固定模板生成 canonical JSON 与 Markdown。这样同一日志重复生成必须逐字节一致；“根因”措辞只能陈述日志直接支持的事实。

## 1. 上下文治理：论文证据与实现谱系

### 1.1 《The Complexity Trap》到底证明了什么

第一方来源：

- 论文：Lindenbauer et al., *The Complexity Trap: Simple Observation Masking Is as Efficient as LLM Summarization for Agent Context Management*, [`arXiv:2508.21433v3`](https://arxiv.org/abs/2508.21433v3)，camera-ready v3。
- 官方代码与分析产物：[`JetBrains-Research/the-complexity-trap@bf15b5f`](https://github.com/JetBrains-Research/the-complexity-trap/tree/bf15b5fb7d279679035a007ac9a81084d6b9a89a)，MIT；该仓库没有 release tag，因此必须固定完整 commit SHA。
- 官方原始轨迹数据：[`JetBrains-Research/the-complexity-trap@03f0312`](https://huggingface.co/datasets/JetBrains-Research/the-complexity-trap/blob/03f031299ea00194235458d6b353242d7f5a9180/README.md)，Apache-2.0。
- Masking 实现：[`LastNObservations`](https://github.com/JetBrains-Research/the-complexity-trap/blob/bf15b5fb7d279679035a007ac9a81084d6b9a89a/sweagent/agent/history_processors.py#L78-L145)。
- Summary 实现：[`SummarizeEveryNTurns`](https://github.com/JetBrains-Research/the-complexity-trap/blob/bf15b5fb7d279679035a007ac9a81084d6b9a89a/sweagent/agent/history_processors.py#L303-L480)。

**[确认] 实验设置。** SWE-agent 在 SWE-bench Verified 全 500 个实例上比较 Raw、Observation Masking 和 LLM-Summary；覆盖 Qwen3-32B（thinking / non-thinking）、Qwen3-Coder 480B、Gemini 2.5 Flash（thinking / non-thinking）5 个模型配置。每组同一 500 实例，turn limit 250。Masking 保留 reasoning/action，只把最近 `M=10` 轮之外的 observation 替成“省略了多少行”的占位符；Summary 每当有 `N+M=31` 个待处理轮次时，递归总结较老 `N=21` 轮并保留最近 `M=10` 轮。agent temperature 为 0.8，summary temperature 为 0。[论文 pp.5–7](https://arxiv.org/pdf/2508.21433v3#page=5)

**[确认] 统计方法。** 对同一 `n=500` 实例做 paired nonparametric bootstrap，`B=10,000`，报告 95% percentile CI 和双侧 bootstrap p-value，`p<0.05` 标 `†`。但检验对象仅为“每个策略 vs Raw”，论文没有报告 Masking-vs-Summary 的直接 p-value，也没有说明多重比较校正。[论文 p.15–16](https://arxiv.org/pdf/2508.21433v3#page=15)

主表的精确结果如下。单元格为 `solve rate / mean instance cost`；`†` 只表示成本相对 Raw 显著，除 Gemini thinking 的 solve rate 外，其余 solve-rate 差异相对 Raw 不显著。

| 模型配置 | Raw | Observation Masking | LLM-Summary |
| --- | --- | --- | --- |
| Qwen3-32B | 17.0±3.3% / $1.12±.18 | 15.0±3.1% / $.55±.09 `(-50.9%)†` | 16.0±3.3% / $.50±.07 `(-55.4%)†` |
| Qwen3-32B thinking | 23.0±3.7% / $.51±.07 | 24.6±3.8% / $.46±.05 `(-9.8%, ns)` | 24.8±3.9% / $.51±.06 `(0%, ns)` |
| Qwen3-Coder 480B | 53.4±4.3% / $1.29±.26 | 54.8±4.4% / $.61±.06 `(-52.7%)†` | 53.8±4.2% / $.64±.06 `(-50.4%)†` |
| Gemini 2.5 Flash | 32.8±4.1% / $.41±.08 | 35.6±4.2% / $.18±.03 `(-56.1%)†` | 36.0±4.1% / $.24±.04 `(-41.5%)†` |
| Gemini 2.5 Flash thinking | 40.4±4.3% / $.56±.10 | 36.4±4.2% `(-9.9%)†` / $.24±.04 `(-57.1%)†` | 31.4±4.0% `(-22.3%)†` / $.25±.05 `(-55.4%)†` |

来源：[论文 Table 1, p.7](https://arxiv.org/pdf/2508.21433v3#page=7)；完整配对统计见 [Table 4, p.16](https://arxiv.org/pdf/2508.21433v3#page=16) 和官方 [`paired_ci_diffs_vs_raw.csv`](https://github.com/JetBrains-Research/the-complexity-trap/blob/bf15b5fb7d279679035a007ac9a81084d6b9a89a/auxiliary-data/paired_ci_diffs_vs_raw.csv)。

**[确认] 复现材料存在需要预注册处理的样本计数差异。** 固定 GitHub commit 中的 `experiment_instance_costs_sweagent.csv` 并非每个实验都恰有 500 行，例如 Gemini Raw 为 498、Gemini Summary 为 496、Coder Summary 为 498；但论文 Table 4 与 `paired_ci_diffs_vs_raw.csv` 按 `n=500` 报告，仓库未在该 CSV 中明确说明缺失轨迹如何补齐或计入。引用论文结论时应以 Table 1/4 为准；自行重算时必须先锁定 `missing trajectory / failed run` 的处理规则，并为每一臂报告 assigned、started、completed、missing、failed、analyzed 的 CONSORT 式计数，不能静默只分析完整行。

这组实验对本项目最有用的解释是：

- **长轨迹且 observation 占比高时收益最大。** 论文预实验中 SWE-agent 轨迹 token 约 84% 来自 observation，主实验出现超过 100K token 的上下文；4/5 配置中 Masking 成本最低。
- **短轨迹中 summary 可能根本不触发。** Qwen thinking 的 median trajectory 约 15 turns；Summary 首次需要 31 turns，所以成本没有下降。Masking 从第 10 轮起生效，仍下降约 9.8%，但未达显著。
- **summary 调用本身不是免费。** 其直接 API 成本占实例总成本的 0.65%–7.20%；Qwen3-Coder 480B 为 `$0.0439/instance`、占 7.20%。[论文 Table 2, p.9](https://arxiv.org/pdf/2508.21433v3#page=9)
- **summary 可能延长轨迹。** 论文报告 Gemini Summary 平均 52 turns、Mask 44、Raw 50；Coder Summary 相对 Raw 约 `+15%`、相对 Mask 约 `+13%`。因此只比较单次 prompt 长度会漏掉总轨迹成本。
- **Masking 的窗口不是跨 Agent 通用常数。** OpenHands Verified-50 上直接复用 `M=10` 时 solve rate 降至 30%；调到 `M=58` 后为 44%，才重新接近 Summary 的 42%。所以本项目的事件语义应显式化，不能把 `M=10` 当作普适默认。[论文 §5.1, p.8](https://arxiv.org/pdf/2508.21433v3#page=8)

#### 对论文 hybrid 结果的审计提醒

**[确认] 论文的 tuned hybrid 使用 `N=43, M=W=10`：** accumulation 期先做 masking，约 30K token 后才把未 mask 的历史送去 summary；论文报告相对纯 Masking / Summary 再降 7% / 11%，solve rate 相对 Raw `+2.6pp`。[论文 pp.9–10](https://arxiv.org/pdf/2508.21433v3#page=9)

**[确认] 但这不是严格的同样本五臂配对 A/B。** Hybrid 点只跑 Verified-50 (`n=50`)，论文图中却与 Table 1 的 Verified 全集 (`n=500`) Raw/Mask/Summary 点比较。官方实例成本数据给出的 tuned hybrid 是 `56% / $0.571410`；同一 `verified-50.txt` 上原三臂 solve rate 都是 56%。因此 7% / 11% 与 `+2.6pp` 只能视为方向性证据，不能直接用作本项目的验收预期。本项目必须在同一 instance set、模型、seed、价格快照和运行上限下重跑全部五臂。来源：官方 [`experiment_instance_costs_sweagent.csv`](https://github.com/JetBrains-Research/the-complexity-trap/blob/bf15b5fb7d279679035a007ac9a81084d6b9a89a/auxiliary-data/experiment_instance_costs_sweagent.csv) 与 [`verified-50.txt`](https://github.com/JetBrains-Research/the-complexity-trap/blob/bf15b5fb7d279679035a007ac9a81084d6b9a89a/config/dataset_filters/verified-50.txt)。

### 1.2 `oh-my-pi`：最接近“先确定性、后 compaction”的生产实现

第一方固定源：[`can1357/oh-my-pi@72000ac` (`v17.4.0`)](https://github.com/can1357/oh-my-pi/tree/72000acfeb902e21816252699482887f34d1a5a4)，MIT；该仓库明确是 [`badlogic/pi-mono`](https://github.com/badlogic/pi-mono) 的 fork。

**[确认] 过期读取淘汰。** [`pruneSupersededToolResults()`](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/packages/agent/src/compaction/pruning.ts#L242-L309) 从后向前按 supersession key 找到较新的同资源读取，把旧结果替成 `[Superseded by a newer read of this file]`；[`readToolSupersedeKey()`](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/packages/agent/src/compaction/pruning.ts#L422-L439) 按规范化 path + selector 分组，无 selector 的整文件读取可覆盖同文件的 selector 读取，URL/内部 scheme 被排除。

**[确认] 它是 prompt-cache-aware，而不只是“能删就删”。** 若旧结果后的 suffix 超过约 8K tokens，修改暖缓存前缀可能产生 cache-write 代价，所以 per-turn pass 只在便宜尾部淘汰；session idle 超过 provider cache lifetime 后才批量 flush。通用 age-based pruning 保护最近 40K tool-output tokens，且需至少估计节省 20K tokens。[源码默认值](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/packages/agent/src/compaction/pruning.ts#L18-L109)；[compaction 文档](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/docs/compaction.md#pre-compaction-pruning)。

**[确认] 生成式 compaction 是持久化的一等事件。** `CompactionEntry` 带 `summary`、`firstKeptEntryId` 和 `tokensBefore`；恢复上下文时使用最新 summary，再接 boundary 后的原始 entries。触发包括 manual、overflow、incomplete output、post-turn threshold、mid-turn threshold 和 idle。[数据类型](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/packages/wire/src/index.ts#L135-L141)；[完整流程](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/docs/compaction.md#session-entry-model)。`compaction.supersedeReads` 与 `compaction.dropUseless` 默认均为 `true`。[设置 schema](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/packages/coding-agent/src/config/settings-schema.ts#L2446-L2466)

**迁移边界。** 这个版本直接证明了“过期 observation 应先机械淘汰、summary 应持久化”的工程可行性，但其 supersession 主规则只覆盖 `read`，并不自动证明“任意相同命令重跑”或“任意失败后成功”都可替代。Witness Core 需要依赖工具作者声明的 effect/resource/replacement policy，而不是工具名猜测。

### 1.3 Comis：更宽的 dead-content evictor，以及不能照搬的风险

第一方固定源：[`comisai/comis@56c4759` (`v1.0.63`)](https://github.com/comisai/comis/tree/56c47590d3e25e148a28a4b0009d09ef96e0195f)，Apache-2.0；核心是 [`dead-content-evictor.ts`](https://github.com/comisai/comis/blob/56c47590d3e25e148a28a4b0009d09ef96e0195f/packages/agent/src/context-engine/dead-content-evictor.ts)。

**[确认]** 它用 forward index 在 O(n) 内处理 tool-call arguments 与结果配对，并为相同文件读取、相同 exec command、相同 web search/fetch、旧图片、短错误结果和空 error turn 生成可检索占位符；保留 tool-call/result 结构，并尊重 cache fence。

**[确认] 不能原样移植。** 源码把完全相同 command 当成 supersession key；但相同命令可能是轮询、时间查询、随机过程、append-only 日志读取或有外部状态变化的测试。它还把 `file_write` 放进 `FILE_READ_TOOLS` 集合。两者都说明“参数相同”不等于“语义上可替代”。本项目必须 fail closed：未声明语义的工具只保留，不淘汰。

### 1.4 相关但不同层级的 token 压缩与厂商 context management

**LLMLingua 系列。** [`LLMLingua`](https://arxiv.org/abs/2310.05736v2) 用 coarse-to-fine prompt compression、budget controller 与 token-level iterative compression 压缩 prompt，论文报告在若干任务上最高约 `20×`；[`LLMLingua-2`](https://arxiv.org/abs/2403.12968v2) 将压缩改写为 task-agnostic token classification / extractive compression。官方实现固定为 [`microsoft/LLMLingua@e0e9d99`](https://github.com/microsoft/LLMLingua/tree/e0e9d99beb94098bbd924aa53c2c112eac41c758)，MIT。

**Selective Context。** [`Selective Context`](https://arxiv.org/abs/2310.06201) 以信息量选择保留词元/句子；作者实现固定为 [`liyucheng09/Selective_Context@3074343`](https://github.com/liyucheng09/Selective_Context/tree/3074343653bbf3559a87a588667e843744bc6f2a)，但该固定快照未提供 LICENSE，不能直接复制代码。

**迁移边界。** 这些方法压缩的是单个 observation 或完整 prompt 内部的 token，属于有损内容选择，不理解“被修改、重读、重跑或成功重试取代”的事件语义。它们可作为额外有损 baseline 或 Tier 2 compressor 候选，不能直接充当 Tier 1；对结构化 tool call/result 或 provider-native opaque item 做 token 级裁剪还可能破坏协议配对。

**Anthropic 的可比产品机制。** 官方 [Context editing](https://platform.claude.com/docs/en/build-with-claude/context-editing) 的 `clear_tool_uses_20250919` 在达到阈值后按时间顺序清除最旧 tool results，可配置保留最近 N 个、排除工具与是否同时清除 tool inputs；清除会使对应 prompt-cache prefix 失效。它是通用 recency 策略，并不判断某个旧观测是否已被后续事件语义替代。官方 [server-side compaction](https://platform.claude.com/docs/en/build-with-claude/compaction) 的 `compact_20260112` 默认在 150K input tokens 触发（下限 50K），生成 `compaction` summary block，后续请求忽略该 block 之前的内容。Claude Code 还提供 [`/autocompact`、`--autocompact` 与 `CLAUDE_CODE_AUTO_COMPACT_WINDOW`](https://code.claude.com/docs/en/model-config#set-the-auto-compact-window)。这些是截至 2026-08-20 可核查的厂商文档行为，不是名为 “AutoCompact” 的独立论文或固定源码算法；应作为比较对象和设计佐证，而不是三级机制的源码来源。

### 1.5 Witness Core 的可移植三级算法

**[本项目设计] 双视图不变量：**

```text
private canonical transcript + append-only journal
                    │ pure projection
                    ▼
Tier 1  deterministic event eviction (zero model calls)
                    │ still over soft/hard budget
                    ▼
Tier 2  content-addressed persistent generative summary
                    │ summary failed / still over budget
                    ▼
Tier 3  deterministic hard-budget fallback or fail closed
                    │
                    ▼
bounded provider-neutral serialized-envelope estimate
```

Canonical transcript 不能因上下文治理被改写或删除；active projection 才能放占位符或 summary。本仓库已经具备适合承载该设计的基础：[`events.py`](../../src/react_agent/events.py) 提供 canonical JSON、版本化 hash chain 与纯 `fold_events()`；[`tools.py`](../../src/react_agent/tools.py) 提供 framework-owned `run_id/call_key/operation_id/attempt/idempotency_key/workspace_path`；PostgreSQL 的 [`010_public_event_projection.sql`](../../migrations/010_public_event_projection.sql) 明确排除 private payload。

从长期接口设计看，每个工具最好能显式声明下列私有语义，默认值应偏向 `OPAQUE/NEVER`：

```text
effect: READ | MUTATE | EXECUTE | OPAQUE
resource_key(args): stable private resource identity
operation_key(args): stable logical execution identity
replacement: LATEST_READ | LATEST_RUN | SUCCESSFUL_RETRY | NEVER
policy_version: stable revision included in agent revision
```

**设计理想与当前编码方式。** 上述 `operation_key / replacement / policy_version` 是较完整的目标接口，不是当前源码已经逐字段暴露的 API。当前 [`ToolContextPolicy`](../../src/react_agent/context.py) 只有 `effect` 与 `identity_fields`：内部 action key 由 `tool name + canonical full arguments` 计算，`identity_fields` 只建立跨 read/mutate 的 resource identity；显式 `EXECUTE` 编码当前的 latest-run 规则，`READ` 编码精确参数重读规则，默认 `OPAQUE` 不淘汰任何成功事实，但允许“完全相同 action 的旧失败 → 新成功”这一保守 successful-retry 淘汰。轮询、时间、随机、分页和增量日志工具应保持 `OPAQUE`，不能声明 `EXECUTE`。当前没有独立 `policy_version` 字段；[`ReActAgent.tool_manifest_hash`](../../src/react_agent/agent.py) 把 tool version、`effect` 和 `identity_fields` 一并纳入 manifest，再由 agent revision 绑定。若未来需要 selector compatibility 或同参数不同 replacement 语义，再演进为上述显式三字段接口，不能把设计草案冒充现状。

Tier 1 当前实现的保守规则：

1. **修改淘汰旧读：** 同 resource 的成功 `MUTATE` 可淘汰它之前的 `READ` observation；写操作自身必须保留为因果证据。
2. **重读：** `READ` 的 tool name + 完整 canonical arguments 相同时，新的成功读取淘汰旧读取；新读取失败不能淘汰旧成功读取。当前没有独立 selector-compatibility API，因此参数不完全相同就保守保留。
3. **成功重试：** 同一内部 action key 的“旧失败 → 新成功”可淘汰旧失败；“旧成功 → 新失败”不能反向淘汰旧成功。该规则也适用于默认 OPAQUE，但 OPAQUE 的成功事实彼此永不互换。
4. **重跑：** 工具作者显式声明 `EXECUTE` 即选择当前 latest-run 语义；action key 完全相同时，成功重跑淘汰全部旧输出，失败重跑只淘汰旧失败而保留旧成功。当前没有单独的 `LATEST_RUN` 枚举；轮询、时间、随机、分页、增量日志和外部状态查询必须保持 `OPAQUE`。
5. **协议结构：** 不孤立删除 tool result；应保留原 assistant call 与结果位置，只把结果替成含 `reason`、`superseded_by`、原内容 SHA-256 的最小 observation。对 provider-native opaque `raw_items`，只能淘汰完整已完成 block 或保留原 call 并缩短 output。
6. **纯函数与幂等：** 同一 canonical transcript、tool policy revision 和 budget 必须产生字节一致的投影与统计；不读取当前文件系统或时钟来决定历史事实。

Tier 2 只有 Tier 1 后仍超过阈值才触发。Summary key 至少应包含：

```text
sha256(algorithm_revision || compressor_model_revision || prompt_revision ||
       source_transcript_hash || summary_limit)
```

Summary 与 source hash 必须私有持久化、原子写入、内容校验；同一 source 在 resume/replay 时只复用，不再次计费。Summary 是“不可信历史数据”，不能拼进高权限 system instructions。模型调用、usage、cost、错误与 cache hit 都必须进入事件/遥测。

Tier 3 当前保证的是 **provider-neutral serialized envelope estimator** 不超过配置的字符预算：[`estimate_context_chars()`](../../src/react_agent/context.py) 对 instructions、tool specs 与规范化 transcript（包括 Responses `raw_items`）做稳定 JSON 序列化后计数。它不是某个 provider 的精确 wire bytes，也不是 provider tokenizer 的精确 token 上限；不同 SDK 的额外 envelope、转义或服务端 tokenization 仍需 provider-specific preflight/token counter 才能给出更强保证。兜底应优先机械缩短旧 observation/arguments，再移除完整旧 turn；最新用户目标若连同 instructions/tool schema 在该 estimator 下本身已超过预算，应显式 `CONTEXT_LIMIT`，不能静默截断目标。

**原创边界。** 本项目借鉴的是 observation masking / compaction 的分层思想、content-preserving tool pairing 与持久化 summary 经验；`modified / reread / rerun / successful-retry` 四类显式事件语义淘汰、包含完整 compressor/prompt/model revision 的内容寻址 summary、Tier 1→Tier 2→hard cap 的固定顺序，是 Witness Core 的工程组合与实现选择，不应反向归因给上述任一论文或仓库。

## 2. 上下文 A/B：测评矩阵与验收门槛

### 2.1 五臂实验，而不是只做一个二选一

| 臂 | 策略 | 回答的问题 |
| --- | --- | --- |
| A | Raw/full history + hard stop | 当前系统的成本与超限基线是什么？ |
| B | 通用 recency Observation Masking | 与公开论文的简单基线能否对齐？ |
| C | 通用 rolling LLM Summary | 用户要求的“通用上下文压缩”基线是什么？ |
| D | Deterministic eviction only | 零模型淘汰本身贡献多少？ |
| E | Deterministic → persistent Summary → hard cap | 三级机制是否在不降成功率的前提下优于 C？ |

主对比是 **E vs C**；A/B 是校准，D 是消融。不得只挑三级机制有利的 replacement-heavy 轨迹报告总平均。

### 2.2 必须分层的场景

| 场景层 | 轨迹构造 | 预期及主要失败风险 |
| --- | --- | --- |
| 同文件反复读取 | `read A` 5–50 次 | D/E 应大量受益；selector 不相容时不得误删 |
| edit → reread | 旧读、成功修改、新读 | 旧读应因 modified/reread 淘汰，写因果保留 |
| test fail → fix → rerun success | 同 operation key | 旧失败可被成功重试替代；需要保留最终成功 |
| success → rerun fail | 相同 command | 新失败不能使旧成功事实消失 |
| 多资源交错读取 | A/B/C 多文件和并行 calls | 不同 resource 绝不相互淘汰，tool pairing 始终合法 |
| 时间敏感/轮询/随机 | wait、status、date、tail、分页 | 默认 OPAQUE/NEVER；这是 false-positive 压力测试 |
| append-only 长任务 | 每轮都是新证据 | Tier 1 几乎无收益，应平稳回落到 C/Tier 2 |
| 短轨迹 | `<10`、`10–30` turns | summary 可能不触发；记录治理固定开销 |
| 长轨迹 | `≥31` turns、>100K context | 同时测 summary 次数、缓存与轨迹长度变化 |
| 崩溃恢复 | prune 前后、summary started/completed 后 | 不重复压缩、不重复主模型/工具副作用 |
| Provider 协议 | Chat Completions、Responses raw items、并行 tools | 100% 请求合法；opaque item 不做危险局部裁剪 |

### 2.3 指标与统计协议

每条轨迹都要记录，而非只报告美元：

- effectiveness：任务 solve/test pass、scripted 轨迹最终答案一致性、hard-limit 终止率；
- main model：每轮及累计 raw/cached input、output、reasoning tokens，调用数、首 token 与总延迟；
- compressor：调用数、source tokens、input/output tokens、cache hit、成本、延迟和失败率；
- context：canonical / deterministic / final chars 或 tokens、淘汰数量与原因、已知过期仍残留的字符、误淘汰数；
- durability：journal bytes、summary store bytes、fold/load p50/p95、resume 是否重复副作用；
- protocol/security：provider 请求合法率、public projection 泄漏数、summary/source hash 校验失败数。

真实任务应使用相同 instance、仓库 revision、模型 revision、temperature、seed（若 provider 支持）、turn/tool/time limits 和 pricing snapshot 做 paired runs；运行顺序随机化，冷缓存与暖缓存分开报告。成功率和成本都做 paired bootstrap 95% CI，并直接检验 E-vs-C，不复用论文只对 Raw 的检验。价格脚本与 token 原始值一起归档，避免 API 定价变化导致结论漂移。

### 2.4 明确验收标准

以下均为 **[本项目门槛]**：

1. 合成语义用例中 false eviction 为 0；不同 resource、旧成功→新失败和 OPAQUE 成功事实 100% 保留；OPAQUE 仅允许完全相同 action 的成功重试淘汰旧失败。
2. 同一输入连续投影 100 次，canonical JSON、投影 JSON、eviction report hash 各自只有 1 个唯一值。
3. 纯 replacement-heavy 用例在 Tier 1 后已低于预算时，E 的 compressor 调用严格为 0；C 至少调用一次。
4. 混合 replacement-heavy strata 中，E 相对 C 的 compressor 调用数或 compressor source tokens 至少下降 50%，且 paired-bootstrap 95% CI 上界仍低于 1.0。50% 是项目目标，不是论文保证。
5. 任何送往模型的 active projection 在 provider-neutral serialized-envelope estimator 下都不超过配置字符预算；summary 空、超长、异常或 store 损坏时仍能确定性 fallback 或显式 fail closed。若验收目标升级为特定 provider 的 wire/token 精确上限，必须另加该 provider 的序列化/token-count preflight，不能用当前字符 estimator 代替。
6. Scripted 场景 E 与 Raw 的最终答案、必要工具执行次数和最终 workspace digest 相同。
7. 真实仓库任务中，E 相对 C 的 solve-rate 差值 95% CI 下界不低于 `-2pp`；同时单独报告 replacement-heavy 与 append-only strata，不能用总体均值掩盖退化。
8. 删除 snapshot 后从 event sequence 1 重放，active context 和 summary reuse 决策逐字节相同；已完成 summary 不再次调用模型。
9. public SSE/API/OTel/PostgreSQL view 不出现原 observation、resource key、summary 正文或完整工具参数。
10. 本地只跑单元、合成和小型集成测试；完整 SWE-bench/仓库级 paired runs 按用户要求放到 `zht-server2` 的固定 Docker image 中，归档 image digest、git SHA、配置、seed、模型与价格快照。

### 2.5 当前实现已经覆盖的机制证据

- [`test_context_governance.py`](../../tests/test_context_governance.py) 已覆盖默认 OPAQUE、跨资源隔离、重读/修改/重跑/成功重试、旧成功→新失败、并行乱序 tool pairing，以及同一输入 100 次 projection/report 字节稳定性。
- 同文件的 Responses `raw_items` 用例验证 hard fallback 把 provider-native items 当作不透明协议单元保留，不对 reasoning/function-call raw item 做危险的局部裁剪；canonical transcript 与 active projection 的双视图也有 Agent 集成测试。
- [`FileContextSummaryStore`](../../src/react_agent/context.py) 使用 `0700` 私有目录、`0600` 临时文件、file `fsync`、first-writer-wins hard link 与 directory `fsync` 发布最终记录；读取时拒绝非 regular file、非私有权限、source/content hash 不一致。并发 collision 与损坏 store fail-closed 分别由专用测试覆盖。这是“原子持久化”的当前证据，而不是仅有接口声明。
- [`test_runtime_atomic_recovery.py`](../../tests/test_runtime_atomic_recovery.py) 对 compression terminal atomic batch 的 **commit 后 ack-loss** 与 **commit 前 crash** 做故障注入：前者恢复后只保留一次已知 usage/cost，后者写入一次 unknown-cost `abandoned`；两者均复用已落盘的内容寻址 summary、compressor 不重调、主工具不重执行。该范围证明已注入的两个 durable seam，不代表所有文件系统/进程崩溃点都已穷举。

## 3. Runtime Debugging：候选参考实现与标准协议

### 3.1 `oh-my-pi` 的 DAP debug tool

固定源仍为 [`oh-my-pi@72000ac`](https://github.com/can1357/oh-my-pi/tree/72000acfeb902e21816252699482887f34d1a5a4)：

- Agent tool：[`packages/coding-agent/src/tools/debug.ts`](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/packages/coding-agent/src/tools/debug.ts)
- DAP transport/client：[`dap/client.ts`](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/packages/coding-agent/src/dap/client.ts)
- state/session manager：[`dap/session.ts`](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/packages/coding-agent/src/dap/session.ts)
- adapter resolution：[`dap/config.ts`](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/packages/coding-agent/src/dap/config.ts)
- built-in adapters：[`dap/defaults.json`](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/packages/coding-agent/src/dap/defaults.json)
- 第一方工具说明：[`docs/tools/debug.md`](https://github.com/can1357/oh-my-pi/blob/72000acfeb902e21816252699482887f34d1a5a4/docs/tools/debug.md)

**[确认]** 它提供 launch/attach、source/function/instruction/data breakpoints、continue/step/pause、threads/stack trace/scopes/variables/evaluate，以及 memory/disassembly/modules/sources/output/terminate/sessions 等完整动作；read-only 动作和执行动作使用不同审批级别。Session manager 缓存当前 thread/frame/stop，维护 breakpoint set，支持 stdio/socket/TCP adapter 与 child sessions，并把 output ring 限在 128 KiB。内置 Python adapter 是：

```json
{
  "command": "python",
  "args": ["-m", "debugpy.adapter"],
  "launchDefaults": {"request": "launch", "justMyCode": false, "stopOnEntry": true}
}
```

**迁移结论。** Witness Core 是 Python-only MVP，无需复制多语言、memory 或 child-session 全集；应保留其“高层工具 → 有状态 session manager → DAP client”的边界，首版只实现 launch、set breakpoints、continue/step/pause、stack、frame select、scopes/variables、stop。`evaluate`、PID attach、write memory 和 data breakpoint 都扩大任意代码执行面，不是用户当前验收的必要条件。

### 3.2 JetBrains Agentic Debugging：MCP 工具面已确认，PyCharm 归属未确认

JetBrains 官方 [`IntelliJ IDEA 2026.2 Agentic debugging`](https://www.jetbrains.com/help/idea/2026.2/agentic-debugging.html)（页面日期 2026-07-31，访问于 2026-08-20）明确说明：外部 Claude、Codex 或 Junie 可经集成 MCP server 启停调试会话，设置/删除断点，step/resume/pause，读取 threads、stack 与 frame values，并 evaluate expression / set variable；工具前缀为 `xdebug_*`。Shared MCP debugger tools 自 IDEA 2026.1.3 起提供，2026.2 的 router mode 则把命令放进按需加载的 debugger skill，并可路由 GDB、LLDB 和 DAP-based debugger。官方也直接把适用场景描述为定位静态源码和日志难以发现的运行时错误值。

**[未确认] PyCharm 等价能力。** [`PyCharm 2026.2 MCP Server`](https://www.jetbrains.com/help/pycharm/2026.2/mcp-server.html)（页面日期 2026-05-13，访问于 2026-08-20）没有与 IDEA 页面等价的 Agentic Debugging 专章或完整 Debugger tools 清单；`/help/pycharm/agentic-debugging.html` 在本次核查时返回 404。PyCharm MCP 文档中的零散 `xdebug_start_debugger_session` 参数提示不足以证明完整工具面。因此本文只把 JetBrains 作为 **IntelliJ IDEA 已确认的 MCP debugger UX**，不把它写成 “Py Agent” 或 “PyCharm 已实现” 的事实。

**迁移结论。** JetBrains 一手资料支持“调试能力应作为 Agent 工具按需暴露，并围绕断点、栈和变量组织”的产品边界；但其 IDE 插件实现不是本文可固定审计的源码来源。Witness Core 的具体 DAP transport/session 仍以 `oh-my-pi`、Microsoft DAP 与 debugpy 为可审计参考。

### 3.3 Microsoft DAP 的不可省略时序

第一方规范：[`microsoft/debug-adapter-protocol@bf8a5d2`](https://github.com/microsoft/debug-adapter-protocol/tree/bf8a5d27e8040044b84b863f90916e08925ee811)；在线规范为 [Debug Adapter Protocol](https://microsoft.github.io/debug-adapter-protocol/specification)。

**[确认] 启动时序：**

```text
spawn/connect adapter
  → initialize request
  ← capabilities response
  → launch or attach request                 ┐ may overlap
  ← initialized event                        │
  → setBreakpoints / setExceptionBreakpoints │ configuration phase
  → configurationDone                        ┘
  ← launch/attach response
```

`setBreakpoints` 是“该 source 的完整 breakpoint 列表”，不是增量追加；客户端必须缓存 desired set，并以 adapter 返回的 actual/verified breakpoint 为准。[DAP overview: launch sequencing](https://github.com/microsoft/debug-adapter-protocol/blob/bf8a5d27e8040044b84b863f90916e08925ee811/overview.md#launch-sequencing)

**[确认] 停止后的观测瀑布：**

```text
stopped event → threads → stackTrace(threadId)
                         → scopes(frameId)
                             → variables(variablesReference)
                                 → variables(...) for children
```

Frame/scope/variable references 只在当前 suspended state 有效；一旦 continue/step 恢复运行就全部失效。实现必须额外维护单调 `stop_id`，所有 frame/variable 工具要求匹配当前 `stop_id`，拒绝 stale reference。[DAP overview: state access and reference lifetime](https://github.com/microsoft/debug-adapter-protocol/blob/bf8a5d27e8040044b84b863f90916e08925ee811/overview.md#stopping-and-accessing-debuggee-state)

DAP stdio framing要求 ASCII `Content-Length: <bytes>\r\n\r\n` header 和 UTF-8 JSON body；客户端测试必须覆盖分片 header/body、一次读多个消息、乱序 response/event、超长 payload、adapter 退出与 timeout。[DAP base protocol](https://github.com/microsoft/debug-adapter-protocol/blob/bf8a5d27e8040044b84b863f90916e08925ee811/overview.md#base-protocol)

### 3.4 debugpy 与 MCP SDK

**debugpy。** 固定主干快照 [`microsoft/debugpy@e5743d3`](https://github.com/microsoft/debugpy/tree/e5743d3a00c6dee7d8140275c7df7e719ebb132f)，MIT。README 确认 script/module launch、PID attach、`wait_for_client()`、programmatic breakpoint、post-mortem exception handler 和 debugger logs。[官方 README](https://github.com/microsoft/debugpy/blob/e5743d3a00c6dee7d8140275c7df7e719ebb132f/README.md)

最重要的安全事实是：debugpy 默认仅监听 `127.0.0.1`；官方明确警告绑定 `0.0.0.0` 后，任何能连接端口的人都可在被调试进程中执行任意代码。Witness Core 初版应直接启动 `python -m debugpy.adapter` 的 stdio DAP，不开放网络端口。

**[确认] 版本边界。** 上述 main 快照比当前发布版更新；实施依赖应固定 PyPI `debugpy==1.8.21`（tag `v1.8.21`，commit [`858b05c`](https://github.com/microsoft/debugpy/commit/858b05c08555cfc54efa7cf90e70184c7495b38e)）并跑完整 handshake contract test。`python -m debugpy.adapter` 是官方源码提供的入口，但不是 README 主推的稳定公共 CLI，所以不能无上限跟随 main。

**MCP Python SDK。** 固定快照 [`modelcontextprotocol/python-sdk@0d92192`](https://github.com/modelcontextprotocol/python-sdk/tree/0d92192765fa7d6a20fbfe7e62e242e44933574f)，MIT，Python `>=3.10`。当前稳定线是 v2，支持 MCP spec `2026-07-28` 及更早版本；`pip install mcp` 已默认安装 2.x，v1.x 仍在维护但迁移有 breaking changes。[官方 README](https://github.com/modelcontextprotocol/python-sdk/blob/0d92192765fa7d6a20fbfe7e62e242e44933574f/README.md)

SDK 的 `MCPServer` 可直接用 type-hinted Python 函数生成工具 schema，并支持 stdio、Streamable HTTP、SSE。对本项目最安全、最容易验收的 transport 是 stdio。若 Agent 只注册一组同名本地 `@tool`、并未通过 MCP `list_tools/call_tool`，则不能宣称“MCP 工具集成已落地”。

**[确认] 版本边界。** 当前发布版为 `mcp==2.0.0`（tag `v2.0.0`，commit [`6f69a37`](https://github.com/modelcontextprotocol/python-sdk/commit/6f69a3758ebf2ee55ce050f58b470ce11af71133)）；main 快照在其后。新实现应约束 `mcp>=2,<3` 并固定 lockfile，已有 v1 集成若尚未迁移则必须单独约束 `mcp>=1.28,<2`，不能让安装器隐式跨 major 升级。

## 4. Runtime Debugging 的论文证据：能借鉴什么，不能外推什么

### 4.1 Debug2Fix

第一方论文：Garg & Huang, *Debug2Fix: Can Interactive Debugging Improve Agentic Software Engineering?*, [`arXiv:2602.18571v2`](https://arxiv.org/abs/2602.18571v2)。论文使用 Java JDB 与 Python PDB，并将低层 debugger 封装在专用 Debug Subagent 后面。

**[确认] 结果。** GitBug-Java 的 186 个可执行实例中，带强制首次调试的 Debug2Fix 将 GPT-5 从 60.2% 提至 73.1%（相对 `+21.8%`），Claude Haiku 4.5 从 71.0% 提至 82.3%（`+15.9%`），Claude Sonnet 4.5 从 75.7% 提至 85.5%（`+12.9%`）。直接把低层 debug tools 暴露给主 Agent 时，GPT-5 仅 60.8%，Sonnet 反而降到 64.5%。SWE-Bench-Live Python 400 例中，GPT-5 为 31.2%→36.2%，Haiku 34.3%→38.5%，Sonnet 39.6%→40.4%，收益随实际 tool call rate 明显变化。[Tables 1–2](https://arxiv.org/pdf/2602.18571v2#page=8)

**[确认] 代价与限制。** Subagent 可额外使用几十步和数十万 tokens；论文没有计 debugger 本身的计算成本。36% 的抽样失败与项目不能 build/attach 有关。论文 Data Availability 明确表示源码因公司政策不能发布，所以没有可审计的官方实现仓库或代码许可证，不能声称“复用了 Debug2Fix 代码”。

以 GPT-5 为例，GitBug-Java Tool Limit 报告主 Agent 645k + Debug Subagent 350k tokens，而 baseline 为 347k；Python 设置为 630k + 350k，对比 baseline 550k。论文失败抽样还包括“调试正确但主 Agent 修错”34%、“需超过 3 次会话”16%、API 错误 8%、退化为静态分析 6%。因此测评必须同时报告 attach/launch 成功率、debug 调用率、证据到修复的转化率、会话数、tokens 和 wall time，不能只报告最终 solve rate。

对本项目的可迁移结论是“低层状态机需要高层、窄接口”，不是“必须再套一个 LLM subagent”。本项目的 DAP session manager 可以承担同样的抽象作用，同时 PR evidence 生成保持零模型调用。

### 4.2 InspectCoder / InspectWare

第一方论文：*InspectCoder: Empowering Large Language Models with Interactive Debugging for Code Repair*, [`arXiv:2510.18327v1`](https://arxiv.org/abs/2510.18327v1)。它以 PDB 为底层，使用 Program Inspector + Patch Coder 双 Agent，并用 InspectWare 将 debugger 抽象为 Start、Runtime State、Runtime Error、Post Mortem、Done 五种状态。

**[确认] 结果范围。** 这是 LLM-generated single-program self-repair，不是 repository-level SWE-bench。BigCodeBench-R 607 例中 InspectCoder resolve rate 67.87%，强基线 LDB block-level 为 64.58%；LiveCodeBench-R 151 例中为 12.58% vs 7.95%。论文报告相对提升 5.10%–60.37%，time efficiency 为最佳基线的 1.67×–2.24×。去掉 InspectWare 时 resolve rate 分别降至 25.70% 和 4.10%，说明状态抽象与输出过滤很重要。[Tables 1–2](https://arxiv.org/pdf/2510.18327v1#page=12)

**[确认] 代码可用性风险。** 论文声称将开放 InspectWare，但官方仓库 [`greenlight2000/InspectCoder_framework@bca652c`](https://github.com/greenlight2000/InspectCoder_framework/tree/bca652c97e492318468b6f7efbfdbfe45780bdd9) 在本快照只有“under a data disclosure process”占位 README，且没有 LICENSE。它是设计证据，不是当前可移植代码来源。

**[确认] 论文叙述数字存在内部冲突。** 摘要/正文称 LiveCodeBench-R 相对 LDB 提升 60.37%，但 Table 2 的 `12.58% vs 7.95%` 复算约为 58.24%（按离散成功数 `19 vs 12` 为 58.33%）；正文称效率为 2.24×，但 Table 2 的 `1.88 / 0.89` 约为 2.11×。这不是普通四舍五入能解释的差异，本文只把它作为方向性设计证据，不把叙述值用作验收基线。

### 4.3 LDB 与 AgentStepper 的边界

- **LDB**：[`arXiv:2402.16906v6`](https://arxiv.org/abs/2402.16906v6)，官方代码 [`FloridSleeves/LLMDebugger@49ac191`](https://github.com/FloridSleeves/LLMDebugger/tree/49ac191f181d47911cf38e5b9944fbbe6d4a6e60)，Apache-2.0。它按 basic block 收集运行时变量，再让 LLM 验证 block；HumanEval/MBPP/TransCoder 的 Pass@1 最多提高 9.8%。它证明运行时中间状态有价值，但不是交互式 DAP client，也不支持零模型 PR evidence。[论文 Table 1](https://arxiv.org/pdf/2402.16906v6#page=4)
- **AgentStepper**：[`arXiv:2602.06593v1`](https://arxiv.org/abs/2602.06593v1)，官方代码 [`sola-st/AgentStepper@e1eeeea`](https://github.com/sola-st/AgentStepper/tree/e1eeeeac6fa518aec6178e4c7b307eea47b78821)，MIT。它给“Agent trajectory”加 breakpoint/step/live edit，调试的是 Agent 自身轨迹，不是 Python debuggee。其 `prompt_helper.py` 会调用 OpenAI（默认 `gpt-5-nano`）为 prompt/response/tool 事件生成摘要，run 又包含随机 UUID 和墙钟时间，因此也不是“零模型、字节确定性证据”的现成实现。结构化事件 UI 可作为展示参考，但不能替代 debugpy/DAP。

## 5. Witness Core 的 Runtime Debugging / MCP 落地设计

### 5.1 模块边界

**[本项目设计] 一个核心，三个 adapter：**

```text
Agent ToolRegistry ─┐
                    ├─ MCP tool adapter ──┐
external MCP host ──┘                     │
                                          ▼
                             PythonRuntimeDebugger
                          state / policy / normalization
                                          │
                                          ▼
                                stdio DAP client
                                          │
                                          ▼
                               python -m debugpy.adapter
```

建议的窄工具集：

1. `python_debug_launch(program, args, breakpoints, exception_policy)`
2. `python_debug_set_breakpoints(file, lines)`
3. `python_debug_control(action=continue|next|step_in|step_out|pause)`
4. `python_debug_stack(levels)`
5. `python_debug_select_frame(stop_id, frame_index)`
6. `python_debug_variables(stop_id, scope=locals|arguments, limits...)`
7. `python_debug_stop()`

同一 handler 同时服务 MCP server 与本仓库 `ToolRegistry` adapter，避免两套行为漂移。严格 MCP 验收还要启动 stdio server，用官方 client 调 `list_tools` 和 `call_tool`；Agent 侧要么真正走 MCP transport，要么明确说明是同核心的 in-process adapter，不能把后者伪装成前者。

### 5.2 状态机与安全不变量

```text
created → initializing → configuring → running ↔ stopped → exited/terminated
```

- 所有 execution-changing 动作串行化且 `parallel_safe=False`；非法状态返回结构化错误，不自动猜测或重试。
- `launch/continue/step/stop` 是副作用工具，默认需审批且 `NEVER_RETRY`；stack/scopes/variables 在同一 `stop_id` 内可幂等读取。
- `DebugContext.workspace_path` 由框架注入，模型不得指定。program/cwd/breakpoint source 必须 realpath 后仍位于 managed workspace；拒绝绝对越界、`..` 与 symlink escape。
- 使用 `asyncio.create_subprocess_exec` 参数数组，不用 `shell=True`；取消、timeout、Runtime close 都必须终止 adapter/debuggee process group 并 await 回收。
- 初版只允许 stdio debugpy adapter，不监听 TCP；不支持任意 PID attach。
- 默认不开放 `evaluate`。变量探测只读取选定 frame 的 Locals/Arguments，限制 stack depth、frame count、variable count、value length、递归深度和 observation bytes。
- secret-like 名称（token/key/password/secret/credential/cookie/authorization 等）及高熵值在持久化前脱敏；绝对路径改成 workspace-relative。原始变量不进入 public SSE、日志或 OTel。
- 调试执行仓库代码本质是任意代码执行，功能默认不注册；只有显式 opt-in、managed workspace 和审批均满足时启用。

### 5.3 可疑栈帧选择必须确定性

不用模型判断 suspicious frame。稳定排序建议为：

1. source realpath 位于当前 workspace；
2. 排除 stdlib、site-packages、debugpy 内部帧；
3. 优先 exception/breakpoint 的当前 thread；
4. 优先调用栈中最深的用户 frame；
5. 并列按 stack index、workspace-relative path、line、function name 排序。

输出必须包含 `selection_rule_version` 和逐条命中理由。DAP `frameId` 只在当前 stop 有效，所以 evidence 使用稳定的 `stop_id + frame_index + normalized source/line/function`，不能在 continue 后复用旧 `frameId`。

### 5.4 Observation schema 与持久化

每次调试工具完成后，先规范化、裁剪、脱敏，再进入现有 `tool_planned → tool_started → tool_completed` 私有事件链。建议 observation 至少包含：

```json
{
  "schema_version": 1,
  "observation_kind": "variables",
  "debug_session_id": "opaque-id",
  "state": "stopped",
  "stop_id": 3,
  "thread_id": 1,
  "selected_frame": {
    "frame_index": 0,
    "function": "unit_price",
    "path": "demo/pricing.py",
    "line": 27
  },
  "variables": [
    {"scope": "locals", "name": "item_count", "type": "int", "value": "0",
     "redacted": false, "truncated": false}
  ],
  "limits": {"truncated": false},
  "observation_sha256": "sha256-of-canonical-payload-without-this-field"
}
```

Public event 只保留 action、成功/失败、数量、truncated flag 和 digest；完整 observation 只写 private checkpoint。现有 event hash chain 提供外层 sequence/hash 内部一致性检查，`observation_sha256` 用于验证 report generator 收到的规范化对象与持久化摘要一致。

两层机制都是无密钥 SHA-256 校验，不提供来源认证，也不证明日志能抵御恶意写入者。没有可信的外部 head、HMAC 或数字签名时，能够改写完整 journal 的主体可以同时重算 observation digest 与后续 event hash；因此这里的安全边界仍依赖数据库权限、append-only trigger、fencing，以及需要时由独立系统保存的可信 head。

## 6. “诊断即取证”：零模型 PR Evidence

### 6.1 纯回放算法

```text
RunJournal.read(run_id)
  → verify sequence + previous_hash + event_hash
  → filter versioned python_debug_* tool lifecycle events
  → pair planned/started/completed by call_key
  → validate observation schema + observation_sha256
  → choose exception stop and suspicious frame by stable rule
  → collect directly supporting locals/arguments
  → canonical JSON (sorted keys, fixed separators)
  → fixed-template Markdown
```

生成器不能调用模型、debugger、文件系统当前状态或网络。时间线使用事件中已记录的 sequence/event_id/occurred_at；排序以 sequence 为准。同一 journal 重放所得 JSON 与 Markdown 必须逐字节一致。

### 6.2 根因措辞的证据边界

允许的固定模板示例：

> Observed `ZeroDivisionError` at `demo/pricing.py:27` in `unit_price`. The selected workspace frame recorded `item_count = 0` and `subtotal = 99.0` at durable event sequence 14.

这句话只陈述异常位置与被捕获变量。若日志没有证明“为什么 item_count 为 0”，报告不得自动写“过滤逻辑错误导致”；那是模型或 reviewer 后续可做的推断。

PR Markdown 至少包含：

- Observed failure / root-cause evidence；
- 规范化 reproduction command；
- selected suspicious frame 与 selection rule；
- captured locals/arguments 和 redaction/truncation；
- durable timeline：sequence、event ID、call key、action、outcome、digest；
- run head hash、evidence JSON hash；
- evidence limitations；
- 固定声明：`Generated deterministically from journal events whose sequence/hash/digest consistency was verified; no model summarization was used.`

## 7. Demo 与 Debugging 测评

### 7.1 Golden demo

构造一个源码静态可见“发生除法”、但关键输入只在运行时形成的订单计价 bug：测试 fixture 组合 feature flags 和嵌套 order 数据，过滤后 `billable_items=[]`，随后 `unit_price(subtotal, item_count)` 触发 `ZeroDivisionError`。静态检索可以看到崩溃行，却难以无运行时状态确定是哪一层配置令 `item_count=0`。

自动 demo 流程：

1. 通过 MCP 启动 debugpy target，并设置业务函数 breakpoint + uncaught exception policy；
2. continue 到 stop，读取 threads/stack；
3. 用确定性规则选中 workspace frame；
4. 读取 Locals，捕获 `item_count=0`、`billable_items=[]`；Arguments 路径由同样的
   debugger facade 测试覆盖；
5. 终止 session，确认无孤儿进程；
6. 从 durable journal 生成 `pr-evidence.json` 与 `pr-evidence.md`；
7. 丢弃任何派生报告，从私有 event journal 的 sequence 1 重放；连续生成 5 次并
   比较 SHA-256。该 standalone journal 本身不使用 snapshot。

### 7.2 功能、安全与证据验收

| 指标 | Golden demo / test suite 门槛 |
| --- | --- |
| `mcp_tools_listed` | 7/7，schema 与 Agent adapter 一致 |
| `breakpoint_exact` | `true`，adapter 返回 verified breakpoint |
| `suspicious_frame_exact` | `true`，命中预设业务 frame |
| `expected_locals_recall` | `1.0`，包含全部预设关键变量 |
| `state_transition_errors` | 合法路径 0；非法路径 100% 结构化拒绝 |
| `required_evidence_coverage` | `1.0`：异常、文件、行、函数、frame、locals、退出状态齐全 |
| `report_model_calls` | `0`；这是 evidence generator 只接收已封存 events、不持有 model dependency 的构造不变量，不是供应商计费遥测 |
| `replay_debugger_calls` | `0`；重放只读 event journal，不持有 debugger dependency |
| `report_hashes_unique` | 连续 5 次各产物均为 `1` |
| `stale_hash_or_digest_mismatch_detection` | 改 payload 但不把对应 hash/digest 一并重算时 100% fail closed；不声称能阻止整链重算 |
| `public_secret_leaks` | `0` |
| `workspace_escape_accepts` | `0` |
| `orphan_processes` | Golden 正常终止为 `0`；真实 debugpy 的 cancel 与 debugger close 由进程级测试验证为 `0` |

真实 utility 不能由一个 demo 证明。建议再生成 20–50 个有 ground truth 的 mutation variants，并做两臂：

- Static-only：相同 traceback、repo search/read/test 工具，无 runtime debugger；
- DAP：在相同基础上增加上述 7 个工具。

比较 root-cause localization exact match、关键变量 recall、修复通过率、主模型 steps/tokens、wall time、调试调用率和失败类型。必须把“项目不可运行/attach 失败”单独统计，不能算作模型诊断错误；同时报告 DAP 未被调用的任务，避免只在成功调用子集上计算收益。

## 8. 版本、许可证与可复用性登记

| 来源 | 固定版本 | 许可证/状态 | 本项目风险与处理 |
| --- | --- | --- | --- |
| The Complexity Trap 代码/分析产物 | [`bf15b5f`](https://github.com/JetBrains-Research/the-complexity-trap/commit/bf15b5fb7d279679035a007ac9a81084d6b9a89a) | MIT；无 release tag | 必须固定完整 SHA；Masking 是 recency 而非事件语义；hybrid 不是严格同样本五臂；原始 CSV 某些臂少于 500 行 |
| The Complexity Trap 原始轨迹数据 | [`03f0312`](https://huggingface.co/datasets/JetBrains-Research/the-complexity-trap/blob/03f031299ea00194235458d6b353242d7f5a9180/README.md) | Apache-2.0 | 与代码仓库许可证不同；复现实验需同时固定 dataset revision 和缺失样本口径 |
| LLMLingua / LLMLingua-2 | 论文 [`2310.05736v2`](https://arxiv.org/abs/2310.05736v2)、[`2403.12968v2`](https://arxiv.org/abs/2403.12968v2)；repo [`e0e9d99`](https://github.com/microsoft/LLMLingua/commit/e0e9d99beb94098bbd924aa53c2c112eac41c758) | MIT | token-level 有损压缩；仅作额外 baseline/Tier 2 候选，不用于事件语义 Tier 1 |
| Selective Context | 论文 [`2310.06201`](https://arxiv.org/abs/2310.06201)；repo [`3074343`](https://github.com/liyucheng09/Selective_Context/commit/3074343653bbf3559a87a588667e843744bc6f2a) | 固定快照无 LICENSE | 只引用论文与行为，不复制实现；同样不是 supersession 算法 |
| Anthropic Context Editing / Compaction / Claude Code auto-compaction | mutable 官方文档；策略 `clear_tool_uses_20250919`、`compact_20260112`；访问日 2026-08-20 | 厂商 API/产品行为，不是固定源码实现 | 作为 recency masking、生成式 compaction 与 prompt-cache 影响的比较对象，不声称代码复用 |
| oh-my-pi | [`v17.4.0`, `72000ac`](https://github.com/can1357/oh-my-pi/commit/72000acfeb902e21816252699482887f34d1a5a4) | MIT | TypeScript/Bun、多语言范围过大；参考接口与算法，在 Python 中独立实现 |
| Comis | [`v1.0.63`, `56c4759`](https://github.com/comisai/comis/commit/56c47590d3e25e148a28a4b0009d09ef96e0195f) | Apache-2.0 | 相同 command 并非必然 supersede；不复制猜测规则，使用显式 tool policy |
| JetBrains Agentic Debugging / MCP Server | IntelliJ IDEA 2026.2 官方文档（2026-07-31）；PyCharm 2026.2 MCP 文档（2026-05-13）；访问日 2026-08-20 | mutable 产品文档；插件源码未作为本项目实现来源 | 只确认 IDEA 的 MCP debugger tool surface；PyCharm 完整等价能力与 “Py Agent” 命名归属均未确认 |
| Microsoft DAP | [`bf8a5d2`](https://github.com/microsoft/debug-adapter-protocol/commit/bf8a5d27e8040044b84b863f90916e08925ee811) | code 由 [`License-code.txt`](https://github.com/microsoft/debug-adapter-protocol/blob/bf8a5d27e8040044b84b863f90916e08925ee811/License-code.txt) 授权为 MIT；文档 [`License.txt`](https://github.com/microsoft/debug-adapter-protocol/blob/bf8a5d27e8040044b84b863f90916e08925ee811/License.txt) 的声明头指定 CC BY 3.0 US，但同一文件随后嵌入完整 CC BY 4.0 文本 | 文档许可文本存在版本歧义；只实现协议、不复制大段规范正文；依 capabilities 协商 |
| debugpy | 研究快照 [`e5743d3`](https://github.com/microsoft/debugpy/commit/e5743d3a00c6dee7d8140275c7df7e719ebb132f)；实施 pin [`v1.8.21`, `858b05c`](https://github.com/microsoft/debugpy/commit/858b05c08555cfc54efa7cf90e70184c7495b38e) | MIT；研究 SHA 是未打 tag 的 main 快照 | 固定已发布版本并做 handshake contract test；adapter module 不是 README 主推接口；永不暴露公网端口 |
| MCP Python SDK | 研究快照 [`0d92192`](https://github.com/modelcontextprotocol/python-sdk/commit/0d92192765fa7d6a20fbfe7e62e242e44933574f)；发布版 [`v2.0.0`, `6f69a37`](https://github.com/modelcontextprotocol/python-sdk/commit/6f69a3758ebf2ee55ce050f58b470ce11af71133) | MIT；Python ≥3.10 | v1→v2 breaking；新实现用 `mcp>=2,<3` + lockfile，stdio 为首选 |
| Debug2Fix | [`arXiv:2602.18571v2`](https://arxiv.org/abs/2602.18571v2) | 论文；作者明确源码不能发布 | 只能借鉴论文架构/指标，不能声称代码复用 |
| InspectCoder | [`arXiv:2510.18327v1`](https://arxiv.org/abs/2510.18327v1)，repo [`bca652c`](https://github.com/greenlight2000/InspectCoder_framework/commit/bca652c97e492318468b6f7efbfdbfe45780bdd9) | repo 无 LICENSE、代码未披露 | 只作设计与实验佐证，不复制实现 |
| LDB | repo [`49ac191`](https://github.com/FloridSleeves/LLMDebugger/commit/49ac191f181d47911cf38e5b9944fbbe6d4a6e60) | Apache-2.0 | 面向生成程序/basic-block trace，不是仓库级交互 DAP |
| AgentStepper | repo [`e1eeeea`](https://github.com/sola-st/AgentStepper/commit/e1eeeeac6fa518aec6178e4c7b307eea47b78821) | MIT | 调试 Agent trajectory，不是 debuggee runtime |

## 9. 研究结论转成实施决策

1. 采用 canonical/private journal 与 active context 双视图；任何治理都不删除审计事实。
2. 先实现显式 tool policy 驱动的确定性淘汰，并以 OPAQUE/NEVER 为默认；不要移植工具名/相同命令猜测。
3. Summary 只在 Tier 1 后超预算时触发，必须 content-addressed、可持久化、可复用、完整计价。
4. provider-neutral serialized-envelope 字符预算是 model call 前的不变量，不是运行结束后的统计；不要把它表述成特定 provider 的 wire/token 精确上限。
5. Runtime Debugging 首版限定 Python + stdio debugpy；实现 DAP 正确时序与 stop-scoped references，再封装 MCP。
6. Debug observations 在持久化前规范化、裁剪和脱敏；现有 tool lifecycle/hash chain 足以承载证据，无需另建一套不可验证日志。
7. PR Evidence 使用纯回放和固定模板，生成阶段 `model_calls=0`、`debugger_calls=0`；sequence/hash/digest 任一内部不一致均 fail closed，但无可信外部 head 时不声称提供 authenticity 或抵御整链重算。
8. 上下文采用五臂、同样本 paired A/B；Debugging 采用 golden demo + static-vs-DAP mutation suite。重型真实仓库实验只在 `zht-server2` Docker 执行。
9. 来源归属必须拆分：`oh-my-pi` 是可审计的 DAP/session architecture 参考，JetBrains 是已确认的 IDEA MCP debugger 工具面；本文没有证据把用户所说 “Py Agent” 唯一映射到其中任一个。
