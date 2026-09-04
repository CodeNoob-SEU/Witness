# Project-level PR Demo 与 Agent Runtime 评测：第一方证据调研

> 调研快照：2026-08-27（Asia/Shanghai）。外部产品能力均可能变化；本文将产品事实标为 **[易变]**，将固定论文、数据集 revision 或源码 commit 标为 **[固定]**。访问日期若无另注均为 2026-08-27。本文只使用官方文档、官方源码仓库、原始论文和 benchmark 官方仓库。

## 0. 结论先行

### 0.1 最重要的定位

**“读仓库 → 改代码 → 跑测试 → 开 PR”已经是 table stakes，不能再作为 Witness 的主差异化。**

- GitHub Copilot cloud agent 已公开支持后台研究仓库、制定计划、在独立的 GitHub Actions 临时环境中改代码和跑测试、创建分支并发起 PR；官方文档还提供 created/merged PR 与 median time-to-merge 指标。[GitHub Docs 固定版本，能力与临时环境](https://github.com/github/docs/blob/f14de77221fa6dcf6ab333a8f4ab7bc600500231/content/copilot/concepts/agents/cloud-agent/about-cloud-agent.md#L24-L59)、[PR outcome metrics](https://github.com/github/docs/blob/f14de77221fa6dcf6ab333a8f4ab7bc600500231/content/copilot/concepts/agents/cloud-agent/about-cloud-agent.md#L83-L93)。
- Codex cloud 已公开支持隔离云环境、后台并行任务、summary/diff 审阅、follow-up 和开 PR；其本地 SDK 也明确支持恢复既有 thread。[Codex cloud](https://developers.openai.com/codex/cloud)、[Cloud environments](https://developers.openai.com/codex/cloud/environments)、[Codex SDK resume](https://learn.chatgpt.com/docs/codex-sdk) **[易变]**。
- Claude Code 已公开支持 PR/issue 驱动的 GitHub Action、会话恢复、worktree 并行隔离、自动 PR review；其 managed Code Review 会用多 agent 在全仓上下文中并行寻找并验证候选问题。[GitHub Actions](https://code.claude.com/docs/en/github-actions)、[sessions](https://code.claude.com/docs/en/sessions)、[worktrees](https://code.claude.com/docs/en/worktrees)、[Code Review](https://code.claude.com/docs/en/code-review) **[易变]**。

因此，Witness 应展示的不是“另一个 coding agent UI”，而是下面这条可验证命题：

> **在真实、跨文件、会运行工具的 PR 任务中，即使进程在预注册的语义 fault boundary 上崩溃、webhook 被重投、工具结果处于不确定状态，Witness 仍能从 durable 事实恢复；它不会盲目重复非幂等副作用，并能从同一事件链无模型地生成可校验、可重放、可归责的 PR 证据。**

这是比“有 resume 按钮”更窄、也更可防守的主张。公开竞品文档确实已经覆盖会话恢复、checkpoint、日志、worktree 和 OTel；但本次审阅的公开保证层没有发现它们承诺 **intent-before-side-effect 的 durable 边界、ambiguous tool outcome 的显式 reconciliation、从 sequence 1 的纯 reducer 重建、无副作用 replay、前向 hash chain，以及同一日志生成字节稳定证据**。这应表述为“未见公开保证”，不能写成“竞品一定没有”。

**当前 readiness：** 仓库已经有事件链、纯 reducer、Resume/reconciliation、lease/fencing、worktree checkpoint 和调试证据生成器。实现更新：fake GitHub sink 的 deterministic reliability demo、最小 Check publisher 和 project-level Evidence Bundle 已落成本地 P0 vertical slice；GitHub App、PR PostgreSQL store、独立进程恢复、通用项目级 coding toolset 与生产 UI 仍是 P1 待贯通能力，不能把本报告中的完整目标架构写成现成功能。

### 0.2 推荐的 hero demo

做一个 4–5 分钟 reliability hero，并保留 12–15 分钟深度版；两者都在单个 GitHub Draft PR 内完成，统一叫 **“Crash-to-Proof PR”**。短版使用预封存的 candidate patch 与 deterministic trajectory 驱动真实 Runtime、数据库和 fault proxy，避免现场模型随机性掩盖 runtime contract；长版再现场展示完整 review/debug/revise：

1. 冻结仓库、candidate PR、base/head SHA、模型、工具、预算与 fault schedule；教学版从一个已知有缺陷的候选 patch 开始 `review → request changes → revise → test`。
2. 在一个确定性 fault hook 处 `SIGKILL` coding worker：先演示“工具意图已 durable、结果尚未确认”的中断。
3. 新进程接管同一 Session。UI 直接展示 reducer 重建、旧 execution 被标记 abandoned、幂等工具可安全恢复；若非幂等调用结果不明，则停在 reconciliation 而不是偷偷重做。
4. 任务继续完成、跑公开测试与对 Agent withheld 的 evaluator tests、生成 patch。
5. publisher 首次创建 check 时，由独立 fault proxy 接收 GitHub `201` 后丢弃响应并杀 worker。恢复后状态先进入 `UNKNOWN`，只枚举远端并收养 exact `(head_sha, app_id, name, external_id)` 匹配项，不立即再次 `POST`；另行重投同一个 `X-GitHub-Delivery` 验证 inbox 去重。受控成功路径上远端物理 check 数与 outbound `POST` 数均为 1；0 个匹配经 bounded poll 后安全停住，多个匹配则判失败。
6. PR 的 Details 页给出 canonical evidence bundle；在断网、只读 workspace 下删除 Snapshot 后重放，重新生成的 `manifest.json`/`summary.md` digest 不变；修改事件副本后 chain consistency 校验失败。

台上只展示一个故事，台下必须同时提供预注册的多任务 paired 结果。**Hero demo 是可理解性证据，不是总体效用证据。**

## 1. 竞品公开能力：什么已经不能当卖点

### 1.1 能力矩阵

| 产品/平台 | 截至快照的第一方公开能力 | 对 Witness 定位的含义 | 公开保证层的边界 |
| --- | --- | --- | --- |
| GitHub Copilot cloud agent | 后台研究、计划、改代码、测试/linters、branch/PR、PR comment follow-up；独立 Actions 临时环境；custom instructions、MCP、hooks、skills；每任务一条 branch/一个 PR，单 session 最长 59 分钟。[固定文档能力](https://github.com/github/docs/blob/f14de77221fa6dcf6ab333a8f4ab7bc600500231/content/copilot/concepts/agents/cloud-agent/about-cloud-agent.md#L24-L49)、[customization 与限制](https://github.com/github/docs/blob/f14de77221fa6dcf6ab333a8f4ab7bc600500231/content/copilot/concepts/agents/cloud-agent/about-cloud-agent.md#L139-L158) | “能自主开 PR、能跑测试、能接 MCP/skills”不是差异化；59 分钟限制可作为长任务外部边界，但不要把竞品 timeout 当成 Witness 胜利条件。 | 官方称 commits/logs 带来透明性，但所审文档未定义 durable event journal、工具副作用恢复协议或 deterministic replay。限制和 preview 状态会变化。 |
| GitHub Copilot code review | 全项目上下文收集、Actions runner、自动/按 push review、Lite/Balanced effort、MCP/skills；官方明确提醒 review 会漏报或出错，仍需人工验证。[固定文档](https://github.com/github/docs/blob/9eaee0af43decd58b41a8ba821d1f1d04499064a/content/copilot/concepts/agents/code-review.md#L67-L112)、[人工验证](https://github.com/github/docs/blob/9eaee0af43decd58b41a8ba821d1f1d04499064a/content/copilot/concepts/agents/code-review.md#L181-L185) | Witness 不能只生成另一份自然语言 review；应输出 review 可以核对的运行证据、测试与状态转换。 | 这是 reviewer 产品，不是 agent runtime durability 规范。 |
| OpenAI Codex | 云端隔离任务、并行、可复现环境设置、summary/diff/follow-up/PR；GitHub 上 `@codex review`、自动 review、`AGENTS.md` rules、`@codex fix`；本地 SDK thread resume。[cloud](https://developers.openai.com/codex/cloud)、[GitHub review](https://developers.openai.com/codex/integrations/github)、[SDK](https://learn.chatgpt.com/docs/codex-sdk) **[易变]** | “隔离环境、PR、review rules、resume”均已是基础能力；公平比较必须承认 Codex 有 resume。 | 所审公开文档没有规定 crash window 下工具调用的 exactly-once/at-most-once 语义，也没有规定 hash-chained canonical journal 或无模型 replay；这是“未公开”，不是不存在。 |
| Claude Code | GitHub Action 可由 issue/PR comment 驱动并 push commit；managed Code Review 是 research preview，以多 agent + verification 在全仓上下文中出 inline findings 和 check run；CLI session 连续保存、可 resume；worktree 隔离；checkpoint 可 rewind。[Action](https://code.claude.com/docs/en/github-actions)、[Code Review](https://code.claude.com/docs/en/code-review)、[sessions](https://code.claude.com/docs/en/sessions)、[checkpointing](https://code.claude.com/docs/en/checkpointing) **[易变]** | “多 agent review、恢复、worktree、checkpoint、OTel”也不能单独作为 Witness headline。 | Claude 官方明确：checkpoint 只跟踪其 file-edit tools，Bash 修改不能 rewind，通常不恢复后台 subagent 编辑，外部修改也不跟踪；本地 JSONL entry schema 是内部格式且可随版本变化。[checkpoint limitations](https://code.claude.com/docs/en/checkpointing#limitations)、[transcript schema warning](https://code.claude.com/docs/en/sessions#where-transcripts-are-stored)。这正好说明“会话可续”与“任意工具副作用可正确恢复”不是同一命题。 |

### 1.2 不应采用的竞品话术

- 不说“只有 Witness 支持 resume/worktree/trace/OTel”。公开第一方资料不支持。
- 不说“竞争产品崩溃必然重复工具”。没有公开实验或规范可支撑。
- 不拿不同模型的单次成功视频证明 runtime 更好；模型能力、scaffold、环境和运气全部混杂。
- 不用 PR 被 merge 当唯一正确性指标。merge 受维护者偏好、项目活跃度、排队时间和社会过程影响；GitHub 自己把它作为 adoption/throughput metric，而不是 patch correctness oracle。[GitHub PR outcome metrics](https://github.com/github/docs/blob/f14de77221fa6dcf6ab333a8f4ab7bc600500231/content/copilot/concepts/agents/cloud-agent/about-cloud-agent.md#L83-L93)。

### 1.3 可防守的差异化层级

按证据强弱排序：

1. **最强：协议/不变量。** intent 先于副作用、sequence/CAS/fencing、ambiguous outcome 不盲重试、snapshot 可删、replay 不触发工具、hash-chain consistency 可验证。
2. **次强：故障注入后的行为。** 在预先声明的 crash point 上恢复率、重复副作用、人工介入、额外 tokens/时间。
3. **次强：PR 证据工件。** 同一日志多次生成字节一致；每个结论可追到 event/call/test/checkpoint；未重算链的修改/删除/重排可检测。若要主张能抵抗有权重写全日志的攻击者，必须再加 HMAC/签名、WORM 存储或外部 head anchor。
4. **较弱：普通任务 solve rate。** 重要但主要受模型影响，不能单独证明 runtime。
5. **最弱：UI、单次成功、测试数量。** 只能证明 plumbing，不应外推优越性。

## 2. Benchmark landscape：测了什么，仍漏了什么

### 2.1 主流与最新相邻基准

| Benchmark | 第一方评测模式 | 可借用的设计 | 对本项目仍然缺失的维度 |
| --- | --- | --- | --- |
| SWE-bench / Verified | 给 codebase + GitHub issue，提交 patch；Docker 中应用 patch 并跑 fail-to-pass/pass-to-pass tests。Verified 是 500 个经人工确认可解的实例。[官方仓库固定 `7a21e05`](https://github.com/SWE-bench/SWE-bench/tree/7a21e05772954cc81471ae19d56f436cecf43c54)、[原始论文](https://arxiv.org/abs/2310.06770) **[固定]** | 真 repo、固定 base、隐藏测试、container、patch-level solve；适合作为功能正确性底座。 | 没有 PR review 对话、webhook、process kill、恢复、重复副作用、事件链、审计包或 reviewer utility。OpenAI 在 2026-02 指出 Verified 存在污染和 flawed tests，并停止把它作为前沿主评测；2026-07 又对 SWE-bench Pro 的评测 signal 提出可靠性担忧。[OpenAI Verified 复盘](https://openai.com/index/why-we-no-longer-evaluate-swe-bench-verified)、[OpenAI SWE-bench Pro 复盘](https://openai.com/index/separating-signal-from-noise-coding-evaluations) **[易变研究结论的官方发布页]**。所以 Verified 更适合固定回归/hero，不适合声称前沿泛化。 |
| SWE-bench-Live | 初版论文为 1,319 个 2024 年后 issue、93 repos、每项 Docker image；官方仓库持续更新。2026-08 的 README 报告 MultiLang 已到 1,077 项，并把 leaderboard split 与持续增长的 full split分开。[论文](https://arxiv.org/abs/2505.23419)、[官方仓库固定 `bc09878`](https://github.com/microsoft/SWE-bench-Live/tree/bc09878a5d192d0804dbd647dc6e650372fcb0ac) **[固定；数据量仍易变]** | 新鲜任务、版本化数据、冻结 leaderboard split、完整 full split；适合抗 cherry-pick pilot/confirmatory pool。 | 核心仍是 issue-to-patch 和 tests；不计 runtime crash semantics、PR 事件或 evidence reproducibility。自动构建环境也可能带来 selection bias，需记录 build eligibility。 |
| SWE-Cycle | 从 Verified/Pro/Multilingual 筛出 489 项；分 Env、Implementation、TestGen 三个 isolated task，以及从 bare repo 开始的 FullCycle；SWE-Judge 结合静态审阅与动态执行。论文报告没有模型的 FullCycle strict solve 超过 14%。[论文](https://arxiv.org/abs/2605.13139)、[官方仓库固定 `1e2bc5d`](https://github.com/tubehao/SWE-Cycle/tree/1e2bc5d074483613a8faa0e8733ee705bb1400a6) **[固定]** | 把环境重建、实现、测试生成连成生命周期；基础设施失败和任务失败应分阶段报告；不要只测预配置环境。 | `FullCycle` 仍未把进程中断、接管、非幂等工具 reconciliation、GitHub PR lifecycle 或审计证据作为计分项；其 judge 依赖易变的外部模型，论文也将其列为限制。 |
| c-CRAB / Code Review Agent Benchmark | 官方 release 表格给出 184 个 PR、67 repos、234 个从人工 review 转成的可执行 tests；review tool 先产出评论，再由同一 coding agent 按评论改 patch，最后以 tests 是否通过衡量评论是否有用；release 包含 PR-Agent、Devin、Claude Code、Codex 的原始结果工件。[论文](https://arxiv.org/abs/2603.23448)、[官方 release 固定 `856dfa3`](https://github.com/c-CRAB-Benchmark/dataset/tree/856dfa3102b8996d168c4f195217b7603e1d1bc6) **[固定]** | 比文本相似度更好：把 review 的 actionability 变成“是否能指导修复并通过可执行 oracle”；可借用 review→revise→test。 | test 是从已知人工评论生成，覆盖的是已被人发现且能测试的 concern；同一 resolver 的能力会混入 review score；仍不测 runtime recovery/证据链。论文 limitations 文字另写 56 repos，与 release 数不一致；正式引用前应在 pinned artifact 上重算 distinct repo 并保留脚本。 |
| SWE-Review-Bench | 500 个 Verified issue 上由三种质量的 generator 生成 1,384 个候选 PR；reviewer 可浏览/执行仓库，输出 accept/request-changes + diagnosis；指标为 Completion Rate、Decision Accuracy、Resolve Rate after Revision。官方仓库同时发布 8,914 条 decision-correct trajectories。[论文](https://arxiv.org/abs/2607.06065)、[官方仓库固定 `95b652e`](https://github.com/SWE-Lego/SWE-Review/tree/95b652e095e5ac8f16f08ae52fd3b26513c56097) **[固定]** | 直接借用 `false accept / false reject` 与 review-guided revision；“review 能否改变最终 resolve”比评论看起来是否专业更重要。 | 功能性 issue-resolution 为主；论文明确不覆盖更广的 feature/refactor/docs/migration/architecture，也不测 event durability、webhook 或 replay。候选 PR 共用 500 个底层 issue，推断时必须按 issue/repo 聚类。 |

### 2.2 本次 source audit 发现的共同空白

在上表五套 protocol 中，以下均未被作为正式排名指标：

- 在明确 crash window 上的自动恢复成功率；
- 已发生但未确认的工具副作用如何判定、是否盲重做；
- webhook redelivery、stale `head_sha`、并发 worker takeover；
- Snapshot 删除后能否从 append-only log 重建同一状态；
- replay 是否保证不调用模型/工具、不修改仓库；
- 证据工件是否字节稳定、是否能检测事件篡改；
- 从模型结论到具体工具结果、测试、frame/locals 的 provenance coverage。

这不是说未来 benchmark 或产品不会覆盖，而是说明 **Witness 可以定义一条独立于模型智力的 runtime reliability evaluation axis**。普通 SWE solve rate 与该轴应并列报告，不能互相替代。

## 3. Hero demo 的具体设计

### 3.1 两层任务，而不是一只玩具 bug

**外层：真实 PR lifecycle。** GitHub App 收到 `pull_request`/`issue_comment` webhook，创建或更新 check run，受控 worker 在隔离 worktree 完成任务，将结果绑定到 `(repository_id, pr_number, base_sha, head_sha)`。

**内层：真实仓库修复。** 选择可执行、跨 2–6 个生产文件、有明确 evaluator tests 的 Python issue；至少包含一次失败复现、一次跨文件定位、一次 patch、一次回归测试。评测时对 Agent withheld 即可，不能把已公开 benchmark tests 宣传成从未公开的“真正隐藏测试”。避免只改一行、无需工具、无需上下文的题。

两层各注入一次故障：

- `F-code`：coding worker 在 durable tool intent 后、result commit 前被杀。
- `F-publish`：GitHub 首次 `Create Check Run` 已向独立 fault proxy 返回 `201`，但响应未到 worker、本地 publish completion 尚未 durable 时被杀；相同 delivery 的 redelivery 作为另一项独立断言测试。

这样同时展示“长任务恢复”和“PR 系统边界幂等性”。只杀一个纯计算步骤太像戏法；只演示 GitHub webhook 又体现不了 agent runtime。

### 3.2 任务建议

#### Hero/golden：`sympy__sympy-13877`

SWE-Review 原始论文正好用它展示“症状补丁阻止 crash，但仍返回错误 `nan`；agentic reviewer 需要跨模块追到上游根因并写/跑 reproducer”。[论文案例 Figure 2 与任务定义](https://arxiv.org/pdf/2607.06065#page=4)。教学 hero 应冻结论文语境中的 flawed candidate PR，明确演示 `review → request changes → revise → test`；不要假设从空 issue 开始的随机模型会自然走出同一调试路径。它适合把 Runtime Debugging、frame/locals 与 review-guided revision 串起来。

但该 issue 创建于 2018 年，已进入公开 benchmark 生态，只能作为**教学 golden**，不能作为“未见题泛化”证据。

#### 备用 golden

以下来自 SWE-bench Verified 固定数据 revision [`78f471b`](https://huggingface.co/datasets/SWE-bench/SWE-bench_Verified/tree/78f471bf655a3137b2e8a75af1501690ec009ec3)；文件数/难度是对该 revision 的离线预筛，不是模型跑后筛选。正式对外引用前应把直接数据行、筛选脚本与结果 manifest 固定进仓库：

- `django__django-16263`：人工难度 `1–4 hours`，gold production patch 涉及 4 个 Django ORM 文件，3 个 fail-to-pass tests；适合项目级数据流/查询优化。
- `sympy__sympy-16597`：`1–4 hours`，gold production patch 涉及 6 个 assumptions/core/printing/tensor 文件，3 个 fail-to-pass tests；适合跨模块不变量。
- `astropy__astropy-13398`：`1–4 hours`，gold production patch 涉及 4 个坐标变换文件，4 个 fail-to-pass tests；适合多路径行为一致性。

选择最终 hero 前只允许做环境 preflight、测试时长和 license 检查；不得先分别跑各 arm 再挑 Witness 赢得最大的题。最终 instance ID、base SHA、image digest、公开/隐藏 tests、fault schedule 必须先写入冻结 manifest。

#### 统计任务池

- 第一阶段 pilot：从 SWE-bench-Live 的冻结 leaderboard split 或按时间 cutoff 冻结的 full split 选 30–50 个独立 issue，覆盖至少 8 个 repo。
- 第二阶段 confirmatory：由 pilot 的 paired discordance、repo clustering 与目标 CI 半宽决定 n；资源不足时明确称为 pilot，不偷换成 benchmark 胜率。
- Verified golden 与 fresh pool 分开报告，避免把熟题成功混入泛化结果。

### 3.3 台上 storyboard

主舞台先压缩成五个镜头：冻结 SHA/contract → check create 的 ACK 丢失与 worker death → `UNKNOWN → observe/adopt` → 展示已封存的 patch/tests/provenance → 断网只读 replay 与 mutation rejection。下表是 12–15 分钟深度版；若现场调用真实模型，必须标成 stochastic run，不能把一次成功当可靠性统计。

| 时间 | 观众看到什么 | 机器上实际验收什么 |
| --- | --- | --- |
| 0:00–2:00 | Issue、base SHA、任务 manifest、四臂协议已经冻结 | repo clean；base/head、image、model、tools、budgets digest 完整 |
| 2:00–4:00 | Agent 复现失败、查跨文件路径、进入 debugger | reproducer exit、breakpoint/stack/frame/locals 事件均 durable |
| 4:00–5:00 | 屏幕显示 fault hook，worker 立即消失 | 精确 fault point、最后 committed sequence、OS exit reason 记录 |
| 5:00–7:00 | 新 worker 接管，Timeline 标出 abandoned/recovered/reconciled | lease/fencing 生效；旧 worker 无法再提交；无重复非幂等副作用 |
| 7:00–10:00 | 修复、多文件 diff、公开与 withheld evaluator tests | base worktree 未漂移；F2P/P2P、patch apply、test logs digest |
| 10:00–11:00 | fault proxy 收到 create `201` 后丢 ACK 并杀 worker；redelivery 另测 | `UNKNOWN → observe/adopt`；outbound POST=1、physical check=1；delivery inbox 只入队一次 |
| 11:00–13:00 | PR Checks/Files changed/Details 显示证据卡 | exact head/app/name/external ID 匹配；annotations 与 evidence URL 可追溯 |
| 13:00–15:00 | 断网、只读 workspace 下删 Snapshot 重放；同一 bundle digest；修改事件副本后验证失败 | 外部调用探针为 0；byte equality；chain consistency mismatch 被拒绝 |

### 3.4 故障矩阵

不要只有一个好看的 kill point。CI 中至少固定：

| Fault | 注入边界 | 正确行为 |
| --- | --- | --- |
| F0 | 无故障 | 估计 runtime 正常开销 |
| F1 | `ModelCallRequested` 已提交、response 未提交 | 旧调用标 abandoned；后续策略与额外 tokens 被记录 |
| F2 | `ToolCallIntent` 已提交、工具尚未开始 | 依工具恢复策略安全开始/拒绝 |
| F3 | 工具已产生外部可见副作用、result 未提交 | 幂等工具凭 operation key reconcile；非幂等且无法查证时进入人工 reconciliation，绝不盲重做 |
| F4 | result 已提交、Snapshot/下轮决策未写 | reducer 从日志恢复，不重复工具 |
| F5 | fault proxy 已收到 check create `201`、worker 未收到 ACK | 标记 `UNKNOWN`；用 remote enumeration + stable external ID observe/adopt，绝不立即重发；0 个匹配 bounded poll 后安全停住，多个匹配判失败 |
| F6 | 同一 webhook redelivery + 旧 head 的迟到 worker | delivery 去重；旧 worker 因 fencing/head mismatch 不能覆盖新 check |

**“安全停住”在 F3 非幂等不确定场景中是成功，不是失败。** 应另报 `automatic_completion` 与 `safe_reconciliation_required`，否则为了好看会诱导系统冒险重试。

## 4. GitHub PR 集成：从 demo 到产品级路径

### 4.1 推荐架构

```text
GitHub webhook
  -> signature verify + event/action allowlist
  -> delivery inbox (X-GitHub-Delivery unique)
  -> 2xx/202 within 10 s
  -> durable queue
  -> bind repo/pr/base/head
  -> Witness Session + isolated worktree
  -> check run: queued -> in_progress -> completed
  -> public safe summary + private evidence bundle
```

GitHub 官方要求/建议提供了直接设计约束：只订阅必要事件、使用 secret 和 HTTPS、10 秒内返回 2XX 并异步处理、用 `X-GitHub-Delivery` 防 replay；redelivery 会复用同一个 delivery ID。[GitHub webhook best practices 固定 `4444204`](https://github.com/github/docs/blob/44442041dd7fe7047d74f324744ffaf4dd8f7946/content/webhooks/using-webhooks/best-practices-for-using-webhooks.md#L13-L60)。签名位于 `X-Hub-Signature-256`，由 payload 与 secret 计算 HMAC-SHA256，比较时应使用 constant-time compare。[签名校验固定 `4444204`](https://github.com/github/docs/blob/44442041dd7fe7047d74f324744ffaf4dd8f7946/content/webhooks/using-webhooks/validating-webhook-deliveries.md#L21-L54)。

建议的 inbox unique key：

```text
(github_app_installation_id, X-GitHub-Delivery)
```

建议的 active PR run key：

```text
(repository_node_id, pull_request_number, head_sha, workflow_name)
```

收到 `synchronize` 时，新 `head_sha` 产生新 run；旧 run 标 superseded/cancelled。任何 publisher 更新 check 前必须再次读取 PR head 并比对，防止迟到结果污染新 commit。

### 4.2 Checks API 呈现

推荐三个 check：

1. `Witness / Functional Verification`：只由可执行 tests、patch apply、确定性 policy gate 决定 success/failure。
2. `Witness / Runtime Integrity`：journal/hash/reducer/fencing/recovery invariants；ambiguous non-idempotent outcome 用 `action_required` 或 neutral + 明确说明，不能伪装 success。
3. `Witness / Agent Review`：模型 findings 默认 neutral，不直接阻塞；组织可另设高置信规则。

Checks API 的 `output` 可包含 Markdown summary/text 和 line annotations，`details_url` 可指向完整证据，`external_id` 可绑定 Witness run；每次 API request 最多 50 个 annotations。创建/写 check run 的 REST 权限仅 GitHub App 可用，需 repository `Checks: write`。[GitHub Checks REST API](https://docs.github.com/en/rest/checks/runs?apiVersion=2022-11-28#create-a-check-run) **[易变 API 文档]**。

但官方只把 `external_id` 定义为 “A reference for the run on the integrator's system”，没有声明唯一约束或 idempotency key。因而必须把两条协议分开：

- **Inbound webhook：** 以 installation + `X-GitHub-Delivery` 做 durable inbox 去重。
- **Outbound create：** 先 durable 写 publish intent，并由单写者 lease/fencing 串行化；响应不明时不得立即再次 `POST`，而应按 exact `head_sha + app_id + name + external_id` 枚举远端。恰好一个则 adopt 其 `check_run_id`，零个在 bounded poll 后进入 `action_required`，多个则判 integrity failure。
- **验收：** fake sink 不提供隐藏的服务端幂等；同时记录 outbound POST 数与远端 physical check 数。只有这样，`physical_check_run_count=1` 才是 runtime 行为的证据，而不是 mock 帮忙去重。

不要把整份 event log 塞进 PR comment：

- check summary 只放结论、恢复次数、tests、base/head、bundle digest 和 Details 链接；
- Files changed annotations 只放可定位且可验证的高信号项；
- 完整 canonical JSON/NDJSON 放受权限控制的 artifact/object store；
- 公共 bundle 使用安全投影，不含 prompts、secret、原始 tool output、绝对路径或环境变量。

### 4.3 最小权限与 fork 安全

GitHub App 建议起点：

- `Metadata: read`（隐含基础权限）；
- `Contents: read`；
- `Pull requests: read`，只有确需发 review/comment/改 branch 时才升为 write；
- `Checks: write`；
- `Actions: read` 仅在需要读取 workflow 结果时开启；
- webhook 只订阅 `pull_request`，可选 `issue_comment`/`check_run`，不用的事件不订阅。

若走 GitHub Actions，必须显式写 job-level `permissions`；GitHub 官方要求 `GITHUB_TOKEN` 使用 least required access。[固定文档 `4e0332d`](https://github.com/github/docs/blob/4e0332d98f45ea2c698a49e67049a857975acb65/content/actions/tutorials/authenticate-with-github_token.md#L33-L82)。

不要在 `pull_request_target` 中 checkout 并执行 fork PR 代码。该事件运行在 base/default branch 的高权限上下文，GitHub 明确建议需要 build/run PR code 时避免使用它。[固定文档 `0dddeeb`](https://github.com/github/docs/blob/0dddeeb8cce75425f9ca0cdffd6a1cbd94926c07/content/actions/reference/workflows-and-actions/events-that-trigger-workflows.md#L658-L677)。安全拆分为：

- 非特权 `pull_request`/sandbox worker 执行不可信代码，无 secrets、read-only token；
- 特权 publisher 只读取经过 schema 校验和 digest 绑定的结果，写 check/comment；
- publisher 不解压或执行来自非特权 job 的任意 artifact；
- 两阶段之间绑定 exact `head_sha`、artifact digest、producer run ID。

## 5. 正式评测：怎样证明是 runtime，而不是模型运气

### 5.1 四臂主实验

同一 task × seed 使用以下四臂：

| Arm | Runtime | Fault | 作用 |
| --- | --- | --- | --- |
| A | 同 scaffold baseline：持久 worktree + 最后已确认 transcript，无 intent journal/reconciliation/fencing | 无 | 模型+工具的普通任务能力 |
| B | 同上 | 预注册 fault | 无 durable recovery contract 时的 fault penalty |
| C | Witness | 无 | Witness 正常运行开销 |
| D | Witness | 同一 fault | 核心恢复收益 |

主对比：`D - B`；开销对比：`C - A`。A/C 不可省，否则无法知道恢复收益是否以正常任务的大幅成本为代价。

Baseline 的重启协议必须预先写死：保留同一持久 worktree、原始任务与最后已确认 transcript/tool results；新 worker 可看当前 diff，但看不到未确认调用的内部结果。它与 Witness 必须共用：

- exact model/provider revision、reasoning/temperature/seed（若 provider 支持）；
- system/task prompt、AGENTS/CLAUDE/instructions 内容；
- tool schema、sandbox、network、repo base、Docker image；
- step/tool/time/token/USD budget；
- fault point 的语义定义，而非随 wall-clock 随机杀死；
- tests 与 scorer。

若资源允许，再加两个机制消融：`journal/replay but no side-effect reconciliation` 与 `reconciliation but no fencing`。主分析除 `D-B` 外，还应报告 fault interaction：`(D-C) - (B-A)`，避免把 baseline 人为做弱。

### 5.2 Native product baseline

Codex、Claude Code、Copilot 可以作为“当前产品 end-to-end”外部 baseline，但单独成表，不能与四臂因果实验混算。每次记录：产品 SKU、CLI/action version、模型选择、effort、review mode、日期、repo instructions、权限、超时和原始可导出工件。

原因是 native 产品通常不能固定相同模型、scaffold、hidden prompt、工具实现和恢复策略。它们回答“用户今天能得到什么”，不回答“Witness runtime 本身贡献多少”。

### 5.3 主指标

#### 功能正确性

- `resolved`: 对 Agent withheld 的 evaluator F2P 全通过且 P2P 无回归；
- `patch_apply_success`；
- `public_test_pass` / `withheld_evaluator_test_pass` 分开；
- `scope_violation`: 是否修改禁区、删测试、改 evaluator；
- feature/refactor 类任务另加预注册 architecture/contract checks，不能只看旧 tests。

#### Runtime reliability

- mutually exclusive outcome：`auto_completed` / `safe_stopped` / `unsafe_completed` / `timeout_or_infra_failed`；
- `recovery_completed_without_human`、`safe_reconciliation_required`；
- `unsafe_duplicate_side_effect_count`（目标 0）；
- `duplicate_model_call_count` 与额外 input/output tokens；
- `resume_latency_s`、`wasted_wall_s`、`wasted_usd`（provider ACK 丢失时允许为区间或 `unknown`）；
- `old_worker_commit_rejected`；
- `base_worktree_drift_count`、`cross_session_contamination`；
- `reducer_state_equal_after_snapshot_delete`；
- `replay_model_calls=0`、`replay_tool_calls=0`、`replay_workspace_writes=0`；除内部计数外，还用断网、只读 workspace 与外部调用探针验收；
- `evidence_byte_equal`、`chain_consistency_violation_detected`。

安全与活性必须并列：永远不执行写操作的系统也能做到零重复，不能因此算成功。重复副作用率应以实际暴露到相应 fault window 的 run 为分母；观测到 `0/n` 时同时报告单侧置信上界。

#### PR/review utility

借用 c-CRAB 与 SWE-Review 的思路：

- `decision_accuracy`、`false_accept_rate`、`false_reject_rate`；
- `resolve_rate_after_revision`；
- finding precision/recall/F1，另报每 PR false-positive 数；
- `actionable_finding_rate`: comment 能否让冻结 resolver 通过对应 withheld evaluator test；
- `evidence_coverage`: 每个 finding 是否至少引用一个 code location + 一个 event/tool/test/debug evidence ID；
- `stale_comment_rate`: push 新 head 后旧 finding 是否错误留存。

#### 成本与体验

- paired total tokens、模型调用次数、工具调用次数、wall time、USD；
- 人工 intervention 次数与分钟数；
- PR 从 opened 到 reviewable 的时间；
- 证据包大小、生成时间；
- reviewer 阅读实验可选测“定位正确根因所需时间”，但必须盲化 arm 标签和随机顺序。

### 5.4 统计单位与分析

- 推断单位是独立 issue/repository revision，不是一次 LLM call、一个 test 或一个 event。
- 同 task 的多个 seed 是 task 内重复；先聚合到 task，或使用 task/repo 层级模型。
- binary `resolved`/`recovery` 用 paired exact McNemar、paired permutation 或 cluster bootstrap；连续成本用 task-level paired difference 与 repo-cluster bootstrap。
- 报绝对数、百分点差、95% CI 和失败构成；不要只报相对百分比。
- ITT 包含 timeout、parse failure、setup failure、rate limit；另给 infrastructure-only sensitivity analysis，但不能事后删掉对 Witness 不利的失败。
- review benchmark 若 1,384 个 PR 只来自 500 个 issue，必须按底层 issue/repo 聚类，不能假设 1,384 个完全独立样本。

## 6. 防 cherry-pick 与可复现性清单

运行前提交一个不可变 `evaluation-manifest.json`，至少包括：

- task pool 查询、snapshot/revision、纳入排除规则、random seed；
- 所有 instance IDs、repo/base SHA、image digest；
- arm、seed 数、随机执行顺序；
- exact prompts/instructions/tool schemas/budgets；
- fault point 与预期判定表；
- public/withheld evaluator tests 的 digest 与 scorer version；
- primary/secondary metrics、统计方法、停止规则；
- infrastructure rerun 的唯一允许条件；
- redaction 与 artifact retention policy。

发布结果时：

1. 一个 task 一行，失败也保留；
2. 原始 arm outputs、test logs、event manifest、cost ledger 均有 digest；
3. leaderboard/fresh split 与 golden demo 分表；
4. 报告所有排除及原因，并给 selection funnel；
5. 不在看到 arm 结果后改难度标签；
6. 产品事实/模型版本变动后新跑应新建 benchmark snapshot，不覆盖旧结果；
7. 如果只能跑少量任务，标题写“pilot/acceptance”，不写“SOTA/全面优越”。

一个简单、公开可核查的反 cherry-pick 方法是：先冻结满足条件的 task ID 排序表，再用公开随机种子抽样；manifest commit 时间必须早于第一条 arm result。

## 7. PR Evidence Bundle 设计

### 7.1 建议目录

```text
evidence/
  manifest.json
  summary.md
  runtime-integrity.json
  tests.json
  debug-evidence.json
  patch.diff
  replay-report.json
  events.safe.ndjson
  SHA256SUMS
```

### 7.2 `manifest.json` 最小字段

```json
{
  "schema_version": "witness.pr-evidence.v1",
  "repository": {
    "id": 0,
    "full_name": "owner/repo",
    "base_sha": "...",
    "head_sha": "...",
    "pull_request": 0
  },
  "task": {
    "source": "swe-bench-live@revision",
    "instance_id": "...",
    "prompt_sha256": "...",
    "instructions_sha256": "..."
  },
  "execution": {
    "session_id": "...",
    "run_ids": ["..."],
    "model": "provider/model/revision",
    "tool_manifest_sha256": "...",
    "environment_image_digest": "sha256:...",
    "fault_schedule_id": "F3"
  },
  "journal": {
    "first_sequence": 1,
    "last_sequence": 0,
    "chain_root": "sha256:...",
    "reducer_schema": "..."
  },
  "recovery": {
    "takeovers": 0,
    "abandoned_operations": [],
    "reconciliations": [],
    "unsafe_duplicates": 0
  },
  "artifacts": [],
  "redaction_policy": "safe-public-v1"
}
```

### 7.3 证据原则

- 每个结论引用稳定 ID，不引用“模型说它做过”。
- test 记录 command、cwd projection、exit code、duration、stdout/stderr digest、environment digest；大输出单独存储。
- debug evidence 记录 breakpoint/stop/frame/scope/variable 的 call pairing 与 digest，只公开必要安全投影。
- `summary.md` 是 canonical renderer 的产物，不再次调用模型。
- `generated_at`、临时路径、随机排序等非确定字段不得进入 byte-equality 工件，或必须规范化。
- 私有 canonical log 与公共 safe projection 分开；审计性不等于把 secret 和 prompt 全公开。
- Check run 的 `details_url` 必须绑定 exact bundle digest，而不是永远指向会变化的 “latest”。

## 8. 项目级 PR 优化路线

### P0：先把可演示不变量做成 CI

- 先做最小 `ProjectPullRequests` vertical slice：独立 PR journal/outbox、revision gate、publisher lease/fencing 和 `UNKNOWN → observe/adopt/action_required`；这些是 accepted-before-receipt 演示成立的前提，不是以后再补的 UI 状态。
- 为 F1–F6 提供 deterministic fault hooks；F5 使用能在收到上游 `201` 后丢 ACK 的独立 proxy，不能靠人工挑时机 kill。
- 提供一条命令生成/验证 evidence bundle；连续生成 5 次 byte-identical。
- 加入 fake GitHub sink：可模拟 accepted-but-response-lost、429/5xx、redelivery、stale head、乱序完成。
- PR publisher 使用 durable intent、单写者 fencing、stable `external_id`、remote observe/adopt reconciliation 与 head SHA guard；`external_id` 不得被当成 GitHub 端唯一键。
- Runtime Integrity check 与模型 Review check 分离。

### P1：GitHub App MVP

- webhook inbox + signature validation + delivery dedupe；
- queued/in_progress/completed check lifecycle；
- details 页面展示 timeline、recovery、tests、diff、cost、bundle digest；
- GitHub App 最小权限、fork PR 两阶段隔离；
- `synchronize` supersede 与旧 worker fencing。

### P2：closed-loop PR

- reviewer 输出结构化 `approve/request_changes + findings[]`；
- findings 必须带 provenance；
- request_changes 进入新 execution，在同一 Session 中修复；
- re-review 只针对新 head/new diff，旧 finding 显式 resolved/superseded；
- 以 RRR/false accept/false positive 衡量，不以评论数量衡量。

### P3：多任务证据

- 30–50 task fresh pilot；
- 按 repo/issue 聚类的 paired analysis；
- 发布预注册 manifest、raw artifacts、failure taxonomy；
- pilot 后再决定正式 n 与是否需要多语言/Windows/多 repo。

## 9. 最终建议的一句话与标题

推荐项目一句话：

> **Witness is the crash-consistent evidence layer for coding agents: it turns a long-running repository task into a resumable, side-effect-aware, replayable and auditable pull request.**

推荐 demo 标题：

> **Crash-to-Proof: one real PR, two process deaths, zero blind duplicate side effects, one replay-verifiable evidence bundle.**

不要把 headline 写成“更聪明地修 bug”。正式证据应是二维的：

```text
功能轴：真实仓库 patch / withheld evaluator tests / review-guided revision
可靠性轴：fault recovery / side-effect safety / replay / provenance
```

只在两轴都通过时，PR 才称为 `reviewable`。

## 10. 来源时效说明

- GitHub Copilot、Codex、Claude Code 的产品名、计划、模型、价格、preview 状态、timeout、review effort 和命令均可能在周级/月级变化；引用已带访问日期或固定 GitHub Docs commit。实施前应重新核对。
- GitHub webhook HMAC、delivery ID、10 秒响应、Checks API 与 Actions 权限属于平台契约，但 API version/权限名也应在发布前按当前官方文档复核。
- SWE-bench-Live 是持续更新数据集；任何数字必须同时写 dataset revision/split/cutoff，不能只写名称。
- 论文中的模型结果只描述论文版本、prompt、scaffold 和当时模型，不应用作 2026-08 产品排行。
- 本文对竞品“未见公开保证”的结论仅覆盖所列第一方文档，不是对闭源内部实现的逆向断言。
