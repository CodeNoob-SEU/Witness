# react-agent-core

一个小而明确、面向生产约束的 Python ReAct Agent 核心。它使用 OpenAI 风格的结构化 function/tool calling，不解析脆弱的 `Thought / Action / Observation` 文本，也不会要求模型公开或保存私有思维链。

框架把一次运行建模为有限循环：

```text
用户目标
  -> 模型返回最终回答 --------------------------> 完成
  -> 模型返回结构化 tool calls
       -> 参数校验 -> 审批策略 -> 执行工具
       -> 将每个结果按 call id 写回模型
       -> 下一轮模型决策
```

每个 assistant tool call 都会得到且只得到一个匹配 ID 的 tool result；未知工具、参数错误、审批拒绝、超时和工具异常也会转换成结构化 observation，而不是破坏消息协议。

## 特性

- 默认使用 OpenAI Responses API；可切换到 Chat Completions，兼容只实现 `/v1/chat/completions` 的服务。
- 使用 Pydantic 从带类型注解的 Python 函数生成严格 JSON Schema，并在本地再次校验模型参数。
- 工具显式注册和白名单分发，不通过模型输出反射执行任意函数。
- 支持同步或异步工具、单工具超时、受控并发和顺序稳定的结果回写。
- 高风险工具可声明 `requires_approval=True`；没有审批处理器时默认拒绝。
- 提供步数、工具调用数、总墙钟时间、并发数、工具输出长度和重复动作限制。
- 返回结构化 `AgentResult`，并产生默认不包含 prompt、参数和工具输出的安全事件。
- 可选的实时 observer 支持模型文本 delta、工具调用与执行生命周期；调试数据不写入
  `AgentResult.events` 或会话 transcript。
- 显式区分完成、截断、不完整输出和拒绝；不执行 `length/incomplete` 响应中的工具参数。
- API key 不进入框架对象的 `repr`；第三方兼容服务的信任边界由调用方明确决定。

## 安装

需要 Python 3.11 或更高版本。在仓库根目录执行：

```bash
uv sync --extra dev
```

若不使用 uv，也可以执行 `python -m pip install -e .`；运行时依赖仅有 OpenAI Python SDK
与 Pydantic。

然后设置至少两个环境变量：

```bash
export OPENAI_API_KEY="你的 API key"
export OPENAI_MODEL="你的模型名"
python examples/quickstart.py
```

`examples/quickstart.py` 中的订单金额工具完全在本地计算、不访问网络；Agent 调用模型本身仍然需要配置好的 API endpoint。

本项目不会自动读取 `.env` 文件。可以参考 [`.env.example`](.env.example) 后使用 shell、部署平台的 secret manager，或应用自己的配置层注入环境变量。

## 环境变量

| 变量 | 是否必需 | 含义 |
| --- | --- | --- |
| `OPENAI_API_KEY` | 是 | OpenAI 或可信兼容服务的 API key；OpenAI SDK 也会读取它。 |
| `OPENAI_MODEL` | 是 | CLI 和 quickstart 使用的模型名。库的 `OpenAIModel(model=...)` 仍要求显式传入模型名。 |
| `OPENAI_BASE_URL` | 否 | 兼容服务入口，例如 `https://example.com/v1`。未设置时使用 OpenAI SDK 默认值。 |
| `OPENAI_API_MODE` | 否 | `responses`（默认）或 `chat_completions`；由 CLI 和 quickstart 读取。 |

不要提交真实 key。第三方 `OPENAI_BASE_URL` 会收到用户消息、工具定义和工具结果，只应连接你信任的服务，并保持 TLS 校验开启。
非回环地址默认必须使用 HTTPS；可信内网若只能使用 HTTP，需要显式传入
`allow_insecure_http=True`（CLI 对应 `--allow-insecure-http`）。

## 快速使用

下面是 quickstart 的核心形式：

```python
import asyncio

from react_agent import AgentConfig, OpenAIModel, ReActAgent, tool


@tool(timeout_s=2.0)
def add(left: float, right: float) -> dict[str, float]:
    """在本地计算两个数的和。"""
    return {"sum": left + right}


async def main() -> None:
    async with OpenAIModel("your-model") as model:
        agent = ReActAgent(
            model,
            tools=[add],
            config=AgentConfig(max_steps=4, max_tool_calls=4),
        )
        result = await agent.run("请务必调用工具计算 19.5 + 22.7。")
        print(result.output or f"停止原因：{result.stop_reason.value}")


asyncio.run(main())
```

`OpenAIModel` 使用 `AsyncOpenAI`，因此框架核心是原生异步的。同步工具会在线程中运行；
Python 无法安全杀掉已经启动的线程，所以 timeout 只会停止等待，底层同步函数仍可能继续。
需要强隔离、可强制终止、不可重复副作用或处理不可信代码的工具，必须放入独立进程/worker，
并使用 `run_id + call_id` 作为服务端幂等键。

## 定义 typed tool

`@tool` 会读取函数签名和 docstring。每个参数都必须有类型注解，不能使用 `*args`、`**kwargs` 或 positional-only 参数：

```python
from typing import Annotated

from pydantic import Field
from react_agent import tool


@tool(
    name="lookup_inventory",
    description="查询本地库存；不执行预留或下单。",
    timeout_s=3.0,
    idempotent=True,
    parallel_safe=True,
)
def lookup_inventory(
    sku: Annotated[str, Field(min_length=1, max_length=64)],
    warehouse_id: Annotated[int, Field(ge=1)],
) -> dict[str, int | str]:
    return {"sku": sku, "available": 12}
```

装饰器生成 OpenAI strict function schema：object 禁止额外字段，并把属性列入 `required`。需要表达“必传但可以为空”的参数时，使用 `T | None`。无论服务端是否支持 strict schema，本地 Pydantic 校验始终执行。
工具名、描述和参数说明应让人无需额外背景也能正确调用；初始暴露的工具应尽量少（官方文档给出的软建议是少于 20 个），大型工具集应在应用层按任务动态筛选。

工具成功和失败都会被封装成 JSON：

```json
{"ok":true,"data":{"sum":42.2},"meta":{"truncated":false}}
```

```json
{"ok":false,"error":{"code":"INVALID_ARGUMENTS","message":"...","retryable":false}}
```

## 审批高风险工具

审批发生在参数解析和校验之后、工具执行之前：

```python
import asyncio

from react_agent import ApprovalRequest, ReActAgent, tool


@tool(
    requires_approval=True,
    idempotent=False,
    parallel_safe=False,
)
def send_message(recipient: str, body: str) -> dict[str, str]:
    """向外部收件人发送消息。"""
    # 在真实应用中调用受控消息服务。
    return {"status": "sent", "recipient": recipient}


async def approve(request: ApprovalRequest) -> bool:
    answer = await asyncio.to_thread(
        input,
        f"允许 {request.tool_name} 使用参数 {dict(request.arguments)!r}？[y/N] ",
    )
    return answer.strip().lower() == "y"


# agent = ReActAgent(model, [send_message], approval_handler=approve)
```

若没有提供 `approval_handler`，所有 `requires_approval=True` 的调用都会得到 `TOOL_DENIED`。审批只是最后一道策略钩子；真实写操作仍应使用最小权限凭据、审计日志和服务端幂等键。
审批回调看到的是参数的深拷贝快照；即使回调误改嵌套值，也不会改变随后真正执行的参数。

## 限制与并发

`AgentConfig` 的所有限制都作用于单次 `run()`：

```python
from react_agent import AgentConfig

config = AgentConfig(
    max_steps=8,
    max_tool_calls=32,
    max_wall_time_s=120.0,
    max_concurrent_tools=8,
    max_tool_output_chars=20_000,
    max_context_chars=200_000,
    parallel_tool_calls=True,
    repeated_action_limit=3,
)
```

- `max_steps`：模型决策轮数上限，包括最后回答所在的模型调用。
- `max_tool_calls`：模型请求的 tool call 总数上限。
- `max_wall_time_s`：模型与工具执行共享的总 deadline；它约束 Agent 停止等待的时间，
  不是对已启动同步线程或外部系统副作用的强制终止保证。
- `max_concurrent_tools`：同一轮最多同时执行的工具数。
- `max_tool_output_chars`：单个 observation 的最大字符数，超出时返回带元数据的安全预览。
- `max_context_chars`：发送模型前的保守字符预算，包含 instructions、工具 schema 和完整
  transcript；超限时明确停止，由应用压缩或总结旧历史后再继续。
- `parallel_tool_calls`：只有同一批工具都允许 `parallel_safe` 时才并发；结果仍按请求顺序写回。
- `repeated_action_limit`：同一次 run 内相同工具与参数达到阈值时停止，即使结果含时间戳或
  随机值也能识别；同时也能识别
  `A/B/A/B` 交替循环。确实需要稳定轮询的工具可显式设置 `allow_repeated=True`。

工具默认 `idempotent=False`、`parallel_safe=False`，需要作者在确认语义后显式放开；框架只在
`idempotent=True` 时把工具超时标记为可重试，但不会擅自重试有副作用的工具。

预算耗尽、循环检测和总超时是正常终态，不会伪装成成功回答。检查 `result.status` 与 `result.stop_reason`，不要只判断 `result.output`。
供应商的 `length`、`incomplete`、`content_filter` 和 refusal 也会映射为明确终态。

## 事件与结果

事件可用于 CLI 进度、结构化日志或 tracing：

```python
from react_agent import AgentEvent, ReActAgent


def on_event(event: AgentEvent) -> None:
    print(
        event.kind.value,
        event.run_id,
        event.step,
        event.tool_name,
        dict(event.data),
    )


# agent = ReActAgent(model, tools, event_sink=on_event)
```

事件包括 `run_started`、模型开始/完成/失败、工具开始/完成/复用、预算耗尽、循环检测和 `run_completed`。默认事件不含原始 prompt、工具参数或工具输出；若应用自行补充日志字段，应先做脱敏。
同步事件回调会在线程中执行，避免阻塞 Agent 的事件循环；事件回调仍应保持短小、无副作用。

需要制作实时调试界面时，可以为单次运行显式传入 `stream_sink`。它与上面的安全日志事件是
两条独立通道：前者是临时、可背压、严格保序的 rich event，后者仍保持 metadata-only。

```python
from react_agent import AgentStreamEvent


async def inspect(event: AgentStreamEvent) -> None:
    # data 可能包含被工具作者显式批准展示的参数或结果，不要直接写入生产日志。
    await websocket_or_queue.send(event)


result = await agent.run("请调用工具计算 21 * 2", stream_sink=inspect)
```

模型端的增量只用于 UI。框架仍会等待 SDK 返回完整终态并重新解析最终 `ModelResponse`，确认
outcome 为 completed 后才执行工具；部分 JSON 参数、截断输出或断线不会触发工具。provider 原始
事件、system instructions、完整 history、reasoning 和 encrypted reasoning 永远不会通过这条
observer 转发。

工具的调试暴露策略默认是 `DebugExposure.METADATA`，只显示名称、字符数、状态和耗时。
只有参数与结果确实适合交给前端时，工具作者才能显式启用完整展示：

```python
from react_agent import DebugExposure, tool


@tool(debug_exposure=DebugExposure.FULL)
def local_calculator(expression: str) -> dict[str, str]:
    """计算不含凭据或个人数据的本地表达式。"""
    return {"expression": expression}
```

不要给读取凭据、文件、用户数据或第三方响应的工具启用 `FULL`。浏览器网络面板同样可以看到
rich event；调试面板不是秘密存储。

`AgentResult` 包含：

- `output`、`status`、`stop_reason` 和可能的安全错误信息；
- `run_id`、模型调用次数、tool call 数和实际工具执行次数；
- 供应商返回的累计 token usage；兼容服务不返回 usage 时对应值为零；
- 完整 typed transcript 和本次运行的事件快照。

## Responses 与 Chat Completions

官方 OpenAI 默认配置：

```python
model = OpenAIModel("your-model", api_mode="responses")
```

默认 `store=False`，并请求/回放加密 reasoning items，使工具循环可以在无服务端持久状态下继续；
完整 provider items 保存在 transcript 的不透明字段中且不会进入 `repr`。若兼容端点不支持该字段，
关闭 `encrypted_reasoning_items`，或改用 Chat Completions。

只提供 Chat Completions 的兼容服务：

```python
model = OpenAIModel(
    "provider-model",
    api_mode="chat_completions",
    api_key="...",
    base_url="https://trusted-provider.example/v1",
)
```

若兼容服务会拒绝 `strict` 或 `parallel_tool_calls` 字段，可显式降级 wire protocol；本地参数校验不会关闭：

```python
from react_agent import AgentConfig, OpenAIModel, ProviderCapabilities, ReActAgent

model = OpenAIModel(
    "provider-model",
    api_mode="chat_completions",
    base_url="https://trusted-provider.example/v1",
    capabilities=ProviderCapabilities(
        strict_tools=False,
        parallel_tool_calls=False,
        store_parameter=False,
        encrypted_reasoning_items=False,
    ),
)
agent = ReActAgent(
    model,
    tools,
    config=AgentConfig(parallel_tool_calls=False),
)
```

兼容服务对 token 参数的支持并不一致。只有确认 endpoint 接受对应参数时再设置 `max_output_tokens`；默认 `None` 不发送该字段。
模型网络重试只交给 OpenAI SDK 的 `max_retries`，Agent 核心不再叠加第二层指数退避，避免重试倍增。

## CLI

CLI 适合验证 endpoint；它本身不注册应用工具：

```bash
react-agent "用一句话解释结构化 tool calling"
react-agent --api-mode chat_completions --compat "你好"
```

`--compat` 会省略 `strict`、`parallel_tool_calls` 和 `store` 等可选 provider 字段。

## ReAct 调试工作台

项目自带一个同源 FastAPI 单页工作台。左侧是流式对话，右侧按顺序显示每轮模型调用、
文本 delta、工具参数、工具执行结果、耗时、usage、预算与最终提交状态。API key 只保留在
服务端环境变量中，不会发送给浏览器。使用兼容端点时，启动方式如下：

```bash
export OPENAI_API_KEY="你的 API key"
export OPENAI_BASE_URL="https://api.hai-lab.cn/v1"
export OPENAI_MODEL="gpt-5.6-terra"
export OPENAI_API_MODE="chat_completions"
uv run react-agent-web
```

然后打开 <http://127.0.0.1:8000/>。页面支持连续对话、停止当前运行、新对话、移动端布局，
并注册了一个只允许基础算术且允许完整调试展示的本地计算工具。`POST /api/chat/stream`
使用 fetch + SSE 返回实时事件；旧的 `POST /api/chat` 非流接口继续保留。默认仅监听
`127.0.0.1`；如需修改，
可设置 `REACT_AGENT_WEB_HOST` 和 `REACT_AGENT_WEB_PORT`。

Web 会话只提交 `completed` 回合；超时、失败或 partial 回合会在界面标记“本轮未写入上下文”，
避免把不完整的 provider transcript 带入下一轮。内置计算工具无副作用；若自行接入写操作工具，
还应在业务层使用幂等键记录实际执行结果。

浏览器断线且未收到 `done` 或 `stream_error` 时，页面会显示“提交状态未知”且不会自动重放，
因为一次工具调用可能已经产生副作用。当前最小实现不提供断线重连；需要跨断线恢复时，应在应用层
增加带客户端幂等 ID 的 run registry、可重放事件 ring 和 `Last-Event-ID`，而不是重新 POST。

## 开发验证

```bash
uv run ruff check .
uv run mypy src/react_agent examples/quickstart.py
uv run pytest -q
uv build
```

默认测试不访问网络：Agent 循环使用 deterministic fake model，协议测试使用真实 OpenAI
SDK 加 `httpx.MockTransport`。真实 endpoint 应通过 quickstart 或 CLI 单独验证，避免把 live
API 调用放进默认 CI。

## OpenAI Docs

- [Function calling：工具定义、call ID、并行调用与 strict mode](https://developers.openai.com/api/docs/guides/function-calling/)
- [迁移到 Responses API](https://developers.openai.com/api/docs/guides/migrate-to-responses/)
- [Responses API：Create a model response](https://developers.openai.com/api/reference/resources/responses/methods/create/)
- [Chat Completions API：Create chat completion](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create/)
