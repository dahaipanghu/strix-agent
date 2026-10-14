# Strix × PentestGPT：Planner / Executor / Critic 协作架构

## 实施状态

这是一个新增、可选的协作引擎，不会改写既有 `strix` CLI、默认扫描入口、
报告系统。既有 `strix/core/runner.py` 和 `strix/agents/factory.py`
保持原样；新 Executor 构建函数复用后者，并限制角色工具。

已实现：结构化任务图、三角色接口、SDK 适配、工具证据引用校验、依赖调度、
有限并发与返工、超时取消、SQLite 幂等记录及事件游标、PentestGPT 服务端协议适配。
提供离线示例和测试。尚未进行真实模型、Docker、目标扫描或浏览器端到端联调。

`PentestGPT-Web` 的基线只有 README，不能把 README 宣称的 Web/API 功能当成已存在。
配套 JavaScript 客户端是服务端协议适配器，不是完整 Web 应用，也不假定已有 HTTP 路由。

## 职责与边界

| 角色 | 输入 | 权限与输出 |
| --- | --- | --- |
| Planner | 目标、服务端授权策略 | 无工具、无 handoff；生成有向无环任务图和验收标准 |
| Executor | 一个任务、直接依赖摘要、上次 Critic 反馈 | 使用受限 Strix 工具；输出候选结论，不能生成子代理或发布正式报告 |
| Critic | 单任务标准、候选结论、工具证据 | 独立上下文、无工具；accept / revise / reject |
| Workflow | 可信 Scope 和版本化 Request | 执行前验作用域；只有通过证据门控的任务才能解锁依赖 |
| PentestGPT | 用户交互、目标/计划、结果呈现 | 已有计划直接传入；否则由 Strix Planner 生成，避免双重规划 |

执行关系：`PentestGPT → Planner（可跳过）→ DAG 调度 → Executor → Critic`。
Critic 返回 revise 时，只允许在预算内对 review 任务返工；active 任务不自动重复。
Critic 不直接执行“复测”，而是提出补充证据需求，再由 Executor 执行并重新审查。

## 目录

- `strix/collaboration/models.py`：版本化、有限大小、拒绝未知字段的 Pydantic 契约。
- `engine.py`：三角色协议、任务调度和验收状态机。
- `store.py`：SQLite 请求账本、事件与工具证据。
- `sdk.py`：ModelPlanner、SandboxExecutor、ModelCritic 和原生 Executor 工厂。
- `bridge.py`：PentestGPTBridge，绑定服务端认证会话与授权 Scope。
- `demo.py`：纯离线示例；不是扫描器或真实 AI 评估。

## 可运行的离线验证

在本分支、已安装项目依赖的环境中：

```bash
uv run python -m strix.collaboration.demo
uv run pytest tests/test_collaboration.py tests/test_collaboration_sdk.py -q
```

示例使用固定 fixture，不需要模型密钥、不启动 Docker、不请求任何目标。
SDK 测试使用接口替身，不能替代真实供应商兼容性验证。

## 接入已有 Strix 沙箱

三角色分别配置模型。Planner/Critic 默认禁用 SDK tracing，且各次调用创建独立
Agent，不共享完整对话。自定义 RunConfig 的 `model` 必须为空，避免覆盖角色模型。
角色调用次数上限不等于美元或 token 预算；生产环境必须另外配置供应商/token/
成本上限。每个角色还应配置输出长度、请求超时及成本监控。

原生接入使用现有 runner 准备好的沙箱 `run_config` 和工具 `context`：

```python
from contextlib import asynccontextmanager
from agents import RunConfig
from strix.collaboration import Workflow, Store
from strix.collaboration.sdk import (
    ExecutorBinding, ModelPlanner, ModelCritic, SandboxExecutor,
    READ_TOOLS, ACTIVE_TOOLS, SHELL_EVIDENCE, build_executor_agent,
)

# 此函数是接入示例。run_config/context 必须由已授权的 Strix 沙箱所有者提供。
# 每个活动任务必须隔离运行状态、工具资源与网络出口；不要复用全局扫描状态。
def build_workflow(*, bind_runtime, planner_model, executor_model, critic_model,
                   planner_config, critic_config, store):
    @asynccontextmanager
    async def bind(ctx):
        async with bind_runtime(ctx) as runtime:
            agent = build_executor_agent(
                ctx.task, model=executor_model,
                **runtime.factory_options,
            )
            evidence_tools = (ACTIVE_TOOLS | SHELL_EVIDENCE
                              if ctx.task.mode == "active" else READ_TOOLS)
            yield ExecutorBinding(agent, runtime.run_config, runtime.context,
                                  frozenset(evidence_tools))

    return Workflow(
        ModelPlanner(planner_model, run_config=planner_config),
        SandboxExecutor(bind, max_turns=16),
        ModelCritic(critic_model, run_config=critic_config),
        store,
    )
```

`bind_runtime(ctx)` 是必须由宿主实现的异步上下文管理器：负责准备/清理沙箱，
返回 `factory_options`、`run_config` 和 `context`，并在超时或取消时清理正在执行的操作。
不能把 `asyncio` 取消等同于外部命令必然已经停止；清理失败必须转人工核对。
本版本未自动接管 `run_strix_scan` 的生命周期，也未新增默认 CLI 开关。

原生 `review` 模式仅提供已有代理流量的只读工具；禁用 shell、文件系统修改、MCP、
子代理与报告写入。`active` 模式保留原生沙箱能力及有限代理工具，必须显式批准。
它不是网络隔离机制；授权边界必须由宿主工具、沙箱和出口策略执行。

## PentestGPT 协议 v1

```json
{
  "protocol_version": "strix-pentestgpt/1",
  "session_id": "server-owned-session",
  "request_id": "stable-request-001",
  "goal": "核查授权资产的已有证据",
  "plan": {
    "tasks": [{
      "id": "review-1",
      "target": "asset:approved",
      "objective": "审查已采集的证据",
      "acceptance_criteria": ["明确说明证据能支持与不能支持的结论"],
      "depends_on": [],
      "mode": "review"
    }]
  }
}
```

`plan: null` 使用 Strix Planner。传入合法计划时跳过本地 Planner。
`Scope` 不接受浏览器或模型输入：宿主认证后从服务端资产授权配置构造，默认
`allow_active=False`、`max_parallel=1`、`max_attempts=2`，最多 24 个任务。
目标按完全相等的资产标识匹配，不进行隐式域名后缀、子域、URL 路径或 CIDR 扩展。

```python
bridge = PentestGPTBridge(workflow)
result = await bridge.run(
    request_json,
    authenticated_session=session_from_server_auth,
    scope=scope_from_server_policy,
)
events = bridge.events(result["job_id"],
                       authenticated_session=session_from_server_auth, after=0)
```

接入方自行实现 HTTP/RPC/队列传输及认证；本版本不提供监听网络的 API 服务。
服务端应将租户/用户/会话映射为唯一授权会话，不能仅信任 JSON 或请求头中的会话 ID。
生产端口须有认证、限流、TLS、请求体限制和租户隔离。

## 效率与可观测性

独立任务并发，直接依赖全部验收后才运行后继。某分支失败不阻断无依赖分支。
传给 Executor 的只有当前任务、直接依赖结果、返工反馈；Critic 只见当前任务证据，
避免传播完整聊天记录。证据内容有截断上限；截断会降低可验证性，不能据此强行验收。

同一个 `(session_id, request_id)` 和相同请求/Scope 返回已持久化结果，不再次调用模型。
相同键但不同请求或策略返回 RequestConflict。运行中重复请求返回 JobInProgress，
异常中断保留 running 状态，需要人工核对；没有自动故障恢复或 exactly-once 外部操作承诺。

事件通过单调递增 sequence 游标读取，每页至多 1000 条；传最后一个 sequence 获取下一页。
数据库保留 job/task/role 事件和已记录证据，不记录私有推理过程。
`completed` 只表示所有计划任务验收，不等于“目标无漏洞”。

## 生产验收前的剩余工作

真实模型结构化输出与工具兼容测试；沙箱生命周期和网络出口策略；宿主认证/队列/传输；
成本与工具进程取消；结果脱敏与数据库访问控制；浏览器 UI；与现有报告发布流水线的门控整合。
SHA-256 仅用于证据内容完整性校验，不证明漏洞真实、证据独立或不可被有写盘权限的人修改。
无证据、跨任务/跨 job 证据、无效 Critic 引用均不能通过验收；模型仍可能误判证据内容，
高风险结果需要确定性复测或人工审核。

SDK 接口依据：OpenAI Agents SDK 官方 Agent、Runner、Lifecycle 文档。
并发取消语义依据：Python 官方 asyncio TaskGroup / timeout 文档。
