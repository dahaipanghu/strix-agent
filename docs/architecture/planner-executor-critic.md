# Planner / Executor / Critic collaboration (v1)

## 状态与迁移边界

本变更新增 `strix.collaboration` 可选协作层，不改变原有 `strix` CLI、
`build_strix_agent()`、`AgentCoordinator` 或 `run_strix_scan()` 的默认行为。
这是一版可运行的编排与适配实现，不是对全部旧代理代码的破坏性替换。

代码基于 `dahaipanghu/strix-agent` 的
`87f2d783d6595d0aa557809e02c67d94e52bda05`。检查时
`dahaipanghu/PentestGPT-Web` 只有 README，没有可对接的实际前后端代码。
所以本次交付的是 **Strix 侧接入协议与服务端桥接模块**，不声称已经上线了
PentestGPT 浏览器界面，也不假设 GreyDGL/PentestGPT 内部任务树有某种未验证的格式。

## 架构

```text
PentestGPT / 可信业务后端
  │ 已认证会话 + 服务端审批范围 + 请求幂等键
  ▼
PentestGPTBridge ── Request / Plan / JobResult / Event 协议
  │
  ├── 已提供 Plan ──────────────┐
  └── 未提供 Plan → Planner ───┤
                               ▼
                    校验目标范围与任务依赖 DAG
                               ▼
                    有界 Executor 并发池
                               │ 实际记录的证据 ID
                               ▼
                    独立 Critic：accept / revise / reject
                               │
       被动任务有限返工 ◄────────┤
       主动任务不自动重放        └── 通过 / 驳回 / 失败 / 阻塞

所有阶段 → SQLite 请求账本、不可更新的证据记录、可续读事件游标
```

Planner 只生成目标、任务依赖和验收条件，没有执行工具。Executor 只执行分配的任务，
返回摘要与证据引用，不决定任务是否通过。Critic 使用新对话检查任务、结果和证据，
不继承 Executor 的对话历史，也没有执行工具。三个角色可以配置不同模型对象或
不同 `RunConfig`，不绑定具体供应商或硬编码模型名称。

Critic 是独立的**证据审查**角色，不是自动重放漏洞的第二套扫描器。
本版本的 `revise` 回传给 Executor，不会自动重写整张计划。
主动操作需要返工时，应由业务后端重新审批并创建新请求，而不是悄悄重复操作。

## 关键机制

| 模块 | 职责 |
|---|---|
| `models.py` | 有版本、额外字段拒绝、长度与数量限制的数据契约；DAG 与范围检查 |
| `engine.py` | 依赖调度、并发上限、角色调用上限、超时、取消及 Critic 门槛 |
| `store.py` | SQLite WAL、请求去重、会话隔离、事件游标、证据哈希 |
| `sdk.py` | 独立 Planner / Critic 模型；被动原始材料审查 Executor |
| `native.py` | 现有 Strix CLI 的独立进程执行适配器 |
| `pentestgpt.py` | 传输无关的服务端桥接，不自建公开 HTTP 服务 |
| `__main__.py` | 无网络、无模型费用的确定性示例 |

只有通过 Critic 的前置任务才会释放下游任务。独立任务可并发，依赖任务只得到其
直接前置任务的摘要，避免把整个会话复制给每个模型。默认并发为 1，可配置到 8。
这是每个 job 的上限，不是整个服务的总并发上限；部署方仍需全局准入和资源配额。

默认最大 24 个任务、每个被动任务最多 2 次执行、100 次角色调用。
角色调用上限不等于 token 或美元上限；SDK 模型另有输出 token 与轮数限制，
native 执行器把每任务预算传给原有 `--max-budget`，整站还需供应商配额。
没有基于真实目标的提速或成本百分比基准测试。

## PentestGPT 协议

请求示例（`scope` 故意不在请求中）：

```json
{
  "protocol_version": "strix-pentestgpt/1",
  "session_id": "server-issued-session",
  "request_id": "review-request-001",
  "goal": "Review the approved configuration artifact",
  "plan": {
    "tasks": [{
      "id": "config-review",
      "target": "approved-config",
      "objective": "Review the supplied configuration",
      "acceptance_criteria": ["Cite the relevant original configuration evidence"],
      "depends_on": [],
      "mode": "review"
    }]
  }
}
```

有 `plan` 时，Strix 不再调用 Planner，避免与 PentestGPT 重复规划。
无 `plan` 时，由 Strix Planner 生成计划。任务树应由 PentestGPT 后端显式转换成
本协议，不能把其他项目的任意内部状态直接当成已兼容的计划。

服务端集成示例：

```python
import asyncio
import os
from pathlib import Path

from strix.collaboration import Request, Scope, Store, Workflow
from strix.collaboration.pentestgpt import PentestGPTBridge
from strix.collaboration.sdk import Artifact, SDKArtifactExecutor, SDKCritic, SDKPlanner

async def main() -> None:
    # Model IDs are operator configuration. Configure provider credentials server-side.
    state = Path("./pec-state")
    state.mkdir(mode=0o700, exist_ok=True)
    store = Store(state / "jobs.sqlite")
    try:
        workflow = Workflow(
            SDKPlanner(os.environ["PLANNER_MODEL"]),
            SDKArtifactExecutor(
                os.environ["EXECUTOR_MODEL"],
                {"approved-config": (Artifact("config.txt", "debug=false"),)},
            ),
            SDKCritic(os.environ["CRITIC_MODEL"]),
            store,
        )
        bridge = PentestGPTBridge(workflow)
        # In a real application this value comes from authenticated server state.
        session_id = "server-issued-session"
        request = Request(
            session_id=session_id, request_id="review-request-001",
            goal="Review only the supplied approved-config artifact",
        )
        result = await bridge.submit(
            request.model_dump_json(),
            authenticated_session_id=session_id,
            approved_scope=Scope(targets=("approved-config",)),
        )
        print(result.model_dump_json(indent=2))
        events = bridge.events(result.job_id, authenticated_session_id=session_id)
        if events:
            cursor = events[-1].sequence
            # Persist cursor and pass after=cursor on the next poll.
    finally:
        store.close()

asyncio.run(main())
```

可以用 `Request.model_json_schema()`、`JobResult.model_json_schema()` 与
`Event.model_json_schema()` 生成对应协议 schema。
生产后端可把 `submit()` 映射到自己的受认证任务接口，把 `events(after=...)`
映射到轮询/SSE/WebSocket。**本提交没有实现这些 HTTP 路由、认证服务或 Web UI。**

`(session_id, request_id)` 是幂等键。相同请求和范围完成后直接返回缓存结果；
相同键但不同内容/策略抛出 `RequestConflict`；已有运行中任务抛出
`JobInProgress(job_id)`，调用方应读取该任务的事件，而不是再执行一次。
每次读取最多 1000 个事件，使用最后的 `sequence` 续读。
会话 ID 必须由后端全局唯一分配，不能直接信任浏览器输入。
更换模型配置、执行后端或原始材料时也必须使用新的 request_id。

## 对接现有 Strix 扫描能力

```python
from strix.collaboration.native import NativeStrixExecutor

executor = NativeStrixExecutor(
    model="your-configured-strix-model-id",
    work_root="./pec-state/native",
    max_turns=30,
    max_budget_usd=5.0,
)
```

此适配器需要完整安装的 Strix、模型凭据、Docker/其已有运行环境以及 POSIX 系统。
它只接受 `mode="active"` 任务；调用方必须用经过审批的 `Scope(allow_active=True, ...)`。
建议由 PentestGPT 传入明确的 active 计划；不要把 native 扫描假称为只读材料审查。
批次同时包含被动材料审查与主动扫描时，由业务后端按 `task.mode` 分派到对应
Executor，不能让 native 后端自动接管被动任务。

每任务单独进程和临时工作目录，避免现有全局报告/config 状态在并发 job 间串用。
使用参数数组而非 shell 拼接；MCP 默认使用空配置。原有 CLI 的内部子代理机制
仍然保留：这是把现有引擎纳入 Executor 层，不是重写其全部内部调度。
退出码 0/2 仍需结合 `run.json.status == completed` 才算执行完成。

原生报告标记为 `model_report`，不能当作已独立复验的证据。
运行状态只能证明扫描运行过，不能证明某个漏洞成立或目标安全。
需要更强验证时，部署方应实现 Executor 协议，把可审查的原始工具结果通过
`ExecutionContext.record()` 记录，而不是仅传模型报告。
原生适配器截取的报告目前上限为每文件 24,000 字节；超过上限会明确失败，
不会悄悄当作完整报告接受。任务临时目录在结束后删除；所捕获材料保留在 Store 中。

取消/超时会终止并回收本地进程组，并处理取消与启动之间的竞态。
**这不保证 Docker/远程运行平台上已经创建的容器、会话或网络动作被回滚。**
生产部署必须有运行平台的配额、任务终止与过期资源清理机制。

## 信任边界与结果含义

范围检查使用精确资产 ID，不自动扩展子域名、CIDR、重定向或本地路径。
它是编排门槛，不是 DNS/网络隔离、只读文件系统或防 SSRF 沙箱。
真正的网络出口、文件挂载、工具权限与凭据隔离属于执行运行环境。

证据由可信 Executor 代码登记，模型不能自行登记证据 ID。
验收引用必须属于当前 job、task 和 target，哈希必须匹配；只有模型报告的结果
不能通过门槛。哈希保证已保存内容的一致性，不证明工具结果在现实世界中一定正确。
模型 Critic 仍可能犯错，重要结论应由人工或独立原始证据复核。
`job.completed` 表示任务的审查全部通过，**不表示目标无漏洞**。

Store 包含潜在敏感材料，应使用私有目录、最小访问权限、备份和保留期限策略。
新数据库以 0600 创建，拒绝 POSIX 下有组/其他用户访问权限的已有数据库。
SDK 默认关闭 tracing，但显式传入自定义 RunConfig 时由调用方负责 tracing 设置。
原始材料会发给配置的模型供应商；送入之前应按业务要求脱敏、分类和审批。
异常只输出类型名，避免把供应商 URL、凭据和原始错误内容写入用户事件。

SQLite 账本防止重复领取，但不是分布式工作队列；没有跨服务自动抢占或崩溃续跑。
崩溃留下的 running 记录必须人工/平台核对，禁止自动重放可能已产生副作用的动作。
本版本没有跨任务语义去重、动态改写 DAG、全局美元账本或模型吞吐基准。

## 验证

```bash
python -m strix.collaboration
python -m pytest -q tests/test_collaboration.py tests/test_collaboration_adapters.py
```

首条命令使用确定性替身，不访问任何目标或调用模型；第二条覆盖 DAG、并发、
幂等、会话范围、证据绑定、复核返工、预算、取消和本地子进程适配。
SDK 测试使用接口替身，不等于真实模型供应商联调。
提交前本地新增测试为 53 项通过；完整原仓库回归、Ruff/Mypy、真实 SDK/Docker/模型
端到端测试未在当前受限环境完成。部署前应在完整开发环境运行 `make check-all`
和 `uv run pytest`，并对自有/已授权测试环境执行冒烟测试。
