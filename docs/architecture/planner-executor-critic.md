# Planner / Executor / Critic 与 PentestGPT 集成

## 交付范围

本实现是在原 Strix 引擎之上新增的 **opt-in 协作层**，入口为
`python -m strix.collaboration`。原 `strix` CLI、`build_strix_agent`、
`run_strix_scan`、内部子智能体图和既有报告流程保持不变。

基线：`87f2d783d6595d0aa557809e02c67d94e52bda05`。
这不是将原引擎所有内部智能体重写为三个角色，也没有默认切换已有部署。
PentestGPT-Web 的对接客户端位于配套仓库 `integrations/strix/`；原仓库只有 README，
本次没有声称已经存在或完成一个 Next.js 前端、身份系统或 HTTP 后端。

## 执行流程

```text
PentestGPT 服务端（认证、可信作用域、request_id）
               |
        版本化 Request 协议
               |
    外部 Plan 已提供？──是──> 校验依赖图与作用域
               | 否                  |
          Planner（无工具）───────────┘
                                     |
                  DAG 调度 / 并发上限 / 同资产串行
                                     |
                       Executor（独立 Strix 进程）
                                     |
                    持久化证据 ID、内容摘要与哈希
                                     |
                       Critic（无工具、独立上下文）
                          /          |          \
                       accept      revise      reject
                         |           |           |
                  解锁依赖任务  只读任务有限重试  阻断依赖
                         |
                JobResult + 可续读的事件游标
```

| 模块 | 职责 |
| --- | --- |
| `models.py` | 版本化、长度受限、禁止多余字段的契约；任务 DAG 和作用域校验 |
| `engine.py` | 任务依赖、调用预算、并发、超时、取消、验收门禁 |
| `roles.py` | 独立配置的 Planner 和 Critic；通过 LiteLLM 发出无工具的 JSON 模型调用 |
| `native.py` | 复用现有 headless Strix；隔离工作目录、进程、预算和报告产物 |
| `store.py` | SQLite WAL、幂等请求、证据绑定、事件持久化 |
| `bridge.py` | Python 服务端集成；校验认证会话并注入可信 Scope |
| `__main__.py` | `schema`、`validate`、`run`、`events` JSON CLI |

### 角色与验收

Planner 只输出目标、验收条件和依赖，不执行工具。PentestGPT 已经提供结构化计划时，
不会再调用 Planner 做第二次规划。外部计划同样必须经过范围、任务数和依赖图校验。

Executor 复用原 Strix AI 引擎，而不是重新实现其工具链。每项任务使用单独的
POSIX 子进程和工作目录，避免原引擎的进程内状态在并发任务之间串扰。
其内部子智能体机制仍然保留；任务指令不是底层 OS/网络权限隔离。

Critic 使用新的消息上下文，可以配置与 Planner/Executor 不同的模型。
它没有执行工具，也不能通过声明一个不存在的证据 ID 来使任务通过。
引擎检查证据是否属于当前 job、task 和 target，是否被改写，以及 Critic 引用的
证据是否确实由 Executor 返回。没有证据不能验收；拒绝或失败会阻断依赖任务。

**重要：`accepted` 表示独立模型完成了证据审阅，不等于独立运行了漏洞复现，
也不表示目标安全。** 原 Strix 的报告本身可能包含模型生成的内容。
内容哈希用于一致性检查，不证明报告中的判断一定真实。需要真实复测的结论，应安排
单独授权的复测任务或扩展 Critic 实现，不能把报告叙述当成复现事实。

## 运行与模型配置

按仓库现有方式安装开发环境后，可以先进行完全离线的协议检查：

```bash
uv run python -m strix.collaboration schema
uv run python -m strix.collaboration validate \
  --request examples/collaboration/request.json \
  --scope examples/collaboration/scope.json
```

示例只检查协议，不调用模型或目标。示例 `mode=review` 且不允许主动操作；
原生适配器会拒绝直接把这个示例当成扫描执行。

模型配置由服务端提供，不接受浏览器上传密钥、执行命令或模型配置：

```bash
export STRIX_PLANNER_MODEL='provider/your-planner-model'
export STRIX_CRITIC_MODEL='provider/your-critic-model'
export STRIX_EXECUTOR_MODEL='provider/your-executor-model'
# 按提供商设置服务端凭据。不要把真实密钥提交到 Git。
# Planner/Critic 可分别设置 STRIX_PLANNER_API_KEY / STRIX_CRITIC_API_KEY。
# 自定义端点：STRIX_PLANNER_API_BASE / STRIX_CRITIC_API_BASE。
```

Planner/Critic 默认使用 JSON mode；提供商不支持该参数时，可以设置
`STRIX_PLANNER_JSON_MODE=0` 或 `STRIX_CRITIC_JSON_MODE=0`，输出仍会经过严格的本地契约校验。
它们直接使用 LiteLLM API 路由，不自动继承 Strix 的订阅登录状态。
Executor 继承原 Strix 的凭据/配置体系，`STRIX_EXECUTOR_MODEL` 只覆盖其模型选择。
没有外部计划时必须配置 Planner；必须配置 Critic 才能验收。

实际运行需要由服务端管理员建立独立的授权策略文件。原生适配器当前只支持明确授权的
HTTP(S) 目标，不支持本地目录；这是为了避免将原 CLI 的可写源码挂载误当成只读审计。
原生扫描必须同时满足 `Scope.allow_active=true` 和 `Task.mode=active`。

```bash
uv run python -m strix.collaboration run \
  --request /trusted/requests/assessment.json \
  --scope /trusted/policies/assessment.json \
  --db /private/strix-pec/state.sqlite \
  --work-root /private/strix-pec/runs \
  --executor-budget 3 --executor-turns 24
```

这些路径均是服务端受控文件，不应直接来自 HTTP 请求参数。原生执行需要原 Strix 的
完整依赖、Docker 和有效模型凭据。没有运行依赖或模型时，离线验证和契约测试仍可运行。

作用域默认每阶段超时 120 秒；原生扫描较慢时，应在可信 Scope 中设置合适的
`phase_timeout_seconds`（例如 600）及总超时。原生适配器自身默认超时 600 秒。
阶段超时、总超时和原生超时中最早达到的限制会终止执行。

## PentestGPT 集成契约

`protocol_version` 固定为 `strix-pentestgpt/1`。`session_id` 必须由服务端身份上下文
决定，不能仅信任浏览器声明。`Scope` 不在 Request 中；传入多余的 `scope` 字段会被拒绝。

服务端 Python 集成可直接构造 `Workflow` 和 `PentestGPTBridge`：

```python
bridge = PentestGPTBridge(workflow)
result = await bridge.submit(
    request_json,
    authenticated_session_id=server_session_id,
    scope=policy_loaded_by_server,
)
progress = bridge.events(
    request_id,
    authenticated_session_id=server_session_id,
    after=last_sequence,
)
```

Node 服务端可使用配套仓库的 `createStrixClient`。它调用本机 Python JSON CLI，
不需要新增一个无认证 HTTP 执行端口。生产 Web 请求应交给受控的长任务 worker；
不要把耗时扫描直接部署到短时限或无 Docker 的 serverless 路由中。
本实现没有创建消息队列、HTTP/SSE/WebSocket 服务或用户登录系统。

按 request_id 可以在执行尚未结束时读取持久化进度：

```bash
uv run python -m strix.collaboration events \
  --db /private/strix-pec/state.sqlite \
  --session-id server-session-id --request-id assessment-id --after 0
```

每页最多 1000 条，按 `next_cursor` 继续获取。事件内容包含任务和审阅信息，
应遵守与报告相同的访问控制。CLI 返回 JSON；传输成功不代表安全检查通过：
调用方必须检查 JobResult.status 和每个 TaskOutcome.status。

## 效率、恢复与边界

任务仅在全部依赖验收后启动；不同目标在 `max_parallel` 范围内并行，同一精确目标串行。
依赖任务传给原生 Executor 的摘要每条最多 2000 字符，避免复制所有历史对话。
原始产物留在私有运行目录，传给 Critic 的内容有明确长度上限和截断标记。
尚未做真实模型成本、延迟或误报率基准，因此不宣称任何百分比性能提升。

`max_role_calls` 限制协作层角色调用，不等于 LLM token 或美元预算。
Planner/Critic 每次调用另有输出 token 和输入字符限制；Executor 使用原引擎的
`--max-budget` 和 `--max-turns`。原引擎预算是调用边界控制，可能有单次调用超支。
多实例部署还需外部队列、每用户配额和宿主机容量控制；本地并发限制不是集群级调度。

`(session_id, request_id)` 是幂等键，指纹绑定规范化 Request 和 Scope。
已完成、失败或取消的同一请求返回缓存结果；复用键但改变请求/策略会拒绝。
正在运行或崩溃留下的 running 请求不会自动重新执行。运维应先核对进程、容器和
目标侧副作用，再用新 request_id 发起明确批准的任务。该机制不是外部工具
副作用的 exactly-once 保证，也不自动续跑未完成的原生扫描。

只有声明为只读 `review` 的自定义 Executor 才会在 Critic 要求 revise 后有限重试。
原生适配器全部属于 active：即使 Critic 要求补证据，也不自动重复主动操作。
执行异常或超时一律不自动重试。

取消会先向原生进程组发送 SIGINT，给引擎清理机会，再按需升级为 SIGTERM/SIGKILL。
强杀或宿主机崩溃后 Docker 容器清理无法保证，需运维核对。数据库、原生运行日志、
报告与证据可能含敏感数据；应存放在 Git 目录外，控制权限、磁盘配额和保留时间。

目标匹配是精确资产匹配，不是 DNS、重定向、网络出口或工具权限隔离。
部署仍需通过 Docker/网络策略限制实际出口，使用测试账号并隔离生产写操作。
原生 Executor 默认沿用用户的 Strix/MCP 配置；需要收窄 MCP 时，可在 Python
NativeConfig.mcp_config 指定管理员维护的配置文件。

## 已执行的验证与未完成的验证

本次在 Python 3.13.5 / Pydantic 2.13.5 环境中运行新增 Python 测试：**42 项通过**。
Node 22.16.0 下客户端测试 **9 项通过**，其中包括跨仓库 Python CLI 的拒绝路径、
幂等结果回读和事件读取。所有测试使用本地合成数据/替身，没有对真实目标发起安全测试。

```bash
python -m pytest -q tests/test_collaboration.py tests/test_collaboration_native.py
node --test integrations/strix/client.test.mjs  # 在 PentestGPT-Web 仓库
# 跨仓库用例需要 STRIX_TEST_PYTHON 指向可导入本实现的 Python，且配置 PYTHONPATH。
```

未运行完整原仓库测试集、Ruff/Mypy/Bandit、Python 3.12/3.14 矩阵、真实模型调用、
Docker 实际扫描或已部署 Web 前端联调。当前改动应作为待完整 CI 和授权环境验收的
功能分支审阅，不应据此声称已经生产部署或完成全部内部架构迁移。
