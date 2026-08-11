# Gateway API 文档

本项目同时提供本地 HTTP/WebSocket 后端并作为 Dora node 运行。HTTP 侧负责 runtime 控制、Agent session、录制回放控制和 Tool discovery/invoke；Dora 侧负责接收状态输入、Tool provider/caller 输入并输出 `PolicyCommand` 与 Tool 消息。

## Web 数据采集面板

`GET /` 返回 Gateway 内置的数据采集面板，静态资源位于 `/static/*`。页面直接使用下述同源 HTTP/WebSocket 接口，可查看实时相机、readiness、关节与录制状态，并执行 start / stop / discard 和场景复位；无需单独部署前端。

Gateway API 当前不包含鉴权。默认 `host: 127.0.0.1` 仅允许本机访问；监听 `0.0.0.0` 时应通过可信局域网、防火墙或认证反向代理限制控制权限。

## Agent API

### `POST /agent/sessions`

创建一个 Agent action session，并把 `action_type` 映射为下游 policy 的 `PolicyCommand`。

请求示例：

```json
{
  "session_id": "session-1",
  "command_id": "command-1",
  "action_type": "grasp",
  "target": "apple",
  "instruction": "pick the apple",
  "inputs": {}
}
```

响应：`202` 返回 `session`、`command` 和当前状态；`400` 表示 action 或参数非法；`409` 表示 session/command 重复或已有活动 session。

### `GET /agent/sessions/{session_id}`

查询 session 及其 command 列表。成功返回 `200`，不存在返回 `404`。

### `POST /agent/sessions/{session_id}/cancel`

取消 session。Gateway 会把 session/command 标记为 `cancelled`，并向原 policy 发送 `command="stop"` 的 `PolicyCommand`。真实停止行为依赖下游 policy 实现 `stop`。

### `GET /agent/runtime/status`

返回 Agent runtime 摘要，包括 readiness、active session、session/command 状态、节点状态、最后一次结果和错误。

### `GET /agent/runtime/context`

返回完整 runtime context，可用于 Agent 读取当前能力、状态快照、session/command 明细和 runtime 状态。

### `GET /agent/runtime/capabilities`

返回当前配置支持的 Agent/runtime 能力。HTTP 输出与 CLI `--print-capabilities` 共用同一 payload。

### `POST /agent/runtime/reset`

Agent-facing reset 入口，内部复用 runtime `reset_scene` 命令。

请求示例：

```json
{
  "inputs": {
    "reason": "paos-agent"
  }
}
```

## Tool API

Gateway 是 caller-visible 的 Tool discovery/routing authority，仅支持 Query operation。

### `GET /tools`

列出当前 lease 有效的 endpoint descriptor，并返回 process-global Directory `revision`。`endpoint_instance_id` 是 Gateway 私有路由状态；public discovery 不暴露 instance、provider Dora route 或内部 monotonic `expires_at`。已过期 registration 会在读取时移除。

```json
{
  "ok": true,
  "data": {
    "revision": 3,
    "tools": [
      {
        "endpoint_id": "vision.yolo",
        "descriptor": {
          "protocol_version": "forge.tool.endpoint/v1alpha1",
          "endpoint_id": "vision.yolo",
          "operations": [
            {
              "name": "detect",
              "semantics": "query",
              "cancellable": false,
              "stoppable": false,
              "status_supported": false,
              "max_concurrency": 1
            }
          ]
        }
      }
    ]
  }
}
```

### `POST /tools/{endpoint_id}/{operation}:invoke`

通过与 Dora caller 相同的 `ToolGatewayService` 调用一个 Query。HTTP handler 只向有界 mailbox 提交请求并异步等待 correlated completion，不直接调用 Dora node。Query deadline 从 Gateway lifecycle admission 的 monotonic `processed_at` 开始；provider response 必须在 deadline 前由 lifecycle 处理完毕。HTTP 与 Dora pending invocation 的总数受 service `outbound_capacity` 限制，provider request 已被 lifecycle claim 后仍计数；达到上限的新调用返回 `FORGE_TOOL_GATEWAY_BUSY`，且不创建 pending state。

```json
{
  "arguments": {
    "image_id": "front"
  },
  "caller_id": "collector-ui",
  "timeout_ms": 5000
}
```

`arguments` 缺省为 `{}`；`caller_id` 和 `timeout_ms` 可省略。`tools.invoke_timeout_ms` 是配置上限而不只是默认值：省略 `timeout_ms` 时使用该值，显式值必须是 `1..tools.invoke_timeout_ms`，超出上限返回 `400`。成功返回 terminal Forge invoke payload。错误状态包括：

- `400`：body、字段类型或 timeout 非法；
- `404`：endpoint 未配置，或 active descriptor 不包含该 Query operation；
- `409`：相同 correlation identity 仍有 pending invocation；
- `422`：provider 明确拒绝 Query；
- `503`：Gateway disabled/closing/busy、endpoint 当前无 active instance、provider transport 失败或 outbound mailbox 已满；
- `504`：pending invocation 超过 deadline。

HTTP 输出不包含 provider-pinned `endpoint_instance_id`。HTTP handler 使用独立 monotonic hard deadline wait；即使 Dora lifecycle sweep 停止，也会通过 service cancellation path 结束 pending invocation。provider response 在 deadline 前被 reader 收到但仍排队时不会延长 HTTP deadline。provider response、transport output failure、caller cancellation、Gateway close 等异步 terminal completion 在同一 service lock 下按 monotonic observation 统一仲裁：`now >= deadline` 时返回 `FORGE_TOOL_INVOKE_TIMEOUT`/`504`，deadline 前已线性化的原结果保持不变。pending 已结束后到达的合法 provider response/error 是预期的 late/duplicate response，会被静默丢弃。Gateway 不执行 retry、dedup、action 或 session 调度。

## Runtime API

### `GET /runtime/status`

返回 readiness 和轻量状态快照。

### `POST /runtime/start`

当 readiness 满足时发送启动命令，默认 command 为 `start`。

请求示例：

```json
{
  "command": "start",
  "inputs": {}
}
```

### `POST /runtime/reset_scene`

发送 `reset_scene` runtime 命令，用于仿真或场景复位。

### `POST /runtime/stop`

请求停止本地 gateway 或关联 launcher。

## Record/Playback API

### `POST /record/control`

控制录制。

```json
{
  "action": "START",
  "output_path": "optional.mcap",
  "metadata": {
    "workflow": {"episode_id": "optional-episode-id"},
    "robot": {"robot_type": "optional-robot-type"}
  }
}
```

`action` 支持 `START`、`STOP`、`DISCARD`。`metadata` 仅用于 `START`，必须是 object；`DISCARD` 会结束当前 session 并删除输出文件。

### `POST /record/set_root`

设置录制根目录。

```json
{
  "root": "/tmp/records"
}
```

### `GET /record/status`

返回最新 `record_status`。

### `POST /playback/control`

控制回放。

```json
{
  "action": "START",
  "mcap_path": "optional.mcap"
}
```

`action` 支持 `START`、`PAUSE`、`RESUME`、`RESET`。

### `GET /playback/status`

返回最新 `playback_status`。

## Policy Command API

### `POST /policy/command`

直接发送 `PolicyCommand`，适合调试或非 Agent 调用方。

```json
{
  "command": "start",
  "inputs": {}
}
```

## WebSocket

### `/ws/state`

按 `state_broadcast_hz` 推送轻量状态快照，包括关节状态、动作、runtime 状态和 readiness。

### `/ws/images`

按 `image_broadcast_hz` 推送图像增量。压缩图像会直接 base64 转发，原始 `Image` 会编码为 JPEG。

## Dora 端口

输入：

- `tick`
- `proprio_state`
- `action`
- `runtime_status`
- `record_status`
- `playback_status`
- `policy_command_status`
- 配置中的 `image_input_ids`
- `tools.request_input_id`：public caller 的 logical `tool.invoke.request`
- `tools.providers[*].input_id`：provider→Gateway 共享输入，接受 `endpoint.register`、`endpoint.unregister`、`tool.invoke.response`、`tool.error`

输出：

- `policy_command`
- `tools.response_output_id`：返回 public caller 的 correlated `tool.invoke.response` 或 `tool.error`
- `tools.providers[*].output_id`：Gateway→provider 共享输出，发送 `endpoint.registry.response` 或 pinned `tool.invoke.request`

每条 provider config 仅包含 `endpoint_id`、`input_id`、`output_id`。Directory 不使用 source ID、source generation、tombstone 或 heartbeat。`endpoint.register` 是幂等 announce/renew；同 route 新 instance 原子替换 current。`endpoint_instance_id` 是 Gateway 私有路由状态：public caller 必须省略且 discovery/response 不暴露，Gateway resolve active registration 后只在 provider-facing invoke 上 pin instance。

Dora reader 为所有 provider/caller Tool 输入捕获 monotonic `received_at`，并把它们放入同一个有界有序 FIFO；不会像图像/状态输入一样按 input ID 合并。`endpoint.register`/`endpoint.unregister` 的 Directory lease effect 继续使用这个 reader observation。Query admission、配置 timeout 起点和 provider response deadline decision 使用 Gateway lifecycle 的 monotonic `processed_at`：response 必须在 deadline 前完成 service processing，deadline 前到达 reader 但仍排队不会获胜或延长 HTTP API deadline。public Dora `ToolContext.deadline_ms` 只能缩短 `tools.invoke_timeout_ms` 配置上限，不能延长它。

provider-facing `tool.invoke.request` 的 dispatch linearization point 是 lifecycle 在 `ToolGatewayService` lock 下从 outbound mailbox claim 有效消息并标记 `dispatch_claimed`。timeout/cancel/close 在 claim 前完成会使消息失效，drain 会跳过且不调用 Dora `send_output`；claim 后 send 仍在 lock 外执行，所以 provider 可能收到并完成 Query，但 invocation 在 terminal completion 前始终占用 pending capacity。所有 pending terminal path 共享 deadline arbiter：在 deadline 或之后线性化的 provider response、output failure、caller cancellation、close 等都完成为 timeout；deadline 前线性化的原结果保持不变。pending 已结束后的合法 provider response/error 静默丢弃，现有 pending 的 wrong-route response 仍拒绝。此语义只允许用于 Query：Gateway 不承诺 provider cancellation，也不会把 claim 后发送扩展到 Action 或 Session。所有 Dora `send_output` 只在 lifecycle thread 执行；每次 drain 有固定上限，shutdown 在拒绝新输入后执行一次 bounded final caller-response drain。Dora pending admission 会在有界 mailbox 预留 terminal response slot；management mutation 前预留 ACK slot。HTTP 与 Dora pending 总数受 service `outbound_capacity` 限制，HTTP thread 通过同一 service 提交请求，独立 hard deadline 与 lifecycle sweep 共享原子 cancellation/completion path。

## Action Manifest

Gateway 默认维护 `actions/{robot_id}/{policy_id}.md`。文件是 Markdown，但运行时只解析 YAML frontmatter。

示例：

```yaml
---
version: 1
robot_id: piper
policy_id: sam3
actions:
  grasp:
    command: grasp_simple
    required_parameters: ["target_name"]
    input_mapping:
      target: target_name
    resources: ["arm", "gripper", "camera"]
    timeout_s: 120
    completion:
      type: policy_status
---
```

执行链路：`action_type` -> `ActionRegistry` -> policy `command` -> `PolicyCommand` -> `policy_command_status` -> session/command 状态更新。

CLI 能力输出：

```bash
uv run python main.py --config config.example.yaml --print-capabilities
```
