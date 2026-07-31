# Gateway API 文档

本项目同时提供本地 HTTP/WebSocket 后端并作为 Dora node 运行。HTTP 侧负责 runtime 控制、Agent session、录制回放控制；Dora 侧负责接收状态输入并输出 `PolicyCommand`。

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

输出：

- `policy_command`

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
