# Forge Gateway

`gateway` 是统一的运行时入口，提供本地 HTTP 控制 API、runtime readiness/start gate，以及状态/图像 WebSocket。

本目录从 `forge_runtime` commit `3d63fcf14eeb29928a9a0598dd3dd550cbebb0d6` 的 tracked snapshot 导出，现作为独立的 `forge-gateway` 项目维护。

完整接口说明见本项目的 [Gateway API 文档](docs/api.md)。

## 独立开发

项目固定使用 Python `3.12`，依赖与开发/构建工具均由 `uv` 管理。

同步全部依赖组：

```bash
uv sync --frozen --all-groups
```

运行测试：

```bash
uv run pytest -q
```

从源码启动并检查帮助信息：

```bash
uv run python main.py --config config.example.yaml
uv run python main.py --help
uv run python main.py --version  # forge-gateway 1.0.1
```

这是一个直接从 checkout 运行的节点项目，不构建或发布 wheel/sdist，也不安装 console script。`pyproject.toml` 和 `uv.lock` 仅用于管理 Python 与依赖；根目录的 `main.py` 是节点入口，`config.py` 保留历史源码导入兼容。Tool Gateway 要求通过 `uv sync --frozen` 使用 lock 中同一 Forge commit 的 `forge-msgs` 与 `forge-tool`，且该 coordinated Forge revision 必须支持 instance-less `tool.*` Dora carrier；dependency pin 由协调变更原子更新。普通 `pip install .` 不属于受支持的原子部署路径。

构建独立可执行文件：

```bash
./scripts/build_pyinstaller.sh
```

构建脚本使用临时 `.venv_build` 环境，将产物写入 `dist/gateway`，并在完成后执行 `dist/gateway --help` 与 `dist/gateway --version` 验证入口和版本。

## 应用生命周期

`GatewayApplication` 统一管理 Runtime、Dora reader、launcher watcher 和 Uvicorn：

- Dora Node/runner 在 HTTP 端口开放前完成初始化。
- Uvicorn 必须完成端口绑定 handshake；端口占用或后台线程提前退出会触发启动回滚。
- `SIGINT`、`SIGTERM`、launcher 消失和 `/runtime/stop` 都进入同一 shutdown 路径。
- shutdown 首先关闭 command/image admission，再停止 HTTP/Dora ingress，最后执行 Runtime close barrier。
- cleanup 会尝试所有已获取的组件；超时或异常通过结构化结果返回，未完成的 close 可以重试。

线程 join 使用统一的 application budget；Runtime 自身仍使用独立的 dispatch/image cleanup budget。如果底层 Dora iterator 永久阻塞，Gateway 会报告 shutdown incomplete 并以非零状态退出，而不会把它误报为正常关闭。

## 配置

```yaml
host: 127.0.0.1
port: 9001
joint_order: [joint1, joint2, joint3, joint4, joint5, joint6, gripper]
image_input_ids:
  - image/wrist
  - image/top
state_broadcast_hz: 50
image_broadcast_hz: 24
ws_send_timeout_sec: 1.0
jpeg_quality: 85
policy_id: default
command_queue_capacity: 256
readiness:
  require_proprio_state: true
  require_images: true
  require_state_client: false
  require_image_client: false
  proprio_stale_after_sec: 2.0  # 设为 null 可显式使用旧的 presence-only 行为
  image_stale_after_sec: 2.0

tools:
  enabled: false
  lease_ttl_ms: 15000
  invoke_timeout_ms: 5000  # 所有 caller timeout 的最大值
  request_input_id: tool_request
  response_output_id: tool_response
  providers: []
  # providers:
  #   - endpoint_id: vision.yolo
  #     input_id: yolo/to_gateway
  #     output_id: gateway/to_yolo
```

Gateway 对配置执行严格校验：未知或重复的 YAML key、字符串形式的布尔值、非有限数值、空/重复 input ID 都会直接报错。`agent.max_active_sessions` 当前只允许整数 `1`；旧的 `broadcast_hz` alias 仍可单独使用，但不能与 `state_broadcast_hz` 同时配置。

当 `readiness.require_images: true` 时，`image_input_ids` 不能为空，否则 readiness 会报告缺少 `image_input_ids`。本体状态必须携带 position、velocity 或 effort 数值，并至少匹配一个 `joint_order` 中的 joint；partial joint state 仍受支持。

未配置 `agent.action_manifests` 时，Gateway 会加载 package 内置的 `piper/sam3.md`，无需复制资源或依赖当前工作目录。显式配置外部 manifest 时，相对路径仍按 YAML 配置文件所在目录解析。

`tools.enabled: true` 时，Gateway 可以作为 Tool-only Dora 节点启动，不要求 `joint_order`。Gateway 是唯一 caller-visible discovery/routing authority：每个 provider 只配置 `endpoint_id`、provider→Gateway 的 `input_id` 和 Gateway→provider 的 `output_id`。provider 在同一输入发送 `endpoint.register`、`endpoint.unregister`、`tool.invoke.response` 或 `tool.error`，Gateway 在同一输出发送 correlated `endpoint.registry.response` 或 pinned `tool.invoke.request`。

Directory 对每个 `endpoint_id` 维护一个带 monotonic lease 的 current instance。`endpoint.register` 同时承担 announce/renew：同 route、instance、descriptor 只续租且 revision 不变；同 route 的新 instance 原子替换 current 并增加 process-global revision。register/unregister 的 lease effect 使用 Dora reader 捕获的 `received_at`；Query admission 和 provider response deadline 则以 Gateway lifecycle 实际处理时的 `processed_at` 判定。`tools.invoke_timeout_ms` 是所有 caller timeout 的配置上限：HTTP 可省略 `timeout_ms` 或传入 `1..invoke_timeout_ms`，public Dora `ToolContext.deadline_ms` 也只能缩短、不能延长该上限。没有 heartbeat、source generation、tombstone、重试、dedup、action 或 session；仅支持 Query operation。`endpoint_instance_id` 是 Gateway 私有路由状态，不出现在 public discovery 或 caller response 中。public Dora caller 从 `tools.request_input_id` 发送 instance-less logical `tool.invoke.request`；Gateway resolve current 后只在 provider-facing request 上 pin instance，并从 `tools.response_output_id` 返回 terminal response/error。所有 Tool 输入进入同一个有界有序 FIFO；HTTP 与 Dora pending invocation 的总数不超过 service `outbound_capacity`，provider dispatch 被 claim 后仍占用 pending capacity。Dora pending invocation 在有界 outbound mailbox 中预留 terminal response slot，HTTP handler 另有独立 hard deadline wait，所有 Dora `send_output` 均由 lifecycle thread 执行。

启动：

```bash
uv run python main.py --config gateway.yaml
```

## 数据采集面板

启动 dataflow 后打开 <http://127.0.0.1:9001>（端口以配置为准）。Gateway 内置的零构建依赖 Web 面板提供：

- 多路实时相机与 readiness / 连接状态；
- 关节实测值、动作指令和 recorder 统计；
- 带唯一输出路径与采集元数据的开始录制；
- 停止并保留、丢弃当前 session、场景复位。

页面与 API 同源，不需要单独启动前端服务。默认仅监听 `127.0.0.1`；如果改为 `0.0.0.0`，同一网络中的客户端也能调用控制 API，部署时应自行增加可信网络隔离或认证代理。

打印当前配置支持的 Agent/runtime 接口（不启动 Dora/HTTP）：

```bash
uv run python main.py --config gateway.yaml --print-capabilities
```

## HTTP API

- `POST /record/control`，body: `{"action":"START|STOP|DISCARD","output_path":"optional.mcap","metadata":"optional object"}`
- `POST /record/set_root`，body: `{"root":"..."}`
- `GET /record/status`
- `POST /playback/control`，body: `{"action":"START|PAUSE|RESUME|RESET","mcap_path":"optional.mcap"}`
- `GET /playback/status`
- `POST /agent/runtime/reset`：Agent-facing 场景复位入口，内部复用 `reset_scene` runtime 命令。
- `POST /runtime/reset_scene`：发送 `reset_scene` runtime 命令，用于仿真场景复位；不同于 playback 的 `RESET`
- `POST /runtime/stop`
- `GET /tools`：列出当前 active Tool descriptor；不暴露 provider instance、route 或 monotonic lease 时间。
- `POST /tools/{endpoint_id}/{operation}:invoke`：调用 Query Tool，body: `{"arguments":{},"caller_id":"optional","timeout_ms":5000}`；`timeout_ms` 可省略，否则必须在 `1..tools.invoke_timeout_ms`；使用与 Dora caller 相同的 Tool Gateway service。

- `POST /policy/command`：直接发送 `PolicyCommand`，body: `{"command":"...","inputs":{}}`
- `GET /runtime/status`：返回状态快照与 readiness
- `POST /runtime/start`：仅在 readiness 满足时发送默认 `start` 命令，可用 body 覆盖 `command` 和 `inputs`

## WebSocket

- `/ws/state`：按 `state_broadcast_hz` 推送轻量 JSON 状态，包括 `proprio_state`、`action`、运行状态与 readiness。
- `/ws/images`：按 `image_broadcast_hz` 推送每路图像的增量消息。压缩图像会直接 base64 转发；原始 `Image` 会编码为 JPEG。
- `ws_send_timeout_sec`：单次 WebSocket 发送超时时间。连接写入停滞超过该时间会被 gateway 主动断开，避免阻塞后续广播。

图像消息包含：

```json
{
  "type": "image",
  "id": "image/wrist",
  "seq": 1,
  "timestamp": 1710000000.0,
  "format": "jpeg",
  "content_type": "image/jpeg",
  "data": "...base64..."
}
```

## Dora 输入输出

输入：

- `tick`
- `proprio_state`
- `action`
- `runtime_status`
- `record_status`
- `playback_status`
- 配置中的 `image_input_ids`
- `tools.request_input_id`：public caller 的 logical `tool.invoke.request`
- `tools.providers[*].input_id`：provider 的 register/unregister/invoke.response/tool.error 共享输入

输出：

- `policy_command`
- `tools.response_output_id`：public caller 的 correlated invoke.response/tool.error
- `tools.providers[*].output_id`：发给 provider 的 registry.response/invoke.request 共享输出

所有 provider/caller Tool 输入使用同一个有界有序 FIFO，不参与图像/状态的 latest-value coalescing；FIFO 满时 Gateway 显式失败，不静默丢失 registration、invoke response 或 error。Tool outbound mailbox 同样有界：management 在 Directory mutation 前保留 ACK capacity，Dora invocation 在 admission 时保留 terminal response capacity；只剩一个 slot 时返回 immediate busy error，没有 slot 时不改变状态。HTTP 与 Dora pending invocation 总数也由同一个 service `outbound_capacity` 限制；达到上限时新 invocation 返回 `FORGE_TOOL_GATEWAY_BUSY` 且不创建 pending state，已被 lifecycle claim 的 provider request 仍计入该上限。register/unregister 的 Directory effect 保留 reader `received_at` 语义；Query deadline 从 lifecycle `processed_at` 开始，并要求 provider response 在 deadline 前完成 lifecycle processing，deadline 前进入 reader FIFO 但尚未处理的 response 不会延长 deadline。

provider invoke 的 dispatch linearization point 是 lifecycle 在 `ToolGatewayService` lock 下从 mailbox claim request，并同步标记 `dispatch_claimed`。timeout/cancel/close 在 claim 前发生时，该 request 会失效且不会调用 Dora `send_output`；claim 后 Dora send 仍在 lock 外执行，因此可能继续发给 provider。所有异步 terminal completion 在 service lock 下统一仲裁：其 monotonic observation 达到或超过 pending deadline 时必须返回 `FORGE_TOOL_INVOKE_TIMEOUT`（HTTP `504`），而不是更晚发生的 provider、transport、caller-cancelled 或 closing 结果；deadline 前已完成线性化的结果保持不变。pending 已结束后的合法 provider response/error 视为 late/duplicate 并静默丢弃。该有限的 claim-after-effect 只适用于无 Gateway retry/cancel 协议的 Query，不适用于 Action 或 Session。每次 lifecycle drain 有固定上限，shutdown 会在 lifecycle thread 做一次最终 bounded caller-response drain。HTTP handler 的独立 monotonic wait 是 hard upper bound；它与 lifecycle sweep 共享同一个原子 cancellation/completion path。
