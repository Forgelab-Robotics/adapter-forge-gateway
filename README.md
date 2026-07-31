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

从源码启动：

```bash
uv run gateway --config config.example.yaml
```

包入口与 console 入口等价，可分别检查帮助信息：

```bash
uv run python -m forge_gateway --help
uv run gateway --help
```

根目录的 `main.py` 与 `config.py` 仅作为源码 checkout 的兼容 shim；wheel 只安装 `forge_gateway` package。

构建独立可执行文件：

```bash
./scripts/build_pyinstaller.sh
```

构建脚本使用临时 `.venv_build` 环境，将产物写入 `dist/gateway`，并在完成后执行 `dist/gateway --help` 验证入口。

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
readiness:
  require_proprio_state: true
  require_images: true
  require_state_client: false
  require_image_client: false
  proprio_stale_after_sec: 2.0  # 设为 null 可显式使用旧的 presence-only 行为
  image_stale_after_sec: 2.0
```

未配置 `agent.action_manifests` 时，Gateway 会加载 package 内置的 `piper/sam3.md`，无需复制资源或依赖当前工作目录。显式配置外部 manifest 时，相对路径仍按 YAML 配置文件所在目录解析。

启动：

```bash
uv run gateway --config gateway.yaml
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
uv run gateway --config gateway.yaml --print-capabilities
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

输出：

- `policy_command`
