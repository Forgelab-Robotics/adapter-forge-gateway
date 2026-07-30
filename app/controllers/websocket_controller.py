"""Gateway WebSocket routes."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

try:
    from forge_common import get_logger
except Exception:  # pragma: no cover - fallback for minimal test envs
    import logging

    def get_logger(name: str) -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

from app.adapters.websocket import StalledWebSocketClient, send_json_with_timeout, sleep

logger = get_logger(__name__)


def register_websocket_routes(app: FastAPI, runtime: Any) -> None:
    @app.websocket("/ws/state")
    async def ws_state(websocket: WebSocket) -> None:
        await websocket.accept()
        with runtime.lock:
            runtime.state_ws_clients += 1
        interval = 1.0 / runtime.config.state_broadcast_hz
        try:
            while True:
                await send_json_with_timeout(
                    websocket,
                    runtime.state_snapshot(),
                    timeout=runtime.config.ws_send_timeout_sec,
                )
                await sleep(interval)
        except WebSocketDisconnect:
            pass
        except StalledWebSocketClient:
            logger.info("gateway: closing stalled state websocket client")
        finally:
            with runtime.lock:
                runtime.state_ws_clients = max(0, runtime.state_ws_clients - 1)

    @app.websocket("/ws/images")
    async def ws_images(websocket: WebSocket) -> None:
        await websocket.accept()
        with runtime.lock:
            runtime.image_ws_clients += 1
        interval = 1.0 / runtime.config.image_broadcast_hz
        cursors: dict[str, int] = {}
        try:
            while True:
                for payload in runtime.latest_image_updates_since(cursors):
                    await send_json_with_timeout(
                        websocket,
                        payload,
                        timeout=runtime.config.ws_send_timeout_sec,
                    )
                await sleep(interval)
        except WebSocketDisconnect:
            pass
        except StalledWebSocketClient:
            logger.info("gateway: closing stalled image websocket client")
        finally:
            with runtime.lock:
                runtime.image_ws_clients = max(0, runtime.image_ws_clients - 1)
