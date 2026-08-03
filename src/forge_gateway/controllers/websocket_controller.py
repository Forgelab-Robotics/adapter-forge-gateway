"""Gateway WebSocket routes."""

from __future__ import annotations

import asyncio
import threading
from contextlib import suppress
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

try:
    from forge_common import get_logger
except Exception:  # pragma: no cover - fallback for minimal test envs
    import logging

    def get_logger(name: str) -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

from forge_gateway.adapters.websocket import (
    StalledWebSocketClient,
    send_json_with_timeout,
    sleep,
)

logger = get_logger(__name__)

_STOP_POLL_INTERVAL_SEC = 0.05


async def _sleep_until_stop(
    seconds: float,
    stop_event: threading.Event | None,
) -> bool:
    if stop_event is None:
        await sleep(seconds)
        return False

    remaining = seconds
    while remaining > 0 and not stop_event.is_set():
        delay = min(remaining, _STOP_POLL_INTERVAL_SEC)
        await sleep(delay)
        remaining -= delay
    return stop_event.is_set()


async def _send_json_until_stop(
    websocket: WebSocket,
    payload: dict[str, Any],
    timeout: float,
    stop_event: threading.Event | None,
) -> bool:
    if stop_event is None:
        await send_json_with_timeout(websocket, payload, timeout)
        return False

    send_task = asyncio.create_task(
        send_json_with_timeout(websocket, payload, timeout)
    )
    try:
        while not stop_event.is_set():
            done, _ = await asyncio.wait(
                (send_task,),
                timeout=_STOP_POLL_INTERVAL_SEC,
            )
            if send_task in done:
                await send_task
                return False
        return True
    finally:
        if not send_task.done():
            send_task.cancel()
            with suppress(asyncio.CancelledError):
                await send_task


async def _close_for_application_stop(
    websocket: WebSocket,
    stop_event: threading.Event | None,
) -> None:
    if stop_event is None or not stop_event.is_set():
        return
    try:
        await websocket.close(code=1001, reason="application shutdown")
    except (OSError, RuntimeError, WebSocketDisconnect):
        logger.debug("gateway: websocket client disconnected during shutdown")


def register_websocket_routes(
    app: FastAPI,
    runtime: Any,
    stop_event: threading.Event | None = None,
) -> None:
    @app.websocket("/ws/state")
    async def ws_state(websocket: WebSocket) -> None:
        await websocket.accept()
        with runtime.lock:
            runtime.state_ws_clients += 1
        interval = 1.0 / runtime.config.state_broadcast_hz
        try:
            while stop_event is None or not stop_event.is_set():
                if await _send_json_until_stop(
                    websocket,
                    runtime.state_snapshot(),
                    runtime.config.ws_send_timeout_sec,
                    stop_event,
                ):
                    break
                if await _sleep_until_stop(interval, stop_event):
                    break
        except WebSocketDisconnect:
            pass
        except StalledWebSocketClient:
            logger.info("gateway: closing stalled state websocket client")
        finally:
            await _close_for_application_stop(websocket, stop_event)
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
            while stop_event is None or not stop_event.is_set():
                for payload in runtime.latest_image_updates_since(cursors):
                    if await _send_json_until_stop(
                        websocket,
                        payload,
                        runtime.config.ws_send_timeout_sec,
                        stop_event,
                    ):
                        break
                if await _sleep_until_stop(interval, stop_event):
                    break
        except WebSocketDisconnect:
            pass
        except StalledWebSocketClient:
            logger.info("gateway: closing stalled image websocket client")
        finally:
            await _close_for_application_stop(websocket, stop_event)
            with runtime.lock:
                runtime.image_ws_clients = max(0, runtime.image_ws_clients - 1)
