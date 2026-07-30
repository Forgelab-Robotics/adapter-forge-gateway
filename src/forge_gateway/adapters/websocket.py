"""WebSocket transport helpers."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import WebSocket


class StalledWebSocketClient(Exception):
    """Raised when a websocket client write stalls long enough to block broadcasts."""


async def send_json_with_timeout(websocket: WebSocket, payload: dict[str, Any], timeout: float) -> None:
    try:
        await asyncio.wait_for(websocket.send_json(payload), timeout=timeout)
    except asyncio.TimeoutError as e:
        try:
            await websocket.close(code=1011, reason="gateway send timeout")
        except Exception:
            pass
        raise StalledWebSocketClient from e


async def sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)
