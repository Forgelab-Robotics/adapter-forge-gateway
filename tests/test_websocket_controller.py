from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Coroutine
from types import SimpleNamespace
from typing import cast

import pytest
from fastapi import FastAPI, WebSocketDisconnect
from fastapi.routing import APIWebSocketRoute
from fastapi.testclient import TestClient

from forge_gateway.controllers.websocket_controller import register_websocket_routes


class FakeRuntime:
    def __init__(self, *, image_updates: bool = False) -> None:
        self.lock: threading.RLock = threading.RLock()
        self.state_ws_clients: int = 0
        self.image_ws_clients: int = 0
        self.config: SimpleNamespace = SimpleNamespace(
            state_broadcast_hz=0.1,
            image_broadcast_hz=0.1,
            ws_send_timeout_sec=1.0,
        )
        self.image_updates = image_updates

    def state_snapshot(self) -> dict[str, object]:
        return {"type": "state"}

    def latest_image_updates_since(
        self,
        cursors: dict[str, int],
    ) -> list[dict[str, object]]:
        del cursors
        if not self.image_updates:
            return []
        return [{"type": "image", "id": "image/front", "seq": 1}]


class FakeWebSocket:
    def __init__(
        self,
        *,
        block_send: bool = False,
        disconnect_on_send: bool = False,
    ) -> None:
        self.block_send = block_send
        self.disconnect_on_send = disconnect_on_send
        self.accepted: threading.Event = threading.Event()
        self.send_started: threading.Event = threading.Event()
        self.send_cancelled: threading.Event = threading.Event()
        self.sent: list[dict[str, object]] = []
        self.close_calls: list[tuple[int, str]] = []

    async def accept(self) -> None:
        self.accepted.set()

    async def send_json(self, payload: dict[str, object]) -> None:
        self.sent.append(payload)
        self.send_started.set()
        if self.disconnect_on_send:
            raise WebSocketDisconnect(code=1000)
        if self.block_send:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.send_cancelled.set()
                raise

    async def close(self, *, code: int, reason: str) -> None:
        self.close_calls.append((code, reason))


def _wait_for_client_count(
    runtime: FakeRuntime,
    counter_name: str,
    expected: int,
) -> None:
    deadline = time.monotonic() + 0.5
    while time.monotonic() < deadline:
        with runtime.lock:
            if getattr(runtime, counter_name) == expected:
                return
        time.sleep(0.001)
    pytest.fail(f"{counter_name} did not become {expected}")


def _websocket_endpoint(
    app: FastAPI,
    path: str,
) -> Callable[[FakeWebSocket], Coroutine[object, object, None]]:
    route = next(
        route
        for route in app.routes
        if isinstance(route, APIWebSocketRoute) and route.path == path
    )
    return cast(
        Callable[[FakeWebSocket], Coroutine[object, object, None]],
        route.endpoint,
    )


def _start_handler(
    endpoint: Callable[[FakeWebSocket], Coroutine[object, object, None]],
    websocket: FakeWebSocket,
) -> tuple[threading.Thread, list[BaseException]]:
    errors: list[BaseException] = []

    def run_handler() -> None:
        try:
            asyncio.run(endpoint(websocket))
        except BaseException as error:  # noqa: BLE001 - surface thread failures
            errors.append(error)

    handler_thread = threading.Thread(target=run_handler, daemon=True)
    handler_thread.start()
    return handler_thread, errors


@pytest.mark.parametrize(
    ("path", "counter_name"),
    [
        ("/ws/state", "state_ws_clients"),
        ("/ws/images", "image_ws_clients"),
    ],
)
def test_connected_websocket_handler_exits_when_application_stops(
    path: str,
    counter_name: str,
) -> None:
    runtime = FakeRuntime()
    stop_event = threading.Event()
    app = FastAPI()
    register_websocket_routes(app, runtime, stop_event=stop_event)
    endpoint = _websocket_endpoint(app, path)
    websocket = FakeWebSocket()
    handler_thread, errors = _start_handler(endpoint, websocket)
    assert websocket.accepted.wait(timeout=0.5)
    _wait_for_client_count(runtime, counter_name, 1)

    stop_event.set()
    handler_thread.join(timeout=0.5)

    assert not handler_thread.is_alive()
    assert errors == []
    assert websocket.close_calls == [(1001, "application shutdown")]
    with runtime.lock:
        assert getattr(runtime, counter_name) == 0


@pytest.mark.parametrize(
    ("path", "counter_name"),
    [
        ("/ws/state", "state_ws_clients"),
        ("/ws/images", "image_ws_clients"),
    ],
)
def test_application_stop_cancels_blocked_websocket_send(
    path: str,
    counter_name: str,
) -> None:
    runtime = FakeRuntime(image_updates=True)
    stop_event = threading.Event()
    app = FastAPI()
    register_websocket_routes(app, runtime, stop_event=stop_event)
    websocket = FakeWebSocket(block_send=True)
    handler_thread, errors = _start_handler(
        _websocket_endpoint(app, path),
        websocket,
    )
    assert websocket.accepted.wait(timeout=0.5)
    assert websocket.send_started.wait(timeout=0.5)
    _wait_for_client_count(runtime, counter_name, 1)

    stop_event.set()
    handler_thread.join(timeout=0.5)

    assert not handler_thread.is_alive()
    assert websocket.send_cancelled.is_set()
    assert errors == []
    assert websocket.close_calls == [(1001, "application shutdown")]
    with runtime.lock:
        assert getattr(runtime, counter_name) == 0


@pytest.mark.parametrize(
    ("path", "counter_name"),
    [
        ("/ws/state", "state_ws_clients"),
        ("/ws/images", "image_ws_clients"),
    ],
)
def test_client_websocket_disconnect_decrements_client_count(
    path: str,
    counter_name: str,
) -> None:
    runtime = FakeRuntime(image_updates=True)
    app = FastAPI()
    register_websocket_routes(app, runtime, stop_event=threading.Event())
    websocket = FakeWebSocket(disconnect_on_send=True)
    handler_thread, errors = _start_handler(
        _websocket_endpoint(app, path),
        websocket,
    )

    handler_thread.join(timeout=0.5)

    assert not handler_thread.is_alive()
    assert websocket.accepted.is_set()
    assert websocket.send_started.is_set()
    assert errors == []
    assert websocket.close_calls == []
    with runtime.lock:
        assert getattr(runtime, counter_name) == 0


def test_testclient_observes_clean_websocket_close_on_application_stop() -> None:
    runtime = FakeRuntime()
    stop_event = threading.Event()
    app = FastAPI()
    register_websocket_routes(app, runtime, stop_event=stop_event)

    with (
        TestClient(app) as client,
        client.websocket_connect("/ws/state") as websocket,
    ):
        assert websocket.receive_json() == {"type": "state"}
        _wait_for_client_count(runtime, "state_ws_clients", 1)

        stop_event.set()

        with pytest.raises(WebSocketDisconnect) as disconnected:
            websocket.receive_json()
        assert disconnected.value.code == 1001

    _wait_for_client_count(runtime, "state_ws_clients", 0)
