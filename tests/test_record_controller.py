from __future__ import annotations

import asyncio
import json
import threading
from typing import Any

from fastapi import FastAPI

from forge_gateway.controllers.record_controller import register_record_routes


class FakeRuntime:
    def __init__(self) -> None:
        self.commands: list[tuple[str, dict[str, Any]]] = []
        self.record_status: dict[str, Any] = {"record_state": "IDLE"}
        self.lock = threading.Lock()
        self.root: str | None = None

    def enqueue_policy_command(
        self,
        command: str,
        inputs: dict[str, Any],
    ) -> None:
        self.commands.append((command, inputs))

    def set_record_root(self, root: str) -> None:
        self.root = root


class FakeRequest:
    def __init__(self, body: dict[str, Any]) -> None:
        self.body = body

    async def json(self) -> dict[str, Any]:
        return self.body


def record_endpoint(runtime: FakeRuntime):
    app = FastAPI()
    register_record_routes(app, runtime)
    return next(
        route.endpoint
        for route in app.routes
        if getattr(route, "path", None) == "/record/control"
    )


def response_json(response: Any) -> dict[str, Any]:
    return json.loads(bytes(response.body))


def test_record_control_maps_start_metadata_stop_and_discard() -> None:
    runtime = FakeRuntime()
    endpoint = record_endpoint(runtime)

    start = asyncio.run(
        endpoint(
            FakeRequest(
                {
                    "action": "START",
                    "output_path": "/tmp/episode.mcap",
                    "metadata": {"workflow": {"episode_id": "episode-1"}},
                }
            )
        )
    )
    stop = asyncio.run(endpoint(FakeRequest({"action": "STOP"})))
    discard = asyncio.run(endpoint(FakeRequest({"action": "DISCARD"})))

    assert start.status_code == 200
    assert stop.status_code == 200
    assert discard.status_code == 200
    assert runtime.commands == [
        (
            "start_recording",
            {
                "output_path": "/tmp/episode.mcap",
                "metadata": {"workflow": {"episode_id": "episode-1"}},
            },
        ),
        ("stop_recording", {}),
        ("discard_recording", {}),
    ]


def test_record_control_rejects_invalid_action_and_metadata() -> None:
    runtime = FakeRuntime()
    endpoint = record_endpoint(runtime)

    invalid_action = asyncio.run(endpoint(FakeRequest({"action": "PAUSE"})))
    invalid_metadata = asyncio.run(
        endpoint(FakeRequest({"action": "START", "metadata": []}))
    )

    assert invalid_action.status_code == 400
    assert "invalid record action" in response_json(invalid_action)["msg"]
    assert invalid_metadata.status_code == 400
    assert response_json(invalid_metadata)["msg"] == "metadata must be an object"
    assert runtime.commands == []
