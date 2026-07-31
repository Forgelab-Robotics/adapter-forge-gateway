"""Characterize current HTTP parsing before fail-closed request handling."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from forge_gateway.controllers.runtime_controller import register_runtime_routes


class _Runtime:
    def __init__(self, *, ready: bool = True) -> None:
        self.ready = ready
        self.commands: list[tuple[str, dict[str, Any]]] = []

    def readiness(self) -> dict[str, Any]:
        return {"ready": self.ready, "missing": [] if self.ready else ["proprio_state"]}

    def state_snapshot(self) -> dict[str, Any]:
        return {}

    def enqueue_policy_command(
        self,
        command: str,
        inputs: dict[str, Any],
    ) -> None:
        self.commands.append((command, inputs))

    def agent_runtime_reset(
        self,
        body: dict[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        inputs = (body or {}).get("inputs") or {}
        self.commands.append(("reset_scene", inputs))
        return 202, {"ok": True, "data": {"command": "reset_scene", "inputs": inputs}}


def _client(runtime: _Runtime) -> TestClient:
    app = FastAPI()
    register_runtime_routes(app, runtime)
    return TestClient(app)


def test_current_legacy_malformed_runtime_start_enqueues_default_start() -> None:
    runtime = _Runtime()

    response = _client(runtime).post(
        "/runtime/start",
        content="{",
        headers={"content-type": "application/json"},
    )

    assert response.status_code == 200
    assert runtime.commands == [("start", {})]


def test_current_legacy_non_object_runtime_start_enqueues_default_start() -> None:
    runtime = _Runtime()

    for body in (None, [], "", 0, False):
        response = _client(runtime).post("/runtime/start", json=body)
        assert response.status_code == 200

    assert runtime.commands == [("start", {})] * 5


def test_current_readiness_gate_runs_before_request_validation() -> None:
    runtime = _Runtime(ready=False)

    response = _client(runtime).post(
        "/runtime/start",
        json={"command": [], "inputs": [1]},
    )

    assert response.status_code == 409
    assert response.json()["msg"] == "runtime is not ready"
    assert runtime.commands == []


def test_current_legacy_malformed_reset_body_triggers_reset_scene() -> None:
    runtime = _Runtime()
    client = _client(runtime)

    response = client.post(
        "/runtime/reset_scene",
        content="{",
        headers={"content-type": "application/json"},
    )

    assert response.status_code == 200
    assert runtime.commands == [("reset_scene", {})]
