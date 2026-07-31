"""Characterize current HTTP parsing before fail-closed request handling."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from forge_gateway.controllers.runtime_controller import register_runtime_routes
from forge_gateway.domain.commands import CommandMailboxUnavailable


class _Runtime:
    def __init__(self, *, ready: bool = True, unavailable: str | None = None) -> None:
        self.ready = ready
        self.unavailable = unavailable
        self.commands: list[tuple[str, dict[str, Any]]] = []

    def readiness(self) -> dict[str, Any]:
        return {"ready": self.ready, "missing": [] if self.ready else ["proprio_state"]}

    def state_snapshot(self) -> dict[str, Any]:
        readiness = self.readiness()
        return {"runtime": {"readiness": readiness}}

    def runtime_status_snapshot(self) -> dict[str, Any]:
        state = self.state_snapshot()
        return {
            "readiness": dict(state["runtime"]["readiness"]),
            "state": state,
        }

    def enqueue_policy_command(
        self,
        command: str,
        inputs: dict[str, Any],
    ) -> None:
        if self.unavailable is not None:
            raise CommandMailboxUnavailable(self.unavailable)
        self.commands.append((command, inputs))

    def enqueue_policy_command_if_ready(
        self,
        command: str,
        inputs: dict[str, Any],
    ) -> tuple[bool, dict[str, Any]]:
        readiness = self.readiness()
        if not readiness["ready"]:
            return False, readiness
        self.enqueue_policy_command(command, inputs)
        return True, readiness

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


def test_malformed_runtime_start_is_rejected_without_enqueuing() -> None:
    runtime = _Runtime()

    response = _client(runtime).post(
        "/runtime/start",
        content="{",
        headers={"content-type": "application/json"},
    )

    assert response.status_code == 400
    assert response.json() == {
        "ok": False,
        "msg": "request body must be a JSON object",
    }
    assert runtime.commands == []


def test_non_object_runtime_start_is_rejected_without_enqueuing() -> None:
    runtime = _Runtime()

    for body in (None, [], "", 0, False):
        response = _client(runtime).post("/runtime/start", json=body)
        assert response.status_code == 400

    assert runtime.commands == []


def test_valid_empty_runtime_start_keeps_default_start_contract() -> None:
    runtime = _Runtime()

    response = _client(runtime).post("/runtime/start", json={})

    assert response.status_code == 200
    assert runtime.commands == [("start", {})]


def test_runtime_start_reports_mailbox_backpressure_as_503() -> None:
    runtime = _Runtime(unavailable="command mailbox is full")

    response = _client(runtime).post("/runtime/start", json={})

    assert response.status_code == 503
    assert response.json() == {"ok": False, "msg": "command mailbox is full"}
    assert runtime.commands == []


def test_current_readiness_gate_runs_before_request_validation() -> None:
    runtime = _Runtime(ready=False)

    response = _client(runtime).post(
        "/runtime/start",
        json={"command": [], "inputs": [1]},
    )

    assert response.status_code == 409
    assert response.json()["msg"] == "runtime is not ready"
    assert runtime.commands == []


def test_malformed_reset_body_is_rejected_without_enqueuing() -> None:
    runtime = _Runtime()
    client = _client(runtime)

    response = client.post(
        "/runtime/reset_scene",
        content="{",
        headers={"content-type": "application/json"},
    )

    assert response.status_code == 400
    assert runtime.commands == []
