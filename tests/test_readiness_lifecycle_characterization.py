"""Characterize readiness and lifecycle edge cases before fail-closed fixes."""

from __future__ import annotations

import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient
from forge_msgs import JointState
import pytest

from forge_gateway import config
from forge_gateway.adapters.dora_adapter import handle_dora_input
from forge_gateway.controllers.runtime_controller import register_runtime_routes
from forge_gateway.services import image_service
from forge_gateway.services.runtime_service import GatewayRuntime


class _RacingRuntime:
    def __init__(self) -> None:
        self.ready = True
        self.commands: list[tuple[str, dict[str, object]]] = []

    def readiness(self) -> dict[str, object]:
        snapshot: dict[str, object] = {"ready": self.ready, "missing": []}
        self.ready = False
        return snapshot

    def state_snapshot(self) -> dict[str, object]:
        return {"runtime": {"readiness": {"ready": False, "missing": ["expired"]}}}

    def enqueue_policy_command(
        self,
        command: str,
        inputs: dict[str, object],
    ) -> None:
        assert self.ready is False
        self.commands.append((command, inputs))

    def agent_runtime_reset(
        self,
        body: dict[str, object] | None = None,
    ) -> tuple[int, dict[str, object]]:
        del body
        return 202, {"ok": True}


def _client(runtime: object) -> TestClient:
    app = FastAPI()
    register_runtime_routes(app, runtime)
    return TestClient(app)


def test_current_config_coerces_strings_and_ignores_unknown_readiness_keys() -> None:
    parsed = config.GatewayConfig.from_dict(
        {
            "joint_order": ["j1"],
            "readiness": {
                "require_images": "false",
                "require_image_clients": True,
            },
            "agent": {
                "enabled": "false",
                "write_context_snapshot": "false",
            },
        }
    )

    assert parsed.readiness.require_images is True
    assert parsed.readiness.require_image_client is False
    assert parsed.agent.enabled is True
    assert parsed.agent.write_context_snapshot is True


def test_current_config_accepts_non_mapping_readiness_and_invalid_identifiers() -> None:
    defaulted = config.GatewayConfig.from_dict(
        {"joint_order": ["j1"], "readiness": []}  # type: ignore[arg-type]
    )
    parsed = config.GatewayConfig.from_dict(
        {
            "joint_order": ["j1", "j1", ""],
            "image_input_ids": ["image/front", "image/front", ""],
            "port": 70_000,
        }
    )

    assert defaulted.readiness == config.ReadinessConfig()
    assert parsed.joint_order == ["j1", "j1", ""]
    assert parsed.image_input_ids == ["image/front", "image/front", ""]
    assert parsed.port == 70_000


def test_current_empty_required_image_set_is_vacuously_ready() -> None:
    runtime = GatewayRuntime(
        config.GatewayConfig.from_dict(
            {
                "joint_order": ["j1"],
                "image_input_ids": [],
                "readiness": {
                    "require_proprio_state": False,
                    "require_images": True,
                },
            }
        )
    )
    try:
        readiness = runtime.readiness()

        assert readiness["ready"] is True
        assert readiness["required_images_ready"] is True
        assert readiness["images"] == {}
    finally:
        runtime.close()


def test_current_unmatched_proprioception_advances_readiness() -> None:
    runtime = GatewayRuntime(
        config.GatewayConfig.from_dict(
            {
                "joint_order": ["j1", "j2"],
                "readiness": {"require_images": False},
            }
        )
    )
    try:
        message = JointState(name=["other"], position=[1.0])

        handle_dora_input(runtime, "proprio_state", message.to_arrow())

        with runtime.lock:
            assert runtime.proprio_state == {}
            assert runtime.latest_proprio_time is not None
        assert runtime.readiness()["ready"] is True
    finally:
        runtime.close()


def test_current_runtime_start_can_enqueue_after_readiness_expires() -> None:
    runtime = _RacingRuntime()

    response = _client(runtime).post("/runtime/start", json={})

    assert response.status_code == 200
    assert runtime.ready is False
    assert runtime.commands == [("start", {})]


def test_current_runtime_status_can_contain_two_readiness_observations() -> None:
    runtime = _RacingRuntime()

    response = _client(runtime).get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["readiness"]["ready"] is True
    assert payload["state"]["runtime"]["readiness"]["ready"] is False


def test_current_runtime_accepts_commands_after_close() -> None:
    runtime = GatewayRuntime(
        config.GatewayConfig.from_dict(
            {
                "joint_order": ["j1"],
                "readiness": {
                    "require_proprio_state": False,
                    "require_images": False,
                },
            }
        )
    )

    runtime.close()
    runtime.enqueue_policy_command("after_close")

    assert runtime.readiness()["ready"] is True
    assert runtime.command_queue.qsize() == 1


def test_current_inflight_image_can_publish_after_runtime_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encode_started = threading.Event()
    release_encode = threading.Event()

    def blocking_payload(
        input_id: str,
        value: object,
        quality: int,
    ) -> dict[str, object]:
        del quality
        encode_started.set()
        assert release_encode.wait(timeout=2.0)
        return {
            "type": "image",
            "id": input_id,
            "format": "jpeg",
            "content_type": "image/jpeg",
            "data": str(value),
        }

    monkeypatch.setattr(image_service, "_image_payload", blocking_payload)
    runtime = GatewayRuntime(
        config.GatewayConfig.from_dict(
            {"joint_order": ["j1"], "image_input_ids": ["image/front"]}
        )
    )
    worker_thread = runtime.image_encoder._thread
    original_join = worker_thread.join
    runtime.image_encoder.submit("image/front", "late", 1.0)
    assert encode_started.wait(timeout=1.0)
    monkeypatch.setattr(worker_thread, "join", lambda timeout=None: None)

    runtime.close()
    release_encode.set()
    original_join(timeout=1.0)

    assert not worker_thread.is_alive()
    with runtime.lock:
        assert runtime.images["image/front"]["data"] == "late"
