"""Characterize readiness and lifecycle edge cases before fail-closed fixes."""

from __future__ import annotations

import threading
import time

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

    def runtime_status_snapshot(self) -> dict[str, object]:
        state = self.state_snapshot()
        return {
            "readiness": {"ready": False, "missing": ["expired"]},
            "state": state,
        }

    def enqueue_policy_command(
        self,
        command: str,
        inputs: dict[str, object],
    ) -> None:
        assert self.ready is False
        self.commands.append((command, inputs))

    def enqueue_policy_command_if_ready(
        self,
        command: str,
        inputs: dict[str, object],
    ) -> tuple[bool, dict[str, object]]:
        readiness: dict[str, object] = {
            "ready": self.ready,
            "missing": [] if self.ready else ["expired"],
        }
        if not self.ready:
            return False, readiness
        self.commands.append((command, inputs))
        return True, readiness

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


def test_config_rejects_string_booleans_and_unknown_readiness_keys() -> None:
    with pytest.raises(ValueError, match="readiness.require_images"):
        config.GatewayConfig.from_dict(
            {
                "joint_order": ["j1"],
                "readiness": {"require_images": "false"},
            }
        )
    with pytest.raises(ValueError, match="require_image_clients"):
        config.GatewayConfig.from_dict(
            {
                "joint_order": ["j1"],
                "readiness": {"require_image_clients": True},
            }
        )
    with pytest.raises(ValueError, match="agent.enabled"):
        config.GatewayConfig.from_dict(
            {
                "joint_order": ["j1"],
                "agent": {"enabled": "false"},
            }
        )


def test_config_rejects_non_mapping_readiness_and_invalid_identifiers() -> None:
    with pytest.raises(ValueError, match="readiness"):
        config.GatewayConfig.from_dict(
            {"joint_order": ["j1"], "readiness": []}  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="joint_order"):
        config.GatewayConfig.from_dict({"joint_order": ["j1", "j1", ""]})
    with pytest.raises(ValueError, match="image_input_ids"):
        config.GatewayConfig.from_dict(
            {
                "joint_order": ["j1"],
                "image_input_ids": ["image/front", "image/front", ""],
            }
        )
    with pytest.raises(ValueError, match="port"):
        config.GatewayConfig.from_dict({"joint_order": ["j1"], "port": 70_000})


def test_required_images_are_not_ready_without_configured_inputs() -> None:
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

        assert readiness["ready"] is False
        assert readiness["required_images_ready"] is False
        assert readiness["images"] == {}
        assert readiness["missing"] == ["image_input_ids"]
    finally:
        runtime.close()


def test_unmatched_proprioception_does_not_advance_readiness() -> None:
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

        with pytest.raises(
            ValueError,
            match="does not contain any configured joints",
        ):
            handle_dora_input(runtime, "proprio_state", message.to_arrow())

        with runtime.lock:
            assert runtime.proprio_state == {}
            assert runtime.latest_proprio_time is None
            assert runtime.nodes["proprio_state"].health == "error"
        assert runtime.readiness()["ready"] is False
    finally:
        runtime.close()


def test_proprioception_without_values_does_not_advance_readiness() -> None:
    runtime = GatewayRuntime(
        config.GatewayConfig.from_dict(
            {
                "joint_order": ["j1"],
                "readiness": {"require_images": False},
            }
        )
    )
    try:
        message = JointState(name=["j1"])

        with pytest.raises(ValueError, match="has no position, velocity, or effort"):
            handle_dora_input(runtime, "proprio_state", message.to_arrow())

        with runtime.lock:
            assert runtime.proprio_state == {}
            assert runtime.latest_proprio_time is None
            assert runtime.nodes["proprio_state"].health == "error"
        assert runtime.readiness()["ready"] is False
    finally:
        runtime.close()


def test_partial_proprioception_remains_supported() -> None:
    runtime = GatewayRuntime(
        config.GatewayConfig.from_dict(
            {
                "joint_order": ["j1", "j2"],
                "readiness": {"require_images": False},
            }
        )
    )
    try:
        message = JointState(name=["j2"], position=[2.0])

        handle_dora_input(runtime, "proprio_state", message.to_arrow())

        with runtime.lock:
            assert runtime.proprio_state == {"j2": 2.0}
            assert runtime.nodes["proprio_state"].health == "ready"
        assert runtime.readiness()["ready"] is True
    finally:
        runtime.close()


def test_invalid_proprioception_preserves_last_valid_sample() -> None:
    runtime = GatewayRuntime(
        config.GatewayConfig.from_dict(
            {
                "joint_order": ["j1"],
                "readiness": {"require_images": False},
            }
        )
    )
    try:
        handle_dora_input(
            runtime,
            "proprio_state",
            JointState(name=["j1"], position=[1.0]).to_arrow(),
        )
        with runtime.lock:
            valid_state = runtime.proprio_state
            valid_timestamp = runtime.latest_proprio_time

        with pytest.raises(ValueError, match="does not contain any configured joints"):
            handle_dora_input(
                runtime,
                "proprio_state",
                JointState(name=["other"], position=[2.0]).to_arrow(),
            )

        with runtime.lock:
            assert runtime.proprio_state is valid_state
            assert runtime.latest_proprio_time == valid_timestamp
            assert runtime.nodes["proprio_state"].health == "error"
        assert runtime.readiness()["ready"] is True
    finally:
        runtime.close()


def test_fresh_timestamp_without_proprio_values_is_not_ready() -> None:
    runtime = GatewayRuntime(
        config.GatewayConfig.from_dict(
            {
                "joint_order": ["j1"],
                "readiness": {"require_images": False},
            }
        )
    )
    try:
        with runtime.lock:
            runtime.latest_proprio_time = time.time()

        readiness = runtime.readiness()

        assert readiness["ready"] is False
        assert readiness["proprio_state_ready"] is False
        assert readiness["missing"] == ["proprio_state"]
    finally:
        runtime.close()


def test_runtime_start_rechecks_readiness_before_enqueue() -> None:
    runtime = _RacingRuntime()

    response = _client(runtime).post("/runtime/start", json={})

    assert response.status_code == 409
    assert response.json()["data"] == {"ready": False, "missing": ["expired"]}
    assert runtime.ready is False
    assert runtime.commands == []


def test_runtime_status_uses_one_readiness_observation() -> None:
    runtime = _RacingRuntime()

    response = _client(runtime).get("/runtime/status")

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["readiness"] == {"ready": False, "missing": ["expired"]}
    assert payload["readiness"] == payload["state"]["runtime"]["readiness"]


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
