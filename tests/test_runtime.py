import threading
import time

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("uvicorn")

from forge_gateway import config
from forge_gateway.adapters.dora_adapter import DoraEventBuffer, handle_dora_input
from forge_gateway.services import image_service
from forge_gateway.services.runtime_service import GatewayRuntime


class FakeNode:
    def __init__(self) -> None:
        self.outputs: list[tuple[str, object]] = []

    def send_output(self, output_id: str, value: object) -> None:
        self.outputs.append((output_id, value))


def test_readiness_reports_missing_inputs() -> None:
    cfg = config.GatewayConfig.from_dict(
        {
            "joint_order": ["j1"],
            "image_input_ids": ["image/front"],
            "readiness": {
                "require_proprio_state": True,
                "require_images": True,
                "require_state_client": True,
            },
        }
    )
    runtime = GatewayRuntime(cfg)
    try:
        readiness = runtime.readiness()

        assert readiness["ready"] is False
        assert readiness["missing"] == ["proprio_state", "image:image/front", "ws:state"]
    finally:
        runtime.close()


def test_readiness_passes_when_required_signals_arrive() -> None:
    cfg = config.GatewayConfig.from_dict(
        {
            "joint_order": ["j1"],
            "image_input_ids": ["image/front"],
            "readiness": {
                "require_proprio_state": True,
                "require_images": True,
                "image_stale_after_sec": 1.0,
            },
        }
    )
    runtime = GatewayRuntime(cfg)
    try:
        now = time.time()
        with runtime.lock:
            runtime.proprio_state = {"j1": 0.0}
            runtime.latest_proprio_time = now
            runtime.images["image/front"] = {"seq": 1, "timestamp": now}

        readiness = runtime.readiness()

        assert readiness["ready"] is True
        assert readiness["missing"] == []
    finally:
        runtime.close()


def test_runtime_status_snapshot_reuses_one_readiness_observation() -> None:
    cfg = config.GatewayConfig.from_dict(
        {
            "joint_order": ["j1"],
            "readiness": {
                "require_proprio_state": False,
                "require_images": False,
            },
        }
    )
    runtime = GatewayRuntime(cfg)
    try:
        status = runtime.runtime_status_snapshot()

        assert status["readiness"] == status["state"]["runtime"]["readiness"]
        assert status["readiness"]["ready"] is True
    finally:
        runtime.close()


def test_atomic_runtime_command_admission_rejects_unready_state() -> None:
    cfg = config.GatewayConfig.from_dict(
        {
            "joint_order": ["j1"],
            "readiness": {"require_images": False},
        }
    )
    runtime = GatewayRuntime(cfg)
    try:
        accepted, readiness = runtime.enqueue_policy_command_if_ready("start")

        assert accepted is False
        assert readiness["ready"] is False
        assert runtime.command_queue.empty()
    finally:
        runtime.close()


def test_latest_image_updates_since_returns_only_current_payload() -> None:
    cfg = config.GatewayConfig.from_dict(
        {
            "joint_order": ["j1"],
            "image_input_ids": ["image/front"],
        }
    )
    runtime = GatewayRuntime(cfg)
    try:
        cursors: dict[str, int] = {}

        with runtime.lock:
            runtime.images["image/front"] = {"id": "image/front", "seq": 1, "timestamp": time.time()}
            runtime.images["image/front"] = {"id": "image/front", "seq": 3, "timestamp": time.time()}

        updates = runtime.latest_image_updates_since(cursors)

        assert [update["seq"] for update in updates] == [3]
        assert cursors == {"image/front": 3}
        assert runtime.latest_image_updates_since(cursors) == []
    finally:
        runtime.close()


def test_dora_event_buffer_coalesces_input_events() -> None:
    buffer = DoraEventBuffer()
    buffer.put({"type": "INPUT", "id": "image/front", "value": "old"})
    buffer.put({"type": "INPUT", "id": "image/front", "value": "new"})
    buffer.put({"type": "INPUT", "id": "proprio_state", "value": "state"})

    assert buffer.get(timeout=0.0) == {"type": "INPUT", "id": "image/front", "value": "new"}
    assert buffer.get(timeout=0.0) == {"type": "INPUT", "id": "proprio_state", "value": "state"}
    assert buffer.get(timeout=0.0) is None


def test_dora_event_buffer_preserves_control_events() -> None:
    buffer = DoraEventBuffer()
    stop_event = {"type": "STOP"}
    buffer.put({"type": "INPUT", "id": "image/front", "value": "frame"})
    buffer.put(stop_event)

    assert buffer.get(timeout=0.0) == stop_event
    assert buffer.get(timeout=0.0) == {"type": "INPUT", "id": "image/front", "value": "frame"}


def test_dora_event_buffer_counts_ticks_without_payload_buildup() -> None:
    buffer = DoraEventBuffer()
    buffer.put({"type": "INPUT", "id": "tick", "value": "a"})
    buffer.put({"type": "INPUT", "id": "tick", "value": "b"})

    assert buffer.get(timeout=0.0) == {"type": "INPUT", "id": "tick", "value": None}
    assert buffer.get(timeout=0.0) == {"type": "INPUT", "id": "tick", "value": None}
    assert buffer.get(timeout=0.0) is None


def test_image_encode_worker_keeps_latest_pending_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    first_encode_started = threading.Event()
    release_first_encode = threading.Event()
    encoded_values: list[str] = []

    def fake_image_payload(input_id: str, value: object, quality: int) -> dict[str, object]:
        encoded_values.append(str(value))
        if value == "old":
            first_encode_started.set()
            assert release_first_encode.wait(timeout=2.0)
        return {
            "type": "image",
            "id": input_id,
            "format": "jpeg",
            "content_type": "image/jpeg",
            "data": str(value),
        }

    monkeypatch.setattr(image_service, "_image_payload", fake_image_payload)
    cfg = config.GatewayConfig.from_dict(
        {
            "joint_order": ["j1"],
            "image_input_ids": ["image/front"],
        }
    )
    runtime = GatewayRuntime(cfg)
    try:
        runtime.image_encoder.submit("image/front", "old", 1.0)
        assert first_encode_started.wait(timeout=2.0)
        runtime.image_encoder.submit("image/front", "middle", 2.0)
        runtime.image_encoder.submit("image/front", "new", 3.0)
        release_first_encode.set()

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            with runtime.lock:
                latest = runtime.images.get("image/front")
            if latest and latest["data"] == "new":
                break
            time.sleep(0.01)

        assert encoded_values == ["old", "new"]
        with runtime.lock:
            assert runtime.images["image/front"]["data"] == "new"
            assert runtime.images["image/front"]["timestamp"] == 3.0
    finally:
        runtime.close()


def test_image_encode_worker_drops_superseded_inflight_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_started = threading.Event()
    release_old = threading.Event()
    new_started = threading.Event()
    release_new = threading.Event()

    def fake_image_payload(input_id: str, value: object, quality: int) -> dict[str, object]:
        del quality
        if value == "old":
            old_started.set()
            assert release_old.wait(timeout=2.0)
        if value == "new":
            new_started.set()
            assert release_new.wait(timeout=2.0)
        return {
            "type": "image",
            "id": input_id,
            "format": "jpeg",
            "content_type": "image/jpeg",
            "data": str(value),
        }

    monkeypatch.setattr(image_service, "_image_payload", fake_image_payload)
    runtime = GatewayRuntime(
        config.GatewayConfig.from_dict(
            {"joint_order": ["j1"], "image_input_ids": ["image/front"]}
        )
    )
    try:
        assert runtime.image_encoder.submit("image/front", "old", 1.0) is True
        assert old_started.wait(timeout=1.0)
        assert runtime.image_encoder.submit("image/front", "new", 2.0) is True
        release_old.set()
        assert new_started.wait(timeout=1.0)

        with runtime.lock:
            assert "image/front" not in runtime.images

        release_new.set()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            with runtime.lock:
                latest = runtime.images.get("image/front")
            if latest is not None:
                break
            time.sleep(0.01)

        with runtime.lock:
            assert runtime.images["image/front"]["data"] == "new"
            assert runtime.images["image/front"]["timestamp"] == 2.0
    finally:
        release_old.set()
        release_new.set()
        runtime.close()


def test_superseded_image_failure_does_not_replace_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_started = threading.Event()
    release_old = threading.Event()

    def fake_image_payload(input_id: str, value: object, quality: int) -> dict[str, object]:
        del quality
        if value == "old":
            old_started.set()
            assert release_old.wait(timeout=2.0)
            raise RuntimeError("stale failure")
        return {
            "type": "image",
            "id": input_id,
            "format": "jpeg",
            "content_type": "image/jpeg",
            "data": str(value),
        }

    monkeypatch.setattr(image_service, "_image_payload", fake_image_payload)
    runtime = GatewayRuntime(
        config.GatewayConfig.from_dict(
            {"joint_order": ["j1"], "image_input_ids": ["image/front"]}
        )
    )
    try:
        assert runtime.image_encoder.submit("image/front", "old", 1.0) is True
        assert old_started.wait(timeout=1.0)
        assert runtime.image_encoder.submit("image/front", "new", 2.0) is True
        release_old.set()

        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            with runtime.lock:
                latest = runtime.images.get("image/front")
            if latest is not None:
                break
            time.sleep(0.01)

        with runtime.lock:
            assert runtime.images["image/front"]["data"] == "new"
            assert runtime.last_error is None
    finally:
        release_old.set()
        runtime.close()
