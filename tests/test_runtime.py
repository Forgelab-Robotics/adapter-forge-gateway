import json
import sys
import threading
import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("uvicorn")

from forge_gateway import cli, config
from forge_gateway.adapters.dora_adapter import DoraEventBuffer, drain_commands, handle_dora_input
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


def test_agent_session_dispatches_policy_command_with_request_id(tmp_path: Path) -> None:
    forge_msgs = pytest.importorskip("forge_msgs")
    cfg = config.GatewayConfig.from_dict(
        {
            "joint_order": ["j1"],
            "agent": {"state_dir": str(tmp_path)},
        }
    )
    runtime = GatewayRuntime(cfg)
    try:
        status_code, response = runtime.create_agent_session(
            {
                "session_id": "session-1",
                "command_id": "command-1",
                "action_type": "grasp",
                "target": "apple",
                "instruction": "pick the apple",
            }
        )

        assert status_code == 202
        assert response["data"]["session"]["status"] == "queued"
        node = FakeNode()
        drain_commands(runtime, node)

        assert len(node.outputs) == 1
        output_id, value = node.outputs[0]
        assert output_id == "policy_command"
        msg = forge_msgs.PolicyCommand.from_arrow(value)
        assert msg.policy_id == "sam3"
        assert msg.command == "grasp_simple"
        assert msg.request_id == "command-1"
        inputs = msg.inputs()
        assert inputs["session_id"] == "session-1"
        assert inputs["command_id"] == "command-1"
        assert inputs["target_name"] == "apple"
        with runtime.lock:
            assert runtime.commands["command-1"].status == "sent"
            assert runtime.sessions["session-1"].status == "running"
        assert (tmp_path / "gateway_events.jsonl").is_file()
        assert (tmp_path / "runtime_context.json").is_file()
    finally:
        runtime.close()


def test_policy_command_status_updates_agent_session(tmp_path: Path) -> None:
    forge_msgs = pytest.importorskip("forge_msgs")
    cfg = config.GatewayConfig.from_dict(
        {
            "joint_order": ["j1"],
            "agent": {"state_dir": str(tmp_path)},
        }
    )
    runtime = GatewayRuntime(cfg)
    try:
        status_code, _ = runtime.create_agent_session(
            {
                "session_id": "session-2",
                "command_id": "command-2",
                "action_type": "grasp",
                "target_name": "banana",
            }
        )
        assert status_code == 202
        node = FakeNode()
        drain_commands(runtime, node)

        status = forge_msgs.PolicyCommandStatus.from_outputs(
            policy_id="sam3",
            command="grasp_simple",
            request_id="command-2",
            status="done",
            outputs={"accepted": True, "command_id": "command-2"},
        )
        handle_dora_input(runtime, "policy_command_status", status.to_arrow())

        session_status, session_response = runtime.get_agent_session("session-2")
        assert session_status == 200
        session = session_response["data"]["session"]
        command = session_response["data"]["commands"][0]
        assert session["status"] == "succeeded"
        assert command["status"] == "succeeded"
        assert command["outputs"]["accepted"] is True
        context = json.loads((tmp_path / "runtime_context.json").read_text(encoding="utf-8"))
        assert context["active_session_id"] is None
        assert context["last_result"]["request_id"] == "command-2"
    finally:
        runtime.close()


def test_agent_session_rejects_unknown_action() -> None:
    cfg = config.GatewayConfig.from_dict({"joint_order": ["j1"]})
    runtime = GatewayRuntime(cfg)
    try:
        status_code, response = runtime.create_agent_session(
            {
                "session_id": "session-unknown",
                "command_id": "command-unknown",
                "action_type": "reset",
            }
        )

        assert status_code == 400
        assert response["ok"] is False
        assert "unknown action_type" in response["msg"]
        assert "grasp" in response["data"]["supported_actions"]
        assert runtime.sessions == {}
        assert runtime.commands == {}
        assert runtime.command_queue.empty()
    finally:
        runtime.close()


def test_agent_runtime_reset_sends_reset_scene_command() -> None:
    forge_msgs = pytest.importorskip("forge_msgs")
    cfg = config.GatewayConfig.from_dict({"joint_order": ["j1"]})
    runtime = GatewayRuntime(cfg)
    try:
        status_code, response = runtime.agent_runtime_reset({"inputs": {"reason": "paos-agent"}})

        assert status_code == 202
        assert response["data"]["command"] == "reset_scene"
        node = FakeNode()
        drain_commands(runtime, node)

        assert len(node.outputs) == 1
        reset_msg = forge_msgs.PolicyCommand.from_arrow(node.outputs[0][1])
        assert reset_msg.command == "reset_scene"
        assert reset_msg.inputs()["reason"] == "paos-agent"
    finally:
        runtime.close()


def test_agent_runtime_context_exposes_capabilities() -> None:
    cfg = config.GatewayConfig.from_dict({"joint_order": ["j1"]})
    runtime = GatewayRuntime(cfg)
    try:
        context = runtime.agent_runtime_context()

        assert context["capabilities"]["api_version"] == "paos-forge-gateway-mvp-plus.v1"
        assert context["capabilities"]["supports"]["sessions"] is True
        assert context["capabilities"]["supports"]["serial_actions_only"] is True
        assert "grasp" in context["capabilities"]["actions"]
        assert context["capabilities"]["actions"]["grasp"]["policy_id"] == "sam3"
        assert "sam3" in context["capabilities"]["policies"]
        assert context["readiness"]["ready"] is False
    finally:
        runtime.close()


def test_cancel_agent_session_sends_policy_stop() -> None:
    forge_msgs = pytest.importorskip("forge_msgs")
    cfg = config.GatewayConfig.from_dict({"joint_order": ["j1"]})
    runtime = GatewayRuntime(cfg)
    try:
        status_code, _ = runtime.create_agent_session(
            {
                "session_id": "session-cancel",
                "command_id": "command-cancel",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )
        assert status_code == 202
        node = FakeNode()
        drain_commands(runtime, node)

        cancel_status, cancel_response = runtime.cancel_agent_session("session-cancel")
        assert cancel_status == 200
        assert cancel_response["data"]["status"] == "cancelled"
        drain_commands(runtime, node)

        assert len(node.outputs) == 2
        stop_msg = forge_msgs.PolicyCommand.from_arrow(node.outputs[1][1])
        assert stop_msg.policy_id == "sam3"
        assert stop_msg.command == "stop"
        assert stop_msg.request_id == "cancel_command-cancel"
        inputs = stop_msg.inputs()
        assert inputs["cancelled_command_id"] == "command-cancel"
        with runtime.lock:
            assert runtime.sessions["session-cancel"].status == "cancelled"
            assert runtime.commands["command-cancel"].status == "cancelled"
    finally:
        runtime.close()


def test_main_print_capabilities_without_config(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("GATEWAY_CONFIG", raising=False)
    monkeypatch.setattr(sys, "argv", ["gateway", "--print-capabilities"])

    assert cli.main() == 0

    capabilities = json.loads(capsys.readouterr().out)
    assert capabilities["policy_id"] == "default"
    assert "grasp" in capabilities["actions"]


def test_main_print_capabilities_from_example_config(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = Path(__file__).parents[1] / "config.example.yaml"
    monkeypatch.setattr(
        sys,
        "argv",
        ["gateway", "--config", str(config_path), "--print-capabilities"],
    )

    assert cli.main() == 0

    capabilities = json.loads(capsys.readouterr().out)
    assert capabilities["supports"]["cancel"] is True
    assert capabilities["supports"]["reset"] is True
    assert "grasp" in capabilities["actions"]
