"""Characterize current readiness, status, and persistence state semantics."""

from __future__ import annotations

from typing import Any, Literal

from forge_msgs import PolicyCommandStatus

from forge_gateway import config
from forge_gateway.adapters.dora_adapter import drain_commands
from forge_gateway.services.runtime_service import GatewayRuntime


class _Node:
    def send_output(self, output_id: str, value: Any) -> None:
        del output_id, value


class _LockProbeStore:
    def __init__(self, runtime: GatewayRuntime) -> None:
        self.runtime = runtime
        self.calls: list[str] = []

    def append_event(
        self,
        event_type: str,
        data: dict[str, Any],
        *,
        now: float,
    ) -> None:
        del data, now
        assert self.runtime.lock.locked()
        assert not self.runtime.lock.acquire(blocking=False)
        self.calls.append(f"event:{event_type}")

    def write_snapshot(self, payload: dict[str, Any]) -> None:
        del payload
        assert self.runtime.lock.locked()
        self.calls.append("snapshot")


def _runtime() -> GatewayRuntime:
    return GatewayRuntime(config.GatewayConfig.from_dict({"joint_order": ["j1"]}))


def _create_and_send(runtime: GatewayRuntime, suffix: str) -> None:
    status, _ = runtime.create_agent_session(
        {
            "session_id": f"session-{suffix}",
            "command_id": f"command-{suffix}",
            "action_type": "grasp",
            "target_name": "apple",
        }
    )
    assert status == 202
    drain_commands(runtime, _Node())


def _status(
    command_id: str,
    status: Literal["accepted", "done"],
    *,
    policy_id: str = "sam3",
    command: str = "grasp_simple",
) -> object:
    return PolicyCommandStatus.from_outputs(
        policy_id=policy_id,
        command=command,
        request_id=command_id,
        status=status,
        outputs={"command_id": command_id},
    ).to_arrow()


def test_current_legacy_proprioception_never_becomes_stale() -> None:
    runtime = _runtime()
    try:
        with runtime.lock:
            runtime.latest_proprio_time = 1.0
            readiness = runtime._readiness_locked(1_000_000.0)

        assert readiness["ready"] is True
        assert readiness["proprio_state_ready"] is True
        assert "proprio_state" not in readiness["missing"]
    finally:
        runtime.close()


def test_current_legacy_future_image_timestamp_counts_as_ready() -> None:
    cfg = config.GatewayConfig.from_dict(
        {
            "joint_order": ["j1"],
            "image_input_ids": ["image/front"],
            "readiness": {"require_proprio_state": False, "require_images": True},
        }
    )
    runtime = GatewayRuntime(cfg)
    try:
        with runtime.lock:
            runtime.images["image/front"] = {"timestamp": 1_000.0}
            readiness = runtime._readiness_locked(100.0)

        assert readiness["ready"] is True
        assert readiness["images"] == {"image/front": True}
    finally:
        runtime.close()


def test_status_requires_matching_identity_and_terminal_state_is_absorbing() -> None:
    runtime = _runtime()
    try:
        _create_and_send(runtime, "regress")
        runtime.apply_policy_command_status(
            _status(
                "command-regress",
                "done",
                policy_id="wrong-policy",
                command="wrong_command",
            )
        )
        with runtime.lock:
            assert runtime.commands["command-regress"].status == "sent"
            assert runtime.sessions["session-regress"].status == "running"

        runtime.apply_policy_command_status(_status("command-regress", "done"))
        runtime.apply_policy_command_status(_status("command-regress", "accepted"))

        with runtime.lock:
            assert runtime.commands["command-regress"].status == "succeeded"
            assert runtime.sessions["session-regress"].status == "succeeded"
            assert runtime.active_session_id is None
    finally:
        runtime.close()


def test_empty_request_id_does_not_fall_back_to_outputs_command_id() -> None:
    runtime = _runtime()
    try:
        _create_and_send(runtime, "fallback")
        status = PolicyCommandStatus.from_outputs(
            policy_id="sam3",
            command="grasp_simple",
            request_id="",
            status="done",
            outputs={"command_id": "command-fallback"},
        )

        runtime.apply_policy_command_status(status.to_arrow())

        with runtime.lock:
            assert runtime.commands["command-fallback"].status == "sent"
            assert runtime.sessions["session-fallback"].status == "running"
    finally:
        runtime.close()


def test_status_before_dispatch_is_ignored_then_command_is_marked_sent() -> None:
    runtime = _runtime()
    try:
        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-before",
                "command_id": "command-before",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )
        assert status == 202
        runtime.apply_policy_command_status(_status("command-before", "done"))
        drain_commands(runtime, _Node())

        with runtime.lock:
            assert runtime.commands["command-before"].status == "sent"
            assert runtime.sessions["session-before"].status == "running"
            assert runtime.active_session_id == "session-before"
    finally:
        runtime.close()


def test_current_legacy_persistence_callbacks_run_under_global_lock() -> None:
    runtime = _runtime()
    probe = _LockProbeStore(runtime)
    runtime.state_store = probe  # type: ignore[assignment]
    try:
        _create_and_send(runtime, "persist")
        runtime.apply_policy_command_status(_status("command-persist", "done"))

        assert probe.calls == [
            "event:session_created",
            "snapshot",
            "event:command_sent",
            "snapshot",
            "event:policy_command_status",
            "snapshot",
        ]
    finally:
        runtime.close()
        assert probe.calls[-1] == "snapshot"
