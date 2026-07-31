"""Characterize current Gateway queue and dispatch failure semantics."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from forge_msgs import PolicyCommand
import pytest

from forge_gateway import config
from forge_gateway.adapters.dora_adapter import drain_commands
from forge_gateway.services.runtime_service import GatewayRuntime


class _CapturingNode:
    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self.fail_on_call = fail_on_call
        self.attempts: list[tuple[str, Any]] = []
        self.outputs: list[tuple[str, Any]] = []

    def send_output(self, output_id: str, value: Any) -> None:
        self.attempts.append((output_id, value))
        if len(self.attempts) == self.fail_on_call:
            raise RuntimeError("send failed")
        self.outputs.append((output_id, value))


def _runtime() -> GatewayRuntime:
    return GatewayRuntime(config.GatewayConfig.from_dict({"joint_order": ["j1"]}))


def _commands(node: _CapturingNode) -> list[PolicyCommand]:
    return [PolicyCommand.from_arrow(value) for _, value in node.outputs]


def test_current_legacy_cancel_before_drain_sends_start_then_stop_and_resurrects() -> None:
    runtime = _runtime()
    try:
        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-1",
                "command_id": "command-1",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )
        assert status == 202
        cancel_status, _ = runtime.cancel_agent_session("session-1")
        assert cancel_status == 200

        node = _CapturingNode()
        drain_commands(runtime, node)

        assert [command.command for command in _commands(node)] == ["grasp_simple", "stop"]
        with runtime.lock:
            assert runtime.commands["command-1"].status == "sent"
            assert runtime.sessions["session-1"].status == "running"
            assert runtime.active_session_id is None
    finally:
        runtime.close()


def test_current_legacy_send_failure_loses_dequeued_command_and_aborts_drain() -> None:
    runtime = _runtime()
    try:
        for command in ("first", "second", "third"):
            runtime.enqueue_policy_command(command, {})
        node = _CapturingNode(fail_on_call=2)

        with pytest.raises(RuntimeError, match="send failed"):
            drain_commands(runtime, node)

        assert [command.command for command in _commands(node)] == ["first"]
        assert runtime.command_queue.qsize() == 1

        node.fail_on_call = None
        drain_commands(runtime, node)
        assert [command.command for command in _commands(node)] == ["first", "third"]
    finally:
        runtime.close()


def test_current_legacy_agent_send_failure_leaves_orphaned_queued_state() -> None:
    runtime = _runtime()
    try:
        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-fail",
                "command_id": "command-fail",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )
        assert status == 202

        with pytest.raises(RuntimeError, match="send failed"):
            drain_commands(runtime, _CapturingNode(fail_on_call=1))

        assert runtime.command_queue.empty()
        with runtime.lock:
            assert runtime.commands["command-fail"].status == "queued"
            assert runtime.sessions["session-fail"].status == "queued"
            assert runtime.last_error is None
    finally:
        runtime.close()


def test_record_root_is_resolved_when_fifo_command_is_drained() -> None:
    runtime = _runtime()
    try:
        runtime.enqueue_policy_command("start_recording", {})
        runtime.set_record_root("/records")
        runtime.enqueue_policy_command("start_recording", {})
        node = _CapturingNode()

        drain_commands(runtime, node)

        commands = _commands(node)
        assert commands[0].inputs()["output_path"] == "recording.mcap"
        assert commands[1].inputs()["output_path"] == str(Path("/records/recording.mcap"))
    finally:
        runtime.close()


def test_current_legacy_command_queue_is_unbounded() -> None:
    runtime = _runtime()
    try:
        assert runtime.command_queue.maxsize == 0
        for index in range(1_024):
            runtime.enqueue_policy_command(f"command_{index}", {})
        assert runtime.command_queue.qsize() == 1_024
    finally:
        runtime.close()
