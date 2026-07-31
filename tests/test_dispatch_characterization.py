"""Characterize current Gateway queue and dispatch failure semantics."""

from __future__ import annotations

from pathlib import Path
import threading
from typing import Any

from forge_msgs import PolicyCommand

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


class _StopFailNode(_CapturingNode):
    def send_output(self, output_id: str, value: Any) -> None:
        self.attempts.append((output_id, value))
        command = PolicyCommand.from_arrow(value)
        if command.command == "stop":
            raise RuntimeError("stop send failed")
        self.outputs.append((output_id, value))


class _BlockingNode(_CapturingNode):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def send_output(self, output_id: str, value: Any) -> None:
        self.entered.set()
        assert self.release.wait(timeout=2.0)
        super().send_output(output_id, value)


def _runtime() -> GatewayRuntime:
    return GatewayRuntime(config.GatewayConfig.from_dict({"joint_order": ["j1"]}))


def _commands(node: _CapturingNode) -> list[PolicyCommand]:
    return [PolicyCommand.from_arrow(value) for _, value in node.outputs]


def test_cancel_before_drain_suppresses_queued_start_without_sending_stop() -> None:
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

        assert _commands(node) == []
        with runtime.lock:
            assert runtime.commands["command-1"].status == "cancelled"
            assert runtime.sessions["session-1"].status == "cancelled"
            assert runtime.active_session_id is None
    finally:
        runtime.close()


def test_current_legacy_send_failure_loses_dequeued_command_and_aborts_drain() -> None:
    runtime = _runtime()
    try:
        for command in ("first", "second", "third"):
            runtime.enqueue_policy_command(command, {})
        node = _CapturingNode(fail_on_call=2)

        drain_commands(runtime, node)

        assert [command.command for command in _commands(node)] == ["first", "third"]
        assert runtime.command_queue.empty()
        with runtime.lock:
            assert runtime.last_error == "policy command dispatch failed: send failed"
    finally:
        runtime.close()


def test_agent_send_failure_marks_command_and_session_failed() -> None:
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

        drain_commands(runtime, _CapturingNode(fail_on_call=1))

        assert runtime.command_queue.empty()
        with runtime.lock:
            assert runtime.commands["command-fail"].status == "failed"
            assert runtime.sessions["session-fail"].status == "failed"
            assert runtime.active_session_id is None
            assert runtime.last_error == "policy command dispatch failed: send failed"
    finally:
        runtime.close()


def test_cancel_while_start_is_in_flight_schedules_stop_without_resurrection() -> None:
    runtime = _runtime()
    try:
        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-flight",
                "command_id": "command-flight",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )
        assert status == 202
        node = _BlockingNode()
        thread = threading.Thread(target=drain_commands, args=(runtime, node))
        thread.start()
        assert node.entered.wait(timeout=2.0)

        cancel_status, _ = runtime.cancel_agent_session("session-flight")
        assert cancel_status == 200
        node.release.set()
        thread.join(timeout=2.0)

        assert not thread.is_alive()
        assert [command.command for command in _commands(node)] == ["grasp_simple", "stop"]
        with runtime.lock:
            assert runtime.commands["command-flight"].status == "cancelled"
            assert runtime.sessions["session-flight"].status == "cancelled"
            assert runtime.active_session_id is None
    finally:
        runtime.close()


def test_cancel_stop_request_id_cannot_claim_new_session_command() -> None:
    runtime = _runtime()
    try:
        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-old",
                "command_id": "command-old",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )
        assert status == 202
        node = _CapturingNode()
        drain_commands(runtime, node)
        runtime.cancel_agent_session("session-old")

        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-new",
                "command_id": "cancel_command-old",
                "action_type": "grasp",
                "target_name": "banana",
            }
        )
        assert status == 202
        drain_commands(runtime, node)

        assert [command.command for command in _commands(node)] == [
            "grasp_simple",
            "stop",
            "grasp_simple",
        ]
        with runtime.lock:
            assert runtime.commands["cancel_command-old"].status == "sent"
            assert runtime.sessions["session-new"].status == "running"
    finally:
        runtime.close()


def test_transient_stop_failure_retries_before_next_session_start() -> None:
    runtime = _runtime()
    try:
        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-old-retry",
                "command_id": "command-old-retry",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )
        assert status == 202
        node = _CapturingNode()
        drain_commands(runtime, node)
        runtime.cancel_agent_session("session-old-retry")
        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-new-retry",
                "command_id": "command-new-retry",
                "action_type": "grasp",
                "target_name": "banana",
            }
        )
        assert status == 202
        node.fail_on_call = 2

        drain_commands(runtime, node)

        assert [command.command for command in _commands(node)] == [
            "grasp_simple",
            "stop",
            "grasp_simple",
        ]
        assert runtime.command_dispatch_allowed() is True
    finally:
        runtime.close()


def test_exhausted_stop_retries_block_later_session_dispatch() -> None:
    runtime = _runtime()
    try:
        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-old-block",
                "command_id": "command-old-block",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )
        assert status == 202
        drain_commands(runtime, _CapturingNode())
        runtime.cancel_agent_session("session-old-block")
        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-new-block",
                "command_id": "command-new-block",
                "action_type": "grasp",
                "target_name": "banana",
            }
        )
        assert status == 202
        node = _StopFailNode()

        drain_commands(runtime, node)

        assert len(node.attempts) == 3
        assert _commands(node) == []
        assert runtime.command_queue.qsize() == 1
        assert runtime.command_dispatch_allowed() is False
        with runtime.lock:
            assert runtime.dispatch_blocked_reason == (
                "safety command dispatch exhausted retries: stop send failed"
            )
        drain_commands(runtime, _CapturingNode())
        assert runtime.command_queue.qsize() == 1
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
