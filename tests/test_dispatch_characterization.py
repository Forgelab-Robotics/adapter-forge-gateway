"""Characterize current Gateway queue and dispatch failure semantics."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import threading
import time
from typing import Any

from forge_msgs import PolicyCommand
import pytest

from forge_gateway import config
from forge_gateway.adapters import dora_adapter
from forge_gateway.adapters.dora_adapter import drain_commands
from forge_gateway.domain.commands import CommandMailboxUnavailable
from forge_gateway.services import image_service
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


class _ObservedRuntimeLock:
    def __init__(
        self,
        observed_thread: threading.Thread,
        acquisition_attempted: threading.Event,
    ) -> None:
        self._lock = threading.Lock()
        self._observed_thread = observed_thread
        self._acquisition_attempted = acquisition_attempted

    def acquire(self, blocking: bool = True, timeout: float = -1.0) -> bool:
        if threading.current_thread() is self._observed_thread:
            self._acquisition_attempted.set()
        return self._lock.acquire(blocking, timeout)

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> _ObservedRuntimeLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback
        self.release()



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


def test_claimed_command_preparation_failure_clears_dispatch_state() -> None:
    runtime = _runtime()
    try:
        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-malformed",
                "command_id": "command-malformed",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )
        assert status == 202
        queued = runtime.take_next_command()
        assert queued is not None
        malformed = replace(queued, payload={})

        with pytest.raises(KeyError, match="command"):
            dora_adapter.handle_command(runtime, _CapturingNode(), malformed)

        with runtime.lock:
            command = runtime.commands["command-malformed"]
            assert command.status == "failed"
            assert command.dispatching is False
            assert runtime.sessions["session-malformed"].status == "failed"
            assert runtime._inflight_dispatches == 0
    finally:
        assert runtime.close() is True


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


def test_command_queue_rejects_work_at_configured_capacity() -> None:
    runtime = _runtime()
    try:
        assert runtime.config.command_queue_capacity == 256
        assert runtime.command_queue.maxsize == 256
        for index in range(256):
            runtime.enqueue_policy_command(f"command_{index}", {})
        with pytest.raises(CommandMailboxUnavailable, match="mailbox is full"):
            runtime.enqueue_policy_command("overflow", {})
        assert runtime.command_queue.qsize() == 256
    finally:
        runtime.close()


def test_safety_stop_overtakes_a_full_normal_mailbox() -> None:
    cfg = config.GatewayConfig.from_dict(
        {"joint_order": ["j1"], "command_queue_capacity": 2}
    )
    runtime = GatewayRuntime(cfg)
    try:
        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-priority",
                "command_id": "command-priority",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )
        assert status == 202
        first_node = _CapturingNode()
        drain_commands(runtime, first_node)
        runtime.enqueue_policy_command("normal_one", {})
        runtime.enqueue_policy_command("normal_two", {})
        runtime.cancel_agent_session("session-priority")
        node = _CapturingNode()

        drain_commands(runtime, node)

        assert [command.command for command in _commands(node)] == [
            "stop",
            "normal_one",
            "normal_two",
        ]
    finally:
        runtime.close()


def test_agent_session_creation_rolls_back_when_mailbox_is_full() -> None:
    cfg = config.GatewayConfig.from_dict(
        {"joint_order": ["j1"], "command_queue_capacity": 1}
    )
    runtime = GatewayRuntime(cfg)
    try:
        runtime.enqueue_policy_command("occupy", {})

        status, response = runtime.create_agent_session(
            {
                "session_id": "session-full",
                "command_id": "command-full",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )

        assert status == 503
        assert response == {"ok": False, "msg": "command mailbox is full"}
        assert runtime.sessions == {}
        assert runtime.commands == {}
        assert runtime.active_session_id is None
        assert runtime.command_queue.qsize() == 1
    finally:
        runtime.close()


def test_blocked_dispatch_rejects_new_session_without_state_artifacts() -> None:
    runtime = _runtime()
    try:
        runtime.block_command_dispatch(RuntimeError("unsafe"))

        status, response = runtime.create_agent_session(
            {
                "session_id": "session-blocked",
                "command_id": "command-blocked",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )

        assert status == 503
        assert "command dispatch is blocked" in response["msg"]
        assert runtime.sessions == {}
        assert runtime.commands == {}
        assert runtime.command_queue.empty()
    finally:
        runtime.close()


def test_blocked_dispatch_close_is_bounded_and_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    runtime._dispatch_close_timeout_sec = 0.05
    node = _BlockingNode()
    drain_thread = threading.Thread(target=drain_commands, args=(runtime, node))
    image_cleanup_attempted = threading.Event()
    original_image_close = runtime.image_encoder.close
    close_results: list[bool] = []
    close_errors: list[BaseException] = []

    def observed_image_close() -> bool:
        image_cleanup_attempted.set()
        return original_image_close()

    def close_runtime() -> None:
        try:
            close_results.append(runtime.close())
        except BaseException as error:
            close_errors.append(error)

    monkeypatch.setattr(runtime.image_encoder, "close", observed_image_close)
    runtime.enqueue_policy_command("blocking_command")
    close_thread = threading.Thread(target=close_runtime)
    try:
        drain_thread.start()
        assert node.entered.wait(timeout=1.0)
        close_thread.start()
        close_thread.join(timeout=1.0)

        assert not close_thread.is_alive()
        assert close_errors == []
        assert close_results == [False]
        assert runtime.phase == "closing"
        assert image_cleanup_attempted.is_set()
        assert node.outputs == []

        node.release.set()
        drain_thread.join(timeout=1.0)
        assert not drain_thread.is_alive()
        assert [command.command for command in _commands(node)] == [
            "blocking_command"
        ]

        assert runtime.close() is True
        assert runtime.phase == "closed"
        sent_after_success = list(node.outputs)
        drain_commands(runtime, node)
        assert node.outputs == sent_after_success
    finally:
        node.release.set()
        drain_thread.join(timeout=1.0)
        close_thread.join(timeout=1.0)
        if runtime.phase != "closed":
            runtime.close()


def test_mailbox_close_failure_still_waits_for_dispatch_and_wakes_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    runtime._dispatch_close_timeout_sec = 2.0
    node = _BlockingNode()
    runtime.enqueue_policy_command("blocking_command")
    drain_thread = threading.Thread(target=drain_commands, args=(runtime, node))
    command_close_attempted = threading.Event()
    dispatch_wait_entered = threading.Event()
    waiter_registered = threading.Event()
    image_cleanup_attempted = threading.Event()
    original_command_close = runtime.command_service.close
    original_image_close = runtime.image_encoder.close
    original_dispatch_wait = runtime._dispatch_condition.wait
    errors: list[BaseException] = []

    def failing_command_close(reason: str) -> bool:
        del reason
        command_close_attempted.set()
        raise RuntimeError("mailbox close failed")

    def observed_dispatch_wait(timeout: float | None = None) -> bool:
        dispatch_wait_entered.set()
        return original_dispatch_wait(timeout=timeout)

    def observed_image_close() -> bool:
        image_cleanup_attempted.set()
        return original_image_close()

    def close_runtime() -> None:
        try:
            runtime.close()
        except BaseException as error:
            errors.append(error)

    monkeypatch.setattr(runtime.command_service, "close", failing_command_close)
    monkeypatch.setattr(runtime._dispatch_condition, "wait", observed_dispatch_wait)
    monkeypatch.setattr(runtime.image_encoder, "close", observed_image_close)
    close_threads = [threading.Thread(target=close_runtime) for _ in range(2)]
    try:
        drain_thread.start()
        assert node.entered.wait(timeout=1.0)
        close_threads[0].start()
        assert command_close_attempted.wait(timeout=1.0)
        assert dispatch_wait_entered.wait(timeout=1.0)
        with runtime.lock:
            attempt = runtime._close_attempt
        assert attempt is not None
        original_attempt_wait = attempt.completed.wait

        def observed_attempt_wait(timeout: float | None = None) -> bool:
            waiter_registered.set()
            return original_attempt_wait(timeout=timeout)

        monkeypatch.setattr(attempt.completed, "wait", observed_attempt_wait)
        close_threads[1].start()
        assert waiter_registered.wait(timeout=1.0)
        assert image_cleanup_attempted.is_set() is False

        node.release.set()
        drain_thread.join(timeout=1.0)
        for thread in close_threads:
            thread.join(timeout=1.0)

        assert not drain_thread.is_alive()
        assert all(not thread.is_alive() for thread in close_threads)
        assert image_cleanup_attempted.is_set()
        assert len(errors) == 2
        assert all(str(error) == "gateway runtime close failed" for error in errors)
        assert runtime.phase == "closing"

        monkeypatch.setattr(runtime.command_service, "close", original_command_close)
        assert runtime.close() is True
        assert runtime.phase == "closed"
    finally:
        node.release.set()
        drain_thread.join(timeout=1.0)
        for thread in close_threads:
            thread.join(timeout=1.0)
        if runtime.phase != "closed":
            monkeypatch.setattr(runtime.command_service, "close", original_command_close)
            runtime.close()


def test_close_attempt_result_is_not_overwritten_by_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    command_close_entered = threading.Event()
    release_command_close = threading.Event()
    waiter_registered = threading.Event()
    both_readers_waiting = threading.Event()
    release_readers = threading.Event()
    original_command_close = runtime.command_service.close
    original_image_close = runtime.image_encoder.close
    original_result = runtime._completed_close_result
    image_close_calls = 0
    first_result_readers = 0
    readers_lock = threading.Lock()
    results: list[bool] = []
    errors: list[BaseException] = []

    def delayed_command_close(reason: str) -> bool:
        command_close_entered.set()
        assert release_command_close.wait(timeout=2.0)
        return original_command_close(reason)

    def first_image_close_times_out() -> bool:
        nonlocal image_close_calls
        image_close_calls += 1
        if image_close_calls == 1:
            return False
        return original_image_close()

    def delayed_result(attempt: Any) -> bool:
        nonlocal first_result_readers
        if attempt is first_attempt:
            with readers_lock:
                first_result_readers += 1
                if first_result_readers == 2:
                    both_readers_waiting.set()
            assert release_readers.wait(timeout=2.0)
        return original_result(attempt)

    def close_runtime() -> None:
        try:
            results.append(runtime.close())
        except BaseException as error:
            errors.append(error)

    monkeypatch.setattr(runtime.command_service, "close", delayed_command_close)
    monkeypatch.setattr(runtime.image_encoder, "close", first_image_close_times_out)
    close_threads = [threading.Thread(target=close_runtime) for _ in range(2)]
    try:
        close_threads[0].start()
        assert command_close_entered.wait(timeout=1.0)
        with runtime.lock:
            first_attempt = runtime._close_attempt
        assert first_attempt is not None
        original_attempt_wait = first_attempt.completed.wait

        def observed_attempt_wait(timeout: float | None = None) -> bool:
            waiter_registered.set()
            return original_attempt_wait(timeout=timeout)

        monkeypatch.setattr(first_attempt.completed, "wait", observed_attempt_wait)
        monkeypatch.setattr(runtime, "_completed_close_result", delayed_result)
        close_threads[1].start()
        assert waiter_registered.wait(timeout=1.0)

        release_command_close.set()
        assert both_readers_waiting.wait(timeout=1.0)
        assert runtime.phase == "closing"
        assert runtime.close() is True
        assert runtime.phase == "closed"

        release_readers.set()
        for thread in close_threads:
            thread.join(timeout=1.0)

        assert all(not thread.is_alive() for thread in close_threads)
        assert errors == []
        assert results == [False, False]
    finally:
        release_command_close.set()
        release_readers.set()
        for thread in close_threads:
            thread.join(timeout=1.0)
        if runtime.phase != "closed":
            runtime.close()


def test_close_waits_for_claimed_untracked_dispatch() -> None:
    runtime = _runtime()
    node = _BlockingNode()
    drain_thread = threading.Thread(target=drain_commands, args=(runtime, node))
    close_results: list[bool] = []
    close_thread = threading.Thread(
        target=lambda: close_results.append(runtime.close())
    )
    runtime.enqueue_policy_command("blocking_command")
    try:
        drain_thread.start()
        assert node.entered.wait(timeout=1.0)
        close_thread.start()

        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and runtime.phase != "closing":
            time.sleep(0.01)
        assert runtime.phase == "closing"
        assert close_thread.is_alive()

        node.release.set()
        drain_thread.join(timeout=1.0)
        close_thread.join(timeout=1.0)

        assert not drain_thread.is_alive()
        assert not close_thread.is_alive()
        assert close_results == [True]
        assert [command.command for command in _commands(node)] == [
            "blocking_command"
        ]
        assert runtime.phase == "closed"
    finally:
        node.release.set()
        drain_thread.join(timeout=1.0)
        close_thread.join(timeout=1.0)
        if runtime.phase != "closed":
            runtime.close()


@pytest.mark.parametrize("set_root", [False, True], ids=["policy", "set-root"])
def test_close_suppresses_dequeued_command_before_claim(
    monkeypatch: pytest.MonkeyPatch,
    set_root: bool,
) -> None:
    runtime = _runtime()
    node = _CapturingNode()
    entered = threading.Event()
    release = threading.Event()
    original_handle = dora_adapter.handle_command

    def delayed_handle(runtime_arg: GatewayRuntime, node_arg: Any, command: Any) -> None:
        entered.set()
        assert release.wait(timeout=2.0)
        original_handle(runtime_arg, node_arg, command)

    monkeypatch.setattr(dora_adapter, "handle_command", delayed_handle)
    if set_root:
        runtime.set_record_root("/records")
    else:
        runtime.enqueue_policy_command("must_not_send")
    drain_thread = threading.Thread(target=drain_commands, args=(runtime, node))
    try:
        drain_thread.start()
        assert entered.wait(timeout=1.0)

        assert runtime.close() is True
        release.set()
        drain_thread.join(timeout=1.0)

        assert not drain_thread.is_alive()
        assert node.attempts == []
        assert runtime.record_root is None
    finally:
        release.set()
        drain_thread.join(timeout=1.0)
        runtime.close()


def test_closing_phase_rejects_admission_before_mailbox_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    close_entered = threading.Event()
    release_close = threading.Event()
    close_results: list[bool] = []
    original_close = runtime.command_service.close

    def delayed_close(reason: str) -> bool:
        close_entered.set()
        assert release_close.wait(timeout=2.0)
        return original_close(reason)

    monkeypatch.setattr(runtime.command_service, "close", delayed_close)
    close_thread = threading.Thread(
        target=lambda: close_results.append(runtime.close())
    )
    try:
        close_thread.start()
        assert close_entered.wait(timeout=1.0)
        assert runtime.phase == "closing"

        with pytest.raises(CommandMailboxUnavailable, match="runtime is closing"):
            runtime.enqueue_policy_command("too_late")

        assert runtime.command_queue.empty()
        release_close.set()
        close_thread.join(timeout=1.0)
        assert close_results == [True]
    finally:
        release_close.set()
        close_thread.join(timeout=1.0)
        if runtime.phase != "closed":
            runtime.close()


def test_closing_phase_rejects_images_before_worker_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    stop_entered = threading.Event()
    release_stop = threading.Event()
    original_stop = runtime.image_encoder.request_stop
    close_results: list[bool] = []

    def delayed_stop() -> None:
        stop_entered.set()
        assert release_stop.wait(timeout=2.0)
        original_stop()

    monkeypatch.setattr(runtime.image_encoder, "request_stop", delayed_stop)
    close_thread = threading.Thread(
        target=lambda: close_results.append(runtime.close())
    )
    try:
        close_thread.start()
        assert stop_entered.wait(timeout=1.0)
        assert runtime.phase == "closing"
        assert runtime.image_encoder.submit("image/front", object(), time.time()) is False

        release_stop.set()
        close_thread.join(timeout=1.0)
        assert not close_thread.is_alive()
        assert close_results == [True]
    finally:
        release_stop.set()
        close_thread.join(timeout=1.0)
        if runtime.phase != "closed":
            runtime.close()


def test_image_worker_and_submit_use_runtime_then_image_lock_order(
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
        if value == "old":
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
    worker_lock_attempted = threading.Event()
    observed_lock = _ObservedRuntimeLock(
        runtime.image_encoder._thread,
        worker_lock_attempted,
    )
    runtime.lock = observed_lock  # type: ignore[assignment]
    results: list[bool] = []
    errors: list[str] = []

    assert runtime.image_encoder.submit("image/front", "old", 1.0) is True
    assert encode_started.wait(timeout=1.0)

    def submit_while_holding_runtime_lock() -> None:
        with runtime.lock:
            release_encode.set()
            if not worker_lock_attempted.wait(timeout=1.0):
                errors.append("image worker did not attempt the runtime lock")
                return
            results.append(
                runtime.image_encoder.submit("image/front", "new", 2.0)
            )

    submit_thread = threading.Thread(
        target=submit_while_holding_runtime_lock,
        daemon=True,
    )
    submit_thread.start()
    submit_thread.join(timeout=1.0)

    assert not submit_thread.is_alive()
    assert errors == []
    assert results == [True]

    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        with runtime.lock:
            latest = runtime.images.get("image/front")
        if latest is not None:
            break
        time.sleep(0.01)

    with runtime.lock:
        assert runtime.images["image/front"]["data"] == "new"
    assert runtime.close() is True


def test_dequeue_rechecks_dispatch_gate_after_allowed_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    try:
        runtime.enqueue_policy_command("must-not-send")
        node = _CapturingNode()

        def allow_then_block() -> bool:
            runtime.command_service.block_command_dispatch("blocked between check and dequeue")
            return True

        monkeypatch.setattr(runtime.command_service, "command_dispatch_allowed", allow_then_block)

        drain_commands(runtime, node)

        assert node.attempts == []
        assert runtime.command_queue.qsize() == 1
        assert runtime.dispatch_blocked_reason == "blocked between check and dequeue"
    finally:
        runtime.close()
