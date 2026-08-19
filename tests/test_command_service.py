"""Unit tests for the outbound command mailbox boundary."""

from __future__ import annotations

import pytest

from forge_gateway.domain.commands import CommandMailboxUnavailable
from forge_gateway.services.command_service import CommandService


def _service(*, capacity: int = 2) -> CommandService:
    return CommandService(default_policy_id="default-policy", capacity=capacity)


def test_policy_command_preserves_metadata_and_enforces_capacity() -> None:
    service = _service(capacity=1)
    service.enqueue_policy_command(
        "grasp",
        {"target": "apple"},
        request_id="request-1",
        policy_id="special-policy",
        tracked_command_id="command-1",
        retry_on_failure=True,
        attempt=2,
    )

    with pytest.raises(CommandMailboxUnavailable, match="command mailbox is full"):
        service.enqueue_policy_command("overflow")

    command = service.take_next_command()
    assert command is not None
    assert command.kind == "POLICY_COMMAND"
    assert command.payload == {
        "command": "grasp",
        "inputs": {"target": "apple"},
        "request_id": "request-1",
        "policy_id": "special-policy",
    }
    assert command.tracked_command_id == "command-1"
    assert command.retry_on_failure is True
    assert command.attempt == 2
    assert service.take_next_command() is None


def test_safety_lane_bypasses_capacity_and_overtakes_normal_fifo() -> None:
    service = _service(capacity=2)
    service.enqueue_policy_command("normal-1")
    service.enqueue_policy_command("normal-2")
    service.enqueue_policy_command("safety-1", safety=True)
    service.enqueue_policy_command("safety-2", safety=True)

    commands = [service.take_next_command() for _ in range(4)]

    assert [command.payload["command"] for command in commands if command is not None] == [
        "safety-1",
        "safety-2",
        "normal-1",
        "normal-2",
    ]


def test_block_is_idempotent_and_rejects_only_normal_admission() -> None:
    service = _service()

    assert service.block_command_dispatch("unsafe") is True
    assert service.block_command_dispatch("ignored") is False
    assert service.dispatch_blocked_reason == "unsafe"
    assert service.command_dispatch_allowed() is False

    with pytest.raises(
        CommandMailboxUnavailable,
        match="command dispatch is blocked: unsafe",
    ):
        service.enqueue_policy_command("normal")

    service.enqueue_policy_command("stop", retry_on_failure=True, safety=True)
    command = service.safety_command_queue.get_nowait()
    assert command.payload["command"] == "stop"
    assert command.retry_on_failure is True


def test_close_rejects_all_admission_and_preserves_pending_work() -> None:
    service = _service()
    service.enqueue_policy_command("pending")

    assert service.close("runtime closing") is True
    assert service.close("ignored") is False
    assert service.closed_reason == "runtime closing"
    assert service.command_dispatch_allowed() is False
    assert service.block_command_dispatch("unsafe") is False

    for safety in (False, True):
        with pytest.raises(
            CommandMailboxUnavailable,
            match="command mailbox is closed: runtime closing",
        ):
            service.enqueue_policy_command("rejected", safety=safety)
    with pytest.raises(CommandMailboxUnavailable, match="command mailbox is closed"):
        service.set_record_root("/records")

    assert service.take_next_command() is None
    assert service.command_queue.qsize() == 1
    assert service.safety_command_queue.empty()


def test_take_rechecks_block_gate_without_dequeueing_pending_work() -> None:
    service = _service()
    service.enqueue_policy_command("normal")
    assert service.command_dispatch_allowed() is True

    service.block_command_dispatch("unsafe")

    assert service.take_next_command() is None
    assert service.command_queue.qsize() == 1


def test_set_record_root_uses_normal_fifo() -> None:
    service = _service()
    service.enqueue_policy_command("before")
    service.set_record_root("/records")

    first = service.take_next_command()
    second = service.take_next_command()

    assert first is not None
    assert first.payload["command"] == "before"
    assert second is not None
    assert second.kind == "SET_ROOT"
    assert second.payload == {"root": "/records"}
    assert service.take_next_command() is None


def test_capacity_must_be_positive() -> None:
    with pytest.raises(ValueError, match="capacity must be positive"):
        CommandService(default_policy_id="default", capacity=0)
