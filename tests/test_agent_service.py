"""Unit tests for the agent session aggregate boundary."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any

import pytest

from forge_gateway.config import AgentConfig, GatewayConfig
from forge_gateway.domain.commands import CommandMailboxUnavailable
from forge_gateway.services.action_registry import ActionRegistry
from forge_gateway.services.agent_service import (
    AgentService,
    PolicyCommandStatusUpdate,
    PreparedSession,
)
from forge_gateway.services.runtime_service import GatewayRuntime


class _Sink:
    def __init__(self, *, unavailable: str | None = None) -> None:
        self.unavailable = unavailable
        self.calls: list[dict[str, Any]] = []

    def enqueue_policy_command(
        self,
        command: str,
        inputs: dict[str, Any] | None = None,
        *,
        request_id: str = "",
        policy_id: str | None = None,
        tracked_command_id: str | None = None,
        retry_on_failure: bool = False,
        attempt: int = 0,
        safety: bool = False,
    ) -> None:
        if self.unavailable is not None:
            raise CommandMailboxUnavailable(self.unavailable)
        self.calls.append(
            {
                "command": command,
                "inputs": dict(inputs or {}),
                "request_id": request_id,
                "policy_id": policy_id,
                "tracked_command_id": tracked_command_id,
                "retry_on_failure": retry_on_failure,
                "attempt": attempt,
                "safety": safety,
            }
        )


class _LockCheckingSink(_Sink):
    def __init__(self, is_locked: Callable[[], bool]) -> None:
        super().__init__()
        self._is_locked = is_locked

    def enqueue_policy_command(
        self,
        command: str,
        inputs: dict[str, Any] | None = None,
        *,
        request_id: str = "",
        policy_id: str | None = None,
        tracked_command_id: str | None = None,
        retry_on_failure: bool = False,
        attempt: int = 0,
        safety: bool = False,
    ) -> None:
        assert self._is_locked()
        super().enqueue_policy_command(
            command,
            inputs,
            request_id=request_id,
            policy_id=policy_id,
            tracked_command_id=tracked_command_id,
            retry_on_failure=retry_on_failure,
            attempt=attempt,
            safety=safety,
        )


def _agent_config() -> AgentConfig:
    return GatewayConfig.from_dict({"joint_order": ["j1"]}).agent


def _service(
    *,
    config: AgentConfig | None = None,
    sink: _Sink | None = None,
) -> tuple[AgentService, _Sink]:
    agent_config = config or _agent_config()
    command_sink = sink or _Sink()
    return (
        AgentService(
            config=agent_config,
            action_registry=ActionRegistry.from_actions(agent_config.actions),
            command_sink=command_sink,
        ),
        command_sink,
    )


def _prepare(
    service: AgentService,
    *,
    session_id: str = "session-1",
    command_id: str = "command-1",
    now: float = 1.0,
) -> PreparedSession:
    prepared = service.prepare_create(
        {
            "session_id": session_id,
            "command_id": command_id,
            "action_type": "grasp",
            "target": "apple",
            "instruction": "pick the apple",
        },
        now=now,
    )
    assert isinstance(prepared, PreparedSession)
    return prepared


def test_prepare_create_is_pure_and_maps_manifest_inputs() -> None:
    service, sink = _service()

    prepared = _prepare(service, now=12.5)

    assert service.sessions == {}
    assert service.commands == {}
    assert service.active_session_id is None
    assert sink.calls == []
    assert prepared.session.to_dict() == {
        "session_id": "session-1",
        "status": "queued",
        "action_type": "grasp",
        "instruction": "pick the apple",
        "source": "paos-agent",
        "target": "apple",
        "command_ids": ["command-1"],
        "created_at": 12.5,
        "updated_at": 12.5,
        "message": "",
    }
    assert prepared.command.policy_id == "sam3"
    assert prepared.command.command == "grasp_simple"
    assert prepared.command.inputs == {
        "target": "apple",
        "instruction": "pick the apple",
        "target_name": "apple",
        "session_id": "session-1",
        "command_id": "command-1",
        "action_type": "grasp",
        "source": "paos-agent",
        "policy_id": "sam3",
    }


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (
            {},
            (400, {"ok": False, "msg": "action_type must be non-empty string"}),
        ),
        (
            {"action_type": "grasp", "inputs": []},
            (400, {"ok": False, "msg": "inputs must be an object"}),
        ),
        (
            {"action_type": "unknown"},
            (
                400,
                {
                    "ok": False,
                    "msg": "unknown action_type: unknown",
                    "data": {
                        "supported_actions": [
                            "check_target",
                            "go_home",
                            "grasp",
                            "place",
                        ]
                    },
                },
            ),
        ),
        (
            {"action_type": "grasp"},
            (400, {"ok": False, "msg": "missing required inputs: target_name"}),
        ),
    ],
)
def test_prepare_create_preserves_validation_responses(
    body: dict[str, Any],
    expected: tuple[int, dict[str, Any]],
) -> None:
    service, _ = _service()

    assert service.prepare_create(body, now=1.0) == expected


def test_prepare_create_rejects_disabled_agent_api() -> None:
    service, _ = _service(config=replace(_agent_config(), enabled=False))

    assert service.prepare_create({}, now=1.0) == (
        404,
        {"ok": False, "msg": "agent API is disabled"},
    )


def test_create_session_commits_state_command_and_event() -> None:
    service, sink = _service()
    prepared = _prepare(service)

    result, event = service.create_session_locked(prepared)

    assert result[0] == 202
    assert result[1]["data"]["status"] == "queued"
    assert event is not None
    assert event.event_type == "session_created"
    assert event.now == 1.0
    assert event.data == {
        "session": result[1]["data"]["session"],
        "command": result[1]["data"]["command"],
    }
    assert service.sessions == {"session-1": prepared.session}
    assert service.commands == {"command-1": prepared.command}
    assert service.active_session_id == "session-1"
    assert sink.calls == [
        {
            "command": "grasp_simple",
            "inputs": prepared.command.inputs,
            "request_id": "command-1",
            "policy_id": "sam3",
            "tracked_command_id": "command-1",
            "retry_on_failure": False,
            "attempt": 0,
            "safety": False,
        }
    ]


def test_create_session_rejects_duplicates_and_active_session() -> None:
    service, sink = _service()
    first = _prepare(service)
    assert service.create_session_locked(first)[0][0] == 202

    duplicate_session, duplicate_session_event = service.create_session_locked(first)
    duplicate_command, duplicate_command_event = service.create_session_locked(
        _prepare(service, session_id="session-2")
    )
    active, active_event = service.create_session_locked(
        _prepare(service, session_id="session-2", command_id="command-2")
    )

    assert duplicate_session == (
        409,
        {"ok": False, "msg": "session already exists: session-1"},
    )
    assert duplicate_command == (
        409,
        {"ok": False, "msg": "command already exists: command-1"},
    )
    assert active == (
        409,
        {
            "ok": False,
            "msg": "another agent session is active",
            "data": {"active_session_id": "session-1"},
        },
    )
    assert duplicate_session_event is None
    assert duplicate_command_event is None
    assert active_event is None
    assert len(sink.calls) == 1


def test_create_session_rolls_back_when_command_sink_rejects() -> None:
    service, _ = _service(sink=_Sink(unavailable="command mailbox is full"))

    result, event = service.create_session_locked(_prepare(service))

    assert result == (503, {"ok": False, "msg": "command mailbox is full"})
    assert event is None
    assert service.sessions == {}
    assert service.commands == {}
    assert service.active_session_id is None


def test_cancel_dispatching_command_sends_untracked_safety_stop() -> None:
    service, sink = _service()
    assert service.create_session_locked(_prepare(service))[0][0] == 202
    sink.calls.clear()
    service.commands["command-1"].dispatching = True

    result, event = service.cancel_session_locked("session-1", now=2.0)

    assert result == (
        200,
        {"ok": True, "data": {"session_id": "session-1", "status": "cancelled"}},
    )
    assert event is not None
    assert event.event_type == "session_cancelled"
    assert service.commands["command-1"].status == "cancelled"
    assert service.active_session_id is None
    assert sink.calls == [
        {
            "command": "stop",
            "inputs": {
                "session_id": "session-1",
                "command_id": "cancel_command-1",
                "cancelled_command_id": "command-1",
                "action_type": "grasp",
                "source": "paos-agent",
                "reason": "agent_cancel",
            },
            "request_id": "cancel_command-1",
            "policy_id": "sam3",
            "tracked_command_id": None,
            "retry_on_failure": True,
            "attempt": 0,
            "safety": True,
        }
    ]


def test_dispatch_and_status_transitions_are_monotonic() -> None:
    service, _ = _service()
    assert service.create_session_locked(_prepare(service))[0][0] == 202

    assert service.claim_command_dispatch_locked("command-1") is True
    assert service.claim_command_dispatch_locked("command-1") is False
    sent_event = service.mark_command_sent_locked("command-1", now=2.0)
    assert sent_event is not None
    assert service.commands["command-1"].status == "sent"
    assert service.sessions["session-1"].status == "running"

    wrong_event = service.apply_policy_command_status_locked(
        PolicyCommandStatusUpdate(
            policy_id="wrong",
            command="wrong",
            request_id="command-1",
            status="done",
            message="ignored",
            outputs={"value": 1},
        ),
        now=3.0,
    )
    assert wrong_event.event_type == "policy_command_status"
    assert service.commands["command-1"].status == "sent"
    assert service.last_result is not None
    assert service.last_result["status"] == "done"

    service.apply_policy_command_status_locked(
        PolicyCommandStatusUpdate(
            policy_id="sam3",
            command="grasp_simple",
            request_id="command-1",
            status="done",
            message="complete",
            outputs={"accepted": True},
        ),
        now=4.0,
    )
    assert service.last_result is not None
    assert service.commands["command-1"].outputs is service.last_result["outputs"]

    service.apply_policy_command_status_locked(
        PolicyCommandStatusUpdate(
            policy_id="sam3",
            command="grasp_simple",
            request_id="command-1",
            status="accepted",
            message="late",
            outputs={"accepted": False},
        ),
        now=5.0,
    )

    assert service.commands["command-1"].status == "succeeded"
    assert service.commands["command-1"].outputs == {"accepted": True}
    assert service.sessions["session-1"].status == "succeeded"
    assert service.active_session_id is None
    assert service.last_result is not None
    assert service.last_result["status"] == "accepted"


def test_runtime_facade_preserves_aliases_and_global_lock_order() -> None:
    runtime = GatewayRuntime(GatewayConfig.from_dict({"joint_order": ["j1"]}))
    sink = _LockCheckingSink(runtime.lock.locked)
    runtime.agent_service.command_sink = sink
    try:
        assert runtime.sessions is runtime.agent_service.sessions
        assert runtime.commands is runtime.agent_service.commands

        status, _ = runtime.create_agent_session(
            {
                "session_id": "session-lock",
                "command_id": "command-lock",
                "action_type": "grasp",
                "target_name": "apple",
            }
        )
        assert status == 202
        assert runtime.active_session_id == runtime.agent_service.active_session_id

        with runtime.lock:
            runtime.commands["command-lock"].dispatching = True
        cancel_status, _ = runtime.cancel_agent_session("session-lock")

        assert cancel_status == 200
        assert [call["command"] for call in sink.calls] == ["grasp_simple", "stop"]
        assert runtime.active_session_id is None
        assert runtime.last_result is runtime.agent_service.last_result

        replacement_sessions = {}
        replacement_commands = {}
        replacement_result = {"status": "manual"}
        runtime.sessions = replacement_sessions
        runtime.commands = replacement_commands
        runtime.active_session_id = "manual-session"
        runtime.last_result = replacement_result

        assert runtime.agent_service.sessions is replacement_sessions
        assert runtime.agent_service.commands is replacement_commands
        assert runtime.agent_service.active_session_id == "manual-session"
        assert runtime.agent_service.last_result is replacement_result
    finally:
        runtime.close()
