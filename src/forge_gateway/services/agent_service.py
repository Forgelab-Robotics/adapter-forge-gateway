"""Agent session preparation and lifecycle state machine."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from forge_gateway.config import AgentConfig
from forge_gateway.domain.commands import (
    CommandMailboxUnavailable,
    CommandState,
    map_policy_status,
)
from forge_gateway.domain.sessions import SessionState, session_status_from_command
from forge_gateway.services.action_registry import ActionRegistry

HttpResult = tuple[int, dict[str, Any]]


class PolicyCommandSink(Protocol):
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
    ) -> None: ...


@dataclass(frozen=True)
class PreparedSession:
    session: SessionState
    command: CommandState


@dataclass(frozen=True)
class AgentEvent:
    event_type: str
    data: dict[str, Any]
    now: float


@dataclass(frozen=True)
class PolicyCommandStatusUpdate:
    policy_id: str
    command: str
    request_id: str
    status: str
    message: str
    outputs: dict[str, Any]


class AgentService:
    """Own the serial agent session aggregate under the caller's lock."""

    def __init__(
        self,
        *,
        config: AgentConfig,
        action_registry: ActionRegistry,
        command_sink: PolicyCommandSink,
    ) -> None:
        self.config = config
        self.action_registry = action_registry
        self.command_sink = command_sink
        self.sessions: dict[str, SessionState] = {}
        self.commands: dict[str, CommandState] = {}
        self.active_session_id: str | None = None
        self.last_result: dict[str, Any] | None = None

    def prepare_create(
        self,
        body: dict[str, Any],
        *,
        now: float,
    ) -> PreparedSession | HttpResult:
        """Validate and prepare a session without mutating aggregate state."""
        if not self.config.enabled:
            return 404, {"ok": False, "msg": "agent API is disabled"}

        action_type = (
            body.get("action_type")
            or body.get("action")
            or body.get("type")
            or body.get("command")
        )
        if not isinstance(action_type, str) or not action_type:
            return 400, {"ok": False, "msg": "action_type must be non-empty string"}

        inputs_raw = body.get("inputs", body.get("parameters", {}))
        if inputs_raw is None:
            inputs_raw = {}
        if not isinstance(inputs_raw, dict):
            return 400, {"ok": False, "msg": "inputs must be an object"}

        source_inputs = dict(inputs_raw)
        for key in ("target", "target_name", "instruction"):
            if key in body and key not in source_inputs:
                source_inputs[key] = body[key]

        action_config = self.action_registry.get(action_type)
        if action_config is None:
            return 400, {
                "ok": False,
                "msg": f"unknown action_type: {action_type}",
                "data": {
                    "supported_actions": self.action_registry.supported_action_names()
                },
            }

        policy_inputs = dict(source_inputs)
        for source_key, target_key in action_config.input_mapping.items():
            if source_key in source_inputs and target_key not in policy_inputs:
                policy_inputs[target_key] = source_inputs[source_key]
        missing = [
            key
            for key in action_config.required_parameters
            if key not in policy_inputs or policy_inputs[key] in (None, "")
        ]
        if missing:
            return 400, {
                "ok": False,
                "msg": f"missing required inputs: {', '.join(missing)}",
            }

        session_id = str(body.get("session_id") or f"session_{uuid.uuid4().hex[:12]}")
        command_id = str(body.get("command_id") or f"command_{uuid.uuid4().hex[:12]}")
        instruction = str(body.get("instruction") or source_inputs.get("instruction") or "")
        source = str(body.get("source") or "paos-agent")
        target = policy_inputs.get("target_name") or policy_inputs.get("target")
        target_value = str(target) if target is not None else None
        policy_inputs.update(
            {
                "session_id": session_id,
                "command_id": command_id,
                "action_type": action_type,
                "source": source,
                "policy_id": action_config.policy_id,
            }
        )
        if instruction:
            policy_inputs["instruction"] = instruction

        return PreparedSession(
            session=SessionState(
                session_id=session_id,
                status="queued",
                action_type=action_type,
                instruction=instruction,
                source=source,
                target=target_value,
                command_ids=[command_id],
                created_at=now,
                updated_at=now,
            ),
            command=CommandState(
                command_id=command_id,
                session_id=session_id,
                policy_id=action_config.policy_id,
                command=action_config.command,
                action_type=action_type,
                inputs=dict(policy_inputs),
                status="queued",
                request_id=command_id,
                created_at=now,
                updated_at=now,
            ),
        )

    def create_session_locked(
        self,
        prepared: PreparedSession,
    ) -> tuple[HttpResult, AgentEvent | None]:
        session = prepared.session
        command = prepared.command
        if session.session_id in self.sessions:
            return (
                409,
                {"ok": False, "msg": f"session already exists: {session.session_id}"},
            ), None
        if command.command_id in self.commands:
            return (
                409,
                {"ok": False, "msg": f"command already exists: {command.command_id}"},
            ), None
        active_count = sum(
            1
            for current in self.sessions.values()
            if current.status in ("queued", "running")
        )
        if active_count >= self.config.max_active_sessions:
            return (
                409,
                {
                    "ok": False,
                    "msg": "another agent session is active",
                    "data": {"active_session_id": self.active_session_id},
                },
            ), None

        self.sessions[session.session_id] = session
        self.commands[command.command_id] = command
        self.active_session_id = session.session_id
        try:
            self.command_sink.enqueue_policy_command(
                command.command,
                command.inputs,
                request_id=command.request_id,
                policy_id=command.policy_id,
                tracked_command_id=command.command_id,
            )
        except CommandMailboxUnavailable as error:
            self.sessions.pop(session.session_id, None)
            self.commands.pop(command.command_id, None)
            if self.active_session_id == session.session_id:
                self.active_session_id = None
            return (503, {"ok": False, "msg": str(error)}), None

        event = AgentEvent(
            "session_created",
            {"session": session.to_dict(), "command": command.to_dict()},
            session.created_at,
        )
        return (
            202,
            {
                "ok": True,
                "data": {
                    "session": session.to_dict(),
                    "command": command.to_dict(),
                    "status": session.status,
                },
            },
        ), event

    def get_session_locked(self, session_id: str) -> HttpResult:
        session = self.sessions.get(session_id)
        if session is None:
            return 404, {"ok": False, "msg": f"session not found: {session_id}"}
        return 200, {
            "ok": True,
            "data": {
                "session": session.to_dict(),
                "commands": [
                    self.commands[command_id].to_dict()
                    for command_id in session.command_ids
                    if command_id in self.commands
                ],
            },
        }

    def cancel_session_locked(
        self,
        session_id: str,
        *,
        now: float,
    ) -> tuple[HttpResult, AgentEvent | None]:
        session = self.sessions.get(session_id)
        if session is None:
            return (
                404,
                {"ok": False, "msg": f"session not found: {session_id}"},
            ), None

        cancellation_requested = False
        for command_id in session.command_ids:
            command = self.commands.get(command_id)
            if command is None or command.status not in ("queued", "sent", "running"):
                continue
            previous_status = command.status
            cancellation_requested = True
            command.status = "cancelled"
            command.updated_at = now
            command.message = "cancel requested by agent"
            if previous_status in ("sent", "running") or command.dispatching:
                self.command_sink.enqueue_policy_command(
                    "stop",
                    {
                        "session_id": session_id,
                        "command_id": f"cancel_{command.command_id}",
                        "cancelled_command_id": command.command_id,
                        "action_type": command.action_type,
                        "source": "paos-agent",
                        "reason": "agent_cancel",
                    },
                    request_id=f"cancel_{command.command_id}",
                    policy_id=command.policy_id,
                    retry_on_failure=True,
                    safety=True,
                )

        event: AgentEvent | None = None
        if cancellation_requested:
            session.status = "cancelled"
            session.updated_at = now
            session.message = "cancel requested by agent"
            if self.active_session_id == session_id:
                self.active_session_id = None
            event = AgentEvent("session_cancelled", {"session_id": session_id}, now)

        return (
            200,
            {
                "ok": True,
                "data": {"session_id": session_id, "status": session.status},
            },
        ), event

    def claim_command_dispatch_locked(self, command_id: str) -> bool:
        command = self.commands.get(command_id)
        if command is None or command.status != "queued" or command.dispatching:
            return False
        command.dispatching = True
        return True

    def mark_command_sent_locked(
        self,
        command_id: str,
        *,
        now: float,
    ) -> AgentEvent | None:
        command = self.commands.get(command_id)
        if command is None:
            return None
        command.dispatching = False
        if command.status != "queued":
            return None
        command.status = "sent"
        command.sent_at = now
        command.updated_at = now
        session = self.sessions.get(command.session_id)
        if session is not None and session.status == "queued":
            session.status = "running"
            session.updated_at = now
        return AgentEvent("command_sent", {"command_id": command_id}, now)

    def mark_command_dispatch_failed_locked(
        self,
        command_id: str,
        *,
        message: str,
        error_text: str,
        now: float,
    ) -> AgentEvent | None:
        command = self.commands.get(command_id)
        if command is None:
            return AgentEvent(
                "command_dispatch_failed",
                {"command_id": command_id, "error": error_text},
                now,
            )
        command.dispatching = False
        if command.status in ("succeeded", "failed", "cancelled"):
            return None
        command.status = "failed"
        command.updated_at = now
        command.message = message
        session = self.sessions.get(command.session_id)
        if session is not None and session.status not in (
            "succeeded",
            "failed",
            "cancelled",
        ):
            session.status = "failed"
            session.updated_at = now
            session.message = message
            if self.active_session_id == session.session_id:
                self.active_session_id = None
        return AgentEvent(
            "command_dispatch_failed",
            {"command_id": command_id, "error": error_text},
            now,
        )

    def apply_policy_command_status_locked(
        self,
        update: PolicyCommandStatusUpdate,
        *,
        now: float,
    ) -> AgentEvent:
        command = self.commands.get(update.request_id)
        mapped_status = map_policy_status(update.status)
        if command is not None:
            identity_matches = (
                update.policy_id == command.policy_id
                and update.command == command.command
            )
            mutable_status = command.status in ("sent", "running")
            if identity_matches and mutable_status:
                command.status = mapped_status
                command.updated_at = now
                command.message = update.message
                command.outputs = update.outputs
                session = self.sessions.get(command.session_id)
                if session is not None and session.status in ("queued", "running"):
                    session.status = session_status_from_command(mapped_status)
                    session.updated_at = now
                    session.message = update.message
                    if (
                        session.status in ("succeeded", "failed", "cancelled")
                        and self.active_session_id == session.session_id
                    ):
                        self.active_session_id = None
            elif identity_matches and command.status == "cancelled":
                command.updated_at = now
                command.outputs = update.outputs

        self.last_result = {
            "policy_id": update.policy_id,
            "command": update.command,
            "request_id": update.request_id,
            "status": update.status,
            "message": update.message,
            "outputs": update.outputs,
        }
        return AgentEvent(
            "policy_command_status",
            dict(self.last_result),
            now,
        )
