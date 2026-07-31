"""Command domain models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


CommandKind = Literal["POLICY_COMMAND", "SET_ROOT"]
CommandStatus = Literal["queued", "sent", "running", "succeeded", "failed", "cancelled"]


class CommandMailboxUnavailable(RuntimeError):
    """Raised when outbound work cannot be accepted safely."""


@dataclass(frozen=True)
class Command:
    kind: CommandKind
    payload: dict[str, Any]
    tracked_command_id: str | None = None
    retry_on_failure: bool = False
    attempt: int = 0


@dataclass
class CommandState:
    command_id: str
    session_id: str
    policy_id: str
    command: str
    action_type: str
    inputs: dict[str, Any]
    status: CommandStatus
    request_id: str
    created_at: float
    updated_at: float
    sent_at: float | None = None
    message: str = ""
    outputs: dict[str, Any] | None = None
    dispatching: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "command_id": self.command_id,
            "session_id": self.session_id,
            "policy_id": self.policy_id,
            "command": self.command,
            "action_type": self.action_type,
            "inputs": dict(self.inputs),
            "status": self.status,
            "request_id": self.request_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "sent_at": self.sent_at,
            "message": self.message,
            "outputs": dict(self.outputs or {}),
        }


def map_policy_status(status: str) -> CommandStatus:
    if status in ("accepted", "running"):
        return "running"
    if status == "done":
        return "succeeded"
    if status in ("rejected", "error"):
        return "failed"
    return "running"
