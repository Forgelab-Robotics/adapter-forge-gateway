"""Agent session domain models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .commands import CommandStatus


SessionStatus = Literal["queued", "running", "succeeded", "failed", "cancelled"]


@dataclass
class SessionState:
    session_id: str
    status: SessionStatus
    action_type: str
    instruction: str
    source: str
    target: str | None
    command_ids: list[str]
    created_at: float
    updated_at: float
    message: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "session_id": self.session_id,
            "status": self.status,
            "action_type": self.action_type,
            "instruction": self.instruction,
            "source": self.source,
            "target": self.target,
            "command_ids": list(self.command_ids),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "message": self.message,
        }


def session_status_from_command(status: CommandStatus) -> SessionStatus:
    if status == "succeeded":
        return "succeeded"
    if status == "failed":
        return "failed"
    if status == "cancelled":
        return "cancelled"
    return "running"
