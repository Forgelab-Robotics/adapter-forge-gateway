"""Command domain models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


CommandKind = Literal["POLICY_COMMAND", "SET_ROOT"]


class CommandMailboxUnavailable(RuntimeError):
    """Raised when outbound work cannot be accepted safely."""


@dataclass(frozen=True)
class Command:
    kind: CommandKind
    payload: dict[str, Any]
