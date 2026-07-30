"""Policy command queue service helpers."""

from __future__ import annotations

import queue
from pathlib import Path
from typing import Any

from app.domain.commands import Command


class CommandService:
    """Owns queued outbound commands before the Dora adapter drains them."""

    def __init__(self, *, default_policy_id: str) -> None:
        self.default_policy_id = default_policy_id
        self.command_queue: queue.Queue[Command] = queue.Queue()
        self.record_root: Path | None = None

    def enqueue_policy_command(
        self,
        command: str,
        inputs: dict[str, Any] | None = None,
        *,
        request_id: str = "",
        policy_id: str | None = None,
    ) -> None:
        self.command_queue.put(
            Command(
                kind="POLICY_COMMAND",
                payload={
                    "command": command,
                    "inputs": dict(inputs or {}),
                    "request_id": request_id,
                    "policy_id": policy_id or self.default_policy_id,
                },
            )
        )

    def set_record_root(self, root: str | None) -> None:
        self.command_queue.put(Command(kind="SET_ROOT", payload={"root": root}))
