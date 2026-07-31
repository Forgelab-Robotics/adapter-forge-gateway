"""Bounded priority mailbox for outbound Gateway commands."""

from __future__ import annotations

import queue
import threading
from typing import Any

from forge_gateway.domain.commands import Command, CommandMailboxUnavailable


class CommandService:
    """Own command admission and ordering before the Dora adapter drains it."""

    def __init__(self, *, default_policy_id: str, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("command mailbox capacity must be positive")
        self.default_policy_id = default_policy_id
        self.command_queue: queue.Queue[Command] = queue.Queue(maxsize=capacity)
        self.safety_command_queue: queue.Queue[Command] = queue.Queue()
        self._lock = threading.Lock()
        self._dispatch_blocked_reason: str | None = None

    @property
    def dispatch_blocked_reason(self) -> str | None:
        with self._lock:
            return self._dispatch_blocked_reason

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
        outbound = Command(
            kind="POLICY_COMMAND",
            payload={
                "command": command,
                "inputs": dict(inputs or {}),
                "request_id": request_id,
                "policy_id": policy_id or self.default_policy_id,
            },
            tracked_command_id=tracked_command_id,
            retry_on_failure=retry_on_failure,
            attempt=attempt,
        )
        self._offer(outbound, safety=safety)

    def set_record_root(self, root: str | None) -> None:
        self._offer(Command(kind="SET_ROOT", payload={"root": root}))

    def take_next_command(self) -> Command | None:
        with self._lock:
            if self._dispatch_blocked_reason is not None:
                return None
            try:
                return self.safety_command_queue.get_nowait()
            except queue.Empty:
                try:
                    return self.command_queue.get_nowait()
                except queue.Empty:
                    return None

    def command_dispatch_allowed(self) -> bool:
        with self._lock:
            return self._dispatch_blocked_reason is None

    def block_command_dispatch(self, reason: str) -> bool:
        """Block normal admission and draining, returning whether state changed."""
        with self._lock:
            if self._dispatch_blocked_reason is not None:
                return False
            self._dispatch_blocked_reason = reason
            return True

    def _offer(self, command: Command, *, safety: bool = False) -> None:
        with self._lock:
            if self._dispatch_blocked_reason is not None and not safety:
                raise CommandMailboxUnavailable(
                    f"command dispatch is blocked: {self._dispatch_blocked_reason}"
                )
            if safety:
                self.safety_command_queue.put_nowait(command)
                return
            try:
                self.command_queue.put_nowait(command)
            except queue.Full as error:
                raise CommandMailboxUnavailable("command mailbox is full") from error
