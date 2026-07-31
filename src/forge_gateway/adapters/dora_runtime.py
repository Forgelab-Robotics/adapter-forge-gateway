"""Dora reader thread and lifecycle loop for Gateway."""

from __future__ import annotations

from collections.abc import Iterator
import threading
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

if TYPE_CHECKING:
    from forge_gateway.services.runtime_service import GatewayRuntime

try:
    from forge_common import get_logger
except Exception:  # pragma: no cover - fallback for minimal test envs
    import logging

    def get_logger(name: str) -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

from forge_gateway.adapters.dora_adapter import (
    DoraEventBuffer,
    drain_commands,
    handle_dora_input,
)

logger = get_logger("forge_gateway.cli")

DoraRunReason = Literal["stop", "error", "eof", "shutdown"]


class DoraNodeLike(Protocol):
    def __iter__(self) -> Iterator[dict[str, Any]]: ...

    def send_output(self, output_id: str, data: Any, /) -> None: ...


class GatewayDoraRunner:
    """Read Dora events in the background and process them on one lifecycle thread."""

    def __init__(
        self,
        *,
        runtime: GatewayRuntime,
        node: DoraNodeLike,
        stop_event: threading.Event,
        poll_timeout: float = 0.2,
    ) -> None:
        self._runtime = runtime
        self._node = node
        self._stop_event = stop_event
        self._poll_timeout = poll_timeout
        self._events = DoraEventBuffer()
        self._end_sentinel = object()
        self._reader_thread = threading.Thread(
            target=self._read_node,
            name="gateway_dora_iter",
            daemon=True,
        )

    def start(self) -> None:
        self._reader_thread.start()

    def run(self) -> DoraRunReason:
        while not self._stop_event.is_set():
            event = self._events.get(timeout=self._poll_timeout)
            if event is self._end_sentinel:
                return "eof"
            reason = self.handle_poll(cast(dict[str, Any] | None, event))
            if reason is not None:
                return reason
        return "shutdown"

    def handle_poll(self, event: dict[str, Any] | None) -> DoraRunReason | None:
        """Process one event-buffer poll result; ``None`` represents a timeout."""
        if event is None:
            drain_commands(self._runtime, self._node)
            return None

        event_type = event.get("type")
        if event_type == "STOP":
            return "stop"
        if event_type == "ERROR":
            with self._runtime.lock:
                self._runtime.last_error = f"dora error: {event.get('error', 'unknown')}"
            return "error"
        if event_type == "INPUT":
            try:
                handle_dora_input(
                    self._runtime,
                    str(event.get("id")),
                    event.get("value"),
                )
            except Exception as error:
                logger.warning(
                    "gateway: handle input %s failed: %s",
                    event.get("id"),
                    error,
                )
                with self._runtime.lock:
                    self._runtime.last_error = (
                        f"input {event.get('id')} failed: {error}"
                    )

        drain_commands(self._runtime, self._node)
        return None

    def join_reader(self, timeout: float) -> bool:
        if self._reader_thread.is_alive():
            self._reader_thread.join(timeout=timeout)
        return not self._reader_thread.is_alive()

    def _read_node(self) -> None:
        try:
            for event in self._node:
                if self._stop_event.is_set():
                    break
                self._events.put(event)
        except Exception as error:
            logger.exception("gateway: dora iteration failed: %s", error)
            self._events.put({"type": "ERROR", "error": str(error)})
        finally:
            self._events.put(self._end_sentinel)
