"""Dora reader thread and lifecycle loop for Gateway."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
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

_TOOL_RECEIVED_AT_KEY = "_forge_gateway_received_monotonic"

DoraRunReason = Literal[
    "stop",
    "error",
    "reader_error",
    "eof",
    "shutdown",
]


class DoraNodeLike(Protocol):
    def __iter__(self) -> Iterator[dict[str, Any]]: ...

    def send_output(self, output_id: str, data: Any, /) -> None: ...


def handle_tool_management_input(
    runtime: GatewayRuntime,
    node: DoraNodeLike,
    input_id: str,
    value: object,
    *,
    received_at: float | None = None,
) -> object:
    from forge_gateway.adapters.tool_dora import handle_tool_management_input as handle

    return handle(runtime, node, input_id, value, received_at=received_at)


def sweep_expired_endpoints(runtime: GatewayRuntime) -> tuple[object, ...]:
    if not runtime.config.tool_registry.enabled:
        return ()
    from forge_gateway.adapters.tool_dora import sweep_expired_endpoints as sweep

    return sweep(runtime)


class GatewayDoraRunner:
    """Read Dora events in the background and process them on one lifecycle thread."""

    def __init__(
        self,
        *,
        runtime: GatewayRuntime,
        node: DoraNodeLike,
        stop_event: threading.Event,
        poll_timeout: float = 0.2,
        tool_input_fifo_capacity: int = 256,
    ) -> None:
        self._runtime = runtime
        self._node = node
        self._stop_event = stop_event
        self._poll_timeout = poll_timeout
        self._events = DoraEventBuffer(
            fifo_input_ids=runtime.tool_input_ids,
            fifo_capacity=tool_input_fifo_capacity,
        )
        self._end_sentinel = object()
        self._reader_started = threading.Event()
        self._reader_error_lock = threading.Lock()
        self._reader_error: BaseException | None = None
        self._reader_thread = threading.Thread(
            target=self._read_node,
            name="gateway_dora_iter",
            daemon=True,
        )

    @property
    def reader_error(self) -> BaseException | None:
        with self._reader_error_lock:
            return self._reader_error

    def start(self) -> None:
        self._reader_thread.start()
        if not self._reader_started.wait(timeout=1.0):
            raise RuntimeError("Dora reader thread did not start")

    def run(self) -> DoraRunReason:
        while True:
            event = self._events.get_priority()
            if event is None:
                if self._apply_reader_error():
                    return "reader_error"
                if self._stop_event.is_set():
                    return "shutdown"
                event = self._events.get(timeout=self._poll_timeout)
            if event is self._end_sentinel:
                return "eof"
            reason = self.handle_poll(cast(dict[str, Any] | None, event))
            if reason is not None:
                return reason

    def handle_poll(self, event: dict[str, Any] | None) -> DoraRunReason | None:
        """Process one event-buffer poll result; ``None`` represents a timeout."""
        if event is None:
            sweep_expired_endpoints(self._runtime)
            drain_commands(self._runtime, self._node)
            return None

        event_type = event.get("type")
        if event_type == "STOP":
            return "stop"
        if event_type == "ERROR":
            with self._runtime.lock:
                self._runtime.last_error = (
                    f"dora error: {event.get('error', 'unknown')}"
                )
            return "error"
        if event_type == "READER_ERROR":
            self._apply_reader_error()
            return "reader_error"
        if event_type == "INPUT":
            input_id = str(event.get("id"))
            try:
                if input_id in self._runtime.tool_management_input_ids:
                    handle_tool_management_input(
                        self._runtime,
                        self._node,
                        input_id,
                        event.get("value"),
                        received_at=event.get(_TOOL_RECEIVED_AT_KEY),
                    )
                else:
                    handle_dora_input(
                        self._runtime,
                        input_id,
                        event.get("value"),
                    )
            except Exception as error:
                logger.warning(
                    "gateway: handle input %s failed: %s",
                    input_id,
                    error,
                )
                with self._runtime.lock:
                    self._runtime.last_error = f"input {input_id} failed: {error}"

        sweep_expired_endpoints(self._runtime)
        drain_commands(self._runtime, self._node)
        return None

    def join_reader(self, timeout: float) -> bool:
        if self._reader_thread.is_alive():
            self._reader_thread.join(timeout=timeout)
        return not self._reader_thread.is_alive()

    def _apply_reader_error(self) -> bool:
        error = self.reader_error
        if error is None:
            return False
        with self._runtime.lock:
            self._runtime.last_error = f"dora reader failed: {error}"
        return True

    def _read_node(self) -> None:
        self._reader_started.set()
        try:
            for event in self._node:
                received_at = time.monotonic()
                if self._stop_event.is_set():
                    break
                if (
                    isinstance(event, dict)
                    and event.get("type") == "INPUT"
                    and str(event.get("id")) in self._runtime.tool_management_input_ids
                ):
                    event = dict(event)
                    event[_TOOL_RECEIVED_AT_KEY] = received_at
                self._events.put(event)
        except BaseException as error:
            with self._reader_error_lock:
                self._reader_error = error
            logger.exception("gateway: dora iteration failed: %s", error)
            self._events.put({"type": "READER_ERROR", "error": str(error)})
        finally:
            self._events.put(self._end_sentinel)
