"""Bounded Dora transport adapter for the Gateway Tool service."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Protocol, cast

import pyarrow as pa
from forge_msgs import ToolMessage, ToolMessageSizeError
from forge_tool import DEFAULT_MAX_MESSAGE_BYTES, ToolEnvelope, ToolProtocolError
from forge_tool.dora import tool_envelope_to_message, tool_message_to_envelope

if TYPE_CHECKING:
    from forge_gateway.services.runtime_service import GatewayRuntime
    from forge_gateway.services.tool_gateway_service import (
        ToolGatewaySweep,
        ToolOutboundMessage,
    )

_CARRIER_OVERHEAD_BYTES = 65_536
DEFAULT_MAX_TOOL_CARRIER_BYTES = DEFAULT_MAX_MESSAGE_BYTES + _CARRIER_OVERHEAD_BYTES
DEFAULT_TOOL_OUTPUT_DRAIN_LIMIT = 64


class ToolOutputNode(Protocol):
    def send_output(self, output_id: str, data: Any, /) -> None: ...


class _ArrowCarrier(Protocol):
    @property
    def nbytes(self) -> int: ...


def _positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _validate_arrow_carrier(value: object, maximum: int) -> None:
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise TypeError(
            "Tool inputs do not accept Arrow IPC bytes; decode them upstream "
            "under bounded framing and decompression limits"
        )
    if not isinstance(value, (pa.RecordBatch, pa.Table, pa.StructArray)):
        raise TypeError("Tool inputs require an in-memory Arrow carrier")
    carrier = cast(_ArrowCarrier, value)
    if carrier.nbytes > maximum:
        raise ToolProtocolError(
            "FORGE_PROTOCOL_MESSAGE_TOO_LARGE",
            f"Arrow carrier size {carrier.nbytes} exceeds limit {maximum}",
        )


def handle_tool_input(
    runtime: GatewayRuntime,
    input_id: str,
    value: object,
    *,
    received_at: float | None = None,
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    max_carrier_bytes: int = DEFAULT_MAX_TOOL_CARRIER_BYTES,
) -> ToolEnvelope:
    """Decode and deliver one ordered Tool input without performing Dora output."""
    if input_id not in runtime.tool_input_ids:
        raise ValueError(f"unconfigured Tool input: {input_id!r}")
    message_maximum = _positive_int(max_message_bytes, "max_message_bytes")
    carrier_maximum = _positive_int(max_carrier_bytes, "max_carrier_bytes")
    receive_observation = time.monotonic() if received_at is None else received_at
    _validate_arrow_carrier(value, carrier_maximum)
    try:
        message = ToolMessage.from_arrow(
            value,
            max_payload_json_bytes=message_maximum,
        )
    except ToolMessageSizeError as error:
        raise ToolProtocolError(
            "FORGE_PROTOCOL_MESSAGE_TOO_LARGE",
            str(error),
        ) from error
    envelope = tool_message_to_envelope(
        message,
        max_message_bytes=message_maximum,
    )
    runtime.tool_gateway.handle_input(
        input_id,
        envelope,
        received_at=receive_observation,
        processed_at=time.monotonic(),
    )
    return envelope


def _send_tool_output(
    runtime: GatewayRuntime,
    node: ToolOutputNode,
    message: ToolOutboundMessage,
    *,
    max_message_bytes: int,
) -> None:
    try:
        value = tool_envelope_to_message(
            message.envelope,
            max_message_bytes=max_message_bytes,
        ).to_arrow()
        node.send_output(message.output_id, value)
    except BaseException as error:
        _ = runtime.tool_gateway.output_failed(message, error)
        raise


def drain_tool_outputs(
    runtime: GatewayRuntime,
    node: ToolOutputNode,
    *,
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    max_messages: int = DEFAULT_TOOL_OUTPUT_DRAIN_LIMIT,
) -> tuple[ToolOutboundMessage, ...]:
    """Dispatch one bounded batch of Tool outputs on the lifecycle thread."""
    message_maximum = _positive_int(max_message_bytes, "max_message_bytes")
    batch_maximum = _positive_int(max_messages, "max_messages")
    dispatched: list[ToolOutboundMessage] = []
    for _ in range(batch_maximum):
        message = runtime.tool_gateway.take_outbound()
        if message is None:
            break
        _send_tool_output(
            runtime,
            node,
            message,
            max_message_bytes=message_maximum,
        )
        dispatched.append(message)
    return tuple(dispatched)


def sweep_tool_gateway(runtime: GatewayRuntime) -> ToolGatewaySweep:
    """Expire elapsed provider leases and pending Query deadlines."""
    return runtime.tool_gateway.sweep(now=time.monotonic())


__all__ = [
    "DEFAULT_MAX_TOOL_CARRIER_BYTES",
    "DEFAULT_TOOL_OUTPUT_DRAIN_LIMIT",
    "drain_tool_outputs",
    "handle_tool_input",
    "sweep_tool_gateway",
]
