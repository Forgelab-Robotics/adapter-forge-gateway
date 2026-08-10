"""Bounded Dora transport adapter for Tool endpoint Registry management."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Protocol

import pyarrow as pa
from forge_msgs import ToolMessage, ToolMessageSizeError
from forge_tool import (
    DEFAULT_MAX_MESSAGE_BYTES,
    ToolEnvelope,
    ToolProtocolError,
    make_endpoint_registry_response_envelope,
)
from forge_tool.dora import tool_envelope_to_message, tool_message_to_envelope

from forge_gateway.domain.endpoint_registry import EndpointSourceAuthority

if TYPE_CHECKING:
    from forge_gateway.domain.endpoint_registry import RegisteredEndpoint
    from forge_gateway.services.runtime_service import GatewayRuntime

_MANAGEMENT_MESSAGE_TYPES = frozenset(
    ("endpoint.register", "endpoint.heartbeat", "endpoint.unregister")
)
_CARRIER_OVERHEAD_BYTES = 65_536
DEFAULT_MAX_TOOL_CARRIER_BYTES = DEFAULT_MAX_MESSAGE_BYTES + _CARRIER_OVERHEAD_BYTES


class ToolOutputNode(Protocol):
    def send_output(self, output_id: str, data: Any, /) -> None: ...


def _positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _validate_arrow_carrier(value: object, maximum: int) -> None:
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise TypeError(
            "Tool Registry management does not accept Arrow IPC bytes; "
            "decode them upstream under bounded framing and decompression limits"
        )
    if not isinstance(value, (pa.RecordBatch, pa.Table, pa.StructArray)):
        raise TypeError("Tool Registry management requires an in-memory Arrow carrier")
    if value.nbytes > maximum:
        raise ToolProtocolError(
            "FORGE_PROTOCOL_MESSAGE_TOO_LARGE",
            f"Arrow carrier size {value.nbytes} exceeds limit {maximum}",
        )


def handle_tool_management_input(
    runtime: GatewayRuntime,
    node: ToolOutputNode,
    input_id: str,
    value: object,
    *,
    received_at: float | None = None,
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    max_carrier_bytes: int = DEFAULT_MAX_TOOL_CARRIER_BYTES,
) -> ToolEnvelope:
    """Handle one configured management input on the caller's lifecycle thread."""

    route = runtime.tool_management_route_for_input(input_id)
    if route is None:
        raise ValueError(f"unconfigured Tool management input: {input_id!r}")
    registry = runtime.endpoint_registry

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

    request = tool_message_to_envelope(
        message,
        max_message_bytes=message_maximum,
    )
    if request.message_type not in _MANAGEMENT_MESSAGE_TYPES:
        raise ValueError(
            "Tool management input accepts only endpoint.register, "
            "endpoint.heartbeat, or endpoint.unregister"
        )

    decision = registry.handle_management(
        request,
        EndpointSourceAuthority(
            source_id=route.source_id,
            generation=route.source_generation,
            endpoint_id=route.endpoint_id,
        ),
        now=receive_observation,
    )
    response = make_endpoint_registry_response_envelope(decision, request)
    response_value = tool_envelope_to_message(
        response,
        max_message_bytes=message_maximum,
    ).to_arrow()
    node.send_output(route.management_response_output_id, response_value)
    return response


def sweep_endpoint_registry(
    runtime: GatewayRuntime,
) -> tuple[RegisteredEndpoint, ...]:
    """Expire elapsed endpoint leases using a fresh monotonic observation."""

    return runtime.endpoint_registry.expire(time.monotonic())


def sweep_expired_endpoints(
    runtime: GatewayRuntime,
) -> tuple[RegisteredEndpoint, ...]:
    """Compatibility spelling for the endpoint Registry expiration sweep."""

    return sweep_endpoint_registry(runtime)


__all__ = [
    "DEFAULT_MAX_TOOL_CARRIER_BYTES",
    "handle_tool_management_input",
    "sweep_endpoint_registry",
    "sweep_expired_endpoints",
]
