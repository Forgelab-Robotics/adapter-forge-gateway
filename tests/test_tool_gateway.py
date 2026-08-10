"""Focused Gateway integration tests for Forge Tool endpoint discovery."""

from __future__ import annotations

import sys
import threading
from types import SimpleNamespace
from typing import Any

import pytest
from forge_gateway import cli
from forge_gateway.adapters import dora_runtime, tool_dora
from forge_gateway.adapters.dora_adapter import (
    DoraEventBuffer,
    DoraEventBufferOverflow,
)
from forge_gateway.adapters.dora_runtime import GatewayDoraRunner
from forge_gateway.adapters.tool_dora import (
    handle_tool_management_input,
    sweep_endpoint_registry,
)
from forge_gateway.services.runtime_service import GatewayRuntime
from forge_msgs import ToolMessage
from forge_tool import (
    TOOL_ENDPOINT_PROTOCOL,
    ToolEndpointDescriptor,
    ToolOperationDescriptor,
    ToolProtocolError,
    endpoint_registry_response_from_payload,
    make_heartbeat_envelope,
    make_registration_envelope,
    validate_management_response_correlation,
)
from forge_tool.dora import tool_envelope_to_message, tool_message_to_envelope

from forge_gateway.config import GatewayConfig


def _route(suffix: str = "yolo") -> dict[str, object]:
    return {
        "endpoint_id": f"vision.{suffix}",
        "source_id": f"dora:{suffix}",
        "source_generation": 3,
        "management_input_id": f"{suffix}/management",
        "management_response_output_id": f"{suffix}/management_response",
        "tool_request_output_id": f"{suffix}/tool_request",
        "tool_response_input_id": f"{suffix}/tool_response",
    }


def _config(*, lease_ttl_ms: int = 15_000) -> GatewayConfig:
    return GatewayConfig.from_dict(
        {
            "joint_order": [],
            "agent": {"action_manifests": []},
            "tool_registry": {
                "enabled": True,
                "lease_ttl_ms": lease_ttl_ms,
                "routes": [_route()],
            },
        }
    )


def _registration(
    *,
    endpoint_id: str = "vision.yolo",
    request_id: str = "register-1",
):
    descriptor = ToolEndpointDescriptor(
        protocol_version=TOOL_ENDPOINT_PROTOCOL,
        endpoint_id=endpoint_id,
        operations=(ToolOperationDescriptor(name="detect", semantics="query"),),
    )
    return make_registration_envelope(
        descriptor,
        endpoint_instance_id="instance-1",
        request_id=request_id,
    )


def _carrier(envelope: object) -> object:
    return tool_envelope_to_message(envelope).to_arrow()  # type: ignore[arg-type]


class _Node:
    def __init__(self, events: list[dict[str, Any]] | None = None) -> None:
        self.outputs: list[tuple[str, Any]] = []
        self.send_thread_ids: list[int] = []
        self.events = events or []

    def __iter__(self):
        return iter(self.events)

    def send_output(self, output_id: str, data: Any, /) -> None:
        self.send_thread_ids.append(threading.get_ident())
        self.outputs.append((output_id, data))


def test_cli_allows_registry_only_gateway_without_joint_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Application:
        def __init__(self, config: GatewayConfig) -> None:
            assert config.tool_registry.enabled is True

        def start(self) -> None:
            pass

        def run(self) -> str:
            return "stop"

        def close(self) -> SimpleNamespace:
            return SimpleNamespace(ok=True)

    monkeypatch.setattr(cli, "load_config", lambda path: _config())
    monkeypatch.setattr(cli, "GatewayApplication", _Application)
    monkeypatch.setattr(
        cli, "_install_shutdown_handlers", lambda application: lambda: None
    )
    monkeypatch.setattr(sys, "argv", ["gateway"])

    assert cli.main() == 0


def test_tool_registry_defaults_disabled_and_builds_runtime_route_indexes() -> None:
    default = GatewayConfig.from_dict(
        {"joint_order": [], "agent": {"action_manifests": []}}
    )
    assert default.tool_registry.enabled is False
    assert default.tool_registry.lease_ttl_ms == 15_000
    assert default.tool_registry.routes == []
    disabled_runtime = GatewayRuntime(default)
    try:
        assert disabled_runtime.endpoint_registry.lease_ttl_ms == 15_000
        assert disabled_runtime.tool_input_ids == frozenset()
    finally:
        disabled_runtime.close()

    runtime = GatewayRuntime(_config(lease_ttl_ms=2_500))
    try:
        assert runtime.endpoint_registry is not None
        assert runtime.endpoint_registry.lease_ttl_ms == 2_500
        assert runtime.tool_route_for_endpoint("vision.yolo") is not None
        assert runtime.tool_management_route_for_input("yolo/management") is not None
        assert runtime.tool_response_route_for_input("yolo/tool_response") is not None
        assert runtime.tool_route_for_input("yolo/tool_response") is not None
        assert runtime.tool_management_input_ids == frozenset({"yolo/management"})
        assert runtime.tool_response_input_ids == frozenset({"yolo/tool_response"})
        assert runtime.tool_input_ids == frozenset(
            {"yolo/management", "yolo/tool_response"}
        )
        assert runtime.tool_control_input_ids == runtime.tool_input_ids
        assert runtime.action_registry is not runtime.endpoint_registry
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("tool_registry", "match"),
    [
        pytest.param({"unknown": True}, "unknown", id="unknown-registry-key"),
        pytest.param({"enabled": "true"}, "enabled", id="strict-enabled"),
        pytest.param({"lease_ttl_ms": 0}, "lease_ttl_ms", id="positive-ttl"),
        pytest.param({"routes": {}}, "routes", id="routes-must-be-list"),
        pytest.param(
            {"enabled": True, "routes": []},
            "must not be empty",
            id="enabled-registry-needs-route",
        ),
        pytest.param(
            {"routes": [{**_route(), "unknown": "value"}]},
            "unknown",
            id="unknown-route-key",
        ),
        pytest.param(
            {
                "routes": [
                    {
                        key: value
                        for key, value in _route().items()
                        if key != "source_id"
                    }
                ]
            },
            "source_id",
            id="missing-route-key",
        ),
        pytest.param(
            {"routes": [{**_route(), "source_generation": -1}]},
            "source_generation",
            id="nonnegative-generation",
        ),
    ],
)
def test_tool_registry_config_is_strict(
    tool_registry: object,
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        GatewayConfig.from_dict(
            {
                "joint_order": [],
                "agent": {"action_manifests": []},
                "tool_registry": tool_registry,
            }
        )


@pytest.mark.parametrize(
    ("field_name", "identifier"),
    [
        ("management_input_id", "tick"),
        ("tool_response_input_id", "proprio_state"),
        ("management_response_output_id", "policy_command"),
        ("tool_request_output_id", "policy_command"),
    ],
)
def test_tool_registry_rejects_reserved_gateway_ids(
    field_name: str,
    identifier: str,
) -> None:
    route = _route()
    route[field_name] = identifier

    with pytest.raises(ValueError, match="reserved Gateway"):
        GatewayConfig.from_dict(
            {
                "joint_order": [],
                "agent": {"action_manifests": []},
                "tool_registry": {"routes": [route]},
            }
        )


def test_tool_registry_rejects_image_input_id_collisions() -> None:
    route = _route()
    route["management_input_id"] = "image/front"

    with pytest.raises(ValueError, match="reserved Gateway input"):
        GatewayConfig.from_dict(
            {
                "joint_order": [],
                "image_input_ids": ["image/front"],
                "agent": {"action_manifests": []},
                "tool_registry": {"routes": [route]},
            }
        )


@pytest.mark.parametrize("duplicate_kind", ["endpoint", "input", "output"])
def test_tool_registry_rejects_duplicate_route_ids(duplicate_kind: str) -> None:
    first = _route("one")
    second = _route("two")
    if duplicate_kind == "endpoint":
        second["endpoint_id"] = first["endpoint_id"]
    elif duplicate_kind == "input":
        second["management_input_id"] = first["tool_response_input_id"]
    else:
        second["tool_request_output_id"] = first["management_response_output_id"]

    with pytest.raises(ValueError, match=f"duplicate Tool route {duplicate_kind} ID"):
        GatewayConfig.from_dict(
            {
                "joint_order": [],
                "agent": {"action_manifests": []},
                "tool_registry": {"routes": [first, second]},
            }
        )


def test_tool_inputs_use_fifo_order_and_surface_overflow() -> None:
    buffer = DoraEventBuffer(
        fifo_input_ids={"yolo/management", "yolo/tool_response"},
        fifo_capacity=2,
    )
    first = {"type": "INPUT", "id": "yolo/management", "value": "first"}
    second = {"type": "INPUT", "id": "yolo/tool_response", "value": "second"}
    buffer.put(first)
    buffer.put(second)

    with pytest.raises(DoraEventBufferOverflow, match="capacity 2"):
        buffer.put({"type": "INPUT", "id": "yolo/management", "value": "third"})

    assert buffer.get(timeout=0.0) == first
    assert buffer.get(timeout=0.0) == second
    assert buffer.get(timeout=0.0) is None


def test_tool_fifo_is_processed_before_accumulated_ticks() -> None:
    buffer = DoraEventBuffer(
        fifo_input_ids={"yolo/management"},
        fifo_capacity=2,
    )
    buffer.put({"type": "INPUT", "id": "tick", "value": "first-tick"})
    management = {"type": "INPUT", "id": "yolo/management", "value": "register"}
    buffer.put(management)
    buffer.put({"type": "INPUT", "id": "tick", "value": "second-tick"})

    assert buffer.get(timeout=0.0) == management
    assert buffer.get(timeout=0.0) == {"type": "INPUT", "id": "tick", "value": None}
    assert buffer.get(timeout=0.0) == {"type": "INPUT", "id": "tick", "value": None}


@pytest.mark.parametrize(
    ("terminal_event", "expected_reason"),
    [
        pytest.param(None, "eof", id="natural-eof"),
        pytest.param({"type": "STOP"}, "stop", id="stop"),
    ],
)
def test_received_management_precedes_later_terminal_event(
    terminal_event: dict[str, Any] | None,
    expected_reason: str,
) -> None:
    runtime = GatewayRuntime(_config())
    events = [
        {
            "type": "INPUT",
            "id": "yolo/management",
            "value": _carrier(_registration()),
        }
    ]
    if terminal_event is not None:
        events.append(terminal_event)
    node = _Node(events)
    runner = GatewayDoraRunner(
        runtime=runtime,
        node=node,
        stop_event=threading.Event(),
    )
    try:
        runner.start()
        assert runner.join_reader(timeout=1.0)

        assert runner.run() == expected_reason
        assert [output_id for output_id, _ in node.outputs] == [
            "yolo/management_response"
        ]
        assert len(runtime.endpoint_registry.registrations(now=0.0)) == 1
    finally:
        runtime.close()


def test_management_adapter_sends_correlated_accepted_response() -> None:
    runtime = GatewayRuntime(_config())
    node = _Node()
    request = _registration()
    try:
        response = handle_tool_management_input(
            runtime,
            node,
            "yolo/management",
            _carrier(request),
        )

        assert [output_id for output_id, _ in node.outputs] == [
            "yolo/management_response"
        ]
        output_message = ToolMessage.from_arrow(node.outputs[0][1])
        output_envelope = tool_message_to_envelope(output_message)
        assert output_envelope == response
        validate_management_response_correlation(request, output_envelope)
        decision = endpoint_registry_response_from_payload(output_envelope.payload)
        assert decision.status == "accepted"
        assert decision.operation == "register"
        assert response.request_id == "register-1"
    finally:
        runtime.close()


def test_management_adapter_correlates_route_identity_rejection() -> None:
    runtime = GatewayRuntime(_config())
    node = _Node()
    request = _registration(endpoint_id="vision.other")
    try:
        response = handle_tool_management_input(
            runtime,
            node,
            "yolo/management",
            _carrier(request),
        )

        validate_management_response_correlation(request, response)
        decision = endpoint_registry_response_from_payload(response.payload)
        assert decision.status == "rejected"
        assert decision.error is not None
        assert decision.error.code == "FORGE_ENDPOINT_SOURCE_UNAUTHORIZED"
        assert runtime.endpoint_registry is not None
        assert runtime.endpoint_registry.registrations(now=0.0) == ()
    finally:
        runtime.close()


@pytest.mark.parametrize("message_type", ["endpoint.heartbeat", "endpoint.unregister"])
def test_management_adapter_rejects_invalid_empty_payload_before_state_change(
    message_type: str,
) -> None:
    runtime = GatewayRuntime(_config())
    node = _Node()
    register = _registration()
    try:
        handle_tool_management_input(
            runtime,
            node,
            "yolo/management",
            _carrier(register),
            received_at=10.0,
        )
        invalid = ToolMessage.from_payload(
            message_type=message_type,  # type: ignore[arg-type]
            request_id=f"invalid-{message_type}",
            endpoint_id="vision.yolo",
            endpoint_instance_id="instance-1",
            payload={"unexpected": True},
        ).to_arrow()

        with pytest.raises(ToolProtocolError, match="unknown fields"):
            handle_tool_management_input(
                runtime,
                node,
                "yolo/management",
                invalid,
                received_at=10.1,
            )

        current = runtime.endpoint_registry.resolve("vision.yolo", "detect", now=10.1)
        assert current is not None
        assert current.expires_at == 25.0
        assert len(node.outputs) == 1
    finally:
        runtime.close()


def test_management_adapter_rejects_bytes_and_carrier_size_before_typed_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = GatewayRuntime(_config())
    node = _Node()

    class _DecodeMustNotRun:
        @classmethod
        def from_arrow(cls, value: object, **kwargs: object) -> object:
            del cls, value, kwargs
            raise AssertionError("typed decode ran")

    monkeypatch.setattr(tool_dora, "ToolMessage", _DecodeMustNotRun)
    try:
        with pytest.raises(TypeError, match="does not accept Arrow IPC bytes"):
            handle_tool_management_input(runtime, node, "yolo/management", b"raw")
        with pytest.raises(ToolProtocolError, match="Arrow carrier size"):
            handle_tool_management_input(
                runtime,
                node,
                "yolo/management",
                _carrier(_registration()),
                max_carrier_bytes=1,
            )
        assert node.outputs == []
    finally:
        runtime.close()


def test_queued_heartbeat_uses_reader_receive_time_before_expiration_sweep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observations = iter((50.0, 51.1))
    monkeypatch.setattr(
        tool_dora,
        "time",
        SimpleNamespace(monotonic=lambda: next(observations)),
    )
    runtime = GatewayRuntime(_config(lease_ttl_ms=1_000))
    node = _Node()
    runner = GatewayDoraRunner(
        runtime=runtime,
        node=node,
        stop_event=threading.Event(),
    )
    register = _registration()
    heartbeat = make_heartbeat_envelope(
        endpoint_id="vision.yolo",
        endpoint_instance_id="instance-1",
        request_id="heartbeat-1",
    )
    try:
        runner.handle_poll(
            {
                "type": "INPUT",
                "id": "yolo/management",
                "value": _carrier(register),
                dora_runtime._TOOL_RECEIVED_AT_KEY: 50.0,
            }
        )
        runner.handle_poll(
            {
                "type": "INPUT",
                "id": "yolo/management",
                "value": _carrier(heartbeat),
                dora_runtime._TOOL_RECEIVED_AT_KEY: 50.9,
            }
        )

        current = runtime.endpoint_registry.resolve("vision.yolo", "detect", now=51.1)
        assert current is not None
        assert current.expires_at == 51.9
    finally:
        runtime.close()


def test_runner_sends_management_response_on_lifecycle_thread() -> None:
    runtime = GatewayRuntime(_config())
    node = _Node()
    runner = GatewayDoraRunner(
        runtime=runtime,
        node=node,
        stop_event=threading.Event(),
    )
    lifecycle_thread_id = threading.get_ident()
    try:
        assert (
            runner.handle_poll(
                {
                    "type": "INPUT",
                    "id": "yolo/management",
                    "value": _carrier(_registration()),
                }
            )
            is None
        )

        assert node.send_thread_ids == [lifecycle_thread_id]
    finally:
        runtime.close()


def test_runner_sweeps_registry_on_every_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = GatewayRuntime(_config())
    calls: list[GatewayRuntime] = []
    monkeypatch.setattr(
        dora_runtime,
        "sweep_expired_endpoints",
        lambda runtime_arg: calls.append(runtime_arg),
    )
    try:
        runner = GatewayDoraRunner(
            runtime=runtime,
            node=_Node(),
            stop_event=threading.Event(),
        )
        assert runner.handle_poll(None) is None
        assert runner.handle_poll({"type": "UNKNOWN"}) is None
        assert calls == [runtime, runtime]
    finally:
        runtime.close()


def test_expiration_sweep_uses_monotonic_observations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observations = iter((50.0, 50.999, 51.0))
    monkeypatch.setattr(
        tool_dora,
        "time",
        SimpleNamespace(monotonic=lambda: next(observations)),
    )
    runtime = GatewayRuntime(_config(lease_ttl_ms=1_000))
    node = _Node()
    try:
        handle_tool_management_input(
            runtime,
            node,
            "yolo/management",
            _carrier(_registration()),
        )

        assert sweep_endpoint_registry(runtime) == ()
        expired = sweep_endpoint_registry(runtime)
        assert [registration.endpoint_id for registration in expired] == ["vision.yolo"]
    finally:
        runtime.close()
