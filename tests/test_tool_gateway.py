"""Gateway Tool discovery, routing, Dora, and HTTP integration tests."""

from __future__ import annotations

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from forge_gateway.adapters import dora_runtime, tool_dora
from forge_gateway.adapters.dora_adapter import (
    DoraEventBuffer,
    DoraEventBufferOverflow,
)
from forge_gateway.adapters.dora_runtime import GatewayDoraRunner
from forge_gateway.adapters.tool_dora import (
    drain_tool_outputs,
    handle_tool_input,
)
from forge_gateway.controllers.tool_controller import register_tool_routes
from forge_gateway.services.runtime_service import GatewayRuntime
from forge_gateway.services.tool_gateway_service import (
    ToolGatewayMailboxFull,
    ToolGatewayService,
    ToolGatewayUnavailable,
    make_logical_invoke_request_envelope,
)
from forge_tool import (
    TOOL_ENDPOINT_PROTOCOL,
    ToolContext,
    ToolEndpointDescriptor,
    ToolEnvelope,
    ToolError,
    ToolExecutionKey,
    ToolOperationDescriptor,
    ToolProtocolError,
    ToolRequest,
    ToolResult,
    endpoint_registry_response_from_payload,
    error_from_payload,
    invoke_request_from_envelope,
    invoke_request_to_payload,
    make_error_response_envelope,
    make_invoke_response_envelope,
    make_registration_envelope,
    make_unregister_envelope,
    validate_management_response_correlation,
    validate_response_correlation,
)
from forge_tool._tool_message import ToolMessage
from forge_tool.dora import tool_envelope_to_message, tool_message_to_envelope

from forge_gateway import cli
from forge_gateway.config import GatewayConfig


def _provider(suffix: str = "yolo") -> dict[str, str]:
    return {
        "endpoint_id": f"vision.{suffix}",
        "input_id": f"{suffix}/to_gateway",
        "output_id": f"gateway/to_{suffix}",
    }


def _config(
    *,
    lease_ttl_ms: int = 15_000,
    invoke_timeout_ms: int = 5_000,
    providers: list[dict[str, str]] | None = None,
) -> GatewayConfig:
    return GatewayConfig.from_dict(
        {
            "joint_order": [],
            "agent": {"action_manifests": []},
            "tools": {
                "enabled": True,
                "lease_ttl_ms": lease_ttl_ms,
                "invoke_timeout_ms": invoke_timeout_ms,
                "request_input_id": "caller/tool_request",
                "response_output_id": "gateway/tool_response",
                "providers": providers or [_provider()],
            },
        }
    )


def _descriptor(
    *,
    endpoint_id: str = "vision.yolo",
    operation: str = "detect",
) -> ToolEndpointDescriptor:
    return ToolEndpointDescriptor(
        protocol_version=TOOL_ENDPOINT_PROTOCOL,
        endpoint_id=endpoint_id,
        operations=(ToolOperationDescriptor(name=operation, semantics="query"),),
    )


def _registration(
    *,
    endpoint_id: str = "vision.yolo",
    instance_id: str = "instance-1",
    operation: str = "detect",
    request_id: str = "register-1",
) -> ToolEnvelope:
    return make_registration_envelope(
        _descriptor(endpoint_id=endpoint_id, operation=operation),
        endpoint_instance_id=instance_id,
        request_id=request_id,
    )


def _invoke_request(
    *,
    endpoint_id: str = "vision.yolo",
    operation: str = "detect",
    endpoint_instance_id: str | None = None,
    request_id: str = "request-1",
    invocation_id: str = "invocation-1",
    attempt_id: str = "attempt-1",
    arguments: dict[str, object] | None = None,
    deadline_ms: int | None = None,
) -> ToolEnvelope:
    context = ToolContext(
        execution_key=ToolExecutionKey(
            invocation_id=invocation_id,
            attempt_id=attempt_id,
        ),
        tool_id=endpoint_id,
        implementation_id=endpoint_id,
        endpoint_id=endpoint_id,
        operation=operation,
        caller_id="dora:test",
        deadline_ms=deadline_ms,
    )
    if endpoint_instance_id is None:
        return make_logical_invoke_request_envelope(
            ToolRequest(arguments or {"image_id": "front"}),
            context,
            request_id=request_id,
        )
    return ToolEnvelope(
        protocol=TOOL_ENDPOINT_PROTOCOL,
        message_type="tool.invoke.request",
        request_id=request_id,
        invocation_id=invocation_id,
        attempt_id=attempt_id,
        endpoint_id=endpoint_id,
        endpoint_instance_id=endpoint_instance_id,
        operation=operation,
        payload=invoke_request_to_payload(
            ToolRequest(arguments or {"image_id": "front"}),
            context,
        ),
    )


def _carrier(envelope: ToolEnvelope) -> object:
    return tool_envelope_to_message(envelope).to_arrow()


def _decode(value: object) -> ToolEnvelope:
    return tool_message_to_envelope(ToolMessage.from_arrow(value))


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


def _announce(
    runtime: GatewayRuntime,
    *,
    now: float | None = None,
) -> ToolEnvelope:
    observation = time.monotonic() if now is None else now
    runtime.tool_gateway.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=observation,
    )
    outbound = runtime.tool_gateway.take_outbound()
    assert outbound is not None
    assert outbound.output_id == "gateway/to_yolo"
    return outbound.envelope


def _wait_for(predicate: Any, *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition was not satisfied before timeout")


def test_cli_allows_tool_only_gateway_without_joint_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Application:
        def __init__(self, config: GatewayConfig) -> None:
            assert config.tools.enabled is True

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


def test_tools_defaults_and_runtime_input_indexes() -> None:
    default = GatewayConfig.from_dict(
        {"joint_order": [], "agent": {"action_manifests": []}}
    )
    assert default.tools.enabled is False
    assert default.tools.lease_ttl_ms == 15_000
    assert default.tools.invoke_timeout_ms == 5_000
    assert default.tools.request_input_id == "tool_request"
    assert default.tools.response_output_id == "tool_response"
    assert default.tools.providers == []
    disabled_runtime = GatewayRuntime(default)
    try:
        assert disabled_runtime.tool_gateway.lease_ttl_ms == 15_000
        assert not hasattr(disabled_runtime.tool_gateway, "directory")
        assert disabled_runtime.tool_input_ids == frozenset()
    finally:
        disabled_runtime.close()

    runtime = GatewayRuntime(_config(lease_ttl_ms=2_500))
    try:
        assert runtime.tool_provider_input_ids == frozenset({"yolo/to_gateway"})
        assert runtime.tool_caller_input_ids == frozenset({"caller/tool_request"})
        assert runtime.tool_input_ids == frozenset(
            {"yolo/to_gateway", "caller/tool_request"}
        )
        assert runtime.action_registry is not runtime.tool_gateway
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("tools", "match"),
    [
        pytest.param({"unknown": True}, "unknown", id="unknown-tools-key"),
        pytest.param({"enabled": "true"}, "enabled", id="strict-enabled"),
        pytest.param({"lease_ttl_ms": 0}, "lease_ttl_ms", id="positive-ttl"),
        pytest.param(
            {"invoke_timeout_ms": True},
            "invoke_timeout_ms",
            id="strict-invoke-timeout",
        ),
        pytest.param({"providers": {}}, "providers", id="providers-must-be-list"),
        pytest.param(
            {"enabled": True, "providers": []},
            "must not be empty",
            id="enabled-tools-needs-provider",
        ),
        pytest.param(
            {"providers": [{**_provider(), "unknown": "value"}]},
            "unknown",
            id="unknown-provider-key",
        ),
        pytest.param(
            {
                "providers": [
                    {
                        key: value
                        for key, value in _provider().items()
                        if key != "input_id"
                    }
                ]
            },
            "input_id",
            id="missing-provider-key",
        ),
        pytest.param(
            {"providers": [{**_provider(), "input_id": " "}]},
            "input_id",
            id="blank-provider-port",
        ),
    ],
)
def test_tools_config_is_strict(tools: object, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        GatewayConfig.from_dict(
            {
                "joint_order": [],
                "agent": {"action_manifests": []},
                "tools": tools,
            }
        )


def test_unmerged_tool_registry_key_has_no_backward_compatibility() -> None:
    with pytest.raises(ValueError, match="tool_registry"):
        GatewayConfig.from_dict(
            {
                "joint_order": [],
                "agent": {"action_manifests": []},
                "tool_registry": {},
            }
        )


@pytest.mark.parametrize(
    ("location", "identifier"),
    [
        ("request_input_id", "tick"),
        ("provider_input_id", "proprio_state"),
        ("response_output_id", "policy_command"),
        ("provider_output_id", "policy_command"),
    ],
)
def test_tools_reject_reserved_gateway_ports(
    location: str,
    identifier: str,
) -> None:
    tools: dict[str, object] = {"enabled": True, "providers": [_provider()]}
    if location == "provider_input_id":
        tools["providers"] = [{**_provider(), "input_id": identifier}]
    elif location == "provider_output_id":
        tools["providers"] = [{**_provider(), "output_id": identifier}]
    else:
        tools[location] = identifier

    with pytest.raises(ValueError, match="reserved Gateway"):
        GatewayConfig.from_dict(
            {
                "joint_order": [],
                "agent": {"action_manifests": []},
                "tools": tools,
            }
        )


def test_tools_reject_image_input_collision() -> None:
    with pytest.raises(ValueError, match="reserved Gateway input"):
        GatewayConfig.from_dict(
            {
                "joint_order": [],
                "image_input_ids": ["image/front"],
                "agent": {"action_manifests": []},
                "tools": {
                    "enabled": True,
                    "providers": [{**_provider(), "input_id": "image/front"}],
                },
            }
        )


@pytest.mark.parametrize("duplicate_kind", ["endpoint", "input", "output"])
def test_tools_reject_duplicate_provider_or_public_ports(
    duplicate_kind: str,
) -> None:
    first = _provider("one")
    second = _provider("two")
    tools: dict[str, object] = {
        "enabled": True,
        "providers": [first, second],
    }
    if duplicate_kind == "endpoint":
        second["endpoint_id"] = first["endpoint_id"]
    elif duplicate_kind == "input":
        tools["request_input_id"] = first["input_id"]
    else:
        tools["response_output_id"] = second["output_id"]

    with pytest.raises(ValueError, match=f"duplicate Tool {duplicate_kind} ID"):
        GatewayConfig.from_dict(
            {
                "joint_order": [],
                "agent": {"action_manifests": []},
                "tools": tools,
            }
        )


def test_all_tool_inputs_share_one_ordered_bounded_fifo() -> None:
    buffer = DoraEventBuffer(
        fifo_input_ids={"yolo/to_gateway", "caller/tool_request"},
        fifo_capacity=2,
    )
    provider = {"type": "INPUT", "id": "yolo/to_gateway", "value": "register"}
    caller = {
        "type": "INPUT",
        "id": "caller/tool_request",
        "value": "invoke",
    }
    buffer.put(provider)
    buffer.put(caller)

    with pytest.raises(DoraEventBufferOverflow, match="capacity 2"):
        buffer.put(
            {"type": "INPUT", "id": "yolo/to_gateway", "value": "unregister"}
        )

    assert buffer.get(timeout=0.0) == provider
    assert buffer.get(timeout=0.0) == caller


def test_provider_register_ack_uses_shared_output_and_is_not_public() -> None:
    runtime = GatewayRuntime(_config())
    node = _Node()
    request = _registration()
    try:
        handled = handle_tool_input(
            runtime,
            "yolo/to_gateway",
            _carrier(request),
            received_at=10.0,
        )
        assert handled == request
        assert node.outputs == []

        dispatched = drain_tool_outputs(runtime, node)

        assert len(dispatched) == 1
        assert [output_id for output_id, _ in node.outputs] == ["gateway/to_yolo"]
        response = _decode(node.outputs[0][1])
        validate_management_response_correlation(request, response)
        decision = endpoint_registry_response_from_payload(response.payload)
        assert decision.status == "accepted"
        assert decision.registry_revision == 1
        assert runtime.tool_gateway.endpoint_registrations(now=10.0)
    finally:
        runtime.close()


def test_forge_protocol_has_no_heartbeat_message() -> None:
    with pytest.raises(ToolProtocolError, match="UNKNOWN_MESSAGE_TYPE"):
        ToolEnvelope(
            protocol=TOOL_ENDPOINT_PROTOCOL,
            message_type="endpoint.heartbeat",  # type: ignore[arg-type]
            request_id="heartbeat-1",
            endpoint_id="vision.yolo",
            endpoint_instance_id="instance-1",
            payload={},
        )


def test_register_announce_renews_and_new_instance_replaces_without_heartbeat() -> None:
    runtime = GatewayRuntime(_config(lease_ttl_ms=1_000))
    try:
        first_ack = _announce(runtime, now=10.0)
        runtime.tool_gateway.handle_input(
            "yolo/to_gateway",
            _registration(request_id="register-renew"),
            received_at=10.5,
        )
        renewed_ack = runtime.tool_gateway.take_outbound()
        assert renewed_ack is not None
        runtime.tool_gateway.handle_input(
            "yolo/to_gateway",
            _registration(instance_id="instance-2", request_id="register-replace"),
            received_at=10.6,
        )
        replaced_ack = runtime.tool_gateway.take_outbound()
        assert replaced_ack is not None

        first = endpoint_registry_response_from_payload(first_ack.payload)
        renewed = endpoint_registry_response_from_payload(renewed_ack.envelope.payload)
        replaced = endpoint_registry_response_from_payload(replaced_ack.envelope.payload)
        assert first.registry_revision == renewed.registry_revision == 1
        assert replaced.registry_revision == 2
        current = runtime.tool_gateway.resolve_registered_endpoint(
            "vision.yolo", "detect", now=10.6
        )
        assert current is not None
        assert current.endpoint_instance_id == "instance-2"
    finally:
        runtime.close()


def test_matching_unregister_ack_and_absent_replay_use_current_revision() -> None:
    runtime = GatewayRuntime(_config())
    request = make_unregister_envelope(
        endpoint_id="vision.yolo",
        endpoint_instance_id="instance-1",
        request_id="unregister-1",
    )
    try:
        _announce(runtime)
        runtime.tool_gateway.handle_input(
            "yolo/to_gateway", request, received_at=11.0
        )
        first = runtime.tool_gateway.take_outbound()
        assert first is not None
        replay = make_unregister_envelope(
            endpoint_id="vision.yolo",
            endpoint_instance_id="instance-1",
            request_id="unregister-2",
        )
        runtime.tool_gateway.handle_input(
            "yolo/to_gateway", replay, received_at=12.0
        )
        second = runtime.tool_gateway.take_outbound()
        assert second is not None

        first_decision = endpoint_registry_response_from_payload(first.envelope.payload)
        second_decision = endpoint_registry_response_from_payload(second.envelope.payload)
        assert first_decision.status == second_decision.status == "accepted"
        assert first_decision.registry_revision == second_decision.registry_revision == 2
    finally:
        runtime.close()


def test_instance_less_caller_request_is_pinned_only_for_provider() -> None:
    runtime = GatewayRuntime(_config())
    caller_request = _invoke_request(endpoint_instance_id=None)
    try:
        _announce(runtime)
        runtime.tool_gateway.handle_input(
            "caller/tool_request",
            caller_request,
            received_at=11.0,
        )
        outbound = runtime.tool_gateway.take_outbound()
        assert outbound is not None
        assert outbound.output_id == "gateway/to_yolo"
        assert outbound.envelope.message_type == "tool.invoke.request"
        assert outbound.envelope.endpoint_instance_id == "instance-1"
        assert outbound.envelope.request_id != caller_request.request_id
        request, context = invoke_request_from_envelope(outbound.envelope)
        assert request.arguments == {"image_id": "front"}
        assert context.caller_id == "dora:test"

        provider_response = make_invoke_response_envelope(
            ToolResult(status="succeeded", outputs={"objects": ["cup"]}),
            outbound.envelope,
        )
        runtime.tool_gateway.handle_input(
            "yolo/to_gateway",
            provider_response,
            received_at=11.1,
        )
        caller_outbound = runtime.tool_gateway.take_outbound()
        assert caller_outbound is not None
        assert caller_outbound.output_id == "gateway/tool_response"
        assert caller_outbound.envelope.endpoint_instance_id is None
        validate_response_correlation(caller_request, caller_outbound.envelope)
        assert caller_outbound.envelope.payload == provider_response.payload
        assert runtime.tool_gateway.pending_count == 0
    finally:
        runtime.close()


def test_provider_tool_error_is_correlated_without_leaking_instance() -> None:
    runtime = GatewayRuntime(_config())
    caller_request = _invoke_request()
    try:
        _announce(runtime)
        runtime.tool_gateway.handle_input(
            "caller/tool_request", caller_request, received_at=11.0
        )
        provider_request = runtime.tool_gateway.take_outbound()
        assert provider_request is not None
        provider_error = make_error_response_envelope(
            ToolError(code="BAD_ARGUMENT", message="bad image"),
            provider_request.envelope,
        )

        runtime.tool_gateway.handle_input(
            "yolo/to_gateway", provider_error, received_at=11.1
        )
        caller_response = runtime.tool_gateway.take_outbound()

        assert caller_response is not None
        assert caller_response.envelope.endpoint_instance_id is None
        validate_response_correlation(caller_request, caller_response.envelope)
        assert error_from_payload(caller_response.envelope.payload).code == "BAD_ARGUMENT"
    finally:
        runtime.close()


def test_wrong_provider_route_cannot_complete_pending_invocation() -> None:
    runtime = GatewayRuntime(_config(providers=[_provider(), _provider("other")]))
    try:
        _announce(runtime)
        request = _invoke_request()
        runtime.tool_gateway.handle_input(
            "caller/tool_request", request, received_at=11.0
        )
        provider_request = runtime.tool_gateway.take_outbound()
        assert provider_request is not None
        response = make_invoke_response_envelope(
            ToolResult(status="succeeded", outputs={}),
            provider_request.envelope,
        )

        with pytest.raises(ValueError, match="wrong configured route"):
            runtime.tool_gateway.handle_input(
                "other/to_gateway", response, received_at=11.1
            )
        assert runtime.tool_gateway.pending_count == 1
    finally:
        runtime.close()


def test_query_accepted_response_becomes_correlated_gateway_error() -> None:
    from forge_tool import ToolAccepted

    runtime = GatewayRuntime(_config())
    caller_request = _invoke_request()
    try:
        _announce(runtime)
        runtime.tool_gateway.handle_input(
            "caller/tool_request", caller_request, received_at=11.0
        )
        provider_request = runtime.tool_gateway.take_outbound()
        assert provider_request is not None
        accepted = make_invoke_response_envelope(
            ToolAccepted(details={"queued": True}),
            provider_request.envelope,
        )

        runtime.tool_gateway.handle_input(
            "yolo/to_gateway", accepted, received_at=11.1
        )
        response = runtime.tool_gateway.take_outbound()

        assert response is not None
        assert response.envelope.message_type == "tool.error"
        assert (
            error_from_payload(response.envelope.payload).code
            == "FORGE_TOOL_QUERY_NON_TERMINAL_RESPONSE"
        )
        validate_response_correlation(caller_request, response.envelope)
    finally:
        runtime.close()


def test_pending_invocation_expires_into_correlated_timeout_error() -> None:
    runtime = GatewayRuntime(_config(invoke_timeout_ms=1_000))
    caller_request = _invoke_request()
    try:
        _announce(runtime, now=10.0)
        runtime.tool_gateway.handle_input(
            "caller/tool_request", caller_request, received_at=10.1
        )
        provider_request = runtime.tool_gateway.take_outbound()
        assert provider_request is not None

        sweep = runtime.tool_gateway.sweep(now=11.1)
        caller_response = runtime.tool_gateway.take_outbound()

        assert sweep.timed_out_invocations == 1
        assert caller_response is not None
        validate_response_correlation(caller_request, caller_response.envelope)
        error = error_from_payload(caller_response.envelope.payload)
        assert error.code == "FORGE_TOOL_INVOKE_TIMEOUT"
        assert runtime.tool_gateway.pending_count == 0
        late_response = make_invoke_response_envelope(
            ToolResult(status="succeeded", outputs={}),
            provider_request.envelope,
        )
        runtime.tool_gateway.handle_input(
            "yolo/to_gateway", late_response, received_at=11.2
        )
    finally:
        runtime.close()


def test_private_provider_request_id_prevents_late_response_aba() -> None:
    service = ToolGatewayService(_config(invoke_timeout_ms=1_000).tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    first_request = _invoke_request(arguments={"generation": "first"})
    service.handle_input(
        "caller/tool_request",
        first_request,
        received_at=2.0,
    )
    first_provider_request = service.take_outbound()
    assert first_provider_request is not None

    sweep = service.sweep(now=3.0)
    first_timeout = service.take_outbound()
    assert sweep.timed_out_invocations == 1
    assert first_timeout is not None
    assert error_from_payload(first_timeout.envelope.payload).code == (
        "FORGE_TOOL_INVOKE_TIMEOUT"
    )

    second_request = _invoke_request(arguments={"generation": "second"})
    service.handle_input(
        "caller/tool_request",
        second_request,
        received_at=3.1,
    )
    second_provider_request = service.take_outbound()
    assert second_provider_request is not None
    assert (
        first_provider_request.envelope.request_id
        != second_provider_request.envelope.request_id
    )

    late_response = make_invoke_response_envelope(
        ToolResult(status="succeeded", outputs={"generation": "first"}),
        first_provider_request.envelope,
    )
    service.handle_input(
        "yolo/to_gateway",
        late_response,
        received_at=3.2,
    )
    assert service.pending_count == 1
    assert service.take_outbound() is None

    current_response = make_invoke_response_envelope(
        ToolResult(status="succeeded", outputs={"generation": "second"}),
        second_provider_request.envelope,
    )
    service.handle_input(
        "yolo/to_gateway",
        current_response,
        received_at=3.3,
    )
    caller_response = service.take_outbound()
    assert caller_response is not None
    validate_response_correlation(second_request, caller_response.envelope)
    assert caller_response.envelope.payload["result"]["outputs"] == {
        "generation": "second"
    }
    assert service.pending_count == 0


def test_unknown_operation_and_inactive_endpoint_return_public_errors() -> None:
    runtime = GatewayRuntime(_config())
    try:
        inactive = _invoke_request(request_id="inactive")
        runtime.tool_gateway.handle_input(
            "caller/tool_request", inactive, received_at=10.0
        )
        inactive_response = runtime.tool_gateway.take_outbound()
        assert inactive_response is not None
        assert (
            error_from_payload(inactive_response.envelope.payload).code
            == "FORGE_TOOL_ENDPOINT_UNAVAILABLE"
        )

        _announce(runtime, now=11.0)
        unknown_operation = _invoke_request(
            operation="segment", request_id="unknown-operation"
        )
        runtime.tool_gateway.handle_input(
            "caller/tool_request", unknown_operation, received_at=11.1
        )
        operation_response = runtime.tool_gateway.take_outbound()
        assert operation_response is not None
        assert (
            error_from_payload(operation_response.envelope.payload).code
            == "FORGE_TOOL_OPERATION_NOT_FOUND"
        )
    finally:
        runtime.close()


def test_forge_carrier_accepts_instance_less_logical_invoke() -> None:
    request = _invoke_request()

    assert _decode(_carrier(request)) == request


def test_tool_adapter_rejects_bytes_and_carrier_size_before_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = GatewayRuntime(_config())

    class _DecodeMustNotRun:
        @classmethod
        def from_arrow(cls, value: object, **kwargs: object) -> object:
            del cls, value, kwargs
            raise AssertionError("typed decode ran")

    monkeypatch.setattr(tool_dora, "ToolMessage", _DecodeMustNotRun)
    try:
        with pytest.raises(TypeError, match="do not accept Arrow IPC bytes"):
            handle_tool_input(runtime, "yolo/to_gateway", b"raw")
        with pytest.raises(ToolProtocolError, match="Arrow carrier size"):
            handle_tool_input(
                runtime,
                "yolo/to_gateway",
                _carrier(_registration()),
                max_carrier_bytes=1,
            )
        assert runtime.tool_gateway.outbound_count == 0
    finally:
        runtime.close()


def test_runner_processes_tool_input_and_sends_only_on_lifecycle_thread() -> None:
    runtime = GatewayRuntime(_config())
    node = _Node()
    runner = GatewayDoraRunner(
        runtime=runtime,
        node=node,
        stop_event=threading.Event(),
    )
    lifecycle_thread_id = threading.get_ident()
    received_at = time.monotonic()
    try:
        assert (
            runner.handle_poll(
                {
                    "type": "INPUT",
                    "id": "yolo/to_gateway",
                    "value": _carrier(_registration()),
                    dora_runtime._TOOL_RECEIVED_AT_KEY: received_at,
                }
            )
            is None
        )

        assert node.send_thread_ids == [lifecycle_thread_id]
        current = runtime.tool_gateway.resolve_registered_endpoint(
            "vision.yolo", "detect", now=received_at + 0.1
        )
        assert current is not None
        assert current.expires_at == received_at + 15.0
    finally:
        runtime.close()


def test_reader_captures_monotonic_time_for_provider_and_caller_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = GatewayRuntime(_config())
    events = [
        {
            "type": "INPUT",
            "id": "yolo/to_gateway",
            "value": _carrier(_registration()),
        },
        {
            "type": "INPUT",
            "id": "caller/tool_request",
            "value": "logical-invoke-carrier",
        },
    ]
    node = _Node(events)
    observations = iter((50.0, 50.1))
    monkeypatch.setattr(
        dora_runtime,
        "time",
        SimpleNamespace(monotonic=lambda: next(observations)),
    )
    runner = GatewayDoraRunner(
        runtime=runtime,
        node=node,
        stop_event=threading.Event(),
    )
    try:
        runner.start()
        assert runner.join_reader(timeout=1.0)
        first = runner._events.get(timeout=0.0)
        second = runner._events.get(timeout=0.0)

        assert first is not None
        assert second is not None
        assert first[dora_runtime._TOOL_RECEIVED_AT_KEY] == 50.0
        assert second[dora_runtime._TOOL_RECEIVED_AT_KEY] == 50.1
    finally:
        runtime.close()


def test_runner_sweeps_and_drains_tool_service_on_every_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = GatewayRuntime(_config())
    sweeps: list[GatewayRuntime] = []
    drains: list[GatewayRuntime] = []
    monkeypatch.setattr(
        dora_runtime,
        "sweep_tool_gateway",
        lambda runtime_arg: sweeps.append(runtime_arg),
    )
    monkeypatch.setattr(
        dora_runtime,
        "drain_tool_outputs",
        lambda runtime_arg, node: drains.append(runtime_arg) or (),
    )
    try:
        runner = GatewayDoraRunner(
            runtime=runtime,
            node=_Node(),
            stop_event=threading.Event(),
        )
        assert runner.handle_poll(None) is None
        assert runner.handle_poll({"type": "UNKNOWN"}) is None
        assert sweeps == [runtime, runtime]
        assert drains == [runtime, runtime]
    finally:
        runtime.close()


def test_get_tools_lists_active_descriptors_without_private_instances() -> None:
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    try:
        _announce(runtime)
        response = TestClient(app).get("/tools")

        assert response.status_code == 200
        assert response.json() == {
            "ok": True,
            "data": {
                "revision": 1,
                "tools": [
                    {
                        "endpoint_id": "vision.yolo",
                        "descriptor": {
                            "protocol_version": TOOL_ENDPOINT_PROTOCOL,
                            "endpoint_id": "vision.yolo",
                            "operations": [
                                {
                                    "name": "detect",
                                    "semantics": "query",
                                    "cancellable": False,
                                    "stoppable": False,
                                    "status_supported": False,
                                    "max_concurrency": 1,
                                }
                            ],
                        },
                    }
                ],
            },
        }
        assert "route" not in response.text
        assert "expires_at" not in response.text
    finally:
        runtime.close()


def test_http_invoke_waits_asynchronously_for_same_service_response() -> None:
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        _announce(runtime)
        with ThreadPoolExecutor(max_workers=1) as executor:
            response_future = executor.submit(
                client.post,
                "/tools/vision.yolo/detect:invoke",
                json={
                    "arguments": {"image_id": "front"},
                    "caller_id": "http:test",
                    "timeout_ms": 2_000,
                },
            )
            _wait_for(lambda: runtime.tool_gateway.pending_count == 1)
            outbound = runtime.tool_gateway.take_outbound()
            assert outbound is not None
            assert outbound.envelope.endpoint_instance_id == "instance-1"
            _, context = invoke_request_from_envelope(outbound.envelope)
            assert context.caller_id == "http:test"
            provider_response = make_invoke_response_envelope(
                ToolResult(status="succeeded", outputs={"objects": ["cup"]}),
                outbound.envelope,
            )
            runtime.tool_gateway.handle_input(
                "yolo/to_gateway",
                provider_response,
                received_at=time.monotonic(),
            )
            response = response_future.result(timeout=1.0)

        assert response.status_code == 200
        body = response.json()
        assert body["ok"] is True
        assert body["data"]["endpoint_id"] == "vision.yolo"
        assert body["data"]["operation"] == "detect"
        assert body["data"]["response"]["outcome"] == "completed"
        assert "endpoint_instance_id" not in response.text
    finally:
        runtime.close()


def test_http_deadline_discards_queued_predeadline_provider_response() -> None:
    runtime = GatewayRuntime(_config(invoke_timeout_ms=100))
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        _announce(runtime)
        with ThreadPoolExecutor(max_workers=1) as executor:
            response_future = executor.submit(
                client.post,
                "/tools/vision.yolo/detect:invoke",
                json={"arguments": {}, "timeout_ms": 100},
            )
            _wait_for(lambda: runtime.tool_gateway.pending_count == 1)
            provider_request = runtime.tool_gateway.take_outbound()
            assert provider_request is not None
            queued_response = make_invoke_response_envelope(
                ToolResult(status="succeeded", outputs={"queued": True}),
                provider_request.envelope,
            )
            received_at = time.monotonic()
            response = response_future.result(timeout=1.0)

        assert response.status_code == 504
        assert response.json()["error"]["code"] == "FORGE_TOOL_INVOKE_TIMEOUT"
        assert runtime.tool_gateway.pending_count == 0
        runtime.tool_gateway.handle_input(
            "yolo/to_gateway",
            queued_response,
            received_at=received_at,
            processed_at=time.monotonic(),
        )
    finally:
        runtime.close()


def test_http_tool_errors_cover_400_404_and_503() -> None:
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        malformed = client.post(
            "/tools/vision.yolo/detect:invoke",
            json={"arguments": [], "timeout_ms": True},
        )
        unknown = client.post(
            "/tools/vision.unknown/detect:invoke",
            json={"arguments": {}},
        )
        inactive = client.post(
            "/tools/vision.yolo/detect:invoke",
            json={"arguments": {}},
        )

        assert malformed.status_code == 400
        assert unknown.status_code == 404
        assert unknown.json()["error"]["code"] == "FORGE_TOOL_ENDPOINT_UNKNOWN"
        assert inactive.status_code == 503
        assert inactive.json()["error"]["code"] == "FORGE_TOOL_ENDPOINT_UNAVAILABLE"
    finally:
        runtime.close()


def test_http_timeout_is_capped_by_configured_maximum() -> None:
    service = ToolGatewayService(_config(invoke_timeout_ms=100).tools)

    omitted = service.submit_http_invoke("vision.yolo", "detect", {}, now=10.0)
    allowed = service.submit_http_invoke(
        "vision.yolo",
        "detect",
        {},
        timeout_ms=100,
        now=20.0,
    )
    shortened = service.submit_http_invoke(
        "vision.yolo",
        "detect",
        {},
        timeout_ms=25,
        now=30.0,
    )

    assert omitted.deadline == pytest.approx(10.1)
    assert allowed.deadline == pytest.approx(20.1)
    assert shortened.deadline == pytest.approx(30.025)
    with pytest.raises(ValueError, match="configured tools.invoke_timeout_ms maximum 100"):
        service.submit_http_invoke(
            "vision.yolo",
            "detect",
            {},
            timeout_ms=101,
            now=40.0,
        )

    app = FastAPI()
    register_tool_routes(app, SimpleNamespace(tool_gateway=service))
    response = TestClient(app).post(
        "/tools/vision.yolo/detect:invoke",
        json={"arguments": {}, "timeout_ms": 101},
    )

    assert response.status_code == 400
    assert "configured tools.invoke_timeout_ms maximum 100" in response.json()["msg"]


def test_http_reports_bounded_outbound_mailbox_backpressure_as_503() -> None:
    tools = _config().tools
    service = ToolGatewayService(tools, outbound_capacity=1)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=time.monotonic(),
    )
    app = FastAPI()
    register_tool_routes(app, SimpleNamespace(tool_gateway=service))

    response = TestClient(app).post(
        "/tools/vision.yolo/detect:invoke",
        json={"arguments": {}},
    )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "FORGE_TOOL_GATEWAY_BUSY"
    assert response.json()["error"]["retryable"] is True


def test_http_closed_gateway_returns_structured_unavailable_error() -> None:
    service = ToolGatewayService(_config().tools)
    service.begin_close(now=1.0)
    app = FastAPI()
    register_tool_routes(app, SimpleNamespace(tool_gateway=service))

    response = TestClient(app).post(
        "/tools/vision.yolo/detect:invoke",
        json={"arguments": {}},
    )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "FORGE_TOOL_GATEWAY_UNAVAILABLE"
    assert response.json()["error"]["retryable"] is True


def test_pending_capacity_remains_full_after_http_dispatch_claim() -> None:
    service = ToolGatewayService(_config().tools, outbound_capacity=1)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    first = service.submit_http_invoke("vision.yolo", "detect", {}, now=2.0)
    claimed = service.take_outbound()
    assert claimed is not None
    assert claimed.kind == "provider.invoke_request"
    assert service.pending_count == 1
    assert service.outbound_count == 0

    second = service.submit_http_invoke("vision.yolo", "detect", {}, now=2.1)

    assert second.pending_key is None
    assert error_from_payload(second.future.result(timeout=0.1).payload).code == (
        "FORGE_TOOL_GATEWAY_BUSY"
    )
    assert service.pending_count == 1
    assert service.outbound_count == 0
    assert service.cancel_http_invoke(first, timed_out=False, now=2.2) is True


def test_runtime_close_completes_pending_http_wait_without_dora_send() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _announce(runtime)
        ticket = runtime.tool_gateway.submit_http_invoke(
            "vision.yolo", "detect", {}
        )
        assert runtime.tool_gateway.pending_count == 1

        runtime.begin_close()
        response = ticket.future.result(timeout=0.1)

        assert error_from_payload(response.payload).code == "FORGE_TOOL_GATEWAY_UNAVAILABLE"
        assert runtime.tool_gateway.pending_count == 0
        assert runtime.tool_gateway.take_outbound() is None
    finally:
        runtime.close()


def test_disabled_tools_do_not_reserve_default_or_configured_ports() -> None:
    config = GatewayConfig.from_dict(
        {
            "joint_order": [],
            "image_input_ids": ["tool_request", "yolo/to_gateway"],
            "agent": {"action_manifests": []},
            "tools": {
                "enabled": False,
                "request_input_id": "tick",
                "response_output_id": "policy_command",
                "providers": [
                    {
                        **_provider(),
                        "input_id": "yolo/to_gateway",
                        "output_id": "policy_command",
                    }
                ],
            },
        }
    )

    runtime = GatewayRuntime(config)
    try:
        assert runtime.tool_input_ids == frozenset()
    finally:
        runtime.close()


def test_registration_replacement_does_not_mutate_without_ack_capacity() -> None:
    service = ToolGatewayService(_config().tools, outbound_capacity=1)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )

    with pytest.raises(ToolGatewayMailboxFull):
        service.handle_input(
            "yolo/to_gateway",
            _registration(instance_id="instance-2", request_id="register-2"),
            received_at=2.0,
        )

    current = service.resolve_registered_endpoint("vision.yolo", "detect", now=2.0)
    assert current is not None
    assert current.endpoint_instance_id == "instance-1"
    assert service.registry_revision == 1
    assert service.outbound_count == 1
    assert service.reserved_outbound_count == 0


def test_dora_invoke_uses_last_slot_for_immediate_busy_response() -> None:
    service = ToolGatewayService(_config().tools, outbound_capacity=1)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None

    service.handle_input(
        "caller/tool_request",
        _invoke_request(),
        received_at=2.0,
    )

    response = service.take_outbound()
    assert response is not None
    assert response.kind == "caller.invoke_response"
    assert error_from_payload(response.envelope.payload).code == "FORGE_TOOL_GATEWAY_BUSY"
    assert service.pending_count == 0
    assert service.reserved_outbound_count == 0


def test_dora_invoke_with_no_response_slot_rejects_without_resolution() -> None:
    service = ToolGatewayService(_config().tools, outbound_capacity=1)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    revision = service.registry_revision

    with pytest.raises(ToolGatewayMailboxFull):
        service.handle_input(
            "caller/tool_request",
            _invoke_request(),
            received_at=2.0,
        )

    assert service.registry_revision == revision
    assert service.pending_count == 0
    assert service.reserved_outbound_count == 0


def test_reserved_dora_terminal_response_cannot_be_crowded_out() -> None:
    service = ToolGatewayService(_config().tools, outbound_capacity=2)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    service.handle_input(
        "caller/tool_request",
        _invoke_request(),
        received_at=2.0,
    )
    provider_request = service.take_outbound()
    assert provider_request is not None
    assert service.reserved_outbound_count == 1

    service.handle_input(
        "yolo/to_gateway",
        _registration(request_id="register-renew"),
        received_at=2.1,
    )
    provider_response = make_invoke_response_envelope(
        ToolResult(status="succeeded", outputs={"ok": True}),
        provider_request.envelope,
    )
    service.handle_input(
        "yolo/to_gateway",
        provider_response,
        received_at=2.2,
    )

    assert service.pending_count == 0
    assert service.reserved_outbound_count == 0
    assert service.outbound_count == 2
    registry_response = service.take_outbound()
    assert registry_response is not None
    assert registry_response.kind == "provider.registry_response"
    caller_response = service.take_outbound()
    assert caller_response is not None
    assert caller_response.kind == "caller.invoke_response"


def test_public_caller_must_not_supply_provider_instance() -> None:
    service = ToolGatewayService(_config().tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None

    with pytest.raises(ValueError, match="must omit endpoint_instance_id"):
        service.handle_input(
            "caller/tool_request",
            _invoke_request(endpoint_instance_id="caller-selected"),
            received_at=2.0,
        )

    assert service.pending_count == 0
    assert service.outbound_count == 0


def test_provider_key_collision_is_rejected_without_overwrite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ToolGatewayService(_config().tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    original_key = service._correlation_key
    forced_provider_key = original_key(
        _invoke_request(endpoint_instance_id="instance-1")
    )

    def correlation_key(envelope: ToolEnvelope):
        if envelope.endpoint_instance_id is not None:
            return forced_provider_key
        return original_key(envelope)

    monkeypatch.setattr(service, "_correlation_key", correlation_key)
    service.handle_input(
        "caller/tool_request",
        _invoke_request(),
        received_at=2.0,
    )
    assert service.take_outbound() is not None
    service.handle_input(
        "caller/tool_request",
        _invoke_request(
            request_id="request-2",
            invocation_id="invocation-2",
            attempt_id="attempt-2",
        ),
        received_at=2.1,
    )

    assert service.pending_count == 1
    conflict = service.take_outbound()
    assert conflict is not None
    assert error_from_payload(conflict.envelope.payload).code == "FORGE_TOOL_REQUEST_CONFLICT"


def test_registry_effects_continue_using_reader_receive_time() -> None:
    service = ToolGatewayService(_config(lease_ttl_ms=1_000).tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=10.0,
        processed_at=20.0,
    )
    assert service.take_outbound() is not None
    registration = service.endpoint_registrations(now=10.5)[0]
    assert registration.expires_at == 11.0

    stale_unregister = make_unregister_envelope(
        endpoint_id="vision.yolo",
        endpoint_instance_id="stale-instance",
        request_id="unregister-stale",
    )
    service.handle_input(
        "yolo/to_gateway",
        stale_unregister,
        received_at=10.75,
        processed_at=20.0,
    )
    outbound = service.take_outbound()
    assert outbound is not None
    decision = endpoint_registry_response_from_payload(outbound.envelope.payload)
    assert decision.status == "rejected"
    assert decision.error is not None
    assert decision.error.code == "FORGE_ENDPOINT_INSTANCE_STALE"
    assert service.endpoint_registrations(now=10.75) == (registration,)


def test_query_deadline_uses_lifecycle_processing_time() -> None:
    service = ToolGatewayService(_config(invoke_timeout_ms=1_000).tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=10.0,
        processed_at=10.0,
    )
    assert service.take_outbound() is not None

    service.handle_input(
        "caller/tool_request",
        _invoke_request(),
        received_at=10.1,
        processed_at=20.0,
    )
    pending = next(iter(service._pending_by_provider_key.values()))
    assert pending.deadline == 21.0
    provider_request = service.take_outbound()
    assert provider_request is not None
    response = make_invoke_response_envelope(
        ToolResult(status="succeeded", outputs={"queued": True}),
        provider_request.envelope,
    )
    service.handle_input(
        "yolo/to_gateway",
        response,
        received_at=20.5,
        processed_at=21.0,
    )

    at_deadline = service.take_outbound()
    assert at_deadline is not None
    assert error_from_payload(at_deadline.envelope.payload).code == "FORGE_TOOL_INVOKE_TIMEOUT"


def test_public_dora_deadline_cannot_extend_configured_timeout() -> None:
    service = ToolGatewayService(_config(invoke_timeout_ms=1_000).tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    caller_deadline_ms = int(time.time() * 1_000) + 10_000

    service.handle_input(
        "caller/tool_request",
        _invoke_request(deadline_ms=caller_deadline_ms),
        received_at=2.0,
        processed_at=2.0,
    )

    pending = next(iter(service._pending_by_provider_key.values()))
    assert pending.deadline == 3.0


def test_absolute_deadline_conversion_does_not_double_subtract_fifo_delay() -> None:
    service = ToolGatewayService(_config(invoke_timeout_ms=5_000).tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=10.0,
    )
    assert service.take_outbound() is not None
    deadline_ms = int(time.time() * 1_000) + 1_000

    service.handle_input(
        "caller/tool_request",
        _invoke_request(deadline_ms=deadline_ms),
        received_at=10.0,
        processed_at=10.5,
    )

    pending = next(iter(service._pending_by_provider_key.values()))
    assert 11.4 <= pending.deadline <= 11.5


def test_already_expired_caller_deadline_never_dispatches_provider_request() -> None:
    service = ToolGatewayService(_config().tools, outbound_capacity=2)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None

    service.handle_input(
        "caller/tool_request",
        _invoke_request(deadline_ms=int(time.time() * 1_000) - 1),
        received_at=2.0,
        processed_at=2.0,
    )

    response = service.take_outbound()
    assert response is not None
    assert response.kind == "caller.invoke_response"
    assert error_from_payload(response.envelope.payload).code == "FORGE_TOOL_INVOKE_TIMEOUT"
    assert service.take_outbound() is None
    assert service.pending_count == 0


def test_timeout_skips_queued_provider_request_and_returns_reserved_error() -> None:
    service = ToolGatewayService(_config(invoke_timeout_ms=100).tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    service.handle_input(
        "caller/tool_request",
        _invoke_request(),
        received_at=2.0,
    )

    sweep = service.sweep(now=2.1)
    response = service.take_outbound()

    assert sweep.timed_out_invocations == 1
    assert response is not None
    assert response.kind == "caller.invoke_response"
    assert error_from_payload(response.envelope.payload).code == "FORGE_TOOL_INVOKE_TIMEOUT"
    assert service.take_outbound() is None


@pytest.mark.parametrize(
    ("terminal", "expected_code"),
    [
        pytest.param("timeout", "FORGE_TOOL_INVOKE_TIMEOUT", id="timeout"),
        pytest.param("close", "FORGE_TOOL_GATEWAY_UNAVAILABLE", id="close"),
    ],
)
def test_timeout_or_close_before_dispatch_claim_skips_provider_send(
    terminal: str,
    expected_code: str,
) -> None:
    service = ToolGatewayService(_config().tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    ticket = service.submit_http_invoke("vision.yolo", "detect", {}, now=2.0)
    node = _Node()
    drain_started = threading.Event()
    runtime = cast(
        GatewayRuntime,
        cast(object, SimpleNamespace(tool_gateway=service)),
    )

    def drain() -> tuple[object, ...]:
        drain_started.set()
        return drain_tool_outputs(runtime, node)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with service._lock:
            drain_future = executor.submit(drain)
            assert drain_started.wait(timeout=1.0)
            if terminal == "timeout":
                assert service.cancel_http_invoke(ticket, timed_out=True) is True
            else:
                service.begin_close(now=2.1)
        assert drain_future.result(timeout=1.0) == ()

    response = ticket.future.result(timeout=0.1)
    assert error_from_payload(response.payload).code == expected_code
    assert node.outputs == []
    assert service.pending_count == 0
    assert service.outbound_count == 0


@pytest.mark.parametrize("transition", ["expiry", "unregister", "replacement"])
def test_endpoint_transition_before_claim_skips_stale_provider_request(
    transition: str,
) -> None:
    service = ToolGatewayService(
        _config(lease_ttl_ms=100, invoke_timeout_ms=5_000).tools
    )
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    ticket = service.submit_http_invoke("vision.yolo", "detect", {}, now=1.05)

    if transition == "expiry":
        sweep = service.sweep(now=1.1)
        assert [item.endpoint_instance_id for item in sweep.expired_registrations] == [
            "instance-1"
        ]
    elif transition == "unregister":
        service.handle_input(
            "yolo/to_gateway",
            make_unregister_envelope(
                endpoint_id="vision.yolo",
                endpoint_instance_id="instance-1",
                request_id="unregister-transition",
            ),
            received_at=1.06,
            processed_at=1.06,
        )
        management_response = service.take_outbound()
        assert management_response is not None
        assert management_response.kind == "provider.registry_response"
    else:
        service.handle_input(
            "yolo/to_gateway",
            _registration(
                instance_id="instance-2",
                request_id="register-replacement",
            ),
            received_at=1.06,
            processed_at=1.06,
        )
        management_response = service.take_outbound()
        assert management_response is not None
        assert management_response.kind == "provider.registry_response"

    response = ticket.future.result(timeout=0.1)
    assert error_from_payload(response.payload).code == (
        "FORGE_TOOL_ENDPOINT_UNAVAILABLE"
    )
    assert service.pending_count == 0
    assert service.take_outbound() is None


def test_expiry_fencing_at_elapsed_deadline_counts_timeout() -> None:
    service = ToolGatewayService(
        _config(lease_ttl_ms=100, invoke_timeout_ms=1_000).tools
    )
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    ticket = service.submit_http_invoke("vision.yolo", "detect", {}, now=1.05)

    sweep = service.sweep(now=3.0)

    response = ticket.future.result(timeout=0.1)
    assert error_from_payload(response.payload).code == "FORGE_TOOL_INVOKE_TIMEOUT"
    assert sweep.timed_out_invocations == 1
    assert service.pending_count == 0
    assert service.take_outbound() is None


@pytest.mark.parametrize("transition", ["expiry", "unregister", "replacement"])
def test_claimed_query_can_complete_after_endpoint_transition(transition: str) -> None:
    service = ToolGatewayService(
        _config(lease_ttl_ms=100, invoke_timeout_ms=5_000).tools
    )
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    ticket = service.submit_http_invoke("vision.yolo", "detect", {}, now=1.05)
    provider_request = service.take_outbound()
    assert provider_request is not None
    assert provider_request.kind == "provider.invoke_request"

    if transition == "expiry":
        service.sweep(now=1.1)
    elif transition == "unregister":
        service.handle_input(
            "yolo/to_gateway",
            make_unregister_envelope(
                endpoint_id="vision.yolo",
                endpoint_instance_id="instance-1",
                request_id="unregister-transition",
            ),
            received_at=1.06,
            processed_at=1.06,
        )
        assert service.take_outbound() is not None
    else:
        service.handle_input(
            "yolo/to_gateway",
            _registration(
                instance_id="instance-2",
                request_id="register-replacement",
            ),
            received_at=1.06,
            processed_at=1.06,
        )
        assert service.take_outbound() is not None

    assert service.pending_count == 1
    assert ticket.future.done() is False
    provider_response = make_invoke_response_envelope(
        ToolResult(status="succeeded", outputs={"instance": "old"}),
        provider_request.envelope,
    )
    service.handle_input(
        "yolo/to_gateway",
        provider_response,
        received_at=1.2,
        processed_at=1.2,
    )

    response = ticket.future.result(timeout=0.1)
    assert response.payload["result"]["outputs"] == {"instance": "old"}
    assert service.pending_count == 0


def test_dispatch_claim_before_cancel_may_send_but_timeout_remains_authoritative() -> None:
    service = ToolGatewayService(_config().tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    ticket = service.submit_http_invoke("vision.yolo", "detect", {}, now=2.0)

    class _BlockedSendNode(_Node):
        def __init__(self) -> None:
            super().__init__()
            self.send_started = threading.Event()
            self.release_send = threading.Event()

        def send_output(self, output_id: str, data: Any, /) -> None:
            self.send_started.set()
            assert self.release_send.wait(timeout=1.0)
            super().send_output(output_id, data)

    node = _BlockedSendNode()
    runtime = cast(
        GatewayRuntime,
        cast(object, SimpleNamespace(tool_gateway=service)),
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        drain_future = executor.submit(drain_tool_outputs, runtime, node)
        assert node.send_started.wait(timeout=1.0)
        assert ticket.pending_key is not None
        pending = service._pending_by_provider_key[ticket.pending_key]
        assert pending.dispatch_claimed is True
        try:
            assert service.cancel_http_invoke(ticket, timed_out=True) is True
        finally:
            node.release_send.set()
        dispatched = drain_future.result(timeout=1.0)

    assert len(dispatched) == 1
    assert len(node.outputs) == 1
    response = ticket.future.result(timeout=0.1)
    assert error_from_payload(response.payload).code == "FORGE_TOOL_INVOKE_TIMEOUT"
    assert service.pending_count == 0
    late_response = make_invoke_response_envelope(
        ToolResult(status="succeeded", outputs={"late": True}),
        dispatched[0].envelope,
    )
    service.handle_input(
        "yolo/to_gateway",
        late_response,
        received_at=2.1,
        processed_at=2.1,
    )


@pytest.mark.parametrize(
    ("terminal", "observed_at", "expected_code"),
    [
        pytest.param(
            "output_failed",
            2.9,
            "FORGE_TOOL_TRANSPORT_UNAVAILABLE",
            id="output-failure-before-deadline",
        ),
        pytest.param(
            "output_failed",
            3.0,
            "FORGE_TOOL_INVOKE_TIMEOUT",
            id="output-failure-at-deadline",
        ),
        pytest.param(
            "close",
            2.9,
            "FORGE_TOOL_GATEWAY_UNAVAILABLE",
            id="close-before-deadline",
        ),
        pytest.param(
            "close",
            3.0,
            "FORGE_TOOL_INVOKE_TIMEOUT",
            id="close-at-deadline",
        ),
    ],
)
def test_async_terminal_completion_is_arbitrated_at_pending_deadline(
    terminal: str,
    observed_at: float,
    expected_code: str,
) -> None:
    service = ToolGatewayService(_config(invoke_timeout_ms=1_000).tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    ticket = service.submit_http_invoke("vision.yolo", "detect", {}, now=2.0)
    provider_request = service.take_outbound()
    assert provider_request is not None

    if terminal == "output_failed":
        assert service.output_failed(
            provider_request,
            RuntimeError("send failed"),
            now=observed_at,
        )
    else:
        service.begin_close(now=observed_at)

    response = ticket.future.result(timeout=0.1)
    assert error_from_payload(response.payload).code == expected_code
    assert service.pending_count == 0


def test_caller_cancellation_after_deadline_completes_as_timeout() -> None:
    service = ToolGatewayService(_config(invoke_timeout_ms=1_000).tools)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    ticket = service.submit_http_invoke("vision.yolo", "detect", {}, now=2.0)

    assert service.cancel_http_invoke(ticket, timed_out=False, now=3.0) is True

    response = ticket.future.result(timeout=0.1)
    assert error_from_payload(response.payload).code == "FORGE_TOOL_INVOKE_TIMEOUT"


def test_begin_close_is_idempotent_and_completes_all_reserved_callers() -> None:
    service = ToolGatewayService(_config().tools, outbound_capacity=4)
    service.handle_input(
        "yolo/to_gateway",
        _registration(),
        received_at=1.0,
    )
    assert service.take_outbound() is not None
    for index in range(3):
        service.handle_input(
            "caller/tool_request",
            _invoke_request(
                request_id=f"request-{index}",
                invocation_id=f"invocation-{index}",
                attempt_id=f"attempt-{index}",
            ),
            received_at=2.0 + index / 10,
        )
        assert service.take_outbound() is not None
    service.handle_input(
        "yolo/to_gateway",
        _registration(request_id="register-renew"),
        received_at=3.0,
    )

    service.begin_close(now=3.1)
    service.begin_close(now=3.1)

    assert service.pending_count == 0
    assert service.reserved_outbound_count == 0
    assert service.outbound_count == 4
    outputs = [service.take_outbound() for _ in range(4)]
    assert outputs[0] is not None
    assert outputs[0].kind == "provider.registry_response"
    for output in outputs[1:]:
        assert output is not None
        assert error_from_payload(output.envelope.payload).code == "FORGE_TOOL_GATEWAY_UNAVAILABLE"


def test_begin_close_rejects_provider_management_before_directory_mutation() -> None:
    service = ToolGatewayService(_config().tools)
    service.begin_close()

    with pytest.raises(ToolGatewayUnavailable, match="closing"):
        service.handle_input(
            "yolo/to_gateway",
            _registration(),
            received_at=1.0,
        )

    assert service.endpoint_registrations(now=1.0) == ()
    assert service.outbound_count == 0


def test_late_provider_response_does_not_set_runner_last_error() -> None:
    runtime = GatewayRuntime(_config())
    node = _Node()
    try:
        _announce(runtime)
        ticket = runtime.tool_gateway.submit_http_invoke(
            "vision.yolo",
            "detect",
            {},
        )
        provider_request = runtime.tool_gateway.take_outbound()
        assert provider_request is not None
        assert runtime.tool_gateway.cancel_http_invoke(ticket, timed_out=True) is True
        late_response = make_invoke_response_envelope(
            ToolResult(status="succeeded", outputs={"late": True}),
            provider_request.envelope,
        )
        runner = GatewayDoraRunner(
            runtime=runtime,
            node=node,
            stop_event=threading.Event(),
        )

        assert (
            runner.handle_poll(
                {
                    "type": "INPUT",
                    "id": "yolo/to_gateway",
                    "value": _carrier(late_response),
                }
            )
            is None
        )

        with runtime.lock:
            assert runtime.last_error is None
    finally:
        runtime.close()


def test_runner_shutdown_finally_drains_caller_errors_not_provider_requests() -> None:
    runtime = GatewayRuntime(_config())
    node = _Node()
    stop_event = threading.Event()
    try:
        _announce(runtime)
        runtime.tool_gateway.handle_input(
            "caller/tool_request",
            _invoke_request(),
            received_at=time.monotonic(),
        )
        stop_event.set()
        runner = GatewayDoraRunner(
            runtime=runtime,
            node=node,
            stop_event=stop_event,
        )

        assert runner.run() == "shutdown"

        assert [output_id for output_id, _ in node.outputs] == [
            "gateway/tool_response"
        ]
        response = _decode(node.outputs[0][1])
        assert error_from_payload(response.payload).code == "FORGE_TOOL_GATEWAY_UNAVAILABLE"
        assert runtime.tool_gateway.pending_count == 0
    finally:
        runtime.close()


def test_tool_output_drain_is_bounded_per_pass() -> None:
    runtime = GatewayRuntime(_config())
    node = _Node()
    try:
        for index in range(3):
            runtime.tool_gateway.handle_input(
                "yolo/to_gateway",
                _registration(request_id=f"register-{index}"),
                received_at=1.0 + index / 10,
            )

        dispatched = drain_tool_outputs(runtime, node, max_messages=2)

        assert len(dispatched) == 2
        assert runtime.tool_gateway.outbound_count == 1
        assert len(node.outputs) == 2
    finally:
        runtime.close()


def test_instance_less_dora_caller_round_trip_keeps_instance_private() -> None:
    runtime = GatewayRuntime(_config())
    node = _Node()
    caller_request = _invoke_request()
    try:
        _announce(runtime)
        handle_tool_input(
            runtime,
            "caller/tool_request",
            _carrier(caller_request),
        )
        dispatched = drain_tool_outputs(runtime, node)
        assert len(dispatched) == 1
        provider_request = _decode(node.outputs.pop()[1])
        assert provider_request.endpoint_instance_id == "instance-1"

        provider_response = make_invoke_response_envelope(
            ToolResult(status="succeeded", outputs={"objects": ["cup"]}),
            provider_request,
        )
        handle_tool_input(
            runtime,
            "yolo/to_gateway",
            _carrier(provider_response),
        )
        dispatched = drain_tool_outputs(runtime, node)
        assert len(dispatched) == 1
        caller_response = _decode(node.outputs.pop()[1])
        assert caller_response.endpoint_instance_id is None
        validate_response_correlation(caller_request, caller_response)
    finally:
        runtime.close()
