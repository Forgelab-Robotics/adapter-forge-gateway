"""Gateway Session tool semantics: lifecycle, stop control, and deadline exemption."""

from __future__ import annotations

import time

from fastapi import FastAPI
from fastapi.testclient import TestClient
from forge_tool import (
    TOOL_ENDPOINT_PROTOCOL,
    ToolAccepted,
    ToolContext,
    ToolControlResponse,
    ToolEndpointDescriptor,
    ToolEvent,
    ToolExecutionKey,
    ToolExecutionStatus,
    ToolOperationDescriptor,
    ToolRequest,
    ToolResult,
    ToolResultResponse,
    invoke_request_from_envelope,
    make_control_response_envelope,
    make_event_envelope,
    make_invoke_response_envelope,
    make_registration_envelope,
    make_result_response_envelope,
    make_status_response_envelope,
)

from forge_gateway.config import GatewayConfig
from forge_gateway.controllers.tool_controller import register_tool_routes
from forge_gateway.services.runtime_service import GatewayRuntime
from forge_gateway.services.tool_gateway_service import (
    make_logical_invoke_request_envelope,
)


def _config(*, lease_ttl_ms: int = 15_000) -> GatewayConfig:
    return GatewayConfig.from_dict(
        {
            "joint_order": [],
            "tools": {
                "enabled": True,
                "lease_ttl_ms": lease_ttl_ms,
                "invoke_timeout_ms": 5_000,
                "invocation_capacity": 2,
                "event_capacity": 2,
                "retention_sec": 60,
                "providers": [
                    {
                        "endpoint_id": "robot.arm",
                        "input_id": "arm/to_gateway",
                        "output_id": "gateway/to_arm",
                    }
                ],
                "specs": [
                    {
                        "tool_id": "robot.pick",
                        "implementation_id": "arm.primary",
                        "endpoint_id": "robot.arm",
                        "operation": "pick",
                        "semantics": "action",
                    },
                    {
                        "tool_id": "robot.serve",
                        "implementation_id": "arm.primary",
                        "endpoint_id": "robot.arm",
                        "operation": "serve",
                        "semantics": "session",
                    },
                ],
            },
        }
    )


def _register(runtime: GatewayRuntime, *, now: float | None = None) -> None:
    descriptor = ToolEndpointDescriptor(
        protocol_version=TOOL_ENDPOINT_PROTOCOL,
        endpoint_id="robot.arm",
        operations=(
            ToolOperationDescriptor(
                name="pick",
                semantics="action",
                cancellable=True,
                status_supported=True,
                max_concurrency=2,
            ),
            ToolOperationDescriptor(
                name="serve",
                semantics="session",
                stoppable=True,
                status_supported=True,
                max_concurrency=1,
            ),
        ),
    )
    runtime.tool_gateway.handle_input(
        "arm/to_gateway",
        make_registration_envelope(
            descriptor,
            endpoint_instance_id="arm-instance-1",
            request_id="register-1",
        ),
        received_at=time.monotonic() if now is None else now,
    )
    assert runtime.tool_gateway.take_outbound() is not None


def test_session_http_lifecycle_stop_and_result_release_concurrency() -> None:
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        _register(runtime)
        created = client.post(
            "/tools/robot.serve:invoke",
            json={"arguments": {"server_url": "ws://127.0.0.1:8000"}},
        )
        assert created.status_code == 202
        created_data = created.json()["data"]
        invocation_id = created_data["invocation_id"]
        # Session deadline exemption: the snapshot never carries a deadline.
        assert "deadline_ms" not in created_data
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        assert provider_invoke.envelope.endpoint_instance_id == "arm-instance-1"

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(
                ToolAccepted(details={"service": "policy"}),
                provider_invoke.envelope,
            ),
            received_at=time.monotonic(),
        )
        assert client.get(f"/invocations/{invocation_id}").json()["data"]["phase"] == (
            "accepted"
        )
        status_request = runtime.tool_gateway.take_outbound()
        assert status_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_status_response_envelope(
                ToolExecutionStatus(phase="running"), status_request.envelope
            ),
            received_at=time.monotonic(),
        )
        # Note: snapshot only — an HTTP GET here would refresh_action() and
        # queue yet another status request ahead of the control request.
        assert runtime.tool_gateway.action_invocation_snapshot(invocation_id)[
            "phase"
        ] == "running"

        stop = client.post(f"/invocations/{invocation_id}/stop")
        assert stop.status_code == 202
        assert stop.json()["data"]["stop_status"] == "requested"
        control_request = runtime.tool_gateway.take_outbound()
        assert control_request is not None
        assert control_request.envelope.payload["command"] == "stop"
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_control_response_envelope(
                ToolControlResponse(command="stop", status="accepted"),
                control_request.envelope,
            ),
            received_at=time.monotonic(),
        )
        snapshot = runtime.tool_gateway.action_invocation_snapshot(invocation_id)
        assert snapshot is not None
        assert snapshot["stop_status"] == "accepted"
        # The phase stays provider-driven; acceptance alone never terminates.
        assert snapshot["phase"] == "running"

        _, tool_context = invoke_request_from_envelope(provider_invoke.envelope)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_event_envelope(
                ToolEvent(type="stopped", data={}),
                tool_context,
                endpoint_instance_id="arm-instance-1",
                sequence=0,
            ),
            received_at=time.monotonic(),
        )
        result_request = runtime.tool_gateway.take_outbound()
        assert result_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_result_response_envelope(
                ToolResultResponse(
                    status="available",
                    result=ToolResult(status="stopped", outputs={}),
                ),
                result_request.envelope,
            ),
            received_at=time.monotonic(),
        )
        result = client.get(f"/invocations/{invocation_id}/result")
        assert result.status_code == 200
        assert result.json()["data"]["result"]["status"] == "stopped"
        final = client.get(f"/invocations/{invocation_id}").json()["data"]
        assert final["phase"] == "stopped"
        assert final["occupies_concurrency"] is False

        # A second stop on a terminal session is answered from the record.
        again = client.post(f"/invocations/{invocation_id}/stop")
        assert again.status_code == 200
        assert again.json()["data"]["stop_status"] == "terminal"

        # The released concurrency fence admits a fresh session invocation.
        reuse = client.post(
            "/tools/robot.serve:invoke",
            json={"arguments": {}},
        )
        assert reuse.status_code == 202
    finally:
        runtime.close()


def test_duplicate_dora_session_execution_key_rejection_preserves_original() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        context = ToolContext(
            execution_key=ToolExecutionKey("duplicate-session", "attempt"),
            tool_id="robot.serve",
            implementation_id="arm.primary",
            endpoint_id="robot.arm",
            operation="serve",
        )
        first = make_logical_invoke_request_envelope(
            ToolRequest({}), context, request_id="first"
        )
        duplicate = make_logical_invoke_request_envelope(
            ToolRequest({}), context, request_id="duplicate"
        )
        runtime.tool_gateway.handle_input(
            "tool_request", first, received_at=time.monotonic()
        )
        provider_request = runtime.tool_gateway.take_outbound()
        assert provider_request is not None
        runtime.tool_gateway.handle_input(
            "tool_request", duplicate, received_at=time.monotonic()
        )
        rejection = runtime.tool_gateway.take_outbound()
        assert rejection is not None
        assert rejection.envelope.request_id == "duplicate"
        assert runtime.tool_gateway.action_invocations.snapshot(
            ("duplicate-session", "attempt"), now=time.monotonic()
        ) is not None
    finally:
        runtime.close()


def test_deadline_exemption_marks_action_unknown_but_spares_session() -> None:
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        _register(runtime)
        action_created = client.post(
            "/tools/robot.pick:invoke",
            json={"arguments": {"object": "cup"}, "caller_id": "test"},
        )
        assert action_created.status_code == 202
        action_data = action_created.json()["data"]
        assert "deadline_ms" in action_data

        session_created = client.post(
            "/tools/robot.serve:invoke",
            json={"arguments": {}, "caller_id": "test"},
        )
        assert session_created.status_code == 202
        session_data = session_created.json()["data"]
        assert "deadline_ms" not in session_data

        changed = runtime.tool_gateway.action_invocations.mark_deadlines_ambiguous(
            now=time.monotonic(),
            epoch_ms=int(time.time() * 1_000) + 100_000,
        )
        assert changed == 1
        action_snapshot = runtime.tool_gateway.action_invocation_snapshot(
            action_data["invocation_id"]
        )
        session_snapshot = runtime.tool_gateway.action_invocation_snapshot(
            session_data["invocation_id"]
        )
        assert action_snapshot is not None
        assert action_snapshot["phase"] == "unknown"
        assert session_snapshot is not None
        assert session_snapshot["phase"] == "dispatching"
    finally:
        runtime.close()
