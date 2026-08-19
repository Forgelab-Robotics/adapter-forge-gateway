from __future__ import annotations

import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from forge_tool import (
    TOOL_ENDPOINT_PROTOCOL,
    EndpointStatus,
    ToolAccepted,
    ToolContext,
    ToolControlResponse,
    ToolEndpointDescriptor,
    ToolEnvelope,
    ToolEvent,
    ToolExecutionKey,
    ToolExecutionStatus,
    ToolOperationDescriptor,
    ToolRequest,
    ToolResult,
    ToolResultResponse,
    error_from_payload,
    invoke_request_from_envelope,
    make_control_response_envelope,
    make_endpoint_status_envelope,
    make_event_envelope,
    make_invoke_response_envelope,
    make_registration_envelope,
    make_result_response_envelope,
    make_status_response_envelope,
)

from forge_gateway.config import GatewayConfig
from forge_gateway.controllers.tool_controller import register_tool_routes
from forge_gateway.domain.action_invocations import (
    ActionInvocation,
    ActionInvocationCapacityError,
    ActionInvocationStore,
)
from forge_gateway.services.runtime_service import GatewayRuntime
from forge_gateway.services.tool_gateway_service import (
    make_logical_invoke_request_envelope,
)


def _config(*, lease_ttl_ms: int = 15_000) -> GatewayConfig:
    return GatewayConfig.from_dict(
        {
            "joint_order": [],
            "agent": {"action_manifests": []},
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
                        "readiness": ["proprio_state"],
                        "robot_frame_profile": {
                            "robot_id": "arm-1",
                            "base_frame": "base",
                            "tool_frame": "gripper",
                            "directions": {
                                "forward": {
                                    "frame": "base",
                                    "axis": "x",
                                    "sign": 1,
                                }
                            },
                        },
                    }
                ],
            },
        }
    )


def _register(
    runtime: GatewayRuntime,
    *,
    now: float | None = None,
    max_concurrency: int = 2,
) -> None:
    descriptor = ToolEndpointDescriptor(
        protocol_version=TOOL_ENDPOINT_PROTOCOL,
        endpoint_id="robot.arm",
        operations=(
            ToolOperationDescriptor(
                name="pick",
                semantics="action",
                cancellable=True,
                status_supported=True,
                max_concurrency=max_concurrency,
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


def test_action_http_lifecycle_result_cancel_and_events() -> None:
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        _register(runtime)
        created = client.post(
            "/tools/robot.pick:invoke",
            json={"arguments": {"object": "cup"}, "caller_id": "test"},
        )
        assert created.status_code == 202
        created_data = created.json()["data"]
        invocation_id = created_data["invocation_id"]
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        assert provider_invoke.envelope.endpoint_instance_id == "arm-instance-1"

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
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

        context = provider_invoke.envelope
        _, tool_context = invoke_request_from_envelope(context)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_event_envelope(
                ToolEvent(type="progress", data={"percent": 25}),
                tool_context,
                endpoint_instance_id="arm-instance-1",
                sequence=0,
            ),
            received_at=time.monotonic(),
        )
        events = runtime.tool_gateway.action_invocations.events_after(
            (invocation_id, created_data["attempt_id"]), -1, now=time.monotonic()
        )
        assert events is not None
        assert events[0].data == {"percent": 25}

        cancel = client.post(f"/invocations/{invocation_id}/cancel")
        assert cancel.status_code == 202
        control_request = runtime.tool_gateway.take_outbound()
        assert control_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_control_response_envelope(
                ToolControlResponse(command="cancel", status="accepted"),
                control_request.envelope,
            ),
            received_at=time.monotonic(),
        )
        snapshot = runtime.tool_gateway.action_invocation_snapshot(invocation_id)
        assert snapshot is not None
        assert snapshot["cancel_status"] == "accepted"
        assert snapshot["phase"] == "running"

        pending = client.get(f"/invocations/{invocation_id}/result")
        assert pending.status_code == 202
        result_request = runtime.tool_gateway.take_outbound()
        assert result_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_result_response_envelope(
                ToolResultResponse(
                    status="available",
                    result=ToolResult(status="cancelled", outputs={}),
                ),
                result_request.envelope,
            ),
            received_at=time.monotonic(),
        )
        result = client.get(f"/invocations/{invocation_id}/result")
        assert result.status_code == 200
        assert result.json()["data"]["result"]["status"] == "cancelled"
    finally:
        runtime.close()


def test_endpoint_status_context_and_lease_loss_become_unknown() -> None:
    runtime = GatewayRuntime(_config(lease_ttl_ms=100))
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        now = time.monotonic()
        _register(runtime, now=now)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_endpoint_status_envelope(
                EndpointStatus(endpoint_id="robot.arm", state="ready"),
                endpoint_instance_id="arm-instance-1",
            ),
            received_at=now + 0.01,
        )
        context = client.get("/tools/robot.pick/context")
        assert context.status_code == 200
        assert context.json()["data"]["endpoint_status"]["state"] == "ready"
        expected_directions = {"forward": {"frame": "base", "axis": "x", "sign": 1}}
        assert context.json()["data"]["robot_frame_profile"]["directions"] == (
            expected_directions
        )
        tool = client.get("/tools/robot.pick")
        assert tool.status_code == 200
        assert tool.json()["data"]["robot_frame_profile"]["directions"] == (
            expected_directions
        )

        created = runtime.tool_gateway.submit_http_action(
            "robot.pick", {}, now=now + 0.02
        )
        runtime.tool_gateway.sweep(now=now + 0.1)
        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"], now=now + 0.1
        )
        assert snapshot is not None
        assert snapshot["phase"] == "failed"
        assert snapshot["result"]["status"] == "failed"
        assert runtime.tool_gateway.take_outbound() is None
    finally:
        runtime.close()


def test_dora_action_invoke_returns_accepted_without_provider_instance() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        context = ToolContext(
            execution_key=ToolExecutionKey("caller-invocation", "caller-attempt"),
            tool_id="robot.pick",
            implementation_id="arm.primary",
            endpoint_id="robot.arm",
            operation="pick",
            caller_id="dora:test",
        )
        caller_request = make_logical_invoke_request_envelope(
            ToolRequest({"object": "cup"}),
            context,
            request_id="caller-request",
        )
        runtime.tool_gateway.handle_input(
            "tool_request",
            caller_request,
            received_at=time.monotonic(),
        )
        provider_request = runtime.tool_gateway.take_outbound()
        assert provider_request is not None
        assert provider_request.envelope.endpoint_instance_id == "arm-instance-1"

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(
                ToolAccepted(details={"queued": True}), provider_request.envelope
            ),
            received_at=time.monotonic(),
        )
        caller_response = runtime.tool_gateway.take_outbound()
        assert caller_response is not None
        assert caller_response.output_id == "tool_response"
        assert caller_response.envelope.endpoint_instance_id is None
        assert caller_response.envelope.payload["outcome"] == "accepted"

        _, provider_context = invoke_request_from_envelope(provider_request.envelope)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_event_envelope(
                ToolEvent(type="progress", data={"percent": 50}),
                provider_context,
                endpoint_instance_id="arm-instance-1",
                sequence=0,
            ),
            received_at=time.monotonic(),
        )
        caller_event = runtime.tool_gateway.take_outbound()
        assert caller_event is not None
        assert caller_event.envelope.message_type == "tool.event"
        assert caller_event.envelope.endpoint_instance_id is None
        assert caller_event.envelope.sequence == 0
    finally:
        runtime.close()


def test_action_phase_never_regresses_and_accepted_is_independent_barrier() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        key = (created["invocation_id"], created["attempt_id"])
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None

        assert runtime.tool_gateway.refresh_action(created["invocation_id"], "status")
        status_request = runtime.tool_gateway.take_outbound()
        assert status_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_status_response_envelope(
                ToolExecutionStatus(phase="running"), status_request.envelope
            ),
            received_at=time.monotonic(),
        )
        _, context = invoke_request_from_envelope(provider_invoke.envelope)
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_event_envelope(
                ToolEvent(type="progress", data={"early": True}),
                context,
                endpoint_instance_id="arm-instance-1",
                sequence=0,
            ),
            received_at=time.monotonic(),
        )
        assert runtime.tool_gateway.action_invocations.events_after(
            key, -1, now=time.monotonic()
        ) == ()

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        snapshot = runtime.tool_gateway.action_invocations.snapshot(
            key, now=time.monotonic()
        )
        assert snapshot is not None
        assert snapshot["phase"] == "running"
        assert snapshot["accepted"] is True
        assert len(
            runtime.tool_gateway.action_invocations.events_after(
                key, -1, now=time.monotonic()
            )
            or ()
        ) == 1
    finally:
        runtime.close()


def test_terminal_status_queues_result_barrier_lookup() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        assert runtime.tool_gateway.refresh_action(created["invocation_id"], "status")
        status_request = runtime.tool_gateway.take_outbound()
        assert status_request is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_status_response_envelope(
                ToolExecutionStatus(phase="completed"), status_request.envelope
            ),
            received_at=time.monotonic(),
        )

        result_request = runtime.tool_gateway.take_outbound()
        assert result_request is not None
        assert result_request.envelope.message_type == "tool.result.request"
        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert snapshot is not None
        assert snapshot["terminal_status_hint"] == "completed"
        assert snapshot["phase"] == "accepted"

        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_result_response_envelope(
                ToolResultResponse(
                    status="available",
                    result=ToolResult(status="succeeded", outputs={"ok": True}),
                ),
                result_request.envelope,
            ),
            received_at=time.monotonic(),
        )
        assert runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )["phase"] == "completed"
    finally:
        runtime.close()


def test_repeated_cancel_uses_one_exchange_and_does_not_regress_status() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )

        assert (
            runtime.tool_gateway.cancel_action(created["invocation_id"])
            == "requested"
        )
        assert (
            runtime.tool_gateway.cancel_action(created["invocation_id"])
            == "requested"
        )
        control = runtime.tool_gateway.take_outbound()
        assert control is not None
        assert control.envelope.message_type == "tool.control.request"
        assert runtime.tool_gateway.take_outbound() is None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_control_response_envelope(
                ToolControlResponse(command="cancel", status="accepted"),
                control.envelope,
            ),
            received_at=time.monotonic(),
        )
        assert (
            runtime.tool_gateway.cancel_action(created["invocation_id"])
            == "accepted"
        )
        assert runtime.tool_gateway.take_outbound() is None
    finally:
        runtime.close()


def test_observation_exchange_failure_does_not_make_execution_unknown() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        runtime.tool_gateway.refresh_action(created["invocation_id"], "status")
        status_request = runtime.tool_gateway.take_outbound()
        assert status_request is not None

        assert runtime.tool_gateway.output_failed(
            status_request, RuntimeError("status send failed")
        )
        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert snapshot is not None
        assert snapshot["phase"] == "accepted"
        assert snapshot["observation_error"]["code"] == (
            "FORGE_TOOL_TRANSPORT_UNAVAILABLE"
        )
    finally:
        runtime.close()


def test_deadline_unknown_keeps_concurrency_until_instance_isolated() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime, max_concurrency=1)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        item = runtime.tool_gateway.action_invocations.get(
            (created["invocation_id"], created["attempt_id"]),
            now=time.monotonic(),
        )
        assert item is not None and item.deadline_ms is not None
        runtime.tool_gateway.action_invocations.mark_deadlines_ambiguous(
            now=time.monotonic(), epoch_ms=item.deadline_ms
        )
        with pytest.raises(ActionInvocationCapacityError):
            runtime.tool_gateway.submit_http_action("robot.pick", {})

        runtime.tool_gateway.action_invocations.mark_instance_ambiguous(
            "robot.arm",
            "arm-instance-1",
            now=time.monotonic(),
            reason="instance isolated",
        )
        replacement = runtime.tool_gateway.submit_http_action("robot.pick", {})
        assert replacement["phase"] == "dispatching"
    finally:
        runtime.close()


def test_action_exchange_timeout_is_swept_without_poisoning_observation() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        runtime.tool_gateway.refresh_action(created["invocation_id"], "status")
        status_request = runtime.tool_gateway.take_outbound()
        assert status_request is not None
        runtime.tool_gateway.sweep(now=time.monotonic() + 6)

        snapshot = runtime.tool_gateway.action_invocation_snapshot(
            created["invocation_id"]
        )
        assert snapshot is not None
        assert snapshot["phase"] == "accepted"
        assert snapshot["observation_error"]["code"] == "FORGE_TOOL_EXCHANGE_TIMEOUT"
        assert not runtime.tool_gateway._action_exchanges
    finally:
        runtime.close()


def test_dora_execution_key_attempts_are_isolated_and_http_id_is_ambiguous() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        for attempt_id in ("attempt-1", "attempt-2"):
            context = ToolContext(
                execution_key=ToolExecutionKey("shared-invocation", attempt_id),
                tool_id="robot.pick",
                implementation_id="arm.primary",
                endpoint_id="robot.arm",
                operation="pick",
            )
            runtime.tool_gateway.handle_input(
                "tool_request",
                make_logical_invoke_request_envelope(
                    ToolRequest({"attempt": attempt_id}),
                    context,
                    request_id=f"request-{attempt_id}",
                ),
                received_at=time.monotonic(),
            )
        provider_requests = [
            runtime.tool_gateway.take_outbound(),
            runtime.tool_gateway.take_outbound(),
        ]
        assert all(request is not None for request in provider_requests)
        assert {
            request.envelope.attempt_id
            for request in provider_requests
            if request is not None
        } == {"attempt-1", "attempt-2"}
        for provider_request in provider_requests:
            assert provider_request is not None
            runtime.tool_gateway.handle_input(
                "arm/to_gateway",
                make_invoke_response_envelope(
                    ToolAccepted(), provider_request.envelope
                ),
                received_at=time.monotonic(),
            )
        assert runtime.tool_gateway.take_outbound() is not None
        assert runtime.tool_gateway.take_outbound() is not None

        for message_type, attempt_id in (
            ("tool.status.request", "attempt-2"),
            ("tool.result.request", "attempt-2"),
            ("tool.control.request", "attempt-1"),
        ):
            payload = (
                {"command": "cancel"}
                if message_type == "tool.control.request"
                else {}
            )
            logical = ToolEnvelope(
                protocol=TOOL_ENDPOINT_PROTOCOL,
                message_type=message_type,  # type: ignore[arg-type]
                request_id=f"{message_type}-{attempt_id}",
                invocation_id="shared-invocation",
                attempt_id=attempt_id,
                endpoint_id="robot.arm",
                endpoint_instance_id=None,
                operation="pick",
                payload=payload,
            )
            runtime.tool_gateway.handle_input(
                "tool_request", logical, received_at=time.monotonic()
            )
            provider_request = runtime.tool_gateway.take_outbound()
            assert provider_request is not None
            assert provider_request.envelope.attempt_id == attempt_id
            assert provider_request.envelope.message_type == message_type
        assert runtime.tool_gateway.action_invocation_snapshot(
            "shared-invocation"
        ) is None
    finally:
        runtime.close()


def test_dora_action_admission_rejection_is_correlated_and_instance_less() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        context = ToolContext(
            execution_key=ToolExecutionKey("bad-invocation", "bad-attempt"),
            tool_id="robot.pick",
            implementation_id="wrong.implementation",
            endpoint_id="robot.arm",
            operation="pick",
        )
        request = make_logical_invoke_request_envelope(
            ToolRequest({}), context, request_id="bad-request"
        )
        runtime.tool_gateway.handle_input(
            "tool_request", request, received_at=time.monotonic()
        )
        response = runtime.tool_gateway.take_outbound()
        assert response is not None
        assert response.envelope.endpoint_instance_id is None
        assert response.envelope.request_id == request.request_id
        assert error_from_payload(response.envelope.payload).code == (
            "FORGE_TOOL_ACTION_ADMISSION_REJECTED"
        )
        assert runtime.tool_gateway.take_outbound() is None
    finally:
        runtime.close()


def test_duplicate_dora_execution_key_rejection_preserves_original() -> None:
    runtime = GatewayRuntime(_config())
    try:
        _register(runtime)
        context = ToolContext(
            execution_key=ToolExecutionKey("duplicate-invocation", "attempt"),
            tool_id="robot.pick",
            implementation_id="arm.primary",
            endpoint_id="robot.arm",
            operation="pick",
        )
        first = make_logical_invoke_request_envelope(
            ToolRequest({"generation": 1}), context, request_id="first"
        )
        duplicate = make_logical_invoke_request_envelope(
            ToolRequest({"generation": 2}), context, request_id="duplicate"
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
            ("duplicate-invocation", "attempt"), now=time.monotonic()
        ) is not None
        assert any(
            exchange.provider_request == provider_request.envelope
            for exchange in runtime.tool_gateway._action_exchanges.values()
        )
    finally:
        runtime.close()


def test_store_total_capacity_is_hard_bounded_by_terminal_eviction() -> None:
    store = ActionInvocationStore(capacity=2, event_capacity=1, retention_seconds=60)

    def invocation(index: int) -> ActionInvocation:
        return ActionInvocation(
            invocation_id=f"inv-{index}",
            attempt_id=f"attempt-{index}",
            tool_id="robot.pick",
            implementation_id="arm.primary",
            endpoint_id="robot.arm",
            endpoint_instance_id="instance",
            operation="pick",
            caller_id=None,
            deadline_ms=None,
            created_at=float(index),
        )

    for index in range(3):
        value = invocation(index)
        store.create(value, now=float(index))
        store.set_phase(
            value.key,
            "completed",
            now=float(index),
            result=ToolResult(status="succeeded", outputs={}),
        )
    assert len(store._items) == 2
    assert store.snapshot(("inv-0", "attempt-0"), now=3.0) is None


def test_toolspec_binding_and_readiness_configuration_are_strict() -> None:
    base = {
        "joint_order": [],
        "agent": {"action_manifests": []},
        "tools": {
            "enabled": True,
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
                }
            ],
        },
    }
    duplicate = {
        **base,
        "tools": {
            **base["tools"],
            "specs": [
                *base["tools"]["specs"],
                {
                    **base["tools"]["specs"][0],
                    "tool_id": "robot.pick.alias",
                },
            ],
        },
    }
    with pytest.raises(ValueError, match="bindings must be unique"):
        GatewayConfig.from_dict(duplicate)

    invalid_readiness = {
        **base,
        "tools": {
            **base["tools"],
            "specs": [
                {
                    **base["tools"]["specs"][0],
                    "readiness": ["proprio_typo"],
                }
            ],
        },
    }
    with pytest.raises(ValueError, match="unknown requirement"):
        GatewayConfig.from_dict(invalid_readiness)


@pytest.mark.parametrize(
    ("directions", "match"),
    [
        pytest.param([], "directions must be a YAML mapping", id="directions-list"),
        pytest.param({"": {}}, "direction name", id="blank-direction-name"),
        pytest.param(
            {"forward": []}, "forward must be a YAML mapping", id="entry-list"
        ),
        pytest.param(
            {"forward": {"frame": "base", "axis": "x"}},
            "missing .*sign",
            id="missing-sign",
        ),
        pytest.param(
            {"forward": {"frame": "base", "axis": "x", "sign": 1, "unit": "m"}},
            "unknown .*unit",
            id="unknown-entry-key",
        ),
        pytest.param(
            {"forward": {"frame": "", "axis": "x", "sign": 1}},
            "forward.frame",
            id="blank-frame",
        ),
        pytest.param(
            {"forward": {"frame": "base", "axis": "forward", "sign": 1}},
            "forward.axis",
            id="invalid-axis",
        ),
        pytest.param(
            {"forward": {"frame": "base", "axis": "x", "sign": 0}},
            "forward.sign",
            id="invalid-sign",
        ),
        pytest.param(
            {"forward": {"frame": "base", "axis": "x", "sign": True}},
            "forward.sign",
            id="boolean-sign",
        ),
    ],
)
def test_robot_frame_directions_configuration_is_strict(
    directions: object, match: str
) -> None:
    spec = {
        "tool_id": "robot.pick",
        "implementation_id": "arm.primary",
        "endpoint_id": "robot.arm",
        "operation": "pick",
        "semantics": "action",
        "robot_frame_profile": {
            "robot_id": "arm-1",
            "base_frame": "base",
            "directions": directions,
        },
    }
    with pytest.raises(ValueError, match=match):
        GatewayConfig.from_dict(
            {
                "joint_order": [],
                "agent": {"action_manifests": []},
                "tools": {"specs": [spec]},
            }
        )


def test_sse_reports_retention_gap_and_sets_streaming_headers() -> None:
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    client = TestClient(app)
    try:
        _register(runtime)
        created = runtime.tool_gateway.submit_http_action("robot.pick", {})
        key = (created["invocation_id"], created["attempt_id"])
        provider_invoke = runtime.tool_gateway.take_outbound()
        assert provider_invoke is not None
        runtime.tool_gateway.handle_input(
            "arm/to_gateway",
            make_invoke_response_envelope(ToolAccepted(), provider_invoke.envelope),
            received_at=time.monotonic(),
        )
        _, context = invoke_request_from_envelope(provider_invoke.envelope)
        for sequence in range(3):
            runtime.tool_gateway.handle_input(
                "arm/to_gateway",
                make_event_envelope(
                    ToolEvent(type="progress", data={"sequence": sequence}),
                    context,
                    endpoint_instance_id="arm-instance-1",
                    sequence=sequence,
                ),
                received_at=time.monotonic(),
            )
        runtime.tool_gateway.action_invocations.set_phase(
            key,
            "completed",
            now=time.monotonic(),
            result=ToolResult(status="succeeded", outputs={}),
        )

        gap = client.get(
            f"/invocations/{created['invocation_id']}/events",
            headers={"Last-Event-ID": "-1"},
        )
        assert gap.status_code == 410
        stream = client.get(
            f"/invocations/{created['invocation_id']}/events",
            headers={"Last-Event-ID": "0"},
        )
        assert stream.status_code == 200
        assert stream.headers["cache-control"] == "no-cache"
        assert stream.headers["x-accel-buffering"] == "no"
        assert "id: 1" in stream.text
        assert "id: 2" in stream.text
    finally:
        runtime.close()


def test_query_unknown_operation_returns_404_instead_of_500() -> None:
    runtime = GatewayRuntime(_config())
    app = FastAPI()
    register_tool_routes(app, runtime)
    try:
        _register(runtime)
        response = TestClient(app).post(
            "/tools/robot.arm/missing:invoke", json={"arguments": {}}
        )
        assert response.status_code == 404
    finally:
        runtime.close()
