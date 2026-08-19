"""Gateway Tool discovery and Query invocation HTTP routes."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from forge_tool import ToolError

from forge_gateway.controllers.utils import json_response, read_json
from forge_gateway.services.tool_gateway_service import tool_error_from_response

_INVOKE_BODY_KEYS = frozenset(("arguments", "caller_id", "timeout_ms"))


def _error_payload(error: ToolError) -> dict[str, Any]:
    return {
        "code": error.code,
        "message": error.message,
        "retryable": error.retryable,
        "details": dict(error.details),
    }


def _status_for_tool_error(error: ToolError) -> int:
    if error.code in (
        "FORGE_TOOL_ENDPOINT_UNKNOWN",
        "FORGE_TOOL_OPERATION_NOT_FOUND",
    ):
        return 404
    if error.code == "FORGE_TOOL_REQUEST_CONFLICT":
        return 409
    if error.code == "FORGE_TOOL_INVOKE_TIMEOUT":
        return 504
    if error.code in (
        "FORGE_TOOL_ENDPOINT_UNAVAILABLE",
        "FORGE_TOOL_GATEWAY_BUSY",
        "FORGE_TOOL_GATEWAY_UNAVAILABLE",
        "FORGE_TOOL_TRANSPORT_UNAVAILABLE",
    ):
        return 503
    if error.code == "FORGE_TOOL_QUERY_NON_TERMINAL_RESPONSE":
        return 502
    return 422


def register_tool_routes(app: FastAPI, runtime: Any) -> None:
    @app.get("/tools")
    async def list_tools() -> dict[str, Any]:
        snapshot = (
            runtime.tool_gateway.tool_spec_snapshot()
            if runtime.tool_gateway.catalog.list()
            else runtime.tool_gateway.discovery_snapshot()
        )
        return {"ok": True, "data": snapshot}

    @app.get("/tools/{tool_id}")
    async def get_tool(tool_id: str) -> JSONResponse:
        spec = runtime.tool_gateway.get_tool_spec(tool_id)
        if spec is None:
            return json_response(404, {"ok": False, "msg": "ToolSpec not found"})
        return json_response(200, {"ok": True, "data": spec.to_dict()})

    @app.get("/tools/{tool_id}/context")
    async def get_tool_context(tool_id: str) -> JSONResponse:
        spec = runtime.tool_gateway.get_tool_spec(tool_id)
        if spec is None:
            return json_response(404, {"ok": False, "msg": "ToolSpec not found"})
        registration = None
        binding_error = None
        try:
            _, registration = runtime.tool_gateway.resolve_tool_spec(tool_id)
        except (KeyError, LookupError, RuntimeError, ValueError) as error:
            binding_error = str(error)
        gateway_readiness = (
            runtime.readiness() if callable(getattr(runtime, "readiness", None)) else {}
        )
        endpoint_status = (
            None
            if registration is None
            else runtime.tool_gateway.endpoint_readiness(
                registration.endpoint_id, registration.endpoint_instance_id
            )
        )
        requirements: dict[str, bool] = {}
        for requirement in spec.readiness:
            if requirement == "proprio_state":
                requirements[requirement] = bool(
                    gateway_readiness.get("proprio_state_ready", False)
                )
            elif requirement == "ws:state":
                requirements[requirement] = bool(
                    gateway_readiness.get("state_client_ready", False)
                )
            elif requirement == "ws:images":
                requirements[requirement] = bool(
                    gateway_readiness.get("image_client_ready", False)
                )
            elif requirement.startswith("image:"):
                requirements[requirement] = bool(
                    gateway_readiness.get("images", {}).get(
                        requirement.partition(":")[2], False
                    )
                )
            else:
                requirements[requirement] = False
        ready = (
            registration is not None
            and all(requirements.values())
            and (
                endpoint_status is None
                or endpoint_status.get("state") in ("ready", "busy")
            )
        )
        return json_response(
            200,
            {
                "ok": True,
                "data": {
                    "tool_id": tool_id,
                    "ready": ready,
                    "requirements": requirements,
                    "gateway_readiness": gateway_readiness,
                    "endpoint_status": endpoint_status,
                    "binding_error": binding_error,
                    "robot_frame_profile": (
                        None
                        if spec.robot_frame_profile is None
                        else spec.robot_frame_profile.to_dict()
                    ),
                },
            },
        )

    async def _action_invoke(tool_id: str, request: Request) -> JSONResponse:
        body = await read_json(request)
        if isinstance(body, JSONResponse):
            return body
        unknown = sorted(set(body) - _INVOKE_BODY_KEYS)
        if unknown:
            return json_response(
                400, {"ok": False, "msg": f"unknown request field(s): {unknown}"}
            )
        arguments = body.get("arguments", {})
        if not isinstance(arguments, dict):
            return json_response(
                400, {"ok": False, "msg": "arguments must be an object"}
            )
        try:
            invocation = runtime.tool_gateway.submit_http_action(
                tool_id,
                arguments,
                caller_id=body.get("caller_id"),
                timeout_ms=body.get("timeout_ms"),
            )
        except KeyError:
            return json_response(404, {"ok": False, "msg": "ToolSpec not found"})
        except LookupError as error:
            return json_response(409, {"ok": False, "msg": str(error)})
        except ValueError as error:
            return json_response(400, {"ok": False, "msg": str(error)})
        except RuntimeError as error:
            return json_response(503, {"ok": False, "msg": str(error)})
        return json_response(202, {"ok": True, "data": invocation})

    app.add_api_route("/tools/{tool_id}:invoke", _action_invoke, methods=["POST"])

    @app.post("/tools/{endpoint_id}/{operation}:invoke")
    async def invoke_tool(
        endpoint_id: str,
        operation: str,
        request: Request,
    ) -> JSONResponse:
        body = await read_json(request)
        if isinstance(body, JSONResponse):
            return body
        unknown_keys = sorted(set(body) - _INVOKE_BODY_KEYS)
        if unknown_keys:
            return json_response(
                400,
                {
                    "ok": False,
                    "msg": "unknown request field(s): "
                    + ", ".join(repr(key) for key in unknown_keys),
                },
            )
        arguments = body.get("arguments", {})
        if not isinstance(arguments, dict):
            return json_response(
                400,
                {"ok": False, "msg": "arguments must be a JSON object"},
            )
        caller_id = body.get("caller_id")
        if caller_id is not None and (
            not isinstance(caller_id, str) or not caller_id.strip()
        ):
            return json_response(
                400,
                {
                    "ok": False,
                    "msg": "caller_id must be a non-empty string when provided",
                },
            )
        timeout_ms = body.get("timeout_ms")
        if timeout_ms is not None and (
            isinstance(timeout_ms, bool)
            or not isinstance(timeout_ms, int)
            or timeout_ms < 1
        ):
            return json_response(
                400,
                {"ok": False, "msg": "timeout_ms must be a positive integer"},
            )
        try:
            registration = runtime.tool_gateway.resolve_registered_endpoint(
                endpoint_id, operation
            )
            operation_descriptor = (
                None
                if registration is None
                else next(
                    item
                    for item in registration.descriptor.operations
                    if item.name == operation
                )
            )
            if (
                operation_descriptor is not None
                and operation_descriptor.semantics == "action"
            ):
                matching_spec = next(
                    (
                        spec
                        for spec in runtime.tool_gateway.catalog.list()
                        if spec.endpoint_id == endpoint_id
                        and spec.operation == operation
                        and spec.semantics == "action"
                    ),
                    None,
                )
                if matching_spec is None:
                    return json_response(
                        409,
                        {
                            "ok": False,
                            "msg": "Action descriptor has no matching ToolSpec binding",
                        },
                    )
                invocation = runtime.tool_gateway.submit_http_action(
                    matching_spec.tool_id,
                    arguments,
                    caller_id=caller_id,
                    timeout_ms=timeout_ms,
                )
                return json_response(202, {"ok": True, "data": invocation})
            ticket = runtime.tool_gateway.submit_http_invoke(
                endpoint_id,
                operation,
                arguments,
                caller_id=caller_id,
                timeout_ms=timeout_ms,
            )
        except LookupError as error:
            return json_response(404, {"ok": False, "msg": str(error)})
        except ValueError as error:
            return json_response(400, {"ok": False, "msg": str(error)})
        except RuntimeError as error:
            return json_response(503, {"ok": False, "msg": str(error)})

        wrapped = asyncio.wrap_future(ticket.future)
        try:
            if ticket.future.done():
                response = ticket.future.result()
            else:
                remaining = max(0.0, ticket.deadline - time.monotonic())
                response = await asyncio.wait_for(
                    asyncio.shield(wrapped),
                    timeout=remaining,
                )
        except TimeoutError:
            runtime.tool_gateway.cancel_http_invoke(ticket, timed_out=True)
            response = await asyncio.shield(wrapped)
        except asyncio.CancelledError:
            runtime.tool_gateway.cancel_http_invoke(ticket, timed_out=False)
            raise

        tool_error = tool_error_from_response(response)
        if tool_error is not None:
            return json_response(
                _status_for_tool_error(tool_error),
                {
                    "ok": False,
                    "msg": tool_error.message,
                    "error": _error_payload(tool_error),
                },
            )
        return json_response(
            200,
            {
                "ok": True,
                "data": {
                    "endpoint_id": endpoint_id,
                    "operation": operation,
                    "response": dict(response.payload),
                },
            },
        )

    @app.get("/invocations/{invocation_id}")
    async def get_invocation(invocation_id: str) -> JSONResponse:
        value = runtime.tool_gateway.action_invocation_snapshot(invocation_id)
        if value is None:
            return json_response(404, {"ok": False, "msg": "invocation not found"})
        if value["phase"] not in (
            "completed",
            "failed",
            "cancelled",
            "stopped",
            "unknown",
        ):
            with contextlib.suppress(RuntimeError):
                runtime.tool_gateway.refresh_action(invocation_id, "status")
        return json_response(200, {"ok": True, "data": value})

    @app.get("/invocations/{invocation_id}/result")
    async def get_invocation_result(invocation_id: str) -> JSONResponse:
        value = runtime.tool_gateway.action_result_snapshot(invocation_id)
        if value is None:
            return json_response(404, {"ok": False, "msg": "invocation not found"})
        if value["status"] == "pending":
            with contextlib.suppress(RuntimeError):
                runtime.tool_gateway.refresh_action(invocation_id, "result")
        status = 200 if value["status"] == "available" else 202
        return json_response(status, {"ok": True, "data": value})

    @app.post("/invocations/{invocation_id}/cancel")
    async def cancel_invocation(invocation_id: str) -> JSONResponse:
        try:
            status = runtime.tool_gateway.cancel_action(invocation_id)
        except KeyError:
            return json_response(404, {"ok": False, "msg": "invocation not found"})
        except RuntimeError as error:
            return json_response(503, {"ok": False, "msg": str(error)})
        code = 202 if status == "requested" else 200
        return json_response(
            code,
            {
                "ok": True,
                "data": {
                    "invocation_id": invocation_id,
                    "cancel_status": status,
                },
            },
        )

    @app.get("/invocations/{invocation_id}/events")
    async def invocation_events(invocation_id: str, request: Request) -> Any:
        if runtime.tool_gateway.action_invocation_snapshot(invocation_id) is None:
            return json_response(404, {"ok": False, "msg": "invocation not found"})
        last_event_id = request.headers.get("last-event-id", "-1")
        try:
            after = int(last_event_id)
        except ValueError:
            return json_response(400, {"ok": False, "msg": "invalid Last-Event-ID"})
        if after < -1:
            return json_response(400, {"ok": False, "msg": "invalid Last-Event-ID"})
        snapshot = runtime.tool_gateway.action_invocation_snapshot(invocation_id)
        if snapshot is None:
            return json_response(404, {"ok": False, "msg": "invocation not found"})
        execution_key = (invocation_id, snapshot["attempt_id"])
        window = runtime.tool_gateway.action_invocations.event_window(
            execution_key, now=time.monotonic()
        )
        if window is None:
            return json_response(404, {"ok": False, "msg": "invocation not found"})
        oldest, next_sequence = window
        if after < oldest - 1:
            return json_response(
                410,
                {
                    "ok": False,
                    "msg": "Last-Event-ID is older than retained event history",
                    "oldest_event_id": oldest,
                },
            )
        if after >= next_sequence and after != -1:
            return json_response(
                400, {"ok": False, "msg": "Last-Event-ID is ahead of event history"}
            )

        async def stream():
            sequence = after
            while True:
                if await request.is_disconnected():
                    return
                events = runtime.tool_gateway.action_invocations.events_after(
                    execution_key, sequence, now=time.monotonic()
                )
                if events is None:
                    return
                if events:
                    for event in events:
                        sequence = event.sequence
                        payload = json.dumps(event.to_dict(), separators=(",", ":"))
                        yield (
                            f"id: {sequence}\nevent: {event.type}\ndata: {payload}\n\n"
                        )
                else:
                    snapshot = runtime.tool_gateway.action_invocation_snapshot(
                        invocation_id
                    )
                    if snapshot is None or snapshot["phase"] in (
                        "completed",
                        "failed",
                        "cancelled",
                        "stopped",
                        "unknown",
                    ):
                        return
                    yield ": keepalive\n\n"
                    await asyncio.sleep(1.0)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )


__all__ = ["register_tool_routes"]
