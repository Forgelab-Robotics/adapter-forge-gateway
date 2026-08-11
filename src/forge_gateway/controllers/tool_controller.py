"""Gateway Tool discovery and Query invocation HTTP routes."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from forge_tool import ToolError

from forge_gateway.controllers.utils import json_response, read_json
from forge_gateway.services.tool_gateway_service import (
    ToolGatewayMailboxFull,
    ToolGatewayUnavailable,
    tool_error_from_response,
)

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
        return {"ok": True, "data": runtime.tool_gateway.discovery_snapshot()}

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
            ticket = runtime.tool_gateway.submit_http_invoke(
                endpoint_id,
                operation,
                arguments,
                caller_id=caller_id,
                timeout_ms=timeout_ms,
            )
        except (ToolGatewayMailboxFull, ToolGatewayUnavailable) as error:
            return json_response(503, {"ok": False, "msg": str(error)})
        except ValueError as error:
            return json_response(400, {"ok": False, "msg": str(error)})

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


__all__ = ["register_tool_routes"]
