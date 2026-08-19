"""Direct policy command HTTP route."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from forge_gateway.controllers.utils import (
    command_unavailable_response,
    json_response,
    read_json,
)
from forge_gateway.domain.commands import CommandMailboxUnavailable


def register_policy_routes(app: FastAPI, runtime: Any) -> None:
    @app.post("/policy/command")
    async def policy_command(request: Request) -> JSONResponse:
        body = await read_json(request)
        if isinstance(body, JSONResponse):
            return body
        command = body.get("command")
        if not isinstance(command, str) or not command:
            return json_response(400, {"ok": False, "msg": "command must be non-empty string"})
        inputs = body.get("inputs") or {}
        if not isinstance(inputs, dict):
            return json_response(400, {"ok": False, "msg": "inputs must be an object"})
        try:
            runtime.enqueue_policy_command(command, inputs)
        except CommandMailboxUnavailable as error:
            return command_unavailable_response(error)
        return json_response(200, {"ok": True})
