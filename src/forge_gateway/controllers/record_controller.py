"""Record HTTP routes."""

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


def register_record_routes(app: FastAPI, runtime: Any) -> None:
    @app.post("/record/control")
    async def record_control(request: Request) -> JSONResponse:
        body = await read_json(request)
        if isinstance(body, JSONResponse):
            return body
        action = body.get("action")
        commands = {
            "START": "start_recording",
            "STOP": "stop_recording",
            "DISCARD": "discard_recording",
        }
        if action not in commands:
            return json_response(400, {"ok": False, "msg": f"invalid record action: {action!r}"})
        inputs: dict[str, Any] = {}
        if action == "START" and "output_path" in body:
            inputs["output_path"] = body["output_path"]
        if action == "START" and "metadata" in body:
            metadata = body["metadata"]
            if not isinstance(metadata, dict):
                return json_response(400, {"ok": False, "msg": "metadata must be an object"})
            inputs["metadata"] = metadata
        try:
            runtime.enqueue_policy_command(commands[action], inputs)
        except CommandMailboxUnavailable as error:
            return command_unavailable_response(error)
        return json_response(200, {"ok": True})

    @app.post("/record/set_root")
    async def record_set_root(request: Request) -> JSONResponse:
        body = await read_json(request)
        if isinstance(body, JSONResponse):
            return body
        root = body.get("root")
        if not isinstance(root, str) or not root:
            return json_response(400, {"ok": False, "msg": "root must be non-empty string"})
        try:
            runtime.set_record_root(root)
        except CommandMailboxUnavailable as error:
            return command_unavailable_response(error)
        return json_response(200, {"ok": True})

    @app.get("/record/status")
    async def record_status() -> dict[str, Any]:
        with runtime.lock:
            return {"ok": True, "data": dict(runtime.record_status)}
