"""Playback HTTP routes."""

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


def register_playback_routes(app: FastAPI, runtime: Any) -> None:
    @app.post("/playback/control")
    async def playback_control(request: Request) -> JSONResponse:
        body = await read_json(request)
        if isinstance(body, JSONResponse):
            return body
        action = body.get("action")
        command_map = {
            "START": "start_playback",
            "PAUSE": "pause_playback",
            "RESUME": "resume_playback",
            "RESET": "reset_playback",
        }
        if action not in command_map:
            return json_response(400, {"ok": False, "msg": f"invalid playback action: {action!r}"})
        inputs: dict[str, Any] = {}
        if "mcap_path" in body:
            inputs["mcap_path"] = body["mcap_path"]
        try:
            runtime.enqueue_policy_command(command_map[str(action)], inputs)
        except CommandMailboxUnavailable as error:
            return command_unavailable_response(error)
        return json_response(200, {"ok": True})

    @app.get("/playback/status")
    async def playback_status() -> dict[str, Any]:
        with runtime.lock:
            return {"ok": True, "data": dict(runtime.playback_status)}
