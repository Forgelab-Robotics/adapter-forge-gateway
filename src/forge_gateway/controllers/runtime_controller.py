"""Runtime HTTP routes."""

from __future__ import annotations

import os
import signal
import threading
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from forge_gateway.controllers.utils import json_response, read_json


def register_runtime_routes(
    app: FastAPI,
    runtime: Any,
    *,
    stop_event: threading.Event | None = None,
) -> None:
    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"ok": True, "msg": "gateway healthy"}

    @app.get("/runtime/status")
    async def runtime_status() -> dict[str, Any]:
        return {
            "ok": True,
            "data": {
                "readiness": runtime.readiness(),
                "state": runtime.state_snapshot(),
            },
        }

    @app.post("/runtime/start")
    async def runtime_start(request: Request) -> JSONResponse:
        body = await read_json(request)
        readiness = runtime.readiness()
        if not readiness["ready"]:
            return json_response(409, {"ok": False, "msg": "runtime is not ready", "data": readiness})
        command = body.get("command", "start")
        if not isinstance(command, str) or not command:
            return json_response(400, {"ok": False, "msg": "command must be non-empty string"})
        inputs = body.get("inputs") or {}
        if not isinstance(inputs, dict):
            return json_response(400, {"ok": False, "msg": "inputs must be an object"})
        runtime.enqueue_policy_command(command, inputs)
        return json_response(200, {"ok": True, "data": readiness})

    @app.post("/runtime/reset_scene")
    async def runtime_reset_scene(request: Request) -> JSONResponse:
        body = await read_json(request)
        status_code, response = runtime.agent_runtime_reset(body)
        return json_response(200 if status_code == 202 else status_code, response)

    @app.post("/runtime/stop")
    async def runtime_stop() -> JSONResponse:
        pid_file = os.environ.get("FORGE_LAUNCHER_PID_FILE")
        if pid_file:
            try:
                pid = int(Path(pid_file).read_text().strip())
                os.kill(pid, signal.SIGTERM)
                return json_response(200, {"ok": True, "msg": "stop requested"})
            except Exception as e:
                return json_response(502, {"ok": False, "msg": f"launcher stop failed: {e}"})
        if stop_event is not None:
            stop_event.set()
        return json_response(200, {"ok": True, "msg": "local stop requested"})
