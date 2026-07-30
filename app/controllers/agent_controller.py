"""Agent HTTP routes."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.controllers.utils import json_response, read_json


def register_agent_routes(app: FastAPI, runtime: Any) -> None:
    @app.post("/agent/sessions")
    async def agent_create_session(request: Request) -> JSONResponse:
        body = await read_json(request)
        status_code, response = runtime.create_agent_session(body)
        return json_response(status_code, response)

    @app.get("/agent/sessions/{session_id}")
    async def agent_get_session(session_id: str) -> JSONResponse:
        status_code, response = runtime.get_agent_session(session_id)
        return json_response(status_code, response)

    @app.post("/agent/sessions/{session_id}/cancel")
    async def agent_cancel_session(session_id: str) -> JSONResponse:
        status_code, response = runtime.cancel_agent_session(session_id)
        return json_response(status_code, response)

    @app.get("/agent/runtime/status")
    async def agent_runtime_status() -> dict[str, Any]:
        return {"ok": True, "data": runtime.agent_runtime_status()}

    @app.get("/agent/runtime/context")
    async def agent_runtime_context() -> dict[str, Any]:
        return {"ok": True, "data": runtime.agent_runtime_context()}

    @app.get("/agent/runtime/capabilities")
    async def agent_runtime_capabilities() -> dict[str, Any]:
        return {"ok": True, "data": runtime.agent_capabilities()}

    @app.post("/agent/runtime/reset")
    async def agent_runtime_reset(request: Request) -> JSONResponse:
        body = await read_json(request)
        status_code, response = runtime.agent_runtime_reset(body)
        return json_response(status_code, response)
