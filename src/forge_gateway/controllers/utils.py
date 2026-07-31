"""Controller helpers."""

from __future__ import annotations

import json
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse

from forge_gateway.domain.commands import CommandMailboxUnavailable


async def read_json(request: Request) -> dict[str, Any] | JSONResponse:
    try:
        value = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return json_response(
            400,
            {"ok": False, "msg": "request body must be a JSON object"},
        )
    if not isinstance(value, dict):
        return json_response(
            400,
            {"ok": False, "msg": "request body must be a JSON object"},
        )
    return value


def json_response(status_code: int, body: dict[str, Any]) -> JSONResponse:
    return JSONResponse(status_code=status_code, content=body)


def command_unavailable_response(error: CommandMailboxUnavailable) -> JSONResponse:
    return json_response(503, {"ok": False, "msg": str(error)})
