"""Controller helpers."""

from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse


async def read_json(request: Request) -> dict[str, Any]:
    try:
        value = await request.json()
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def json_response(status_code: int, body: dict[str, Any]) -> JSONResponse:
    return JSONResponse(status_code=status_code, content=body)
