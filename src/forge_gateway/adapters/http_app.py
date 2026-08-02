"""FastAPI application factory."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from forge_gateway import __version__
from forge_gateway.controllers.agent_controller import register_agent_routes
from forge_gateway.controllers.playback_controller import register_playback_routes
from forge_gateway.controllers.policy_controller import register_policy_routes
from forge_gateway.controllers.record_controller import register_record_routes
from forge_gateway.controllers.runtime_controller import register_runtime_routes
from forge_gateway.controllers.websocket_controller import register_websocket_routes


STATIC_DIR = Path(__file__).resolve().parents[1] / "resources" / "static"


def create_app(runtime: Any, stop_event: threading.Event | None = None) -> FastAPI:
    app = FastAPI(title="Forge Gateway", version=__version__)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False, response_class=FileResponse)
    async def collector_ui() -> FileResponse:
        return FileResponse(
            STATIC_DIR / "index.html",
            headers={"Cache-Control": "no-cache"},
        )

    register_runtime_routes(app, runtime, stop_event=stop_event)
    register_agent_routes(app, runtime)
    register_record_routes(app, runtime)
    register_playback_routes(app, runtime)
    register_policy_routes(app, runtime)
    register_websocket_routes(app, runtime)
    return app
