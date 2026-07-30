"""Gateway CLI bootstrap."""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
from dataclasses import replace
from pathlib import Path

import uvicorn

try:
    from forge_common import get_logger
except Exception:  # pragma: no cover - fallback for minimal test envs
    import logging

    def get_logger(name: str) -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

from forge_gateway.adapters.dora_adapter import DoraEventBuffer, drain_commands, handle_dora_input
from forge_gateway.adapters.http_app import create_app
from forge_gateway.config import load_config
from forge_gateway.services.runtime_service import GatewayRuntime

logger = get_logger(__name__)

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Forge unified gateway node")
    parser.add_argument("--config", type=str, default=None, help="YAML config path")
    parser.add_argument("--host", type=str, default=None, help="HTTP/WebSocket host override")
    parser.add_argument("--port", type=int, default=None, help="HTTP/WebSocket port override")
    parser.add_argument(
        "--print-capabilities",
        action="store_true",
        help="Print supported agent/runtime interfaces as JSON and exit",
    )
    return parser.parse_args()


def _run_server(runtime: GatewayRuntime, stop_event: threading.Event) -> uvicorn.Server:
    app = create_app(runtime, stop_event=stop_event)
    config = uvicorn.Config(
        app,
        host=runtime.config.host,
        port=runtime.config.port,
        log_level="info",
        lifespan="off",
        ws_ping_interval=None,
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="gateway_uvicorn", daemon=True)
    thread.start()
    server.thread = thread  # type: ignore[attr-defined]
    return server


def _watch_launcher(stop_event: threading.Event) -> threading.Thread:
    def _watch() -> None:
        pid_file = os.environ.get("FORGE_LAUNCHER_PID_FILE")
        if not pid_file:
            return
        path = Path(pid_file)
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and not path.is_file():
            if stop_event.wait(timeout=0.2):
                return
        if not path.is_file():
            logger.error("FORGE_LAUNCHER_PID_FILE=%s did not appear; gateway exits", pid_file)
            stop_event.set()
            return
        while not stop_event.wait(timeout=1.0):
            if not path.is_file():
                logger.info("forge_launcher PID file disappeared; gateway exits")
                stop_event.set()
                return
            try:
                pid = int(path.read_text().strip())
                os.kill(pid, 0)
            except ProcessLookupError:
                logger.info("forge_launcher is no longer running; gateway exits")
                stop_event.set()
                return
            except (PermissionError, OSError, ValueError):
                continue

    thread = threading.Thread(target=_watch, name="forge_launcher_watch", daemon=True)
    thread.start()
    return thread


def main() -> int:
    args = _parse_args()
    config = load_config(args.config)
    if args.host is not None:
        config = replace(config, host=args.host)
    if args.port is not None:
        config = replace(config, port=args.port)

    if args.print_capabilities:
        runtime = GatewayRuntime(config)
        try:
            print(json.dumps(runtime.agent_capabilities(), ensure_ascii=False, indent=2, sort_keys=True))
        finally:
            runtime.close()
        return 0

    if not config.joint_order:
        logger.error("gateway requires joint_order in config")
        return 1

    stop_event = threading.Event()
    runtime = GatewayRuntime(config)
    server = _run_server(runtime, stop_event)
    _watch_launcher(stop_event)
    logger.info("gateway serving http/ws on %s:%s", config.host, config.port)

    from dora import Node

    node = Node()
    event_queue = DoraEventBuffer()
    end_sentinel = object()

    def _dora_iter() -> None:
        try:
            for event in node:
                if stop_event.is_set():
                    break
                event_queue.put(event)
        except Exception as e:
            logger.exception("gateway: dora iteration failed: %s", e)
            event_queue.put({"type": "ERROR", "error": str(e)})
        finally:
            event_queue.put(end_sentinel)

    iter_thread = threading.Thread(target=_dora_iter, name="gateway_dora_iter", daemon=True)
    iter_thread.start()

    try:
        while not stop_event.is_set():
            event = event_queue.get(timeout=0.2)

            if event is end_sentinel:
                break
            if event is not None:
                event_type = event.get("type")
                if event_type == "STOP":
                    break
                if event_type == "ERROR":
                    with runtime.lock:
                        runtime.last_error = f"dora error: {event.get('error', 'unknown')}"
                    break
                if event_type == "INPUT":
                    try:
                        handle_dora_input(runtime, str(event.get("id")), event.get("value"))
                    except Exception as e:
                        logger.warning("gateway: handle input %s failed: %s", event.get("id"), e)
                        with runtime.lock:
                            runtime.last_error = f"input {event.get('id')} failed: {e}"

            drain_commands(runtime, node)
    except KeyboardInterrupt:
        logger.info("gateway received KeyboardInterrupt")
    finally:
        stop_event.set()
        runtime.close()
        server.should_exit = True
        thread = getattr(server, "thread", None)
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        if iter_thread.is_alive():
            iter_thread.join(timeout=2.0)
            if iter_thread.is_alive():
                logger.error("Dora thread did not exit; forcing process exit")
                os._exit(1)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
