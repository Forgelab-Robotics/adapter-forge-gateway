"""Gateway CLI bootstrap."""

from __future__ import annotations

import argparse
import json
import signal
from collections.abc import Callable
from dataclasses import replace
from types import FrameType
from typing import Any

try:
    from forge_common import get_logger
except Exception:  # pragma: no cover - fallback for minimal test envs
    import logging

    def get_logger(name: str) -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

from forge_gateway.application import GatewayApplication
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


def _install_shutdown_handlers(
    application: GatewayApplication,
) -> Callable[[], None]:
    previous: dict[signal.Signals, Any] = {}

    def request_shutdown(signum: int, frame: FrameType | None) -> None:
        del frame
        logger.info("gateway received signal %s", signum)
        application.request_shutdown()

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, request_shutdown)
    except ValueError:
        for signum, handler in previous.items():
            try:
                signal.signal(signum, handler)
            except ValueError:
                pass
        return lambda: None

    def restore() -> None:
        for signum, handler in previous.items():
            signal.signal(signum, handler)

    return restore


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
            try:
                close_succeeded = runtime.close()
            except Exception as error:
                logger.error("gateway capability runtime cleanup failed: %s", error)
                close_succeeded = False
        return 0 if close_succeeded else 1

    if not config.joint_order:
        logger.error("gateway requires joint_order in config")
        return 1

    application = GatewayApplication(config)
    restore_signal_handlers = _install_shutdown_handlers(application)
    try:
        try:
            application.start()
        except Exception as error:
            logger.error("gateway startup failed: %s", error)
            try:
                rollback = application.close()
            except BaseException as close_error:
                logger.error("gateway startup rollback failed: %s", close_error)
                return 1
            if not rollback.quiescent:
                logger.error(
                    "gateway startup rollback incomplete: %s",
                    rollback.describe_failures(),
                )
            return 1

        logger.info("gateway serving http/ws on %s:%s", config.host, config.port)
        run_failed = False
        try:
            reason = application.run()
            logger.info("gateway Dora loop exited: %s", reason)
            run_failed = reason == "reader_error"
        except KeyboardInterrupt:
            logger.info("gateway received KeyboardInterrupt")
        except Exception as error:
            logger.exception("gateway run loop failed: %s", error)
            run_failed = True
        finally:
            try:
                close_result = application.close()
            except BaseException as error:
                logger.error("gateway shutdown orchestration failed: %s", error)
                close_result = None

        if close_result is None:
            return 1
        if not close_result.ok:
            logger.error(
                "gateway shutdown incomplete: %s",
                close_result.describe_failures(),
            )
            return 1
        return 1 if run_failed else 0
    finally:
        restore_signal_handlers()


if __name__ == "__main__":
    raise SystemExit(main())
