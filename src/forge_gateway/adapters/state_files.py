"""Runtime snapshot and event-log file store."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    from forge_common import get_logger
except Exception:  # pragma: no cover - fallback for minimal test envs
    import logging

    def get_logger(name: str) -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

logger = get_logger(__name__)


class StateFileStore:
    def __init__(self, state_dir: str | None, *, write_context_snapshot: bool) -> None:
        self.enabled = False
        self.write_context_snapshot = write_context_snapshot
        self.state_dir: Path | None = Path(state_dir) if state_dir else None
        self.event_log_path: Path | None = None
        self.snapshot_path: Path | None = None
        if self.state_dir is None:
            return
        try:
            self.state_dir.mkdir(parents=True, exist_ok=True)
            self.event_log_path = self.state_dir / "gateway_events.jsonl"
            self.snapshot_path = self.state_dir / "runtime_context.json"
            self.enabled = True
        except Exception as exc:
            logger.warning("gateway: disable agent state files: %s", exc)
            self.state_dir = None

    def append_event(self, event_type: str, data: dict[str, Any], *, now: float) -> None:
        if self.event_log_path is None:
            return
        event = {"ts": now, "type": event_type, "data": data}
        try:
            with self.event_log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception as exc:
            logger.warning("gateway: append agent event failed: %s", exc)

    def write_snapshot(self, payload: dict[str, Any]) -> None:
        if not self.write_context_snapshot or self.snapshot_path is None:
            return
        try:
            tmp_path = self.snapshot_path.with_suffix(".json.tmp")
            tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp_path.replace(self.snapshot_path)
        except Exception as exc:
            logger.warning("gateway: write runtime context snapshot failed: %s", exc)
