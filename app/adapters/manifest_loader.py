"""Markdown action manifest loader."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml

from app.domain.action_manifest import ActionDefinition, ActionManifest


def default_action_manifests() -> list[Path]:
    candidates: list[Path] = []
    bundle_root = getattr(sys, "_MEIPASS", None)
    if bundle_root:
        candidates.append(Path(bundle_root) / "actions" / "piper" / "sam3.md")
    candidates.append(Path(__file__).resolve().parents[2] / "actions" / "piper" / "sam3.md")
    candidates.append(Path.cwd() / "packages" / "nodes" / "gateway" / "actions" / "piper" / "sam3.md")
    candidates.append(Path.cwd() / "actions" / "piper" / "sam3.md")
    for path in candidates:
        if path.is_file():
            return [path]
    return []


def resolve_path(path: Path, base_dir: Path | None) -> Path:
    if path.is_absolute():
        return path
    if base_dir is not None:
        return (base_dir / path).resolve()
    return (Path.cwd() / path).resolve()


def load_action_manifests(paths: list[Path]) -> list[ActionManifest]:
    manifests: list[ActionManifest] = []
    seen_actions: dict[str, str] = {}
    for path in paths:
        manifest = load_action_manifest(path)
        for action_name, action in manifest.actions.items():
            if action_name in seen_actions:
                raise ValueError(
                    f"duplicate agent action {action_name!r} in {action.manifest_path}; "
                    f"already defined in {seen_actions[action_name]}"
                )
            seen_actions[action_name] = action.manifest_path
        manifests.append(manifest)
    return manifests


def load_action_manifest(path: Path) -> ActionManifest:
    if not path.is_file():
        raise FileNotFoundError(f"action manifest not found: {path}")
    frontmatter = read_frontmatter(path)
    version = frontmatter.get("version", 1)
    if int(version) != 1:
        raise ValueError(f"unsupported action manifest version {version!r}: {path}")
    robot_id = frontmatter.get("robot_id")
    policy_id = frontmatter.get("policy_id")
    if not isinstance(robot_id, str) or not robot_id:
        raise ValueError(f"action manifest requires robot_id: {path}")
    if not isinstance(policy_id, str) or not policy_id:
        raise ValueError(f"action manifest requires policy_id: {path}")
    raw_actions = frontmatter.get("actions")
    if not isinstance(raw_actions, dict):
        raise ValueError(f"action manifest requires actions mapping: {path}")
    actions = {
        str(name): ActionDefinition.from_dict(
            str(name),
            value,
            policy_id=policy_id,
            robot_id=robot_id,
            manifest_path=str(path),
        )
        for name, value in raw_actions.items()
    }
    return ActionManifest(
        version=int(version),
        robot_id=robot_id,
        policy_id=policy_id,
        path=str(path),
        actions=actions,
        policy_command_topic=str(frontmatter.get("policy_command_topic") or ""),
        status_topic=str(frontmatter.get("status_topic") or ""),
    )


def read_frontmatter(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        raise ValueError(f"action manifest must start with YAML frontmatter: {path}")
    for idx, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            raw = "\n".join(lines[1:idx])
            data = yaml.safe_load(raw)
            if not isinstance(data, dict):
                raise ValueError(f"action manifest frontmatter must be a mapping: {path}")
            return data
    raise ValueError(f"action manifest frontmatter is not closed: {path}")
