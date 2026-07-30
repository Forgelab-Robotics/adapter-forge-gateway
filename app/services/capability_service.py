"""Capability payload generation."""

from __future__ import annotations

from typing import Any

from app.services.action_registry import ActionRegistry


class CapabilityService:
    def __init__(
        self,
        *,
        policy_id: str,
        action_manifests: list[str],
        action_registry: ActionRegistry,
    ) -> None:
        self._policy_id = policy_id
        self._action_manifests = list(action_manifests)
        self._action_registry = action_registry

    def payload(self) -> dict[str, Any]:
        policies: dict[str, dict[str, Any]] = {}
        actions = self._action_registry.list_actions()
        for name, action in actions.items():
            policy = policies.setdefault(
                action.policy_id,
                {
                    "policy_id": action.policy_id,
                    "robot_id": action.robot_id,
                    "manifest": action.manifest_path,
                    "actions": {},
                },
            )
            policy["actions"][name] = action.to_capability(include_identity=False)

        return {
            "api_version": "paos-forge-gateway-mvp-plus.v1",
            "policy_id": self._policy_id,
            "action_manifests": list(self._action_manifests),
            "supports": {
                "sessions": True,
                "command_id": True,
                "cancel": True,
                "reset": True,
                "estop": False,
                "runtime_context": True,
                "serial_actions_only": True,
            },
            "policies": policies,
            "actions": {name: action.to_capability(include_identity=True) for name, action in actions.items()},
        }
