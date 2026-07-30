"""Agent action registry."""

from __future__ import annotations

from forge_gateway.domain.action_manifest import ActionDefinition, ActionManifest


class ActionRegistry:
    """Index action manifests by public action name."""

    def __init__(self, manifests: list[ActionManifest]) -> None:
        self._manifests = list(manifests)
        self._actions: dict[str, ActionDefinition] = {}
        for manifest in self._manifests:
            for name, action in manifest.actions.items():
                if name in self._actions:
                    raise ValueError(
                        f"duplicate agent action {name!r} in {action.manifest_path}; "
                        f"already defined in {self._actions[name].manifest_path}"
                    )
                self._actions[name] = action

    @classmethod
    def from_actions(cls, actions: dict[str, ActionDefinition]) -> "ActionRegistry":
        registry = cls([])
        registry._actions = dict(actions)
        return registry

    def get(self, action_type: str) -> ActionDefinition | None:
        return self._actions.get(action_type)

    def list_actions(self) -> dict[str, ActionDefinition]:
        return dict(self._actions)

    def supported_action_names(self) -> list[str]:
        return sorted(self._actions)
