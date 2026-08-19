"""Action manifest domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class CompletionSpec:
    values: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.values)


@dataclass(frozen=True)
class ActionDefinition:
    name: str
    command: str
    policy_id: str
    robot_id: str
    manifest_path: str = ""
    required_parameters: list[str] = field(default_factory=list)
    input_mapping: dict[str, str] = field(default_factory=dict)
    resources: list[str] = field(default_factory=list)
    timeout_s: float | None = None
    result_semantics: str = "command_completed"
    completion: dict[str, Any] = field(default_factory=dict)
    description: str = ""

    @classmethod
    def from_dict(
        cls,
        action_name: str,
        data: dict[str, Any] | str,
        *,
        policy_id: str = "default",
        robot_id: str = "",
        manifest_path: str = "",
    ) -> "ActionDefinition":
        if isinstance(data, str):
            return cls(
                name=action_name,
                command=data,
                policy_id=policy_id,
                robot_id=robot_id,
                manifest_path=manifest_path,
            )
        if not isinstance(data, dict):
            raise ValueError(f"agent action {action_name} must be a mapping or command string")

        command = data.get("command")
        if not isinstance(command, str) or not command:
            raise ValueError(f"agent action {action_name} requires non-empty command")

        required = data.get("required_parameters", data.get("required", []))
        if not isinstance(required, list) or not all(isinstance(x, str) for x in required):
            raise ValueError(f"agent action {action_name}.required_parameters must be a list of strings")

        input_mapping = data.get("input_mapping", data.get("map", {}))
        if not isinstance(input_mapping, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in input_mapping.items()
        ):
            raise ValueError(f"agent action {action_name}.input_mapping must map strings to strings")

        resources = data.get("resources", [])
        if not isinstance(resources, list) or not all(isinstance(x, str) for x in resources):
            raise ValueError(f"agent action {action_name}.resources must be a list of strings")

        completion = data.get("completion", {"type": "policy_status"})
        if completion is None:
            completion = {}
        if not isinstance(completion, dict):
            raise ValueError(f"agent action {action_name}.completion must be a mapping")

        timeout_raw = data.get("timeout_s")
        description = data.get("description", "")
        return cls(
            name=action_name,
            command=command,
            policy_id=policy_id,
            robot_id=robot_id,
            manifest_path=manifest_path,
            required_parameters=list(required),
            input_mapping=dict(input_mapping),
            resources=list(resources),
            timeout_s=max(1.0, float(timeout_raw)) if timeout_raw is not None else None,
            result_semantics=str(data.get("result_semantics", "command_completed")),
            completion=dict(completion),
            description=str(description) if description is not None else "",
        )

    def to_capability(self, *, include_identity: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "command": self.command,
            "required_parameters": list(self.required_parameters),
            "input_mapping": dict(self.input_mapping),
            "resources": list(self.resources),
            "timeout_s": self.timeout_s,
            "result_semantics": self.result_semantics,
            "completion": dict(self.completion),
            "description": self.description,
        }
        if include_identity:
            payload = {
                "policy_id": self.policy_id,
                "robot_id": self.robot_id,
                **payload,
            }
        return payload


@dataclass(frozen=True)
class ActionManifest:
    version: int
    robot_id: str
    policy_id: str
    path: str
    actions: dict[str, ActionDefinition]
    policy_command_topic: str = ""
    status_topic: str = ""
