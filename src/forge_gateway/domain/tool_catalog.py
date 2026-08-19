"""Configuration-owned caller-facing Tool specifications."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from forge_gateway.domain.tool_directory import RegisteredEndpoint


def _non_blank(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


@dataclass(frozen=True)
class RobotFrameProfile:
    """Minimal robot/frame context exposed to Tool callers."""

    robot_id: str
    base_frame: str
    tool_frame: str | None = None
    frames: Mapping[str, str] = field(default_factory=dict)
    directions: Mapping[str, Mapping[str, str | int]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _non_blank(self.robot_id, "robot_id")
        _non_blank(self.base_frame, "base_frame")
        if self.tool_frame is not None:
            _non_blank(self.tool_frame, "tool_frame")
        if not isinstance(self.frames, Mapping) or any(
            not isinstance(key, str)
            or not key.strip()
            or not isinstance(value, str)
            or not value.strip()
            for key, value in self.frames.items()
        ):
            raise ValueError("frames must map non-empty names to non-empty frame IDs")
        object.__setattr__(self, "frames", dict(self.frames))
        if not isinstance(self.directions, Mapping):
            raise TypeError("directions must be a mapping")
        directions: dict[str, dict[str, str | int]] = {}
        for name, direction in self.directions.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError("directions must use non-empty direction names")
            if not isinstance(direction, Mapping):
                raise TypeError(f"direction {name!r} must be a mapping")
            if set(direction) != {"frame", "axis", "sign"}:
                raise ValueError(
                    f"direction {name!r} must contain exactly frame, axis, and sign"
                )
            frame = direction["frame"]
            axis = direction["axis"]
            sign = direction["sign"]
            if not isinstance(frame, str) or not frame.strip():
                raise ValueError(f"directions.{name}.frame must be a non-empty string")
            if axis not in ("x", "y", "z"):
                raise ValueError(f"directions.{name}.axis must be x, y, or z")
            if type(sign) is not int or sign not in (-1, 1):
                raise ValueError(f"directions.{name}.sign must be -1 or 1")
            directions[name] = {"frame": frame, "axis": axis, "sign": sign}
        object.__setattr__(self, "directions", directions)

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "robot_id": self.robot_id,
            "base_frame": self.base_frame,
            "frames": dict(self.frames),
            "directions": {
                name: dict(direction) for name, direction in self.directions.items()
            },
        }
        if self.tool_frame is not None:
            value["tool_frame"] = self.tool_frame
        return value


@dataclass(frozen=True)
class ToolSpec:
    """One stable Tool ID bound to exactly one endpoint operation."""

    tool_id: str
    implementation_id: str
    endpoint_id: str
    operation: str
    semantics: str
    description: str = ""
    input_schema: Mapping[str, Any] = field(default_factory=dict)
    output_schema: Mapping[str, Any] = field(default_factory=dict)
    readiness: tuple[str, ...] = ()
    robot_frame_profile: RobotFrameProfile | None = None

    def __post_init__(self) -> None:
        for name in ("tool_id", "implementation_id", "endpoint_id", "operation"):
            _non_blank(getattr(self, name), name)
        if self.semantics not in ("query", "action"):
            raise ValueError("ToolSpec semantics must be query or action")
        if not isinstance(self.description, str):
            raise TypeError("description must be a string")
        if not isinstance(self.input_schema, Mapping) or not isinstance(
            self.output_schema, Mapping
        ):
            raise TypeError("input_schema and output_schema must be objects")
        requirements = tuple(self.readiness)
        if any(not isinstance(item, str) or not item.strip() for item in requirements):
            raise ValueError("readiness entries must be non-empty strings")
        if len(requirements) != len(set(requirements)):
            raise ValueError("readiness entries must be unique")
        object.__setattr__(self, "input_schema", dict(self.input_schema))
        object.__setattr__(self, "output_schema", dict(self.output_schema))
        object.__setattr__(self, "readiness", requirements)

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "tool_id": self.tool_id,
            "implementation_id": self.implementation_id,
            "endpoint_id": self.endpoint_id,
            "operation": self.operation,
            "semantics": self.semantics,
            "description": self.description,
            "input_schema": dict(self.input_schema),
            "output_schema": dict(self.output_schema),
            "readiness": list(self.readiness),
        }
        if self.robot_frame_profile is not None:
            value["robot_frame_profile"] = self.robot_frame_profile.to_dict()
        return value


class ToolSpecCatalog:
    """Immutable, configuration-driven ToolSpec lookup."""

    def __init__(self, specs: tuple[ToolSpec, ...] | list[ToolSpec] = ()) -> None:
        values = tuple(specs)
        identifiers = [spec.tool_id for spec in values]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("ToolSpec tool_id values must be unique")
        bindings = [(spec.endpoint_id, spec.operation) for spec in values]
        if len(bindings) != len(set(bindings)):
            raise ValueError("ToolSpec endpoint/operation bindings must be unique")
        self._specs = {spec.tool_id: spec for spec in values}

    def get(self, tool_id: str) -> ToolSpec | None:
        return self._specs.get(tool_id)

    def list(self) -> tuple[ToolSpec, ...]:
        return tuple(self._specs[key] for key in sorted(self._specs))

    @staticmethod
    def validate_binding(spec: ToolSpec, registration: RegisteredEndpoint) -> None:
        operation = next(
            (
                item
                for item in registration.descriptor.operations
                if item.name == spec.operation
            ),
            None,
        )
        if registration.endpoint_id != spec.endpoint_id or operation is None:
            raise ValueError("ToolSpec binding is absent from the active descriptor")
        if operation.semantics != spec.semantics:
            raise ValueError(
                "ToolSpec semantics does not match the active endpoint descriptor"
            )


__all__ = ["RobotFrameProfile", "ToolSpec", "ToolSpecCatalog"]
