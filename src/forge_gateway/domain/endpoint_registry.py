"""Transport-independent ToolEndpoint Registry with monotonic leases."""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass
from typing import Literal

from forge_tool import (
    EndpointRegistryResponse,
    ToolEndpointDescriptor,
    ToolEnvelope,
    ToolError,
    validate_message_envelope,
    validate_registration_envelope,
)

_MAX_SAFE_JSON_INTEGER = 9_007_199_254_740_991
_MANAGEMENT_OPERATIONS = {
    "endpoint.register": "register",
    "endpoint.heartbeat": "heartbeat",
    "endpoint.unregister": "unregister",
}


@dataclass(frozen=True)
class EndpointSourceAuthority:
    """Trusted transport identity allowed to manage one logical endpoint."""

    source_id: str
    generation: int
    endpoint_id: str

    def __post_init__(self) -> None:
        for field_name in ("source_id", "endpoint_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or not 0 <= self.generation <= _MAX_SAFE_JSON_INTEGER
        ):
            raise ValueError(f"generation must be in [0, {_MAX_SAFE_JSON_INTEGER}]")


@dataclass(frozen=True)
class RegisteredEndpoint:
    """One accepted current endpoint instance and its trusted source binding."""

    descriptor: ToolEndpointDescriptor
    endpoint_instance_id: str
    authority: EndpointSourceAuthority
    registry_revision: int
    expires_at: float

    @property
    def endpoint_id(self) -> str:
        return self.descriptor.endpoint_id


@dataclass(frozen=True)
class _EndpointTombstone:
    endpoint_instance_id: str
    authority: EndpointSourceAuthority
    registry_revision: int
    reason: Literal["unregister", "expired"]


class EndpointRegistry:
    """Thread-safe Registry enforcing one current leased instance per endpoint ID."""

    def __init__(self, *, lease_ttl_ms: int = 15_000) -> None:
        if (
            isinstance(lease_ttl_ms, bool)
            or not isinstance(lease_ttl_ms, int)
            or not 1 <= lease_ttl_ms <= _MAX_SAFE_JSON_INTEGER
        ):
            raise ValueError(f"lease_ttl_ms must be in [1, {_MAX_SAFE_JSON_INTEGER}]")
        self._lease_ttl_ms = lease_ttl_ms
        self._lease_ttl_seconds = lease_ttl_ms / 1_000
        self._lock = threading.RLock()
        self._revision = 0
        self._current: dict[str, RegisteredEndpoint] = {}
        self._tombstones: dict[str, _EndpointTombstone] = {}

    @property
    def lease_ttl_ms(self) -> int:
        return self._lease_ttl_ms

    @property
    def revision(self) -> int:
        with self._lock:
            return self._revision

    @staticmethod
    def _validate_now(now: float) -> float:
        if isinstance(now, bool) or not isinstance(now, (int, float)):
            raise ValueError("now must be a finite monotonic time")
        value = float(now)
        if not math.isfinite(value) or value < 0:
            raise ValueError("now must be a finite non-negative monotonic time")
        return value

    def _next_revision_locked(self) -> int:
        if self._revision >= _MAX_SAFE_JSON_INTEGER:
            raise RuntimeError("endpoint Registry revision is exhausted")
        self._revision += 1
        return self._revision

    def _accepted(
        self,
        operation: str,
        *,
        lease: bool,
        registry_revision: int | None = None,
    ) -> EndpointRegistryResponse:
        return EndpointRegistryResponse(
            operation=operation,  # type: ignore[arg-type]
            status="accepted",
            registry_revision=(
                self._revision if registry_revision is None else registry_revision
            ),
            lease_ttl_ms=self._lease_ttl_ms if lease else None,
        )

    def _rejected(
        self,
        operation: str,
        code: str,
        message: str,
        *,
        retryable: bool = False,
    ) -> EndpointRegistryResponse:
        return EndpointRegistryResponse(
            operation=operation,  # type: ignore[arg-type]
            status="rejected",
            registry_revision=self._revision,
            error=ToolError(
                code=code,
                message=message,
                retryable=retryable,
            ),
        )

    def _expire_locked(self, now: float) -> tuple[RegisteredEndpoint, ...]:
        expired = tuple(
            registration
            for registration in self._current.values()
            if registration.expires_at <= now
        )
        if expired and self._revision > _MAX_SAFE_JSON_INTEGER - len(expired):
            raise RuntimeError("endpoint Registry revision is exhausted")
        for registration in expired:
            revision = self._next_revision_locked()
            del self._current[registration.endpoint_id]
            self._tombstones[registration.endpoint_id] = _EndpointTombstone(
                endpoint_instance_id=registration.endpoint_instance_id,
                authority=registration.authority,
                registry_revision=revision,
                reason="expired",
            )
        return expired

    def expire(self, now: float) -> tuple[RegisteredEndpoint, ...]:
        current_time = self._validate_now(now)
        with self._lock:
            return self._expire_locked(current_time)

    def handle_management(
        self,
        request: ToolEnvelope,
        authority: EndpointSourceAuthority,
        *,
        now: float,
    ) -> EndpointRegistryResponse:
        if not isinstance(request, ToolEnvelope):
            raise TypeError("request must be a ToolEnvelope")
        if not isinstance(authority, EndpointSourceAuthority):
            raise TypeError("authority must be an EndpointSourceAuthority")
        operation = _MANAGEMENT_OPERATIONS.get(request.message_type)
        if operation is None:
            raise ValueError(
                "request must be endpoint.register, heartbeat, or unregister"
            )
        validate_message_envelope(request)
        current_time = self._validate_now(now)
        with self._lock:
            self._expire_locked(current_time)
            if request.endpoint_id != authority.endpoint_id:
                return self._rejected(
                    operation,
                    "FORGE_ENDPOINT_SOURCE_UNAUTHORIZED",
                    "the trusted source is not authorized for this endpoint",
                )
            if request.message_type == "endpoint.register":
                return self._register_locked(request, authority, current_time)
            if request.message_type == "endpoint.heartbeat":
                return self._heartbeat_locked(request, authority, current_time)
            return self._unregister_locked(request, authority)

    def _register_locked(
        self,
        request: ToolEnvelope,
        authority: EndpointSourceAuthority,
        now: float,
    ) -> EndpointRegistryResponse:
        descriptor = validate_registration_envelope(request)
        endpoint_id = descriptor.endpoint_id
        endpoint_instance_id = request.endpoint_instance_id
        assert endpoint_instance_id is not None
        current = self._current.get(endpoint_id)
        if current is not None:
            if current.authority.source_id != authority.source_id:
                return self._rejected(
                    "register",
                    "FORGE_ENDPOINT_SOURCE_CONFLICT",
                    "another trusted source owns the current endpoint lease",
                    retryable=True,
                )
            if authority.generation < current.authority.generation:
                return self._rejected(
                    "register",
                    "FORGE_ENDPOINT_GENERATION_STALE",
                    "the source generation is older than the current endpoint lease",
                )
            if authority.generation == current.authority.generation:
                if endpoint_instance_id != current.endpoint_instance_id:
                    return self._rejected(
                        "register",
                        "FORGE_ENDPOINT_INSTANCE_STALE",
                        "a different instance requires a newer trusted source generation",
                        retryable=True,
                    )
                if descriptor != current.descriptor:
                    return self._rejected(
                        "register",
                        "FORGE_ENDPOINT_DESCRIPTOR_CONFLICT",
                        "the current endpoint instance cannot change its descriptor",
                    )
                self._current[endpoint_id] = RegisteredEndpoint(
                    descriptor=current.descriptor,
                    endpoint_instance_id=current.endpoint_instance_id,
                    authority=current.authority,
                    registry_revision=current.registry_revision,
                    expires_at=now + self._lease_ttl_seconds,
                )
                return self._accepted("register", lease=True)

        tombstone = self._tombstones.get(endpoint_id)
        if (
            tombstone is not None
            and tombstone.reason == "unregister"
            and tombstone.endpoint_instance_id == endpoint_instance_id
            and tombstone.authority == authority
        ):
            return self._rejected(
                "register",
                "FORGE_ENDPOINT_INSTANCE_TOMBSTONED",
                "the endpoint instance was explicitly unregistered",
            )

        revision = self._next_revision_locked()
        self._tombstones.pop(endpoint_id, None)
        self._current[endpoint_id] = RegisteredEndpoint(
            descriptor=descriptor,
            endpoint_instance_id=endpoint_instance_id,
            authority=authority,
            registry_revision=revision,
            expires_at=now + self._lease_ttl_seconds,
        )
        return self._accepted("register", lease=True)

    def _current_for_request_locked(
        self,
        request: ToolEnvelope,
        authority: EndpointSourceAuthority,
        operation: str,
    ) -> RegisteredEndpoint | EndpointRegistryResponse:
        endpoint_id = request.endpoint_id
        assert endpoint_id is not None
        current = self._current.get(endpoint_id)
        if current is None:
            return self._rejected(
                operation,
                "FORGE_ENDPOINT_NOT_REGISTERED",
                "the endpoint has no current registration",
                retryable=True,
            )
        if current.authority.source_id != authority.source_id:
            return self._rejected(
                operation,
                "FORGE_ENDPOINT_SOURCE_STALE",
                "the source does not own the current endpoint lease",
            )
        if current.authority.generation != authority.generation:
            return self._rejected(
                operation,
                "FORGE_ENDPOINT_GENERATION_STALE",
                "the source generation does not own the current endpoint lease",
            )
        if request.endpoint_instance_id != current.endpoint_instance_id:
            return self._rejected(
                operation,
                "FORGE_ENDPOINT_INSTANCE_STALE",
                "the instance does not own the current endpoint lease",
            )
        return current

    def _heartbeat_locked(
        self,
        request: ToolEnvelope,
        authority: EndpointSourceAuthority,
        now: float,
    ) -> EndpointRegistryResponse:
        current = self._current_for_request_locked(request, authority, "heartbeat")
        if isinstance(current, EndpointRegistryResponse):
            return current
        self._current[current.endpoint_id] = RegisteredEndpoint(
            descriptor=current.descriptor,
            endpoint_instance_id=current.endpoint_instance_id,
            authority=current.authority,
            registry_revision=current.registry_revision,
            expires_at=now + self._lease_ttl_seconds,
        )
        return self._accepted("heartbeat", lease=True)

    def _unregister_locked(
        self,
        request: ToolEnvelope,
        authority: EndpointSourceAuthority,
    ) -> EndpointRegistryResponse:
        endpoint_id = request.endpoint_id
        endpoint_instance_id = request.endpoint_instance_id
        assert endpoint_id is not None
        assert endpoint_instance_id is not None
        current = self._current.get(endpoint_id)
        if current is None:
            tombstone = self._tombstones.get(endpoint_id)
            if (
                tombstone is not None
                and tombstone.endpoint_instance_id == endpoint_instance_id
                and tombstone.authority == authority
            ):
                return self._accepted(
                    "unregister",
                    lease=False,
                    registry_revision=tombstone.registry_revision,
                )
            return self._rejected(
                "unregister",
                "FORGE_ENDPOINT_NOT_REGISTERED",
                "the endpoint has no current registration",
                retryable=True,
            )
        checked = self._current_for_request_locked(request, authority, "unregister")
        if isinstance(checked, EndpointRegistryResponse):
            return checked
        revision = self._next_revision_locked()
        del self._current[checked.endpoint_id]
        self._tombstones[checked.endpoint_id] = _EndpointTombstone(
            endpoint_instance_id=checked.endpoint_instance_id,
            authority=checked.authority,
            registry_revision=revision,
            reason="unregister",
        )
        return self._accepted("unregister", lease=False)

    def resolve(
        self,
        endpoint_id: str,
        operation: str,
        *,
        now: float,
    ) -> RegisteredEndpoint | None:
        if not isinstance(endpoint_id, str) or not endpoint_id.strip():
            raise ValueError("endpoint_id must be a non-empty string")
        if not isinstance(operation, str) or not operation.strip():
            raise ValueError("operation must be a non-empty string")
        current_time = self._validate_now(now)
        with self._lock:
            self._expire_locked(current_time)
            current = self._current.get(endpoint_id)
            if current is None:
                return None
            if operation not in {
                descriptor.name for descriptor in current.descriptor.operations
            }:
                return None
            return current

    def registrations(self, *, now: float) -> tuple[RegisteredEndpoint, ...]:
        current_time = self._validate_now(now)
        with self._lock:
            self._expire_locked(current_time)
            return tuple(
                self._current[endpoint_id] for endpoint_id in sorted(self._current)
            )


__all__ = [
    "EndpointRegistry",
    "EndpointSourceAuthority",
    "RegisteredEndpoint",
]
