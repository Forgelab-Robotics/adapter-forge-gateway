"""Transport-independent Tool endpoint Directory with monotonic leases."""

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


@dataclass(frozen=True)
class ToolProviderRouteIdentity:
    """Stable identity of one configured provider route."""

    endpoint_id: str
    input_id: str
    output_id: str

    def __post_init__(self) -> None:
        for field_name in ("endpoint_id", "input_id", "output_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")


@dataclass(frozen=True)
class RegisteredEndpoint:
    """One current endpoint instance pinned to its configured route."""

    descriptor: ToolEndpointDescriptor
    endpoint_instance_id: str
    route: ToolProviderRouteIdentity
    registry_revision: int
    expires_at: float

    @property
    def endpoint_id(self) -> str:
        return self.descriptor.endpoint_id


@dataclass(frozen=True)
class ToolDirectorySnapshot:
    """One internally consistent Directory discovery observation."""

    revision: int
    registrations: tuple[RegisteredEndpoint, ...]


class ToolOperationNotFoundError(LookupError):
    """The active endpoint descriptor does not expose a requested Query operation."""

    def __init__(self, endpoint_id: str, operation: str) -> None:
        self.endpoint_id = endpoint_id
        self.operation = operation
        super().__init__(
            f"endpoint {endpoint_id!r} does not expose Query operation {operation!r}"
        )


class EndpointDirectory:
    """Thread-safe current-instance Directory with one process-wide revision."""

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
            raise ValueError("now must be a finite monotonic time")  # noqa: TRY004
        value = float(now)
        if not math.isfinite(value) or value < 0:
            raise ValueError("now must be a finite non-negative monotonic time")
        return value

    @staticmethod
    def _validate_identifier(value: str, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_name} must be a non-empty string")
        return value

    def _next_revision_locked(self) -> int:
        if self._revision >= _MAX_SAFE_JSON_INTEGER:
            raise RuntimeError("endpoint Directory revision is exhausted")
        self._revision += 1
        return self._revision

    def _accepted(
        self,
        operation: Literal["register", "unregister"],
        *,
        lease: bool,
    ) -> EndpointRegistryResponse:
        return EndpointRegistryResponse(
            operation=operation,  # type: ignore[arg-type]
            status="accepted",
            registry_revision=self._revision,
            lease_ttl_ms=self._lease_ttl_ms if lease else None,
        )

    def _rejected(
        self,
        operation: Literal["register", "unregister"],
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

    def _expired_locked(self, now: float) -> tuple[RegisteredEndpoint, ...]:
        return tuple(
            self._current[endpoint_id]
            for endpoint_id in sorted(self._current)
            if self._current[endpoint_id].expires_at <= now
        )

    def _ensure_revision_capacity_locked(self, count: int) -> None:
        if count < 0 or self._revision > _MAX_SAFE_JSON_INTEGER - count:
            raise RuntimeError("endpoint Directory revision is exhausted")

    def _apply_expired_locked(
        self,
        expired: tuple[RegisteredEndpoint, ...],
    ) -> None:
        for registration in expired:
            self._next_revision_locked()
            del self._current[registration.endpoint_id]

    def _expire_locked(self, now: float) -> tuple[RegisteredEndpoint, ...]:
        expired = self._expired_locked(now)
        self._ensure_revision_capacity_locked(len(expired))
        self._apply_expired_locked(expired)
        return expired

    def expire(self, now: float) -> tuple[RegisteredEndpoint, ...]:
        current_time = self._validate_now(now)
        with self._lock:
            return self._expire_locked(current_time)

    def register(
        self,
        request: ToolEnvelope,
        route: ToolProviderRouteIdentity,
        *,
        now: float,
    ) -> EndpointRegistryResponse:
        """Announce or renew one provider instance on its configured route."""
        if not isinstance(request, ToolEnvelope):
            raise TypeError("request must be a ToolEnvelope")
        if not isinstance(route, ToolProviderRouteIdentity):
            raise TypeError("route must be a ToolProviderRouteIdentity")
        if request.message_type != "endpoint.register":
            raise ValueError("request must be endpoint.register")
        descriptor = validate_registration_envelope(request)
        current_time = self._validate_now(now)

        with self._lock:
            expired = self._expired_locked(current_time)
            expired_ids = {registration.endpoint_id for registration in expired}
            current = self._current.get(descriptor.endpoint_id)
            if descriptor.endpoint_id in expired_ids:
                current = None
            additional_revision = 0
            if (
                descriptor.endpoint_id == route.endpoint_id
                and all(
                    operation.semantics == "query"
                    for operation in descriptor.operations
                )
                and (
                    current is None
                    or (
                        current.route == route
                        and current.endpoint_instance_id != request.endpoint_instance_id
                    )
                )
            ):
                additional_revision = 1
            self._ensure_revision_capacity_locked(len(expired) + additional_revision)
            self._apply_expired_locked(expired)
            if descriptor.endpoint_id != route.endpoint_id:
                return self._rejected(
                    "register",
                    "FORGE_ENDPOINT_ROUTE_UNAUTHORIZED",
                    "the configured provider route is not authorized for this endpoint",
                )
            if any(
                operation.semantics != "query" for operation in descriptor.operations
            ):
                return self._rejected(
                    "register",
                    "FORGE_TOOL_SEMANTICS_UNSUPPORTED",
                    "Gateway Tool providers may register Query operations only",
                )

            endpoint_instance_id = request.endpoint_instance_id
            assert endpoint_instance_id is not None
            current = self._current.get(descriptor.endpoint_id)
            if current is not None:
                if current.route != route:
                    return self._rejected(
                        "register",
                        "FORGE_ENDPOINT_ROUTE_CONFLICT",
                        "another configured provider route owns the current "
                        "endpoint lease",
                        retryable=True,
                    )
                if current.endpoint_instance_id == endpoint_instance_id:
                    if current.descriptor != descriptor:
                        return self._rejected(
                            "register",
                            "FORGE_ENDPOINT_DESCRIPTOR_CONFLICT",
                            "the current endpoint instance cannot change its "
                            "descriptor",
                        )
                    self._current[descriptor.endpoint_id] = RegisteredEndpoint(
                        descriptor=current.descriptor,
                        endpoint_instance_id=current.endpoint_instance_id,
                        route=current.route,
                        registry_revision=current.registry_revision,
                        expires_at=current_time + self._lease_ttl_seconds,
                    )
                    return self._accepted("register", lease=True)

            revision = self._next_revision_locked()
            self._current[descriptor.endpoint_id] = RegisteredEndpoint(
                descriptor=descriptor,
                endpoint_instance_id=endpoint_instance_id,
                route=route,
                registry_revision=revision,
                expires_at=current_time + self._lease_ttl_seconds,
            )
            return self._accepted("register", lease=True)

    def unregister(
        self,
        request: ToolEnvelope,
        route: ToolProviderRouteIdentity,
        *,
        now: float,
    ) -> EndpointRegistryResponse:
        """Remove a matching registration; absence is effect-idempotent."""
        if not isinstance(request, ToolEnvelope):
            raise TypeError("request must be a ToolEnvelope")
        if not isinstance(route, ToolProviderRouteIdentity):
            raise TypeError("route must be a ToolProviderRouteIdentity")
        if request.message_type != "endpoint.unregister":
            raise ValueError("request must be endpoint.unregister")
        validate_message_envelope(request)
        current_time = self._validate_now(now)

        with self._lock:
            expired = self._expired_locked(current_time)
            expired_ids = {registration.endpoint_id for registration in expired}
            current = self._current.get(route.endpoint_id)
            if route.endpoint_id in expired_ids:
                current = None
            removes_current = (
                request.endpoint_id == route.endpoint_id
                and current is not None
                and current.route == route
                and current.endpoint_instance_id == request.endpoint_instance_id
            )
            self._ensure_revision_capacity_locked(len(expired) + int(removes_current))
            self._apply_expired_locked(expired)
            if request.endpoint_id != route.endpoint_id:
                return self._rejected(
                    "unregister",
                    "FORGE_ENDPOINT_ROUTE_UNAUTHORIZED",
                    "the configured provider route is not authorized for this endpoint",
                )
            endpoint_instance_id = request.endpoint_instance_id
            if endpoint_instance_id is None:
                return self._rejected(
                    "unregister",
                    "FORGE_PROTOCOL_INVALID_MESSAGE",
                    "endpoint.unregister requires endpoint_instance_id",
                )
            current = self._current.get(route.endpoint_id)
            if current is None:
                return self._accepted("unregister", lease=False)
            if current.route != route:
                return self._rejected(
                    "unregister",
                    "FORGE_ENDPOINT_ROUTE_CONFLICT",
                    "another configured provider route owns the current endpoint lease",
                )
            if current.endpoint_instance_id != endpoint_instance_id:
                return self._rejected(
                    "unregister",
                    "FORGE_ENDPOINT_INSTANCE_STALE",
                    "the instance does not own the current endpoint lease",
                )
            self._next_revision_locked()
            del self._current[current.endpoint_id]
            return self._accepted("unregister", lease=False)

    def resolve(
        self,
        endpoint_id: str,
        operation: str,
        *,
        now: float,
    ) -> RegisteredEndpoint | None:
        endpoint = self._validate_identifier(endpoint_id, "endpoint_id")
        operation_name = self._validate_identifier(operation, "operation")
        current_time = self._validate_now(now)
        with self._lock:
            self._expire_locked(current_time)
            current = self._current.get(endpoint)
            if current is None:
                return None
            matching_operation = next(
                (
                    descriptor
                    for descriptor in current.descriptor.operations
                    if descriptor.name == operation_name
                ),
                None,
            )
            if matching_operation is None or matching_operation.semantics != "query":
                raise ToolOperationNotFoundError(endpoint, operation_name)
            return current

    def snapshot(self, *, now: float) -> ToolDirectorySnapshot:
        current_time = self._validate_now(now)
        with self._lock:
            self._expire_locked(current_time)
            return ToolDirectorySnapshot(
                revision=self._revision,
                registrations=tuple(
                    self._current[endpoint_id] for endpoint_id in sorted(self._current)
                ),
            )

    def registrations(self, *, now: float) -> tuple[RegisteredEndpoint, ...]:
        return self.snapshot(now=now).registrations


__all__ = [
    "EndpointDirectory",
    "RegisteredEndpoint",
    "ToolDirectorySnapshot",
    "ToolOperationNotFoundError",
    "ToolProviderRouteIdentity",
]
