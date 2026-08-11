"""Gateway-owned Tool discovery and Query invocation application service."""

from __future__ import annotations

import math
import threading
import time
import uuid
from collections import deque
from collections.abc import Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Literal

from forge_tool import (
    TOOL_ENDPOINT_PROTOCOL,
    ToolAccepted,
    ToolContext,
    ToolEnvelope,
    ToolError,
    ToolExecutionKey,
    ToolRequest,
    endpoint_descriptor_to_payload,
    error_from_payload,
    error_to_payload,
    invoke_request_from_envelope,
    invoke_request_to_payload,
    invoke_response_from_payload,
    make_endpoint_registry_response_envelope,
    make_invoke_request_envelope,
    validate_message_envelope,
    validate_response_correlation,
)

from forge_gateway.config import ToolGatewayConfig, ToolProviderRouteConfig
from forge_gateway.domain.tool_directory import (
    EndpointDirectory,
    RegisteredEndpoint,
    ToolOperationNotFoundError,
    ToolProviderRouteIdentity,
)

_MAX_SAFE_JSON_INTEGER = 9_007_199_254_740_991
DEFAULT_TOOL_OUTBOUND_CAPACITY = 256

LogicalToolMessageType = Literal[
    "tool.invoke.request",
    "tool.invoke.response",
    "tool.error",
]
ToolOutboundKind = Literal[
    "provider.registry_response",
    "provider.invoke_request",
    "caller.invoke_response",
]
CorrelationKey = tuple[str, str, str, str, str | None, str]


class ToolGatewayUnavailable(RuntimeError):
    """The Tool Gateway cannot admit a new invocation."""


class ToolGatewayMailboxFull(ToolGatewayUnavailable):
    """The bounded Tool outbound mailbox has no remaining capacity."""


@dataclass(frozen=True)
class ToolOutboundMessage:
    """One envelope awaiting lifecycle-thread Dora dispatch."""

    output_id: str
    envelope: ToolEnvelope
    kind: ToolOutboundKind
    pending_key: CorrelationKey | None = None
    pending_token: str | None = None


@dataclass(frozen=True)
class ToolInvocationTicket:
    """Thread-safe completion handle returned to an HTTP caller."""

    request: ToolEnvelope
    future: Future[ToolEnvelope]
    pending_key: CorrelationKey | None
    deadline: float


@dataclass(frozen=True)
class ToolGatewaySweep:
    """State removed by one monotonic service sweep."""

    expired_registrations: tuple[RegisteredEndpoint, ...]
    timed_out_invocations: int


@dataclass
class _PendingInvocation:
    logical_request: ToolEnvelope
    provider_request: ToolEnvelope
    route: ToolProviderRouteIdentity
    logical_key: CorrelationKey
    provider_key: CorrelationKey
    pending_token: str
    deadline: float
    future: Future[ToolEnvelope] | None
    terminal_slot_reserved: bool
    dispatch_claimed: bool = False


def _logical_envelope(
    *,
    message_type: LogicalToolMessageType,
    payload: Mapping[str, Any],
    request_id: str,
    invocation_id: str,
    attempt_id: str,
    endpoint_id: str,
    operation: str,
) -> ToolEnvelope:
    """Construct a validated instance-less public Gateway envelope."""
    return ToolEnvelope(
        protocol=TOOL_ENDPOINT_PROTOCOL,
        message_type=message_type,  # type: ignore[arg-type]
        request_id=request_id,
        invocation_id=invocation_id,
        attempt_id=attempt_id,
        endpoint_id=endpoint_id,
        endpoint_instance_id=None,
        operation=operation,
        sequence=None,
        payload=dict(payload),
    )


def make_logical_invoke_request_envelope(
    request: ToolRequest,
    context: ToolContext,
    *,
    request_id: str,
) -> ToolEnvelope:
    """Build the public Gateway invoke shape, which never requires an instance."""
    return _logical_envelope(
        message_type="tool.invoke.request",
        request_id=request_id,
        invocation_id=context.invocation_id,
        attempt_id=context.attempt_id,
        endpoint_id=context.endpoint_id,
        operation=context.operation,
        payload=invoke_request_to_payload(request, context),
    )


def _logical_error_response(error: ToolError, request: ToolEnvelope) -> ToolEnvelope:
    assert request.request_id is not None
    assert request.invocation_id is not None
    assert request.attempt_id is not None
    assert request.endpoint_id is not None
    assert request.operation is not None
    if request.endpoint_instance_id is not None:
        raise ValueError("logical caller request must not contain endpoint_instance_id")
    return _logical_envelope(
        message_type="tool.error",
        request_id=request.request_id,
        invocation_id=request.invocation_id,
        attempt_id=request.attempt_id,
        endpoint_id=request.endpoint_id,
        operation=request.operation,
        payload=error_to_payload(error),
    )


def _logical_provider_response(
    response: ToolEnvelope,
    request: ToolEnvelope,
) -> ToolEnvelope:
    assert request.request_id is not None
    assert request.invocation_id is not None
    assert request.attempt_id is not None
    assert request.endpoint_id is not None
    assert request.operation is not None
    if response.message_type not in ("tool.invoke.response", "tool.error"):
        raise ValueError("provider response must be invoke.response or tool.error")
    if request.endpoint_instance_id is not None:
        raise ValueError("logical caller request must not contain endpoint_instance_id")
    return _logical_envelope(
        message_type=response.message_type,
        request_id=request.request_id,
        invocation_id=request.invocation_id,
        attempt_id=request.attempt_id,
        endpoint_id=request.endpoint_id,
        operation=request.operation,
        payload=response.payload,
    )


class ToolGatewayService:
    """Single caller-visible authority for Tool discovery and Query routing."""

    def __init__(
        self,
        config: ToolGatewayConfig,
        *,
        outbound_capacity: int = DEFAULT_TOOL_OUTBOUND_CAPACITY,
    ) -> None:
        if not isinstance(config, ToolGatewayConfig):
            raise TypeError("config must be a ToolGatewayConfig")
        if (
            isinstance(outbound_capacity, bool)
            or not isinstance(outbound_capacity, int)
            or outbound_capacity < 1
        ):
            raise ValueError("outbound_capacity must be a positive integer")
        self.config = config
        self._directory = EndpointDirectory(lease_ttl_ms=config.lease_ttl_ms)
        routes = (
            tuple(self._route_identity(provider) for provider in config.providers)
            if config.enabled
            else ()
        )
        self._routes_by_endpoint_id = {route.endpoint_id: route for route in routes}
        self._routes_by_input_id = {route.input_id: route for route in routes}
        self._lock = threading.RLock()
        self._accepting = config.enabled
        self._outbound_capacity = outbound_capacity
        self._outbound: deque[ToolOutboundMessage] = deque()
        self._reserved_outbound_slots = 0
        self._pending_by_provider_key: dict[CorrelationKey, _PendingInvocation] = {}
        self._pending_logical_keys: set[CorrelationKey] = set()

    @staticmethod
    def _route_identity(provider: ToolProviderRouteConfig) -> ToolProviderRouteIdentity:
        return ToolProviderRouteIdentity(
            endpoint_id=provider.endpoint_id,
            input_id=provider.input_id,
            output_id=provider.output_id,
        )

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    @property
    def provider_input_ids(self) -> frozenset[str]:
        if not self.config.enabled:
            return frozenset()
        return frozenset(self._routes_by_input_id)

    @property
    def caller_input_ids(self) -> frozenset[str]:
        if not self.config.enabled:
            return frozenset()
        return frozenset((self.config.request_input_id,))

    @property
    def input_ids(self) -> frozenset[str]:
        if not self.config.enabled:
            return frozenset()
        return self.provider_input_ids | self.caller_input_ids

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending_by_provider_key)

    @property
    def outbound_capacity(self) -> int:
        return self._outbound_capacity

    @property
    def lease_ttl_ms(self) -> int:
        return self._directory.lease_ttl_ms

    @property
    def registry_revision(self) -> int:
        return self._directory.revision

    def endpoint_registrations(
        self,
        *,
        now: float | None = None,
    ) -> tuple[RegisteredEndpoint, ...]:
        observation = time.monotonic() if now is None else self._validate_now(now)
        with self._lock:
            expired = self._directory.expire(observation)
            _ = self._finish_unclaimed_for_removed_locked(
                expired,
                observed_at=observation,
            )
            return self._directory.registrations(now=observation)

    def resolve_registered_endpoint(
        self,
        endpoint_id: str,
        operation: str,
        *,
        now: float | None = None,
    ) -> RegisteredEndpoint | None:
        observation = time.monotonic() if now is None else self._validate_now(now)
        with self._lock:
            expired = self._directory.expire(observation)
            _ = self._finish_unclaimed_for_removed_locked(
                expired,
                observed_at=observation,
            )
            return self._directory.resolve(endpoint_id, operation, now=observation)

    @property
    def outbound_count(self) -> int:
        with self._lock:
            return len(self._outbound)

    @property
    def reserved_outbound_count(self) -> int:
        with self._lock:
            return self._reserved_outbound_slots

    @staticmethod
    def _correlation_key(envelope: ToolEnvelope) -> CorrelationKey:
        request_id = envelope.request_id
        invocation_id = envelope.invocation_id
        attempt_id = envelope.attempt_id
        endpoint_id = envelope.endpoint_id
        operation = envelope.operation
        if None in (request_id, invocation_id, attempt_id, endpoint_id, operation):
            raise ValueError("invoke envelope is missing correlation identity")
        assert request_id is not None
        assert invocation_id is not None
        assert attempt_id is not None
        assert endpoint_id is not None
        assert operation is not None
        return (
            request_id,
            invocation_id,
            attempt_id,
            endpoint_id,
            envelope.endpoint_instance_id,
            operation,
        )

    @staticmethod
    def _validate_now(now: float) -> float:
        if isinstance(now, bool) or not isinstance(now, (int, float)):
            raise ValueError("now must be a finite monotonic time")  # noqa: TRY004
        value = float(now)
        if not math.isfinite(value) or value < 0:
            raise ValueError("now must be a finite non-negative monotonic time")
        return value

    @staticmethod
    def _validate_timeout_ms(timeout_ms: int) -> int:
        if (
            isinstance(timeout_ms, bool)
            or not isinstance(timeout_ms, int)
            or not 1 <= timeout_ms <= _MAX_SAFE_JSON_INTEGER
        ):
            raise ValueError(f"timeout_ms must be in [1, {_MAX_SAFE_JSON_INTEGER}]")
        return timeout_ms

    def discovery_snapshot(self, *, now: float | None = None) -> dict[str, object]:
        observation = time.monotonic() if now is None else self._validate_now(now)
        with self._lock:
            expired = self._directory.expire(observation)
            _ = self._finish_unclaimed_for_removed_locked(
                expired,
                observed_at=observation,
            )
            snapshot = self._directory.snapshot(now=observation)
            return {
                "revision": snapshot.revision,
                "tools": [
                    {
                        "endpoint_id": registration.endpoint_id,
                        "descriptor": endpoint_descriptor_to_payload(
                            registration.descriptor
                        )["descriptor"],
                    }
                    for registration in snapshot.registrations
                ],
            }

    def handle_input(
        self,
        input_id: str,
        envelope: ToolEnvelope,
        *,
        received_at: float,
        processed_at: float | None = None,
    ) -> None:
        """Consume one decoded provider or public caller envelope.

        Valid late or duplicate provider invoke responses are quietly ignored.
        """
        if not isinstance(envelope, ToolEnvelope):
            raise TypeError("envelope must be a ToolEnvelope")
        receive_observation = self._validate_now(received_at)
        process_observation = (
            receive_observation
            if processed_at is None
            else self._validate_now(processed_at)
        )
        with self._lock:
            route = self._routes_by_input_id.get(input_id)
            if route is not None and envelope.message_type in (
                "tool.invoke.response",
                "tool.error",
            ):
                _ = self._handle_provider_response_locked(
                    route,
                    envelope,
                    processed_at=process_observation,
                )
                return
            self._ensure_accepting_locked()
            if route is not None:
                self._handle_provider_input_locked(
                    route,
                    envelope,
                    received_at=receive_observation,
                    processed_at=process_observation,
                )
                return
            if input_id == self.config.request_input_id:
                self._handle_caller_invoke_locked(
                    envelope,
                    processed_at=process_observation,
                )
                return
            raise ValueError(f"unconfigured Tool input: {input_id!r}")

    def _handle_provider_input_locked(
        self,
        route: ToolProviderRouteIdentity,
        envelope: ToolEnvelope,
        *,
        received_at: float,
        processed_at: float,
    ) -> None:
        if envelope.message_type in ("endpoint.register", "endpoint.unregister"):
            self._reserve_outbound_slots_locked()
            reserved = True
            try:
                if envelope.message_type == "endpoint.register":
                    mutation = self._directory.register_with_change(
                        envelope,
                        route,
                        now=received_at,
                    )
                else:
                    mutation = self._directory.unregister_with_change(
                        envelope,
                        route,
                        now=received_at,
                    )
                self._append_reserved_outbound_locked(
                    ToolOutboundMessage(
                        output_id=route.output_id,
                        envelope=make_endpoint_registry_response_envelope(
                            mutation.response,
                            envelope,
                        ),
                        kind="provider.registry_response",
                    )
                )
                reserved = False
                _ = self._finish_unclaimed_for_removed_locked(
                    mutation.removed,
                    observed_at=processed_at,
                )
            finally:
                if reserved:
                    self._release_outbound_slots_locked()
            return
        if envelope.message_type in ("tool.invoke.response", "tool.error"):
            _ = self._handle_provider_response_locked(
                route,
                envelope,
                processed_at=processed_at,
            )
            return
        raise ValueError(
            "provider input accepts only endpoint.register, endpoint.unregister, "
            "tool.invoke.response, or tool.error"
        )

    def _handle_caller_invoke_locked(
        self,
        request: ToolEnvelope,
        *,
        processed_at: float,
    ) -> None:
        if request.message_type != "tool.invoke.request":
            raise ValueError("public Tool input accepts only tool.invoke.request")
        if request.endpoint_instance_id is not None:
            raise ValueError(
                "public Tool invoke must omit endpoint_instance_id; "
                "Gateway owns provider instance routing"
            )
        validate_message_envelope(request)
        _, context = invoke_request_from_envelope(request)
        deadline = processed_at + self.config.invoke_timeout_ms / 1_000
        if context.deadline_ms is not None:
            remaining_ms = context.deadline_ms - int(time.time() * 1_000)
            absolute_deadline = processed_at + remaining_ms / 1_000
            deadline = min(deadline, absolute_deadline)
        _ = self._admit_invoke_locked(
            request,
            processed_at=processed_at,
            deadline=deadline,
            future=None,
        )

    def submit_http_invoke(
        self,
        endpoint_id: str,
        operation: str,
        arguments: dict[str, object],
        *,
        caller_id: str | None = None,
        timeout_ms: int | None = None,
        now: float | None = None,
    ) -> ToolInvocationTicket:
        """Admit an HTTP Query without performing Dora I/O on the HTTP thread."""
        if not isinstance(endpoint_id, str) or not endpoint_id.strip():
            raise ValueError("endpoint_id must be a non-empty string")
        if not isinstance(operation, str) or not operation.strip():
            raise ValueError("operation must be a non-empty string")
        if not isinstance(arguments, dict):
            raise ValueError("arguments must be a JSON object")  # noqa: TRY004
        if caller_id is not None and (
            not isinstance(caller_id, str) or not caller_id.strip()
        ):
            raise ValueError("caller_id must be a non-empty string when provided")
        effective_timeout = self._validate_timeout_ms(
            self.config.invoke_timeout_ms if timeout_ms is None else timeout_ms
        )
        if effective_timeout > self.config.invoke_timeout_ms:
            raise ValueError(
                "timeout_ms must not exceed configured tools.invoke_timeout_ms "
                f"maximum {self.config.invoke_timeout_ms}"
            )
        observation = time.monotonic() if now is None else self._validate_now(now)
        identity = uuid.uuid4().hex
        context = ToolContext(
            execution_key=ToolExecutionKey(
                invocation_id=f"gateway-{identity}",
                attempt_id=f"gateway-{identity}",
            ),
            tool_id=endpoint_id,
            implementation_id=endpoint_id,
            endpoint_id=endpoint_id,
            operation=operation,
            caller_id=caller_id,
            deadline_ms=min(
                _MAX_SAFE_JSON_INTEGER,
                int(time.time() * 1_000) + effective_timeout,
            ),
        )
        request = make_logical_invoke_request_envelope(
            ToolRequest(arguments),
            context,
            request_id=f"gateway-{identity}",
        )
        future: Future[ToolEnvelope] = Future()
        deadline = observation + effective_timeout / 1_000
        with self._lock:
            try:
                pending_key = self._admit_invoke_locked(
                    request,
                    processed_at=observation,
                    deadline=deadline,
                    future=future,
                )
            except ToolGatewayUnavailable as error:
                pending_key = None
                self._complete_without_pending_locked(
                    request,
                    self._tool_error(
                        "FORGE_TOOL_GATEWAY_UNAVAILABLE",
                        str(error),
                        retryable=True,
                    ),
                    future,
                    terminal_slot_reserved=False,
                )
        return ToolInvocationTicket(
            request=request,
            future=future,
            pending_key=pending_key,
            deadline=deadline,
        )

    def _admit_invoke_locked(
        self,
        logical_request: ToolEnvelope,
        *,
        processed_at: float,
        deadline: float,
        future: Future[ToolEnvelope] | None,
    ) -> CorrelationKey | None:
        self._ensure_accepting_locked()
        terminal_reserved = False
        provider_reserved = False

        if future is None:
            self._reserve_outbound_slots_locked()
            terminal_reserved = True

        def complete(error: ToolError) -> None:
            nonlocal provider_reserved, terminal_reserved
            if provider_reserved:
                self._release_outbound_slots_locked()
                provider_reserved = False
            self._complete_without_pending_locked(
                logical_request,
                error,
                future,
                terminal_slot_reserved=terminal_reserved,
            )
            terminal_reserved = False

        try:
            if len(self._pending_by_provider_key) >= self._outbound_capacity:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_GATEWAY_BUSY",
                        "Tool Gateway pending invocation capacity is exhausted",
                        retryable=True,
                    )
                )
                return None

            try:
                self._reserve_outbound_slots_locked()
                provider_reserved = True
            except ToolGatewayMailboxFull:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_GATEWAY_BUSY",
                        "Tool Gateway outbound mailbox is busy",
                        retryable=True,
                    )
                )
                return None

            if deadline <= processed_at:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_INVOKE_TIMEOUT",
                        "Tool Query invocation deadline already elapsed",
                        retryable=True,
                    )
                )
                return None

            endpoint_id = logical_request.endpoint_id
            operation = logical_request.operation
            assert endpoint_id is not None
            assert operation is not None
            route = self._routes_by_endpoint_id.get(endpoint_id)
            if route is None:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_ENDPOINT_UNKNOWN",
                        f"endpoint {endpoint_id!r} is not configured",
                    )
                )
                return None
            expired = self._directory.expire(processed_at)
            _ = self._finish_unclaimed_for_removed_locked(
                expired,
                observed_at=processed_at,
            )
            try:
                registration = self._directory.resolve(
                    endpoint_id,
                    operation,
                    now=processed_at,
                )
            except ToolOperationNotFoundError as error:
                complete(self._tool_error("FORGE_TOOL_OPERATION_NOT_FOUND", str(error)))
                return None
            if registration is None:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_ENDPOINT_UNAVAILABLE",
                        f"endpoint {endpoint_id!r} has no active provider instance",
                        retryable=True,
                    )
                )
                return None

            tool_request, context = invoke_request_from_envelope(logical_request)
            provider_request = make_invoke_request_envelope(
                tool_request,
                context,
                request_id=f"gateway-provider-{uuid.uuid4().hex}",
                endpoint_instance_id=registration.endpoint_instance_id,
            )
            logical_key = self._correlation_key(logical_request)
            if logical_key in self._pending_logical_keys:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_REQUEST_CONFLICT",
                        "an invocation with the same correlation identity is pending",
                    )
                )
                return None
            provider_key = self._correlation_key(provider_request)
            if provider_key in self._pending_by_provider_key:
                complete(
                    self._tool_error(
                        "FORGE_TOOL_REQUEST_CONFLICT",
                        "provider correlation identity is already pending",
                    )
                )
                return None

            pending_token = uuid.uuid4().hex
            pending = _PendingInvocation(
                logical_request=logical_request,
                provider_request=provider_request,
                route=route,
                logical_key=logical_key,
                provider_key=provider_key,
                pending_token=pending_token,
                deadline=deadline,
                future=future,
                terminal_slot_reserved=terminal_reserved,
            )
            self._append_reserved_outbound_locked(
                ToolOutboundMessage(
                    output_id=route.output_id,
                    envelope=provider_request,
                    kind="provider.invoke_request",
                    pending_key=provider_key,
                    pending_token=pending_token,
                )
            )
            provider_reserved = False
            self._pending_by_provider_key[provider_key] = pending
            self._pending_logical_keys.add(logical_key)
            terminal_reserved = False
            return provider_key
        finally:
            if provider_reserved:
                self._release_outbound_slots_locked()
            if terminal_reserved:
                self._release_outbound_slots_locked()

    def _handle_provider_response_locked(
        self,
        route: ToolProviderRouteIdentity,
        response: ToolEnvelope,
        *,
        processed_at: float,
    ) -> bool:
        validate_message_envelope(response)
        provider_key = self._correlation_key(response)
        pending = self._pending_by_provider_key.get(provider_key)
        if pending is None:
            return False
        if pending.route != route:
            raise ValueError("provider response arrived on the wrong configured route")
        validate_response_correlation(pending.provider_request, response)
        if response.message_type == "tool.invoke.response" and isinstance(
            invoke_response_from_payload(response.payload), ToolAccepted
        ):
            caller_response = _logical_error_response(
                self._tool_error(
                    "FORGE_TOOL_QUERY_NON_TERMINAL_RESPONSE",
                    "a Query provider must return a terminal invoke response",
                ),
                pending.logical_request,
            )
        else:
            caller_response = _logical_provider_response(
                response,
                pending.logical_request,
            )
        return self._finish_pending_locked(
            pending,
            caller_response,
            observed_at=processed_at,
        )

    @staticmethod
    def _tool_error(
        code: str,
        message: str,
        *,
        retryable: bool = False,
    ) -> ToolError:
        return ToolError(code=code, message=message, retryable=retryable)

    def _complete_without_pending_locked(
        self,
        request: ToolEnvelope,
        error: ToolError,
        future: Future[ToolEnvelope] | None,
        *,
        terminal_slot_reserved: bool,
    ) -> None:
        response = _logical_error_response(error, request)
        if future is not None:
            if not future.done():
                future.set_result(response)
            return
        if not terminal_slot_reserved:
            raise RuntimeError("Dora caller completion has no reserved outbound slot")
        self._append_reserved_outbound_locked(
            ToolOutboundMessage(
                output_id=self.config.response_output_id,
                envelope=response,
                kind="caller.invoke_response",
            )
        )

    def _timeout_response_locked(
        self,
        pending: _PendingInvocation,
    ) -> ToolEnvelope:
        return _logical_error_response(
            self._tool_error(
                "FORGE_TOOL_INVOKE_TIMEOUT",
                "Tool Query invocation timed out",
                retryable=True,
            ),
            pending.logical_request,
        )

    def _finish_unclaimed_for_removed_locked(
        self,
        removed: tuple[RegisteredEndpoint, ...],
        *,
        observed_at: float,
    ) -> tuple[int, int]:
        if not removed:
            return 0, 0
        removed_routes = {
            (
                registration.endpoint_id,
                registration.endpoint_instance_id,
                registration.route,
            )
            for registration in removed
        }
        affected = tuple(
            pending
            for pending in self._pending_by_provider_key.values()
            if not pending.dispatch_claimed
            and (
                pending.provider_request.endpoint_id,
                pending.provider_request.endpoint_instance_id,
                pending.route,
            )
            in removed_routes
        )
        completed = 0
        timed_out = 0
        for pending in affected:
            endpoint_id = pending.provider_request.endpoint_id
            instance_id = pending.provider_request.endpoint_instance_id
            assert endpoint_id is not None
            assert instance_id is not None
            response = _logical_error_response(
                self._tool_error(
                    "FORGE_TOOL_ENDPOINT_UNAVAILABLE",
                    f"endpoint {endpoint_id!r} instance {instance_id!r} "
                    "became unavailable before provider dispatch",
                    retryable=True,
                ),
                pending.logical_request,
            )
            deadline_elapsed = observed_at >= pending.deadline
            if self._finish_pending_locked(
                pending,
                response,
                observed_at=observed_at,
            ):
                completed += 1
                timed_out += int(deadline_elapsed)
        return completed, timed_out

    def _remove_pending_locked(self, pending: _PendingInvocation) -> None:
        current = self._pending_by_provider_key.get(pending.provider_key)
        if current is not pending:
            return
        del self._pending_by_provider_key[pending.provider_key]
        self._pending_logical_keys.discard(pending.logical_key)

    def _finish_pending_locked(
        self,
        pending: _PendingInvocation,
        response: ToolEnvelope,
        *,
        observed_at: float,
        timed_out: bool = False,
    ) -> bool:
        if self._pending_by_provider_key.get(pending.provider_key) is not pending:
            return False
        if timed_out or observed_at >= pending.deadline:
            response = self._timeout_response_locked(pending)
        if pending.future is not None:
            if not pending.future.done():
                pending.future.set_result(response)
            self._remove_pending_locked(pending)
            return True
        if not pending.terminal_slot_reserved:
            raise RuntimeError("Dora pending invocation has no terminal reservation")
        self._append_reserved_outbound_locked(
            ToolOutboundMessage(
                output_id=self.config.response_output_id,
                envelope=response,
                kind="caller.invoke_response",
            )
        )
        pending.terminal_slot_reserved = False
        self._remove_pending_locked(pending)
        return True

    def _reserve_outbound_slots_locked(self, count: int = 1) -> None:
        occupied = len(self._outbound) + self._reserved_outbound_slots
        if occupied + count > self._outbound_capacity:
            raise ToolGatewayMailboxFull(
                f"Tool outbound mailbox capacity {self._outbound_capacity} exceeded"
            )
        self._reserved_outbound_slots += count

    def _release_outbound_slots_locked(self, count: int = 1) -> None:
        if count < 1 or self._reserved_outbound_slots < count:
            raise RuntimeError("Tool outbound mailbox reservation underflow")
        self._reserved_outbound_slots -= count

    def _append_reserved_outbound_locked(self, message: ToolOutboundMessage) -> None:
        if self._reserved_outbound_slots < 1:
            raise RuntimeError("Tool outbound message has no capacity reservation")
        self._reserved_outbound_slots -= 1
        self._outbound.append(message)

    def take_outbound(self) -> ToolOutboundMessage | None:
        """Claim and pop one valid output for lifecycle-thread dispatch."""
        with self._lock:
            while self._outbound:
                message = self._outbound.popleft()
                if message.kind == "provider.invoke_request":
                    pending_key = message.pending_key
                    if pending_key is None:
                        continue
                    pending = self._pending_by_provider_key.get(pending_key)
                    if (
                        pending is None
                        or pending.pending_token != message.pending_token
                        or pending.dispatch_claimed
                    ):
                        continue
                    pending.dispatch_claimed = True
                return message
            return None

    def cancel_http_invoke(
        self,
        ticket: ToolInvocationTicket,
        *,
        timed_out: bool,
        now: float | None = None,
    ) -> bool:
        """Atomically stop an HTTP wait and invalidate any unclaimed dispatch."""
        if not isinstance(ticket, ToolInvocationTicket):
            raise TypeError("ticket must be a ToolInvocationTicket")
        pending_key = ticket.pending_key
        if pending_key is None:
            return False
        explicit_observation = None if now is None else self._validate_now(now)
        with self._lock:
            observation = (
                time.monotonic()
                if explicit_observation is None
                else explicit_observation
            )
            pending = self._pending_by_provider_key.get(pending_key)
            if pending is None or pending.future is not ticket.future:
                return False
            error = self._tool_error(
                "FORGE_TOOL_INVOKE_TIMEOUT"
                if timed_out
                else "FORGE_TOOL_CALLER_CANCELLED",
                "Tool Query invocation timed out"
                if timed_out
                else "HTTP caller stopped waiting for Tool Query invocation",
                retryable=timed_out,
            )
            response = _logical_error_response(error, pending.logical_request)
            return self._finish_pending_locked(
                pending,
                response,
                observed_at=observation,
                timed_out=timed_out,
            )

    def output_failed(
        self,
        message: ToolOutboundMessage,
        error: BaseException,
        *,
        now: float | None = None,
    ) -> bool:
        """Fail an invocation whose one-shot provider dispatch did not succeed."""
        if message.kind != "provider.invoke_request" or message.pending_key is None:
            return False
        explicit_observation = None if now is None else self._validate_now(now)
        with self._lock:
            observation = (
                time.monotonic()
                if explicit_observation is None
                else explicit_observation
            )
            pending = self._pending_by_provider_key.get(message.pending_key)
            if pending is None or pending.pending_token != message.pending_token:
                return False
            response = _logical_error_response(
                self._tool_error(
                    "FORGE_TOOL_TRANSPORT_UNAVAILABLE",
                    f"provider request dispatch failed: {error}",
                    retryable=True,
                ),
                pending.logical_request,
            )
            return self._finish_pending_locked(
                pending,
                response,
                observed_at=observation,
            )

    def sweep(self, *, now: float | None = None) -> ToolGatewaySweep:
        explicit_observation = None if now is None else self._validate_now(now)
        with self._lock:
            observation = (
                time.monotonic()
                if explicit_observation is None
                else explicit_observation
            )
            expired = self._directory.expire(observation)
            _, transition_timeouts = self._finish_unclaimed_for_removed_locked(
                expired,
                observed_at=observation,
            )
            timed_out = tuple(
                pending
                for pending in self._pending_by_provider_key.values()
                if pending.deadline <= observation
            )
            completed = transition_timeouts
            for pending in timed_out:
                response = _logical_error_response(
                    self._tool_error(
                        "FORGE_TOOL_INVOKE_TIMEOUT",
                        "Tool Query invocation timed out",
                        retryable=True,
                    ),
                    pending.logical_request,
                )
                if self._finish_pending_locked(
                    pending,
                    response,
                    observed_at=observation,
                ):
                    completed += 1
            return ToolGatewaySweep(
                expired_registrations=expired,
                timed_out_invocations=completed,
            )

    def begin_close(self, *, now: float | None = None) -> None:
        explicit_observation = None if now is None else self._validate_now(now)
        with self._lock:
            observation = (
                time.monotonic()
                if explicit_observation is None
                else explicit_observation
            )
            self._accepting = False
            pending_invocations = tuple(self._pending_by_provider_key.values())
            for pending in pending_invocations:
                response = _logical_error_response(
                    self._tool_error(
                        "FORGE_TOOL_GATEWAY_UNAVAILABLE",
                        "Tool Gateway is closing",
                        retryable=True,
                    ),
                    pending.logical_request,
                )
                _ = self._finish_pending_locked(
                    pending,
                    response,
                    observed_at=observation,
                )

    def close(self, *, now: float | None = None) -> None:
        self.begin_close(now=now)

    def _ensure_accepting_locked(self) -> None:
        if not self.config.enabled:
            raise ToolGatewayUnavailable("Tool Gateway is disabled")
        if not self._accepting:
            raise ToolGatewayUnavailable("Tool Gateway is closing")


def tool_error_from_response(response: ToolEnvelope) -> ToolError | None:
    """Return a rejected/error outcome from a completed caller response."""
    if response.message_type == "tool.error":
        return error_from_payload(response.payload)
    if response.message_type != "tool.invoke.response":
        raise ValueError("response must be tool.invoke.response or tool.error")
    outcome = invoke_response_from_payload(response.payload)
    return outcome if isinstance(outcome, ToolError) else None


__all__ = [
    "DEFAULT_TOOL_OUTBOUND_CAPACITY",
    "ToolGatewayMailboxFull",
    "ToolGatewayService",
    "ToolGatewaySweep",
    "ToolGatewayUnavailable",
    "ToolInvocationTicket",
    "ToolOutboundMessage",
    "make_logical_invoke_request_envelope",
    "tool_error_from_response",
]
