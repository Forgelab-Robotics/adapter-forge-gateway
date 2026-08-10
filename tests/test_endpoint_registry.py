from __future__ import annotations

import pytest
from forge_tool import (
    TOOL_ENDPOINT_PROTOCOL,
    ToolEndpointDescriptor,
    ToolEnvelope,
    ToolOperationDescriptor,
    ToolProtocolError,
    make_heartbeat_envelope,
    make_registration_envelope,
    make_unregister_envelope,
)

from forge_gateway.domain.endpoint_registry import (
    EndpointRegistry,
    EndpointSourceAuthority,
)


def descriptor(operation: str = "detect") -> ToolEndpointDescriptor:
    return ToolEndpointDescriptor(
        protocol_version=TOOL_ENDPOINT_PROTOCOL,
        endpoint_id="vision.yolo",
        operations=(
            ToolOperationDescriptor(
                name=operation,
                semantics="query",
                max_concurrency=1,
            ),
        ),
    )


def authority(
    *,
    source_id: str = "dora:yolo",
    generation: int = 1,
    endpoint_id: str = "vision.yolo",
) -> EndpointSourceAuthority:
    return EndpointSourceAuthority(
        source_id=source_id,
        generation=generation,
        endpoint_id=endpoint_id,
    )


def registration(
    *,
    instance_id: str = "instance-1",
    operation: str = "detect",
    request_id: str = "register-1",
):
    return make_registration_envelope(
        descriptor(operation),
        endpoint_instance_id=instance_id,
        request_id=request_id,
    )


def heartbeat(
    *,
    instance_id: str = "instance-1",
    request_id: str = "heartbeat-1",
):
    return make_heartbeat_envelope(
        endpoint_id="vision.yolo",
        endpoint_instance_id=instance_id,
        request_id=request_id,
    )


def unregister(
    *,
    instance_id: str = "instance-1",
    request_id: str = "unregister-1",
):
    return make_unregister_envelope(
        endpoint_id="vision.yolo",
        endpoint_instance_id=instance_id,
        request_id=request_id,
    )


def test_register_is_visible_only_after_an_accepted_registry_decision() -> None:
    registry = EndpointRegistry(lease_ttl_ms=15_000)

    response = registry.handle_management(
        registration(),
        authority(),
        now=10.0,
    )

    assert response.operation == "register"
    assert response.status == "accepted"
    assert response.registry_revision == 1
    assert response.lease_ttl_ms == 15_000
    current = registry.resolve("vision.yolo", "detect", now=10.0)
    assert current is not None
    assert current.endpoint_instance_id == "instance-1"
    assert current.authority == authority()
    assert registry.resolve("vision.yolo", "segment", now=10.0) is None


def test_duplicate_register_is_idempotent_and_renews_the_lease() -> None:
    registry = EndpointRegistry(lease_ttl_ms=1_000)
    request = registration()
    source = authority()
    first = registry.handle_management(request, source, now=1.0)

    duplicate = registry.handle_management(request, source, now=1.5)

    assert first.registry_revision == 1
    assert duplicate.status == "accepted"
    assert duplicate.registry_revision == 1
    assert registry.resolve("vision.yolo", "detect", now=2.49) is not None
    assert registry.resolve("vision.yolo", "detect", now=2.5) is None
    assert registry.revision == 2


def test_current_instance_cannot_change_its_descriptor() -> None:
    registry = EndpointRegistry()
    registry.handle_management(registration(), authority(), now=1.0)

    response = registry.handle_management(
        registration(operation="segment", request_id="register-2"),
        authority(),
        now=2.0,
    )

    assert response.status == "rejected"
    assert response.error is not None
    assert response.error.code == "FORGE_ENDPOINT_DESCRIPTOR_CONFLICT"
    assert registry.resolve("vision.yolo", "detect", now=2.0) is not None
    assert registry.resolve("vision.yolo", "segment", now=2.0) is None


def test_new_instance_requires_a_newer_trusted_generation() -> None:
    registry = EndpointRegistry()
    registry.handle_management(registration(), authority(generation=4), now=1.0)

    stale = registry.handle_management(
        registration(instance_id="instance-2", request_id="register-2"),
        authority(generation=4),
        now=2.0,
    )
    replacement = registry.handle_management(
        registration(instance_id="instance-2", request_id="register-3"),
        authority(generation=5),
        now=3.0,
    )

    assert stale.status == "rejected"
    assert stale.error is not None
    assert stale.error.code == "FORGE_ENDPOINT_INSTANCE_STALE"
    assert replacement.status == "accepted"
    assert replacement.registry_revision == 2
    current = registry.resolve("vision.yolo", "detect", now=3.0)
    assert current is not None
    assert current.endpoint_instance_id == "instance-2"
    assert current.authority.generation == 5


def test_another_source_cannot_replace_or_renew_the_current_lease() -> None:
    registry = EndpointRegistry()
    registry.handle_management(registration(), authority(), now=1.0)

    replacement = registry.handle_management(
        registration(instance_id="instance-2", request_id="register-2"),
        authority(source_id="dora:other", generation=2),
        now=2.0,
    )
    renewal = registry.handle_management(
        heartbeat(),
        authority(source_id="dora:other"),
        now=2.0,
    )

    assert replacement.status == "rejected"
    assert replacement.error is not None
    assert replacement.error.code == "FORGE_ENDPOINT_SOURCE_CONFLICT"
    assert renewal.status == "rejected"
    assert renewal.error is not None
    assert renewal.error.code == "FORGE_ENDPOINT_SOURCE_STALE"


def test_heartbeat_renews_only_the_current_instance() -> None:
    registry = EndpointRegistry(lease_ttl_ms=1_000)
    registry.handle_management(registration(), authority(), now=1.0)

    stale = registry.handle_management(
        heartbeat(instance_id="stale-instance"),
        authority(),
        now=1.5,
    )
    renewed = registry.handle_management(heartbeat(), authority(), now=1.5)

    assert stale.status == "rejected"
    assert stale.error is not None
    assert stale.error.code == "FORGE_ENDPOINT_INSTANCE_STALE"
    assert renewed.status == "accepted"
    assert renewed.registry_revision == 1
    assert registry.resolve("vision.yolo", "detect", now=2.49) is not None


def test_expired_registration_is_removed_and_heartbeat_requests_reregister() -> None:
    registry = EndpointRegistry(lease_ttl_ms=1_000)
    registry.handle_management(registration(), authority(), now=1.0)

    expired = registry.expire(2.0)
    response = registry.handle_management(heartbeat(), authority(), now=2.1)

    assert [item.endpoint_id for item in expired] == ["vision.yolo"]
    assert registry.revision == 2
    assert response.status == "rejected"
    assert response.error is not None
    assert response.error.code == "FORGE_ENDPOINT_NOT_REGISTERED"
    assert response.error.retryable is True


def test_unregister_replay_is_effect_idempotent_after_removal() -> None:
    registry = EndpointRegistry()
    registry.handle_management(registration(), authority(), now=1.0)

    removed = registry.handle_management(unregister(), authority(), now=2.0)
    other_descriptor = ToolEndpointDescriptor(
        protocol_version=TOOL_ENDPOINT_PROTOCOL,
        endpoint_id="vision.other",
        operations=(ToolOperationDescriptor(name="detect", semantics="query"),),
    )
    registry.handle_management(
        make_registration_envelope(
            other_descriptor,
            endpoint_instance_id="other-instance",
            request_id="register-other",
        ),
        authority(source_id="dora:other", endpoint_id="vision.other"),
        now=2.5,
    )
    duplicate = registry.handle_management(
        unregister(request_id="unregister-2"),
        authority(),
        now=3.0,
    )

    assert registry.revision == 3
    assert removed.status == "accepted"
    assert removed.registry_revision == 2
    assert removed.lease_ttl_ms is None
    assert duplicate.status == "accepted"
    assert duplicate.registry_revision == removed.registry_revision
    assert duplicate.error is None


def test_explicit_unregister_tombstone_blocks_delayed_instance_resurrection() -> None:
    registry = EndpointRegistry()
    registry.handle_management(registration(), authority(), now=1.0)
    registry.handle_management(unregister(), authority(), now=2.0)

    delayed = registry.handle_management(
        registration(request_id="register-delayed"),
        authority(),
        now=3.0,
    )
    replacement = registry.handle_management(
        registration(instance_id="instance-2", request_id="register-replacement"),
        authority(),
        now=4.0,
    )

    assert delayed.status == "rejected"
    assert delayed.error is not None
    assert delayed.error.code == "FORGE_ENDPOINT_INSTANCE_TOMBSTONED"
    assert delayed.error.retryable is False
    assert replacement.status == "accepted"
    current = registry.resolve("vision.yolo", "detect", now=4.0)
    assert current is not None
    assert current.endpoint_instance_id == "instance-2"


def test_expiry_tombstone_allows_the_same_live_provider_to_reregister() -> None:
    registry = EndpointRegistry(lease_ttl_ms=1_000)
    registry.handle_management(registration(), authority(), now=1.0)
    registry.expire(2.0)

    recovered = registry.handle_management(
        registration(request_id="register-recovered"),
        authority(),
        now=2.1,
    )

    assert recovered.status == "accepted"
    assert recovered.registry_revision == 3
    current = registry.resolve("vision.yolo", "detect", now=2.1)
    assert current is not None
    assert current.endpoint_instance_id == "instance-1"


def test_unregister_tombstone_does_not_authorize_a_stale_instance() -> None:
    registry = EndpointRegistry()
    registry.handle_management(registration(), authority(), now=1.0)
    removed = registry.handle_management(unregister(), authority(), now=2.0)

    stale = registry.handle_management(
        unregister(instance_id="stale-instance", request_id="unregister-stale"),
        authority(),
        now=3.0,
    )

    assert removed.status == "accepted"
    assert stale.status == "rejected"
    assert stale.error is not None
    assert stale.error.code == "FORGE_ENDPOINT_NOT_REGISTERED"


@pytest.mark.parametrize("message_type", ["endpoint.heartbeat", "endpoint.unregister"])
def test_registry_rejects_nonempty_management_payloads(message_type: str) -> None:
    registry = EndpointRegistry()
    registry.handle_management(registration(), authority(), now=1.0)
    invalid = ToolEnvelope(
        protocol=TOOL_ENDPOINT_PROTOCOL,
        message_type=message_type,  # type: ignore[arg-type]
        request_id="invalid-management",
        endpoint_id="vision.yolo",
        endpoint_instance_id="instance-1",
        payload={"unexpected": True},
    )

    with pytest.raises(ToolProtocolError, match="unknown fields"):
        registry.handle_management(invalid, authority(), now=2.0)

    assert registry.resolve("vision.yolo", "detect", now=2.0) is not None
    assert registry.revision == 1


def test_revision_exhaustion_does_not_partially_remove_current_state() -> None:
    registry = EndpointRegistry(lease_ttl_ms=1_000)
    registry.handle_management(registration(), authority(), now=1.0)
    registry._revision = 9_007_199_254_740_991

    with pytest.raises(RuntimeError, match="revision is exhausted"):
        registry.expire(2.0)

    assert "vision.yolo" in registry._current


def test_source_authority_cannot_manage_a_different_endpoint() -> None:
    registry = EndpointRegistry()

    response = registry.handle_management(
        registration(),
        authority(endpoint_id="vision.other"),
        now=1.0,
    )

    assert response.status == "rejected"
    assert response.error is not None
    assert response.error.code == "FORGE_ENDPOINT_SOURCE_UNAUTHORIZED"
    assert registry.registrations(now=1.0) == ()


@pytest.mark.parametrize("now", [-1.0, float("nan"), float("inf"), True])
def test_registry_rejects_invalid_monotonic_time(now: object) -> None:
    registry = EndpointRegistry()

    with pytest.raises(ValueError, match="monotonic"):
        registry.registrations(now=now)  # type: ignore[arg-type]
