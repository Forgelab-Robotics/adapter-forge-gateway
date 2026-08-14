from __future__ import annotations

from typing import Literal, cast

import pytest
from forge_tool import (
    TOOL_ENDPOINT_PROTOCOL,
    ToolEndpointDescriptor,
    ToolOperationDescriptor,
    make_registration_envelope,
    make_unregister_envelope,
)

from forge_gateway.domain.tool_directory import (
    EndpointDirectory,
    ToolOperationNotFoundError,
    ToolProviderRouteIdentity,
)


def descriptor(
    operation: str = "detect",
    *,
    semantics: Literal["query", "action", "session"] = "query",
    endpoint_id: str = "vision.yolo",
) -> ToolEndpointDescriptor:
    return ToolEndpointDescriptor(
        protocol_version=TOOL_ENDPOINT_PROTOCOL,
        endpoint_id=endpoint_id,
        operations=(
            ToolOperationDescriptor(
                name=operation,
                semantics=semantics,
                status_supported=semantics != "query",
                max_concurrency=1,
            ),
        ),
    )


def route(
    *,
    endpoint_id: str = "vision.yolo",
    input_id: str = "yolo/provider",
    output_id: str = "yolo/gateway",
) -> ToolProviderRouteIdentity:
    return ToolProviderRouteIdentity(
        endpoint_id=endpoint_id,
        input_id=input_id,
        output_id=output_id,
    )


def registration(
    *,
    instance_id: str = "instance-1",
    operation: str = "detect",
    semantics: Literal["query", "action", "session"] = "query",
    endpoint_id: str = "vision.yolo",
    request_id: str = "register-1",
):
    return make_registration_envelope(
        descriptor(operation, semantics=semantics, endpoint_id=endpoint_id),
        endpoint_instance_id=instance_id,
        request_id=request_id,
    )


def unregister(
    *,
    instance_id: str = "instance-1",
    endpoint_id: str = "vision.yolo",
    request_id: str = "unregister-1",
):
    return make_unregister_envelope(
        endpoint_id=endpoint_id,
        endpoint_instance_id=instance_id,
        request_id=request_id,
    )


def test_register_exposes_descriptor_instance_route_and_global_revision() -> None:
    directory = EndpointDirectory(lease_ttl_ms=15_000)

    response = directory.register(registration(), route(), now=10.0)

    assert response.status == "accepted"
    assert response.operation == "register"
    assert response.registry_revision == 1
    assert response.lease_ttl_ms == 15_000
    current = directory.resolve("vision.yolo", "detect", now=10.0)
    assert current is not None
    assert current.endpoint_instance_id == "instance-1"
    assert current.route == route()
    assert current.registry_revision == 1
    assert current.expires_at == 25.0


def test_same_route_instance_and_descriptor_renews_without_revision() -> None:
    directory = EndpointDirectory(lease_ttl_ms=1_000)
    request = registration()
    configured_route = route()
    first = directory.register(request, configured_route, now=1.0)

    renewed = directory.register(request, configured_route, now=1.5)

    assert first.registry_revision == renewed.registry_revision == 1
    assert directory.resolve("vision.yolo", "detect", now=2.49) is not None
    assert directory.resolve("vision.yolo", "detect", now=2.5) is None
    assert directory.revision == 2


def test_same_instance_cannot_change_descriptor() -> None:
    directory = EndpointDirectory()
    directory.register(registration(), route(), now=1.0)

    response = directory.register(
        registration(operation="segment", request_id="register-2"),
        route(),
        now=2.0,
    )

    assert response.status == "rejected"
    assert response.error is not None
    assert response.error.code == "FORGE_ENDPOINT_DESCRIPTOR_CONFLICT"
    assert directory.revision == 1
    assert directory.resolve("vision.yolo", "detect", now=2.0) is not None


def test_new_instance_on_same_route_atomically_replaces_current() -> None:
    directory = EndpointDirectory()
    directory.register(registration(), route(), now=1.0)

    mutation = directory.register_with_change(
        registration(
            instance_id="instance-2",
            operation="segment",
            request_id="register-2",
        ),
        route(),
        now=2.0,
    )

    replacement = mutation.response
    assert replacement.status == "accepted"
    assert replacement.registry_revision == 2
    assert [item.endpoint_instance_id for item in mutation.removed] == ["instance-1"]
    current = directory.resolve("vision.yolo", "segment", now=2.0)
    assert current is not None
    assert current.endpoint_instance_id == "instance-2"
    with pytest.raises(ToolOperationNotFoundError, match="detect"):
        directory.resolve("vision.yolo", "detect", now=2.0)


def test_different_route_cannot_replace_current() -> None:
    directory = EndpointDirectory()
    directory.register(registration(), route(), now=1.0)
    conflicting_route = route(input_id="other/in", output_id="other/out")

    response = directory.register(
        registration(instance_id="instance-2", request_id="register-2"),
        conflicting_route,
        now=2.0,
    )

    assert response.status == "rejected"
    assert response.error is not None
    assert response.error.code == "FORGE_ENDPOINT_ROUTE_CONFLICT"
    assert directory.revision == 1


def test_register_rejects_route_mismatch_and_session_but_accepts_action() -> None:
    directory = EndpointDirectory()

    unauthorized = directory.register(
        registration(endpoint_id="vision.other"),
        route(),
        now=1.0,
    )
    action = directory.register(
        registration(semantics="action", request_id="register-action"),
        route(),
        now=2.0,
    )
    unsupported = directory.register(
        registration(
            semantics="session",
            instance_id="instance-session",
            request_id="register-session",
        ),
        route(),
        now=3.0,
    )

    assert unauthorized.status == "rejected"
    assert unauthorized.error is not None
    assert unauthorized.error.code == "FORGE_ENDPOINT_ROUTE_UNAUTHORIZED"
    assert action.status == "accepted"
    assert unsupported.status == "rejected"
    assert unsupported.error is not None
    assert unsupported.error.code == "FORGE_TOOL_SEMANTICS_UNSUPPORTED"
    assert directory.revision == 1


def test_matching_unregister_removes_and_absent_unregister_is_effect_idempotent() -> (
    None
):
    directory = EndpointDirectory()
    directory.register(registration(), route(), now=1.0)

    removal = directory.unregister_with_change(unregister(), route(), now=2.0)
    replay = directory.unregister_with_change(
        unregister(request_id="unregister-2"),
        route(),
        now=3.0,
    )

    removed = removal.response
    absent = replay.response
    assert removed.status == absent.status == "accepted"
    assert removed.registry_revision == absent.registry_revision == 2
    assert removed.lease_ttl_ms is None
    assert [item.endpoint_instance_id for item in removal.removed] == ["instance-1"]
    assert replay.removed == ()
    assert directory.registrations(now=3.0) == ()


def test_absent_unregister_returns_current_process_revision() -> None:
    directory = EndpointDirectory()
    other_route = route(
        endpoint_id="vision.other",
        input_id="other/in",
        output_id="other/out",
    )
    directory.register(
        registration(
            endpoint_id="vision.other",
            instance_id="other-instance",
            request_id="register-other",
        ),
        other_route,
        now=1.0,
    )

    response = directory.unregister(unregister(), route(), now=2.0)

    assert response.status == "accepted"
    assert response.registry_revision == 1


def test_unregister_rejects_nonmatching_route_or_instance() -> None:
    directory = EndpointDirectory()
    directory.register(registration(), route(), now=1.0)

    wrong_route = directory.unregister(
        unregister(),
        route(input_id="other/in", output_id="other/out"),
        now=2.0,
    )
    wrong_instance = directory.unregister(
        unregister(instance_id="stale"),
        route(),
        now=2.0,
    )

    assert wrong_route.status == "rejected"
    assert wrong_route.error is not None
    assert wrong_route.error.code == "FORGE_ENDPOINT_ROUTE_CONFLICT"
    assert wrong_instance.status == "rejected"
    assert wrong_instance.error is not None
    assert wrong_instance.error.code == "FORGE_ENDPOINT_INSTANCE_STALE"
    assert directory.revision == 1


def test_no_tombstone_blocks_reregister_after_unregister() -> None:
    directory = EndpointDirectory()
    directory.register(registration(), route(), now=1.0)
    directory.unregister(unregister(), route(), now=2.0)

    response = directory.register(
        registration(request_id="register-again"),
        route(),
        now=3.0,
    )

    assert response.status == "accepted"
    assert response.registry_revision == 3
    assert directory.resolve("vision.yolo", "detect", now=3.0) is not None


def test_expire_removes_each_elapsed_registration_and_increments_revision() -> None:
    directory = EndpointDirectory(lease_ttl_ms=1_000)
    directory.register(registration(), route(), now=1.0)
    other_route = route(
        endpoint_id="vision.other",
        input_id="other/in",
        output_id="other/out",
    )
    directory.register(
        registration(
            endpoint_id="vision.other",
            instance_id="other-instance",
            request_id="register-other",
        ),
        other_route,
        now=1.5,
    )

    first = directory.expire(2.0)
    second = directory.expire(2.5)

    assert [item.endpoint_id for item in first] == ["vision.yolo"]
    assert [item.endpoint_id for item in second] == ["vision.other"]
    assert directory.revision == 4


def test_resolve_validates_identifiers_and_operation_membership() -> None:
    directory = EndpointDirectory()
    directory.register(registration(), route(), now=1.0)

    with pytest.raises(ValueError, match="endpoint_id"):
        directory.resolve("", "detect", now=1.0)
    with pytest.raises(ValueError, match="operation"):
        directory.resolve("vision.yolo", "", now=1.0)
    with pytest.raises(ToolOperationNotFoundError, match="segment"):
        directory.resolve("vision.yolo", "segment", now=1.0)


@pytest.mark.parametrize("now", [-1.0, float("nan"), float("inf"), True])
def test_directory_rejects_invalid_monotonic_time(now: object) -> None:
    directory = EndpointDirectory()

    with pytest.raises(ValueError, match="monotonic"):
        directory.snapshot(now=cast(float, now))


def test_revision_exhaustion_does_not_partially_expire_state() -> None:
    directory = EndpointDirectory(lease_ttl_ms=1_000)
    directory.register(registration(), route(), now=1.0)
    directory._revision = 9_007_199_254_740_991

    with pytest.raises(RuntimeError, match="revision is exhausted"):
        directory.expire(2.0)

    assert "vision.yolo" in directory._current


def test_revision_exhaustion_does_not_partially_unregister_state() -> None:
    directory = EndpointDirectory()
    directory.register(registration(), route(), now=1.0)
    directory._revision = 9_007_199_254_740_991

    with pytest.raises(RuntimeError, match="revision is exhausted"):
        directory.unregister(unregister(), route(), now=2.0)

    current = directory.resolve("vision.yolo", "detect", now=2.0)
    assert current is not None
    assert current.endpoint_instance_id == "instance-1"


def test_register_preflights_expiry_and_replacement_revisions_together() -> None:
    directory = EndpointDirectory(lease_ttl_ms=1_000)
    directory.register(registration(), route(), now=1.0)
    directory._revision = 9_007_199_254_740_990

    with pytest.raises(RuntimeError, match="revision is exhausted"):
        directory.register(
            registration(instance_id="instance-2", request_id="register-2"),
            route(),
            now=2.0,
        )

    assert "vision.yolo" in directory._current
    assert directory._current["vision.yolo"].endpoint_instance_id == "instance-1"
    assert directory.revision == 9_007_199_254_740_990
