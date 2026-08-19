"""Application-level startup, rollback, and shutdown tests."""

from __future__ import annotations

import signal
import socket
import sys
import threading
import time
from collections.abc import Iterator
from typing import Any

import pytest

from forge_gateway import cli, config
from forge_gateway.adapters.dora_runtime import DoraNodeLike, DoraRunReason
from forge_gateway.application import (
    ApplicationCloseResult,
    CloseStepResult,
    GatewayApplication,
    GatewayStartupError,
)
from forge_gateway.domain.commands import CommandMailboxUnavailable
from forge_gateway.services.runtime_service import GatewayRuntime


class _Node:
    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(())

    def send_output(self, output_id: str, data: Any, /) -> None:
        del output_id, data


class _Server:
    def __init__(self) -> None:
        self.started = False
        self.should_exit = False
        self.exited = threading.Event()

    def run(self) -> None:
        self.started = True
        try:
            while not self.should_exit:
                time.sleep(0.001)
        finally:
            self.exited.set()


class _FailingServer(_Server):
    def run(self) -> None:
        raise OSError("bind failed")


class _EarlyExitServer(_Server):
    def run(self) -> None:
        return


class _NeverStartServer(_Server):
    def __init__(self) -> None:
        super().__init__()
        self.run_entered = threading.Event()

    def run(self) -> None:
        self.run_entered.set()
        try:
            while not self.should_exit:
                time.sleep(0.001)
        finally:
            self.exited.set()


class _StartedThenFailServer(_Server):
    def __init__(self) -> None:
        super().__init__()
        self._started = False
        self.release_failure = threading.Event()
        self.wrapper_done: threading.Event | None = None

    @property
    def started(self) -> bool:
        if self._started:
            self.release_failure.set()
            assert self.wrapper_done is not None
            assert self.wrapper_done.wait(timeout=1.0)
        return self._started

    @started.setter
    def started(self, value: bool) -> None:
        self._started = value

    def run(self) -> None:
        self._started = True
        assert self.release_failure.wait(timeout=1.0)
        raise RuntimeError("server failed after bind")


class _Runner:
    def __init__(
        self,
        *,
        reason: DoraRunReason = "stop",
        start_error: BaseException | None = None,
        join_results: list[bool | BaseException] | None = None,
    ) -> None:
        self.reason: DoraRunReason = reason
        self.start_error = start_error
        self.reader_error: BaseException | None = None
        self.join_results = list(join_results or [True])
        self.start_calls = 0
        self.run_calls = 0
        self.join_calls = 0
        self.stop_event: threading.Event | None = None
        self.runtime: GatewayRuntime | None = None

    def start(self) -> None:
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error

    def run(self) -> DoraRunReason:
        self.run_calls += 1
        return self.reason

    def join_reader(self, timeout: float) -> bool:
        del timeout
        self.join_calls += 1
        result = self.join_results.pop(0) if self.join_results else True
        if isinstance(result, BaseException):
            raise result
        return result


class _BlockingRunner(_Runner):
    def __init__(self) -> None:
        super().__init__(reason="shutdown")
        self.run_entered = threading.Event()
        self.release_run = threading.Event()

    def run(self) -> DoraRunReason:
        self.run_calls += 1
        self.run_entered.set()
        assert self.release_run.wait(timeout=2.0)
        return self.reason


class _LateMutationRunner(_BlockingRunner):
    def run(self) -> DoraRunReason:
        reason = super().run()
        assert self.runtime is not None
        with self.runtime.lock:
            self.runtime.current_frame_count += 1
        return reason


class _RunnerFactory:
    def __init__(self, runner: _Runner) -> None:
        self.runner = runner
        self.calls = 0

    def __call__(
        self,
        *,
        runtime: GatewayRuntime,
        node: DoraNodeLike,
        stop_event: threading.Event,
    ) -> _Runner:
        del node
        self.calls += 1
        self.runner.runtime = runtime
        self.runner.stop_event = stop_event
        return self.runner


def _config(*, port: int = 9001) -> config.GatewayConfig:
    return config.GatewayConfig.from_dict(
        {
            "host": "127.0.0.1",
            "port": port,
            "joint_order": ["j1"],
            "readiness": {"require_images": False},
        }
    )


def _watch_until_stopped(stop_event: threading.Event) -> None:
    stop_event.wait(timeout=2.0)


def _application(
    server: _Server,
    runner: _Runner,
    *,
    node_factory: Any = _Node,
    watcher_target: Any = _watch_until_stopped,
    startup_timeout_sec: float = 0.5,
    thread_join_timeout_sec: float = 0.5,
) -> GatewayApplication:
    return GatewayApplication(
        _config(),
        server_factory=lambda runtime, stop_event: server,
        node_factory=node_factory,
        runner_factory=_RunnerFactory(runner),
        watcher_target=watcher_target,
        startup_timeout_sec=startup_timeout_sec,
        thread_join_timeout_sec=thread_join_timeout_sec,
    )


def test_application_owns_normal_start_run_and_close() -> None:
    server = _Server()
    runner = _Runner(reason="error")
    application = _application(server, runner)

    application.start()

    assert application.phase == "running"
    assert server.started is True
    assert runner.start_calls == 1
    assert application.run() == "error"

    result = application.close()

    assert result.ok is True
    assert result.quiescent is True
    assert application.phase == "stopped"
    assert server.should_exit is True
    assert server.exited.is_set()
    assert runner.join_calls == 1
    assert application.runtime.phase == "closed"
    assert application.close() is result


def test_uvicorn_startup_error_rolls_back_runtime_and_reader() -> None:
    server = _FailingServer()
    runner = _Runner()
    application = _application(server, runner)

    with pytest.raises(GatewayStartupError, match="Uvicorn failed during startup"):
        application.start()

    assert application.phase == "stopped"
    assert application.runtime.phase == "closed"
    assert runner.join_calls == 1
    result = application.close()
    assert result.quiescent is True
    assert result.ok is False
    assert isinstance(result.server.error, OSError)


def test_early_server_exit_is_startup_failure() -> None:
    application = _application(_EarlyExitServer(), _Runner())

    with pytest.raises(GatewayStartupError, match="Uvicorn"):
        application.start()

    assert application.runtime.phase == "closed"
    assert application.close().quiescent is True


def test_server_failure_after_started_flag_cannot_commit_startup() -> None:
    server = _StartedThenFailServer()
    application = _application(server, _Runner())
    server.wrapper_done = application._server_done

    with pytest.raises(
        GatewayStartupError,
        match="server failed after bind",
    ):
        application.start()

    assert server.release_failure.is_set()
    assert application.phase == "stopped"
    assert application.runtime.phase == "closed"


def test_close_cancels_uvicorn_startup_wait() -> None:
    server = _NeverStartServer()
    application = _application(
        server,
        _Runner(),
        startup_timeout_sec=5.0,
    )
    startup_errors: list[BaseException] = []

    def start_application() -> None:
        try:
            application.start()
        except BaseException as error:
            startup_errors.append(error)

    start_thread = threading.Thread(target=start_application)
    start_thread.start()
    assert server.run_entered.wait(timeout=1.0)

    started_at = time.monotonic()
    close_result = application.close()
    elapsed = time.monotonic() - started_at
    start_thread.join(timeout=1.0)

    assert elapsed < 1.0
    assert not start_thread.is_alive()
    assert len(startup_errors) == 1
    assert isinstance(startup_errors[0], GatewayStartupError)
    assert close_result.quiescent is True
    assert application.runtime.phase == "closed"



def test_real_uvicorn_port_collision_is_reported_and_rolled_back() -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = int(listener.getsockname()[1])
    runner = _Runner()
    application = GatewayApplication(
        _config(port=port),
        node_factory=_Node,
        runner_factory=_RunnerFactory(runner),
        watcher_target=_watch_until_stopped,
        startup_timeout_sec=1.0,
        thread_join_timeout_sec=0.5,
    )
    try:
        with pytest.raises(GatewayStartupError, match="Uvicorn failed during startup"):
            application.start()
    finally:
        listener.close()

    assert application.runtime.phase == "closed"
    assert application.close().quiescent is True


def test_node_failure_rolls_back_before_opening_http_ingress() -> None:
    server_factory_calls = 0

    def fail_node() -> DoraNodeLike:
        raise RuntimeError("node initialization failed")

    def server_factory(runtime: GatewayRuntime, stop_event: threading.Event) -> _Server:
        nonlocal server_factory_calls
        del runtime, stop_event
        server_factory_calls += 1
        return _Server()

    application = GatewayApplication(
        _config(),
        server_factory=server_factory,
        node_factory=fail_node,
        runner_factory=_RunnerFactory(_Runner()),
        watcher_target=_watch_until_stopped,
    )

    with pytest.raises(RuntimeError, match="node initialization failed"):
        application.start()

    assert server_factory_calls == 0
    assert application.runtime.phase == "closed"
    assert application.close().ok is True


def test_immediate_reader_failure_prevents_startup_commit() -> None:
    class FailingNode(_Node):
        def __iter__(self) -> Iterator[dict[str, Any]]:
            raise RuntimeError("reader failed during startup")
            yield  # pragma: no cover

    server = _Server()

    def server_factory(
        runtime: GatewayRuntime,
        stop_event: threading.Event,
    ) -> _Server:
        del runtime, stop_event
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            runner = application._runner
            if runner is not None and runner.reader_error is not None:
                return server
            time.sleep(0.001)
        raise RuntimeError("reader error was not published")

    application = GatewayApplication(
        _config(),
        server_factory=server_factory,
        node_factory=FailingNode,
        watcher_target=_watch_until_stopped,
    )

    with pytest.raises(GatewayStartupError, match="Dora reader failed during startup"):
        application.start()

    assert application.phase == "stopped"
    assert application.runtime.phase == "closed"
    assert application.close().dora_reader.error is not None


def test_runner_start_failure_still_attempts_reader_cleanup() -> None:
    runner = _Runner(start_error=RuntimeError("reader start failed"))
    application = _application(_Server(), runner)

    with pytest.raises(RuntimeError, match="reader start failed"):
        application.start()

    assert runner.start_calls == 1
    assert runner.join_calls == 1
    assert application.runtime.phase == "closed"
    assert application.close().ok is True


def test_close_attempts_every_component_after_cleanup_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = _Server()
    runner = _Runner(
        join_results=[RuntimeError("reader join failed"), True],
    )
    watcher_calls = 0

    def failing_watcher(stop_event: threading.Event) -> None:
        nonlocal watcher_calls
        stop_event.wait(timeout=2.0)
        watcher_calls += 1
        raise RuntimeError("watcher failed")

    application = _application(
        server,
        runner,
        watcher_target=failing_watcher,
    )
    application.start()
    runtime = application.runtime
    original_runtime_close = runtime.close
    runtime_close_calls = 0

    def failing_runtime_close() -> bool:
        nonlocal runtime_close_calls
        runtime_close_calls += 1
        raise RuntimeError("runtime close failed")

    monkeypatch.setattr(runtime, "close", failing_runtime_close)
    first = application.close()

    assert first.ok is False
    assert first.server.completed is True
    assert isinstance(first.dora_reader.error, RuntimeError)
    assert isinstance(first.launcher_watcher.error, RuntimeError)
    assert isinstance(first.runtime.error, RuntimeError)
    assert watcher_calls == 1
    assert runner.join_calls == 1
    assert runtime_close_calls == 1
    assert application.phase == "incomplete"

    monkeypatch.setattr(runtime, "close", original_runtime_close)
    second = application.close()

    assert second.runtime.completed is True
    assert second.quiescent is True
    assert second.ok is False
    assert runner.join_calls == 2
    assert runtime.phase == "closed"
    assert application.phase == "stopped"


def test_runtime_close_timeout_keeps_application_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    application = _application(_Server(), _Runner())
    application.start()
    runtime = application.runtime
    original_close = runtime.close
    calls = 0

    def timeout_once() -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            return False
        return original_close()

    monkeypatch.setattr(runtime, "close", timeout_once)

    first = application.close()
    assert first.runtime.completed is False
    assert application.phase == "incomplete"

    second = application.close()
    assert second.runtime.completed is True
    assert second.quiescent is True
    assert application.phase == "stopped"
    assert runtime.phase == "closed"


def test_concurrent_close_callers_share_one_cleanup_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _BlockingRunner()
    application = _application(_Server(), runner)
    application.start()
    runtime = application.runtime
    original_runtime_close = runtime.close
    runtime_close_calls = 0
    run_thread = threading.Thread(target=application.run)
    run_thread.start()
    assert runner.run_entered.wait(timeout=1.0)

    def observed_runtime_close() -> bool:
        nonlocal runtime_close_calls
        runtime_close_calls += 1
        return original_runtime_close()

    monkeypatch.setattr(runtime, "close", observed_runtime_close)
    results: list[ApplicationCloseResult] = []
    close_threads = [
        threading.Thread(target=lambda: results.append(application.close()))
        for _ in range(2)
    ]
    close_threads[0].start()
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline and runtime.phase == "running":
        time.sleep(0.001)
    with application._lifecycle_lock:
        attempt = application._close_attempt
    assert attempt is not None
    waiter_registered = threading.Event()
    original_wait = attempt.completed.wait

    def observed_wait(timeout: float | None = None) -> bool:
        waiter_registered.set()
        return original_wait(timeout=timeout)

    monkeypatch.setattr(attempt.completed, "wait", observed_wait)
    close_threads[1].start()
    assert waiter_registered.wait(timeout=1.0)

    runner.release_run.set()
    run_thread.join(timeout=1.0)
    for thread in close_threads:
        thread.join(timeout=1.0)

    assert not run_thread.is_alive()
    assert all(not thread.is_alive() for thread in close_threads)
    assert len(results) == 2
    assert results[0] is results[1]
    assert runner.join_calls == 1
    assert runtime_close_calls == 1


def test_close_orchestration_error_wakes_waiters_and_allows_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    application = _application(_Server(), _Runner())
    application.start()
    cleanup_entered = threading.Event()
    release_cleanup = threading.Event()
    original_cleanup = application._close_resources
    errors: list[BaseException] = []

    def failing_cleanup() -> ApplicationCloseResult:
        cleanup_entered.set()
        assert release_cleanup.wait(timeout=2.0)
        raise RuntimeError("orchestration failed")

    def close_application() -> None:
        try:
            application.close()
        except BaseException as error:
            errors.append(error)

    monkeypatch.setattr(application, "_close_resources", failing_cleanup)
    close_threads = [threading.Thread(target=close_application) for _ in range(2)]
    close_threads[0].start()
    assert cleanup_entered.wait(timeout=1.0)
    with application._lifecycle_lock:
        attempt = application._close_attempt
    assert attempt is not None
    waiter_registered = threading.Event()
    original_wait = attempt.completed.wait

    def observed_wait(timeout: float | None = None) -> bool:
        waiter_registered.set()
        return original_wait(timeout=timeout)

    monkeypatch.setattr(attempt.completed, "wait", observed_wait)
    close_threads[1].start()
    assert waiter_registered.wait(timeout=1.0)
    release_cleanup.set()
    for thread in close_threads:
        thread.join(timeout=1.0)

    assert all(not thread.is_alive() for thread in close_threads)
    assert len(errors) == 2
    assert all(str(error) == "gateway application close failed" for error in errors)
    assert all(
        isinstance(error.__cause__, RuntimeError)
        and str(error.__cause__) == "orchestration failed"
        for error in errors
    )
    assert application.phase == "incomplete"

    monkeypatch.setattr(application, "_close_resources", original_cleanup)
    assert application.close().quiescent is True
    assert application.runtime.phase == "closed"


def test_incomplete_run_loop_defers_final_runtime_close() -> None:
    runner = _LateMutationRunner()
    application = _application(
        _Server(),
        runner,
        thread_join_timeout_sec=0.05,
    )
    application.start()
    runtime = application.runtime
    run_thread = threading.Thread(target=application.run)
    run_thread.start()
    assert runner.run_entered.wait(timeout=1.0)

    first = application.close()

    assert first.run_loop.completed is False
    assert first.runtime.completed is False
    assert application.phase == "incomplete"
    assert runtime.phase == "closing"
    assert runtime.current_frame_count == 0

    runner.release_run.set()
    run_thread.join(timeout=1.0)
    assert not run_thread.is_alive()
    assert runtime.phase == "closing"
    assert runtime.current_frame_count == 1

    second = application.close()
    assert second.ok is True
    assert runtime.phase == "closed"
    assert application.phase == "stopped"


def test_shutdown_rejects_late_command_before_run_loop_stops() -> None:
    runner = _BlockingRunner()
    application = _application(_Server(), runner)
    application.start()
    run_thread = threading.Thread(target=application.run)
    run_thread.start()
    assert runner.run_entered.wait(timeout=1.0)
    close_results: list[ApplicationCloseResult] = []
    close_thread = threading.Thread(
        target=lambda: close_results.append(application.close())
    )
    close_thread.start()

    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline and application.runtime.phase == "running":
        time.sleep(0.001)
    assert application.runtime.phase == "closing"
    with pytest.raises(CommandMailboxUnavailable, match="gateway runtime is closing"):
        application.runtime.enqueue_policy_command("too_late")

    runner.release_run.set()
    run_thread.join(timeout=1.0)
    close_thread.join(timeout=1.0)
    assert close_results[0].ok is True


def test_join_errors_do_not_skip_reader_or_runtime_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _Runner(join_results=[True, True])
    application = _application(_Server(), runner)
    application.start()
    runtime = application.runtime
    runtime_close_calls = 0
    original_runtime_close = runtime.close
    server_thread = application._server_thread
    watcher_thread = application._watcher_thread
    assert server_thread is not None
    assert watcher_thread is not None
    original_server_join = server_thread.join
    original_watcher_join = watcher_thread.join

    def fail_server_join(timeout: float | None = None) -> None:
        del timeout
        raise RuntimeError("server join failed")

    def fail_watcher_join(timeout: float | None = None) -> None:
        del timeout
        raise RuntimeError("watcher join failed")

    def observed_runtime_close() -> bool:
        nonlocal runtime_close_calls
        runtime_close_calls += 1
        return original_runtime_close()

    monkeypatch.setattr(server_thread, "join", fail_server_join)
    monkeypatch.setattr(watcher_thread, "join", fail_watcher_join)
    monkeypatch.setattr(runtime, "close", observed_runtime_close)

    first = application.close()

    assert first.ok is False
    assert isinstance(first.server.error, RuntimeError)
    assert isinstance(first.launcher_watcher.error, RuntimeError)
    assert runner.join_calls == 1
    assert runtime_close_calls == 0
    assert runtime.phase == "closing"

    monkeypatch.setattr(server_thread, "join", original_server_join)
    monkeypatch.setattr(watcher_thread, "join", original_watcher_join)
    second = application.close()
    assert second.quiescent is True
    assert second.ok is True
    assert runtime_close_calls == 1
    assert runtime.phase == "closed"


def test_server_is_stopped_before_runtime_final_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = _Server()
    application = _application(server, _Runner())
    application.start()
    runtime = application.runtime
    original_close = runtime.close

    def observed_runtime_close() -> bool:
        assert server.should_exit is True
        assert server.exited.is_set()
        return original_close()

    monkeypatch.setattr(runtime, "close", observed_runtime_close)

    assert application.close().ok is True


def test_close_before_start_and_double_start_are_explicit() -> None:
    closed = _application(_Server(), _Runner())
    assert closed.close().ok is True
    with pytest.raises(RuntimeError, match="cannot start from stopped"):
        closed.start()

    running = _application(_Server(), _Runner())
    running.start()
    try:
        with pytest.raises(RuntimeError, match="cannot start from running"):
            running.start()
    finally:
        running.close()


def test_cli_signal_handlers_request_shutdown_and_restore_previous_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    application = _application(_Server(), _Runner())
    previous_handlers = {
        signal.SIGINT: object(),
        signal.SIGTERM: object(),
    }
    installed: list[tuple[signal.Signals, Any]] = []

    monkeypatch.setattr(
        signal,
        "getsignal",
        lambda signum: previous_handlers[signum],
    )
    monkeypatch.setattr(
        signal,
        "signal",
        lambda signum, handler: installed.append((signum, handler)),
    )

    restore = cli._install_shutdown_handlers(application)

    assert [signum for signum, _ in installed] == [signal.SIGINT, signal.SIGTERM]
    handler = installed[1][1]
    assert callable(handler)
    handler(signal.SIGTERM, None)
    assert application.stop_event.is_set()

    restore()
    assert installed[-2:] == [
        (signal.SIGINT, previous_handlers[signal.SIGINT]),
        (signal.SIGTERM, previous_handlers[signal.SIGTERM]),
    ]
    application.close()


@pytest.mark.parametrize(
    ("reason", "expected_exit"),
    [("error", 0), ("reader_error", 1)],
)
def test_cli_distinguishes_upstream_and_local_dora_errors(
    monkeypatch: pytest.MonkeyPatch,
    reason: DoraRunReason,
    expected_exit: int,
) -> None:
    class FakeApplication:
        def __init__(self, cfg: config.GatewayConfig) -> None:
            del cfg

        def start(self) -> None:
            return

        def run(self) -> DoraRunReason:
            return reason

        def close(self) -> ApplicationCloseResult:
            return ApplicationCloseResult.empty()

    monkeypatch.setattr(cli, "GatewayApplication", FakeApplication)
    monkeypatch.setattr(cli, "load_config", lambda path: _config())
    monkeypatch.setattr(sys, "argv", ["gateway"])

    assert cli.main() == expected_exit


def test_cli_returns_failure_for_incomplete_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incomplete = ApplicationCloseResult(
        run_loop=CloseStepResult.skipped(),
        server=CloseStepResult.skipped(),
        dora_reader=CloseStepResult.skipped(),
        launcher_watcher=CloseStepResult.skipped(),
        runtime=CloseStepResult(attempted=True, completed=False),
    )

    class FakeApplication:
        def __init__(self, cfg: config.GatewayConfig) -> None:
            del cfg

        def start(self) -> None:
            return

        def run(self) -> DoraRunReason:
            return "stop"

        def close(self) -> ApplicationCloseResult:
            return incomplete

    monkeypatch.setattr(cli, "GatewayApplication", FakeApplication)
    monkeypatch.setattr(cli, "load_config", lambda path: _config())
    monkeypatch.setattr(sys, "argv", ["gateway"])

    assert cli.main() == 1
