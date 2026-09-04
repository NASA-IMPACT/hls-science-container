from __future__ import annotations

import os
import signal
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from hls_nextgen_orchestration.base import Asset, DataSource, Task
from hls_nextgen_orchestration.cli import _exit_on_sigterm, cli
from hls_nextgen_orchestration.pipeline import Pipeline, PipelineBuilder

COMMANDS = {
    "sentinel": (
        "hls_nextgen_orchestration.sentinel.workflow",
        ["sentinel", "--granule-list", "S2A_ABC"],
    ),
    "landsat-ac": (
        "hls_nextgen_orchestration.landsat_ac.workflow",
        ["landsat-ac", "--granule", "LC08_ABC"],
    ),
    "landsat-tile": (
        "hls_nextgen_orchestration.landsat_tile.workflow",
        ["landsat-tile"],
    ),
}


@pytest.fixture
def captured_kwargs(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> dict[str, Any]:
    """Stub out the workflow's ``construct_pipeline`` and record its kwargs."""
    module_name, _ = COMMANDS[request.param]
    kwargs: dict[str, Any] = {}

    def fake_construct_pipeline(**call_kwargs: Any) -> Pipeline:
        kwargs.update(call_kwargs)
        return PipelineBuilder().build()

    monkeypatch.setattr(
        f"{module_name}.construct_pipeline", fake_construct_pipeline, raising=True
    )
    return kwargs


@pytest.mark.parametrize("captured_kwargs", COMMANDS, indirect=True)
def test_cleanup_working_dir_defaults_to_true(
    captured_kwargs: dict[str, Any], request: pytest.FixtureRequest
) -> None:
    _, args = COMMANDS[request.node.callspec.params["captured_kwargs"]]

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 0, result.output
    assert captured_kwargs["cleanup_working_dir"] is True


@pytest.mark.parametrize("captured_kwargs", COMMANDS, indirect=True)
def test_no_cleanup_working_dir_flag(
    captured_kwargs: dict[str, Any], request: pytest.FixtureRequest
) -> None:
    _, args = COMMANDS[request.node.callspec.params["captured_kwargs"]]

    result = CliRunner().invoke(cli, [*args, "--no-cleanup-working-dir"])

    assert result.exit_code == 0, result.output
    assert captured_kwargs["cleanup_working_dir"] is False


@pytest.mark.parametrize(
    ("value", "expected"),
    [("false", False), ("0", False), ("true", True), ("1", True)],
)
@pytest.mark.parametrize("captured_kwargs", COMMANDS, indirect=True)
def test_cleanup_working_dir_from_envvar(
    captured_kwargs: dict[str, Any],
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    expected: bool,
) -> None:
    monkeypatch.setenv("CLEANUP_WORKING_DIR", value)
    _, args = COMMANDS[request.node.callspec.params["captured_kwargs"]]

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 0, result.output
    assert captured_kwargs["cleanup_working_dir"] is expected


@pytest.mark.parametrize("captured_kwargs", ["landsat-tile"], indirect=True)
def test_flag_overrides_envvar(
    captured_kwargs: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLEANUP_WORKING_DIR", "false")

    result = CliRunner().invoke(cli, ["landsat-tile", "--cleanup-working-dir"])

    assert result.exit_code == 0, result.output
    assert captured_kwargs["cleanup_working_dir"] is True


@pytest.fixture(autouse=True)
def restore_sigterm() -> Iterator[None]:
    """Keep the CLI's SIGTERM handler from leaking into other tests."""
    original = signal.getsignal(signal.SIGTERM)
    yield
    signal.signal(signal.SIGTERM, original)


@pytest.mark.parametrize("captured_kwargs", ["landsat-tile"], indirect=True)
def test_sigterm_handler_installed(captured_kwargs: dict[str, Any]) -> None:
    CliRunner().invoke(cli, ["landsat-tile"])

    assert signal.getsignal(signal.SIGTERM) is _exit_on_sigterm


def test_sigterm_cleans_up_working_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A SIGTERM mid-run must still unwind into the working dir cleanup."""
    working_dir = tmp_path / "job-1"

    @dataclass
    class Config:
        working_dir: Path

    CONFIG = Asset("CONFIG", Config)

    @dataclass(frozen=True)
    class ConfigSource(DataSource):
        provides = (CONFIG,)

        def fetch(self) -> dict[Asset[Any], Any]:
            working_dir.mkdir(parents=True)
            return {CONFIG: Config(working_dir=working_dir)}

    @dataclass(frozen=True)
    class Terminated(Task):
        requires = (CONFIG,)

        def run(self, inputs: dict[Asset[Any], Any]) -> dict[Asset[Any], Any]:
            os.kill(os.getpid(), signal.SIGTERM)
            return {}

    def fake_construct_pipeline(**kwargs: Any) -> Pipeline:
        return (
            PipelineBuilder()
            .add(ConfigSource("EnvConfig"))
            .add(Terminated("Terminated"))
            .build()
        )

    monkeypatch.setattr(
        "hls_nextgen_orchestration.landsat_tile.workflow.construct_pipeline",
        fake_construct_pipeline,
    )

    result = CliRunner().invoke(cli, ["landsat-tile"])

    assert result.exit_code == 143
    assert not working_dir.exists()
