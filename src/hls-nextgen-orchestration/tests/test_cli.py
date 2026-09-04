from __future__ import annotations

from typing import Any

import pytest
from click.testing import CliRunner

from hls_nextgen_orchestration.cli import cli
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
