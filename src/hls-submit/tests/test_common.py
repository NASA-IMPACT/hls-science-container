from __future__ import annotations

from pathlib import Path
from typing import Any

import click
import pytest

from hls_submit import lasrc
from hls_submit.common import SubmitOptions, job_name, l1_sensor, read_granules

S2_A = "S2A_MSIL1C_20200101T102431_N0208_R065_T32TQM_20200101T122841"
S2_B = "S2B_MSIL1C_20200102T102431_N0208_R065_T33UUP_20200102T122841"
LC08 = "LC08_L1TP_044032_20250111_20250111_02_RT"


def _options(csv_file: Path, **overrides: Any) -> SubmitOptions:
    values: dict[str, Any] = {
        "csv_file": csv_file,
        "granule_column": "L1_ID",
        "sensor": "S30",
        "rows": None,
        "row_offset": 0,
        "experiments": {},
        "stack_name": "my-stack",
        "region": "us-west-2",
        "job_queue": None,
        "job_definition": None,
        "input_bucket": None,
        "input_prefix": None,
        "requester_pays": False,
        "output_bucket": "out",
        "debug_bucket": None,
        "metric_log_group": None,
        "threads": 2,
        "submit": False,
    }
    values.update(overrides)
    return SubmitOptions(**values)


@pytest.mark.parametrize(
    ("l1_id", "expected"),
    [(LC08, "L30"), ("LC09_L1TP_x", "L30"), (S2_A, "S30"), ("S2C_MSIL1C_x", "S30")],
)
def test_l1_sensor(l1_id: str, expected: str) -> None:
    assert l1_sensor(l1_id) == expected


def test_l1_sensor_rejects_hls_output_id() -> None:
    assert l1_sensor("HLS.S30.T32TQM.2020001T102431.v2.0") is None


def test_read_granules_named_column(granules_csv: Path) -> None:
    assert read_granules(granules_csv, "S30", column="L1_ID") == [S2_A, S2_B]
    assert read_granules(granules_csv, "L30", column="L1_ID") == [LC08]


@pytest.mark.parametrize("header", ["", "my_granules\n"])
def test_read_granules_single_column(tmp_path: Path, header: str) -> None:
    path = tmp_path / "list.csv"
    path.write_text(f"{header}{S2_A}.SAFE\n{LC08}\n\n{S2_B}\n")

    assert read_granules(path, "S30") == [S2_A, S2_B]


def test_read_granules_multi_column_needs_name(granules_csv: Path) -> None:
    with pytest.raises(click.ClickException, match="pass --granule-column"):
        read_granules(granules_csv, "S30")


def test_read_granules_unknown_column(granules_csv: Path) -> None:
    with pytest.raises(click.ClickException, match="no column 'nope'"):
        read_granules(granules_csv, "S30", column="nope")


def test_read_granules_rejects_output_ids(granules_csv: Path) -> None:
    with pytest.raises(click.ClickException, match="not a Landsat or Sentinel-2"):
        read_granules(granules_csv, "S30", column="Granule_ID")


@pytest.mark.parametrize(
    ("row_offset", "rows", "expected"),
    [(0, 1, [S2_A]), (2, None, [S2_B]), (0, None, [S2_A, S2_B])],
)
def test_read_granules_offset_and_rows(
    granules_csv: Path, row_offset: int, rows: int | None, expected: list[str]
) -> None:
    granules = read_granules(
        granules_csv, "S30", column="L1_ID", row_offset=row_offset, rows=rows
    )

    assert granules == expected


def test_job_name_sanitizes_and_truncates() -> None:
    assert job_name("fmask", None, "my.label", "S2A") == "fmask-my-label-S2A"
    assert len(job_name("x" * 200)) == 128


def test_base_environment_sentinel_defaults(granules_csv: Path) -> None:
    env = _options(granules_csv).base_environment("default/prefix")

    assert env["INPUT_BUCKET"] == "hls-science-container-testing"
    assert env["INPUT_PREFIX"] == "default/prefix"
    assert env["DEBUG_BUCKET"] == "out"
    assert env["METRIC_LOG_GROUP_NAME"] == "/hls-orchestration/my-stack/log-metrics"
    assert "INPUT_REQUESTER_PAYS" not in env
    assert not any(k.startswith("HLS_EXPERIMENT_") for k in env)


def test_base_environment_overrides(granules_csv: Path) -> None:
    options = _options(
        granules_csv,
        input_bucket="hls-validation-datasets",
        input_prefix="datasets/x/S30/",
        requester_pays=True,
        debug_bucket="dbg",
        metric_log_group="/custom",
        threads=4,
        experiments={"FMASK_BUILD": "baseline", "RUN": "2"},
    )
    env = options.base_environment("default/prefix")

    assert env["INPUT_BUCKET"] == "hls-validation-datasets"
    assert env["INPUT_PREFIX"] == "datasets/x/S30/"
    assert env["INPUT_REQUESTER_PAYS"] == "true"
    assert env["DEBUG_BUCKET"] == "dbg"
    assert env["METRIC_LOG_GROUP_NAME"] == "/custom"
    assert env["OMP_NUM_THREADS"] == "4"
    assert env["RAYON_NUM_THREADS"] == "4"
    assert env["HLS_EXPERIMENT_FMASK_BUILD"] == "baseline"
    assert env["HLS_EXPERIMENT_RUN"] == "2"


def test_base_environment_no_stack_skips_metrics(granules_csv: Path) -> None:
    env = _options(granules_csv, stack_name=None).base_environment()

    assert "METRIC_LOG_GROUP_NAME" not in env


def test_base_environment_landsat_has_no_prefix(granules_csv: Path) -> None:
    env = _options(granules_csv, sensor="L30").base_environment("default/prefix")

    assert env["INPUT_BUCKET"] == "usgs-landsat"
    assert "INPUT_PREFIX" not in env


def test_lasrc_jobs_tag_impl(granules_csv: Path) -> None:
    options = _options(granules_csv, sensor="L30", experiments={"RUN": "r1"})
    jobs = lasrc.build_jobs(options, ("c", "rust"))

    assert [j.name for j in jobs] == [f"lasrc-r1-c-{LC08}", f"lasrc-r1-rust-{LC08}"]
    assert [j.environment["HLS_EXPERIMENT_LASRC_IMPL"] for j in jobs] == ["c", "rust"]
    assert all(j.environment["HLS_EXPERIMENT_RUN"] == "r1" for j in jobs)
