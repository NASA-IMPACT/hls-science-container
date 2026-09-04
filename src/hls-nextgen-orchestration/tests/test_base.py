from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from hls_nextgen_orchestration.base import (
    Asset,
    TaskContext,
)

A = Asset("A", str)


def test_context_missing_data() -> None:
    """
    Manually test TaskContext behavior when data is missing.
    """
    ctx = TaskContext()
    with pytest.raises(ValueError):
        ctx.get(A)


@dataclass
class _Config:
    working_dir: Path


CONFIG = Asset("CONFIG", _Config)
OTHER_CONFIG = Asset("OTHER_CONFIG", _Config)


def test_context_working_dirs_empty() -> None:
    assert TaskContext().working_dirs() == set()


def test_context_working_dirs_ignores_values_without_one() -> None:
    ctx = TaskContext()
    ctx.put(A, "not-a-config")

    assert ctx.working_dirs() == set()


def test_context_working_dirs_deduplicates() -> None:
    """Config revisions from ``replace()`` share a single working directory."""
    ctx = TaskContext()
    ctx.put(CONFIG, _Config(working_dir=Path("/scratch/job-1")))
    ctx.put(OTHER_CONFIG, _Config(working_dir=Path("/scratch/job-1")))

    assert ctx.working_dirs() == {Path("/scratch/job-1")}


def test_context_working_dirs_collects_distinct_dirs() -> None:
    ctx = TaskContext()
    ctx.put(CONFIG, _Config(working_dir=Path("/scratch/job-1")))
    ctx.put(OTHER_CONFIG, _Config(working_dir=Path("/scratch/job-2")))

    assert ctx.working_dirs() == {Path("/scratch/job-1"), Path("/scratch/job-2")}
