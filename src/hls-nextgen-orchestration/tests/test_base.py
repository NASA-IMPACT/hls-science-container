from __future__ import annotations

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
