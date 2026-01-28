from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from hls_nextgen_orchestration.base import (
    Asset,
    DataSource,
    PipelineBuilder,
    Task,
    TaskContext,
)

# ----- Test setup (example assets / sources / tasks) and fixtures
A = Asset("A", str)
B = Asset("B", str)
C = Asset("C", str)
D = Asset("D", str)


@dataclass(frozen=True)
class SimpleSource(DataSource):
    """Returns 'val_{key}' for every asset it provides."""

    def fetch(self) -> dict[Asset[str], str]:
        return {asset: f"val_{asset.key}" for asset in self.provides}


@dataclass(frozen=True)
class SimpleTask(Task):
    """Concatenates input values and returns 'val_{out}_from_{in}'."""

    def run(self, inputs: dict[Asset[Any], Any]) -> dict[Asset[str], str]:
        # Deterministic concatenation of input values
        input_str = "_".join(sorted(str(v) for v in inputs.values()))
        return {asset: f"val_{asset.key}_from_{input_str}" for asset in self.provides}


@pytest.fixture
def builder() -> PipelineBuilder:
    return PipelineBuilder()


# ----- tests
def test_linear_pipeline(builder):
    """
    Graph: Source(A) -> Task(A -> B)
    """
    source = SimpleSource("Src", provides=(A,))
    task = SimpleTask("T1", requires=(A,), provides=(B,))

    pipeline = builder.add(source).add(task).build()

    # Verify Execution Order
    assert len(pipeline.execution_order) == 2
    assert pipeline.execution_order[0] == source
    assert pipeline.execution_order[1] == task

    # Verify Data Flow
    context = pipeline.run()
    assert context.get(A) == "val_A"
    assert context.get(B) == "val_B_from_val_A"


def test_diamond_dependency(builder):
    """
    Graph:
          /-> Task1(A->B) -\
    Source(A)               -> Task3(B,C -> D)
          \\-> Task2(A->C) -/
    """
    source = SimpleSource("Src", provides=(A,))
    task1 = SimpleTask("T1", requires=(A,), provides=(B,))
    task2 = SimpleTask("T2", requires=(A,), provides=(C,))
    task3 = SimpleTask("T3", requires=(B, C), provides=(D,))

    pipeline = builder.add(source).add(task1).add(task2).add(task3).build()

    order = [node.name for node in pipeline.execution_order]

    # Source must be first
    assert order[0] == "Src"
    # Task3 must be last
    assert order[-1] == "T3"
    # T1 and T2 must be in the middle (order between them doesn't strictly matter)
    assert "T1" in order[1:3]
    assert "T2" in order[1:3]

    context = pipeline.run()
    assert context.get(B) == "val_B_from_val_A"
    assert context.get(C) == "val_C_from_val_A"

    # Task 3 combines B and C. Input string will be sorted values of inputs.
    # Sorted values: "val_B_from_val_A", "val_C_from_val_A"
    expected_d = "val_D_from_val_B_from_val_A_val_C_from_val_A"
    assert context.get(D) == expected_d


def test_independent_branches(builder):
    """
    Graph:
    Src1(A) -> T1(A->B)
    Src2(C) -> T2(C->D)
    """
    src1 = SimpleSource("S1", provides=(A,))
    task1 = SimpleTask("T1", requires=(A,), provides=(B,))

    src2 = SimpleSource("S2", provides=(C,))
    task2 = SimpleTask("T2", requires=(C,), provides=(D,))

    pipeline = builder.add(src1).add(task1).add(src2).add(task2).build()

    assert len(pipeline.execution_order) == 4
    context = pipeline.run()
    assert context.get(B) == "val_B_from_val_A"
    assert context.get(D) == "val_D_from_val_C"


# --- Sad Paths (Construction Time) ---


def test_missing_dependency(builder):
    """
    Task requires A, but nothing provides A.
    """
    task = SimpleTask("Orphan", requires=(A,), provides=(B,))

    builder.add(task)

    with pytest.raises(ValueError, match="requires 'A'.*no provider exists"):
        builder.build()


def test_duplicate_provider(builder):
    """
    Two nodes provide A.
    """
    src1 = SimpleSource("S1", provides=(A,))
    src2 = SimpleSource("S2", provides=(A,))

    builder.add(src1)

    with pytest.raises(ValueError, match="Conflict.*provided by both 'S1' and 'S2'"):
        builder.add(src2)


def test_cycle_detection_direct(builder):
    """
    T1(A->B) <-> T2(B->A)
    """
    t1 = SimpleTask("T1", requires=(A,), provides=(B,))
    t2 = SimpleTask("T2", requires=(B,), provides=(A,))

    builder.add(t1)
    builder.add(t2)

    with pytest.raises(RuntimeError, match="Cycle detected"):
        builder.build()


def test_self_reference_cycle(builder):
    """
    Task requires A, provides A.
    """
    t1 = SimpleTask("Ouroboros", requires=(A,), provides=(A,))
    builder.add(t1)

    with pytest.raises(RuntimeError, match="Cycle detected"):
        builder.build()


# --- Sad Paths (Runtime) ---


def test_contract_breach_missing_output(builder):
    """
    Task promises B, but returns empty dict.
    """

    @dataclass(frozen=True)
    class BrokenTask(Task):
        """Simulates a bug: returns an empty dict despite promising outputs."""

        def run(self, inputs: dict[Asset[Any], Any]) -> dict[Asset[Any], Any]:
            return {}

    src = SimpleSource("Src", provides=(A,))
    bad_task = BrokenTask("Bad", requires=(A,), provides=(B,))

    pipeline = builder.add(src).add(bad_task).build()

    with pytest.raises(RuntimeError, match="Bad failed to provide promised output: B"):
        pipeline.run()


def test_context_missing_data():
    """
    Manually test TaskContext behavior when data is missing.
    """
    ctx = TaskContext()
    with pytest.raises(ValueError):
        ctx.get(A)
