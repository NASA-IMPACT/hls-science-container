from __future__ import annotations

from dataclasses import dataclass, make_dataclass
from typing import Any

import pytest

from hls_nextgen_orchestration.base import (
    Asset,
    Assets,
    DataSource,
    Task,
)
from hls_nextgen_orchestration.pipeline import PipelineBuilder

# ----- Test setup (example assets / sources / tasks) and fixtures
A = Asset("A", str)
B = Asset("B", str)
C = Asset("C", str)
D = Asset("D", str)


def simple_source(provides: Assets) -> type[DataSource]:
    return make_dataclass(
        "Source",
        [("name", str)],
        bases=(DataSource,),
        namespace={
            "requires": (),
            "provides": provides,
            "fetch": lambda self: {
                asset: f"val_{asset.key}" for asset in self.provides
            },
        },
        frozen=True,
    )


def simple_task(requires: Assets, provides: Assets) -> type[Task]:
    def run(self: Task, inputs: dict[Asset[Any], Any]) -> dict[Asset[Any], Any]:
        # Deterministic concatenation of input values
        input_str = "_".join(sorted(str(v) for v in inputs.values()))
        return {asset: f"val_{asset.key}_from_{input_str}" for asset in self.provides}

    return make_dataclass(
        "Task",
        [("name", "str")],
        bases=(Task,),
        namespace={
            "requires": requires,
            "provides": provides,
            "run": run,
        },
        frozen=True,
    )


@pytest.fixture
def builder() -> PipelineBuilder:
    return PipelineBuilder()


# ----- tests
def test_linear_pipeline(builder: PipelineBuilder) -> None:
    """
    Graph: Source(A) -> Task(A -> B)
    """
    source = simple_source(provides=(A,))("Src")
    task = simple_task(requires=(A,), provides=(B,))("T1")

    pipeline = builder.add(source).add(task).build()

    # Verify Execution Order
    assert len(pipeline.execution_order) == 2
    assert pipeline.execution_order[0] == source
    assert pipeline.execution_order[1] == task

    # Verify Data Flow
    context = pipeline.run()
    assert context.get(A) == "val_A"
    assert context.get(B) == "val_B_from_val_A"


def test_diamond_dependency(builder: PipelineBuilder) -> None:
    """
    Graph:
          /-> Task1(A->B) -\
    Source(A)               -> Task3(B,C -> D)
          \\-> Task2(A->C) -/
    """
    source = simple_source(provides=(A,))("Src")
    task1 = simple_task(requires=(A,), provides=(B,))("T1")
    task2 = simple_task(requires=(A,), provides=(C,))("T2")
    task3 = simple_task(requires=(B, C), provides=(D,))("T3")

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


def test_independent_branches(builder: PipelineBuilder) -> None:
    """
    Graph:
    Src1(A) -> T1(A->B)
    Src2(C) -> T2(C->D)
    """
    src1 = simple_source(provides=(A,))("S1")
    task1 = simple_task(requires=(A,), provides=(B,))("T1")

    src2 = simple_source(provides=(C,))("S2")
    task2 = simple_task(requires=(C,), provides=(D,))("T2")

    pipeline = builder.add(src1).add(task1).add(src2).add(task2).build()

    assert len(pipeline.execution_order) == 4
    context = pipeline.run()
    assert context.get(B) == "val_B_from_val_A"
    assert context.get(D) == "val_D_from_val_C"


# --- Sad Paths (Construction Time) ---


def test_missing_dependency(builder: PipelineBuilder) -> None:
    """
    Task requires A, but nothing provides A.
    Verification happens at .add() time now.
    """
    task = simple_task(requires=(A,), provides=(B,))("Orphan")

    with pytest.raises(ValueError, match="Integrity Error.*requires 'A'.*no provider"):
        builder.add(task)


def test_asset_shadowing(builder: PipelineBuilder) -> None:
    """
    Test that a new task can overwrite (shadow) an existing asset.
    Source(A="val_A") -> UpdateTask(A="val_A_updated") -> Consumer(A->B)
    """
    src = simple_source(provides=(A,))("Src")

    # Task that requires A and provides a NEW version of A
    @dataclass(frozen=True)
    class UpdateTask(Task):
        requires = (A,)
        provides = (A,)

        def run(self, inputs: dict[Asset[Any], Any]) -> dict[Asset[Any], Any]:
            return {A: inputs[A] + "_updated"}

    updater = UpdateTask("Updater")

    # Consumer should see the updated version
    consumer = simple_task(requires=(A,), provides=(B,))("Consumer")

    # Add order matters: Src -> Updater -> Consumer
    pipeline = builder.add(src).add(updater).add(consumer).build()

    context = pipeline.run()

    # Check that context holds the LATEST value for A
    assert context.get(A) == "val_A_updated"
    # Check that consumer used the LATEST value
    assert context.get(B) == "val_B_from_val_A_updated"


def test_out_of_order_addition(builder: PipelineBuilder) -> None:
    """
    Test that adding a consumer before a provider fails.
    """
    simple_source(provides=(A,))("Src")
    task = simple_task(requires=(A,), provides=(B,))("T1")

    # Adding task first should fail
    with pytest.raises(ValueError, match="Integrity Error.*requires 'A'"):
        builder.add(task)


# --- Sad Paths (Runtime) ---


def test_contract_breach_missing_output(builder: PipelineBuilder) -> None:
    """
    Task promises B, but returns empty dict.
    """

    @dataclass(frozen=True)
    class BrokenTask(Task):
        """Simulates a bug: returns an empty dict despite promising outputs."""

        requires = (A,)
        provides = (B,)

        def run(self, inputs: dict[Asset[Any], Any]) -> dict[Asset[Any], Any]:
            return {}

    src = simple_source(provides=(A,))("Src")
    bad_task = BrokenTask("Bad")

    pipeline = builder.add(src).add(bad_task).build()

    with pytest.raises(RuntimeError, match="Bad failed to provide promised output: B"):
        pipeline.run()


def test_type_mismatch_error(builder: PipelineBuilder) -> None:
    """
    Task promises B (str), but returns an int.
    """

    @dataclass(frozen=True)
    class WrongTypeTask(Task):
        requires = (A,)
        provides = (B,)

        def run(self, inputs: dict[Asset[Any], Any]) -> dict[Asset[Any], Any]:
            return {B: 123}  # Int instead of str

    src = simple_source(provides=(A,))("Src")
    bad_task = WrongTypeTask("BadType")

    pipeline = builder.add(src).add(bad_task).build()

    # The pipeline catches exceptions and logs them, but ultimately re-raises unexpected exceptions.
    # The TypeError generated by TaskContext.put() is considered unexpected here.
    with pytest.raises(TypeError, match="Asset 'B' expected type str, but got int"):
        pipeline.run()
