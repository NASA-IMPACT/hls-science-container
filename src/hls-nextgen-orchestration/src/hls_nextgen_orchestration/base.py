from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, TypeVar


class TaskFailure(Exception):
    """Exception raised by a task indicating a specific exit code."""

    def __init__(self, message: str, exit_code: int = 1):
        super().__init__(message)
        self.exit_code = exit_code


# Define a generic type variable for Assets
T = TypeVar("T")


@dataclass(frozen=True)
class Asset[T]:
    """
    Represents a unique identifier for a piece of data with a specific type.

    Attributes
    ----------
    key : str
        The unique string identifier for this asset.
    type_class : Type[T]
        The class type used for runtime validation and static typing.
    """

    key: str
    type_class: type[T]

    def __repr__(self) -> str:
        return f"<{self.key} [{self.type_class.__name__}]>"


@dataclass
class TaskContext:
    """
    Mutable container for data during execution.
    """

    exit_code: int = 0
    _store: dict[str, Any] = field(default_factory=dict)

    def put(self, asset: Asset[T], value: T) -> None:
        """
        Store a value for an asset, validating its type at runtime.
        """
        logging.info(f"[Context] Storing {asset.key}")

        # Runtime Type Check
        if not isinstance(value, asset.type_class):
            # Special handling: generic aliases like dict[str, str] or list[int]
            # don't work well with isinstance. We strictly check the origin class.
            # If explicit None is allowed, handle Optional logic here (omitted for strictness).
            raise TypeError(
                f"Asset '{asset.key}' expected type {asset.type_class.__name__}, "
                f"but got {type(value).__name__}: {value}"
            )

        logging.debug(f"          Value: {value}")
        self._store[asset.key] = value

    def get(self, asset: Asset[T]) -> T:
        """
        Retrieve a value for an asset with type hinting.
        """
        if asset.key not in self._store:
            raise ValueError(f"Missing dependency data for: {asset.key}")

        val = self._store[asset.key]
        assert isinstance(val, asset.type_class)
        return val


@dataclass(frozen=True)
class NodeBase(ABC):
    """
    Base class defines the identity (name) and the interface.
    """

    name: str
    requires: tuple[Asset[Any], ...] = ()
    provides: tuple[Asset[Any], ...] = ()

    @abstractmethod
    def execute(self, context: TaskContext) -> None:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"


@dataclass(frozen=True)
class DataSource(NodeBase):
    """
    DataSource generally only uses 'provides'.
    """

    def fetch(self) -> dict[Asset[Any], Any]:
        raise NotImplementedError

    def execute(self, context: TaskContext) -> None:
        logging.info(f"Running DataSource: {self.name}")
        results = self.fetch()

        for asset in self.provides:
            if asset not in results:
                raise RuntimeError(
                    f"{self.name} failed to provide promised asset: {asset.key}"
                )
            context.put(asset, results[asset])


@dataclass(frozen=True)
class Task(NodeBase):
    """
    Task uses both 'requires' and 'provides'.
    """

    def run(self, inputs: dict[Asset[T], Any]) -> dict[Asset[Any], Any]:
        raise NotImplementedError

    def execute(self, context: TaskContext) -> None:
        logging.info(f"Running Task: {self.name}")

        # 1. Gather Inputs
        inputs = {asset: context.get(asset) for asset in self.requires}

        # 2. Run Logic
        outputs = self.run(inputs)

        # 3. Validate & Store Outputs
        for asset in self.provides:
            if asset not in outputs:
                raise RuntimeError(
                    f"{self.name} failed to provide promised output: {asset.key}"
                )
            context.put(asset, outputs[asset])


@dataclass(frozen=True)
class Pipeline:
    """
    An immutable, compiled execution plan.
    """

    execution_order: tuple[NodeBase, ...]

    def run(self) -> TaskContext:
        """
        Executes the pipeline and returns the final context state.
        """
        logging.info("--- Starting Pipeline Execution ---")
        context = TaskContext()

        for i, node in enumerate(self.execution_order, 1):
            logging.info(f"Step {i}/{len(self.execution_order)}: {node.name}")
            try:
                node.execute(context)
            except TaskFailure as e:
                logging.warning(f"Pipeline stopped at step '{node.name}': {e}")
                context.exit_code = e.exit_code
                return context
            except Exception:
                logging.exception(f"Pipeline failed unexpectedly at step '{node.name}'")
                context.exit_code = 1
                raise

        logging.info("--- Execution Complete ---")
        return context

    def __str__(self) -> str:
        """
        Pretty-print the execution plan.
        """
        plan = "\n".join(
            f"{i + 1}. {node}" for i, node in enumerate(self.execution_order)
        )
        return f"Pipeline Execution Plan:\n{plan}"


@dataclass
class PipelineBuilder:
    """
    Mutable builder for constructing a Pipeline.
    """

    nodes: list[NodeBase] = field(default_factory=list)
    catalog: dict[str, NodeBase] = field(default_factory=dict)

    def add(self, node: NodeBase) -> PipelineBuilder:
        self.nodes.append(node)

        for asset in node.provides:
            if asset.key in self.catalog:
                existing = self.catalog[asset.key]
                raise ValueError(
                    f"Conflict: Asset '{asset.key}' is provided by both "
                    f"'{existing.name}' and '{node.name}'"
                )
            self.catalog[asset.key] = node

        return self

    def build(self) -> Pipeline:
        logging.info("Building Pipeline...")

        # 1. Build Dependency Graph
        adjacency: dict[NodeBase, set[NodeBase]] = {n: set() for n in self.nodes}
        in_degree: dict[NodeBase, int] = {n: 0 for n in self.nodes}

        for node in self.nodes:
            for req in node.requires:
                if req.key not in self.catalog:
                    raise ValueError(
                        f"Integrity Error: '{node.name}' requires "
                        f"'{req.key}', but no provider exists."
                    )

                provider = self.catalog[req.key]

                if provider in self.nodes:
                    if node not in adjacency[provider]:
                        adjacency[provider].add(node)
                        in_degree[node] += 1

        # 2. Topological Sort (Kahn's Algorithm)
        queue = [n for n in self.nodes if in_degree[n] == 0]
        sorted_nodes = []

        while queue:
            current = queue.pop(0)
            sorted_nodes.append(current)

            for dependent in adjacency[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # 3. Cycle Detection
        if len(sorted_nodes) != len(self.nodes):
            remaining = set(self.nodes) - set(sorted_nodes)
            names = [n.name for n in remaining]
            raise RuntimeError(f"Cycle detected! Unresolved nodes: {names}")

        return Pipeline(execution_order=tuple(sorted_nodes))
