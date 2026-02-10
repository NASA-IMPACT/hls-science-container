from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar, TypeVar

logger = logging.getLogger(__name__)


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
        logger.info(f"[Context] Storing {asset.key}")

        # Runtime Type Check
        if not isinstance(value, asset.type_class):
            # Special handling: generic aliases like dict[str, str] or list[int]
            # don't work well with isinstance. We strictly check the origin class.
            # If explicit None is allowed, handle Optional logic here (omitted for strictness).
            raise TypeError(
                f"Asset '{asset.key}' expected type {asset.type_class.__name__}, "
                f"but got {type(value).__name__}: {value}"
            )

        logger.debug(f"          Value: {value}")
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


Assets = tuple[Asset[Any], ...]


@dataclass(frozen=True)
class NodeBase(ABC):
    """
    Base class defines the identity (name) and the interface.
    """

    name: str
    requires: ClassVar[Assets] = ()
    provides: ClassVar[Assets] = ()

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
        logger.info(f"Running DataSource: {self.name}")

        cwd = os.getcwd()
        try:
            results = self.fetch()
        finally:
            os.chdir(cwd)

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

    def run(self, inputs: dict[Asset[Any], Any]) -> dict[Asset[Any], Any]:
        raise NotImplementedError

    def execute(self, context: TaskContext) -> None:
        logger.info(f"Running Task: {self.name}")

        # 1. Gather Inputs
        inputs = {asset: context.get(asset) for asset in self.requires}

        # 2. Run Logic
        cwd = os.getcwd()
        try:
            outputs = self.run(inputs)
        finally:
            os.chdir(cwd)

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
        logger.info("--- Starting Pipeline Execution ---")
        context = TaskContext()

        for i, node in enumerate(self.execution_order, 1):
            logger.info(f"Step {i}/{len(self.execution_order)}: {node.name}")
            try:
                node.execute(context)
            except TaskFailure as e:
                logger.warning(f"Pipeline stopped at step '{node.name}': {e}")
                context.exit_code = e.exit_code
                return context
            except Exception:
                logger.exception(f"Pipeline failed unexpectedly at step '{node.name}'")
                context.exit_code = 1
                raise

        logger.info("--- Execution Complete ---")
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
    _adjacency: dict[NodeBase, set[NodeBase]] = field(default_factory=dict)
    _in_degree: dict[NodeBase, int] = field(default_factory=dict)

    def add(self, node: NodeBase) -> PipelineBuilder:
        # Initialize graph tracking for this node
        if node not in self._adjacency:
            self._adjacency[node] = set()
            self._in_degree[node] = 0

        # 1. Resolve Dependencies (Eager Resolution)
        # This enforces that providers must be added before consumers.
        for req in node.requires:
            if req.key not in self.catalog:
                raise ValueError(
                    f"Integrity Error: '{node.name}' requires "
                    f"'{req.key}', but no provider exists yet."
                )

            provider = self.catalog[req.key]

            # Record dependency: provider -> node
            if node not in self._adjacency[provider]:
                self._adjacency[provider].add(node)
                self._in_degree[node] += 1

        # 2. Register Provided Assets
        self.nodes.append(node)

        for asset in node.provides:
            # We allow overwriting previous providers to support
            # "update" patterns (e.g., OldConfig -> Task -> NewConfig).
            # The dependency resolution above locks in the version used by this node.
            self.catalog[asset.key] = node

        return self

    def build(self) -> Pipeline:
        logger.info("Building Pipeline...")

        # Topological Sort (Kahn's Algorithm)
        # We work on a copy of in-degrees to avoid mutating the builder state permanently
        in_degree = self._in_degree.copy()

        queue = [n for n in self.nodes if in_degree[n] == 0]
        sorted_nodes = []

        while queue:
            current = queue.pop(0)
            sorted_nodes.append(current)

            for dependent in self._adjacency[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # Cycle Detection
        if len(sorted_nodes) != len(self.nodes):
            remaining = set(self.nodes) - set(sorted_nodes)
            names = [n.name for n in remaining]
            raise RuntimeError(f"Cycle detected! Unresolved nodes: {names}")

        return Pipeline(execution_order=tuple(sorted_nodes))
