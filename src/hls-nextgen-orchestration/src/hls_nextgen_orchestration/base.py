from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Asset:
    """
    Represents a unique identifier for a piece of data.
    """

    key: str
    description: str = ""

    def __repr__(self):
        return f"<{self.key}>"


@dataclass
class TaskContext:
    """
    Mutable container for data during execution.
    """

    _store: dict[str, Any] = field(default_factory=dict)

    def put(self, asset: Asset, value: Any):
        logging.info(f"[Context] Storing {asset.key}")
        logging.debug(f"          Value: {value}")
        self._store[asset.key] = value

    def get(self, asset: Asset) -> Any:
        if asset.key not in self._store:
            raise ValueError(f"Missing dependency data for: {asset.key}")
        return self._store[asset.key]


@dataclass(frozen=True)
class NodeBase:
    """
    Base class defines the identity (name) and the interface.
    """

    name: str
    requires: tuple[Asset, ...] = ()
    provides: tuple[Asset, ...] = ()

    def execute(self, context: TaskContext):
        raise NotImplementedError

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"


@dataclass(frozen=True)
class DataSource(NodeBase):
    """
    DataSource generally only uses 'provides'.
    """

    def fetch(self) -> dict[Asset, Any]:
        raise NotImplementedError

    def execute(self, context: TaskContext):
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

    def run(self, inputs: dict[Asset, Any]) -> dict[Asset, Any]:
        raise NotImplementedError

    def execute(self, context: TaskContext):
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


# --- Pipeline Construction & Execution ---


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
            node.execute(context)

        logging.info("--- Execution Complete ---")
        return context

    def __str__(self):
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
                    f"Conflict: Asset '{asset.key}' is provided by both '{existing.name}' and '{node.name}'"
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
                        f"Integrity Error: '{node.name}' requires '{req.key}', but no provider exists."
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
