from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from hls_nextgen_orchestration.base import (
    Asset,
    DataSource,
    MappedTask,
    MergeTask,
    NodeBase,
    TaskContext,
    TaskFailure,
)
from hls_nextgen_orchestration.metrics import MetricsCollector

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Pipeline:
    """
    An immutable, compiled execution plan.
    """

    execution_order: tuple[NodeBase, ...]
    metrics: MetricsCollector

    def run(self) -> TaskContext:
        """
        Executes the pipeline and returns the final context state.
        """
        logger.info("--- Starting Pipeline Execution ---")
        context = TaskContext()

        for i, node in enumerate(self.execution_order, 1):
            logger.info(f"Step {i}/{len(self.execution_order)}: {node.name}")
            try:
                with self.metrics.collect(node):
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


def _node_css_class(node: NodeBase) -> str:
    """Return the Mermaid CSS class name for a node based on its type."""
    if isinstance(node, MergeTask):
        return "merge"
    elif isinstance(node, MappedTask):
        return "mapped"
    elif isinstance(node, DataSource):
        return "datasource"
    else:
        return "task"


@dataclass
class PipelineBuilder:
    """
    Mutable builder for constructing a Pipeline.
    """

    nodes: list[NodeBase] = field(default_factory=list)
    catalog: dict[str, NodeBase] = field(default_factory=dict)
    _adjacency: dict[NodeBase, set[NodeBase]] = field(default_factory=dict)
    _in_degree: dict[NodeBase, int] = field(default_factory=dict)
    _edge_assets: dict[tuple[NodeBase, NodeBase], list[Asset[Any]]] = field(
        default_factory=dict
    )

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

            # Track which assets flow along each edge for visualization
            edge_key = (provider, node)
            self._edge_assets.setdefault(edge_key, []).append(req)

        # 2. Register Provided Assets
        self.nodes.append(node)

        for asset in node.provides:
            # We allow overwriting previous providers to support
            # "update" patterns (e.g., OldConfig -> Task -> NewConfig).
            # The dependency resolution above locks in the version used by this node.
            self.catalog[asset.key] = node

        return self

    def _topological_sort(self) -> list[NodeBase]:
        """Topological sort (Kahn's Algorithm) over the current node graph."""
        # We work on a copy of in-degrees to avoid mutating the builder state permanently
        in_degree = self._in_degree.copy()

        # Use a stack (LIFO) instead of a queue (FIFO) to encourage Depth-First execution.
        # This prevents interleaved execution of parallel branches.
        # We reverse the initial list so that the first added node ends up at the
        # top of the stack (Last In, First Out -> First In needs to be last pushed).
        stack = [n for n in reversed(self.nodes) if in_degree[n] == 0]
        sorted_nodes = []

        while stack:
            current = stack.pop()
            sorted_nodes.append(current)

            # Sort dependents to ensure deterministic execution order.
            # We sort in descending order (reverse=True) because we are pushing to a stack;
            # the last item pushed is the first one processed (LIFO).
            # Example: [A, B] -> push B, push A -> pop A, pop B.
            dependents = sorted(
                self._adjacency[current], key=lambda n: n.name, reverse=True
            )

            for dependent in dependents:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    stack.append(dependent)

        # Cycle Detection
        if len(sorted_nodes) != len(self.nodes):
            remaining = set(self.nodes) - set(sorted_nodes)
            names = [n.name for n in remaining]
            raise RuntimeError(f"Cycle detected! Unresolved nodes: {names}")

        return sorted_nodes

    def build(self, metrics: MetricsCollector | None = None) -> Pipeline:
        """Build the pipeline into a DAG"""
        logger.info("Building Pipeline...")
        return Pipeline(
            execution_order=tuple(self._topological_sort()),
            metrics=metrics or MetricsCollector(),
        )

    def visualize(self) -> str:
        """Render the pipeline as a Mermaid flowchart diagram string."""
        sorted_nodes = self._topological_sort()
        node_id = {node: f"node_{i}" for i, node in enumerate(sorted_nodes)}

        # Detect MappedTask instances created via .map() — they have 'granule_id'
        # set directly in the dynamically created class __dict__.
        # Group them by their user-defined base class for subgraph rendering.
        mapped_groups: dict[type, list[NodeBase]] = {}
        for node in sorted_nodes:
            if isinstance(node, MappedTask) and "granule_id" in type(node).__dict__:
                base_cls = type(node).__bases__[0]
                mapped_groups.setdefault(base_cls, []).append(node)
        grouped_nodes = {node for nodes in mapped_groups.values() for node in nodes}

        lines = ["flowchart TD"]

        # Emit all nodes flat — mapped nodes get their granule_id as the label.
        # Skipping subgraph wrappers intentionally: Mermaid's TD layout stacks nodes
        # inside a subgraph vertically, which defeats the goal of showing parallel
        # branches side-by-side. The shared edges + `mapped` color class are sufficient
        # to visually communicate the grouping.
        for node in sorted_nodes:
            nid = node_id[node]
            css = _node_css_class(node)
            if node in grouped_nodes:
                granule_id = type(node).__dict__["granule_id"]
                base_name = type(node).__bases__[0].__name__
                lines.append(f'    {nid}["{base_name}<br>{granule_id}"]:::{css}')
            else:
                lines.append(f'    {nid}["{node.name}"]:::{css}')

        lines.append("")

        # Emit edges with asset labels
        for (provider, consumer), assets in self._edge_assets.items():
            pid = node_id[provider]
            cid = node_id[consumer]
            label = ", ".join(a.key for a in assets)
            lines.append(f'    {pid} -->|"{label}"| {cid}')

        lines.append("")

        # Node type color coding
        lines.append("    classDef datasource fill:#4CAF50,stroke:#388E3C,color:#fff")
        lines.append("    classDef task fill:#2196F3,stroke:#1565C0,color:#fff")
        lines.append("    classDef mapped fill:#9C27B0,stroke:#6A1B9A,color:#fff")
        lines.append("    classDef merge fill:#FF9800,stroke:#E65100,color:#fff")

        return "\n".join(lines)
