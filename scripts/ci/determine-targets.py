#!/usr/bin/env python3
import os
import subprocess
import sys
import tomllib
import yaml
from collections import defaultdict, deque
from pathlib import Path


def run_command(command: str) -> str:
    """
    Run a shell command and return its output.

    Parameters
    ----------
    command : str
        The shell command to execute.

    Returns
    -------
    str
        The stripped standard output of the command. Returns an empty string
        if the command fails (e.g., non-zero exit code).
    """
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        # If git fails (e.g. no history), return empty
        return ""


def get_changed_files(base_sha: str, head_sha: str = "HEAD") -> set[str]:
    """
    Get a set of files changed between two git references.

    Parameters
    ----------
    base_sha : str
        The base commit SHA or reference to compare against.
    head_sha : str, optional
        The head commit SHA or reference (default is "HEAD").

    Returns
    -------
    set[str]
        A set of file paths that differ between the two references.
    """
    print(f"🔍 Diffing {base_sha}..{head_sha}", file=sys.stderr)
    cmd = f"git diff --name-only {base_sha} {head_sha}"
    output = run_command(cmd)
    return set(output.splitlines()) if output else set()


def get_dependency_graph(
    packages_dir: Path
) -> tuple[dict[str, set[str]], dict[str, str]]:
    """
    Parse recipe.yml files to build a dependency graph

    Parameters
    ----------
    packages_dir : Path
        Path object pointing to the directory containing package subdirectories.

    Returns
    -------
    tuple
        - graph : dict[str, set[str]] (pkg -> dependencies)
        - dir_map : dict[str, str] (pkg_name -> directory_name)
    """
    graph = {}
    dir_map = {}

    if not packages_dir.exists():
        return graph, dir_map

    for d in packages_dir.iterdir():
        if not d.is_dir():
            continue

        recipe_path = d / "recipe.yml"
        if recipe_path.exists():
            try:
                with recipe_path.open("r") as f:
                    data = yaml.safe_load(f)

                pkg_name = data.get("package", {}).get("name")

                dir_map[pkg_name] = d.name

                # Extract dependencies
                deps = set()
                requirements = data.get("requirements", {})
                for section in ["host", "build", "run"]:
                    req_list = requirements.get(section, []) or []
                    if req_list:
                        for req in req_list:
                            # Clean string (e.g., "package_a >=1.0" -> "package_a")
                            clean_req = (
                                req.split(" ")[0]
                                .split("=")[0]
                                .split(">")[0]
                                .split("<")[0]
                            )
                            deps.add(clean_req)
                graph[pkg_name] = deps
            except Exception as e:
                print(f"⚠️ Error parsing {d.name}: {e}", file=sys.stderr)

    return graph, dir_map


def get_reverse_graph(graph: dict[str, set[str]]) -> dict[str, list[str]]:
    """
    Build a reverse dependency graph (adjacency list).

    Parameters
    ----------
    graph : dict[str, set[str]]
        The forward dependency graph (Package -> Dependencies).

    Returns
    -------
    dict[str, list[str]]
        Dependency -> [Consumers]
    """
    rev_adj = defaultdict(list)
    for u, deps in graph.items():
        for v in deps:
            if v in graph:  # Only track internal dependencies
                rev_adj[v].append(u)
    return rev_adj


def get_transitive_impact(
    changed_pkgs: set[str], rev_graph: dict[str, list[str]]
) -> set[str]:
    """
    Calculate the full set of impacted packages using transitive dependencies (BFS).

    Parameters
    ----------
    changed_pkgs : set[str]
        The set of package names explicitly changed.
    rev_graph : dict[str, list[str]]
        The reverse dependency graph.

    Returns
    -------
    set[str]
        Changed packages + transitive downstream dependencies.
    """
    impacted = set(changed_pkgs)
    queue = deque(changed_pkgs)

    while queue:
        pkg = queue.popleft()
        for dependent in rev_graph.get(pkg, []):
            if dependent not in impacted:
                impacted.add(dependent)
                queue.append(dependent)

    return impacted


def topological_sort(graph: dict[str, set[str]]) -> list[str]:
    """
    Sort the dependency graph using Kahn's Algorithm.

    Parameters
    ----------
    graph : dict[str, set[str]]
        The dependency graph.

    Returns
    -------
    list[str]
        Package names sorted in build order.
    """
    # 1. Build Adjacency List (Dependency -> Dependents)
    adj = defaultdict(list)
    in_degree = {u: 0 for u in graph}

    for u, deps in graph.items():
        for v in deps:
            if v in graph:
                adj[v].append(u)
                in_degree[u] += 1

    # 2. Queue for 0 in-degree
    queue = deque([u for u in graph if in_degree[u] == 0])
    sorted_list = []

    while queue:
        u = queue.popleft()
        sorted_list.append(u)

        for v in adj[u]:
            in_degree[v] -= 1
            if in_degree[v] == 0:
                queue.append(v)

    if len(sorted_list) != len(graph):
        print(
            "⚠️ Cyclic dependency detected or incomplete graph. Returning best effort.",
            file=sys.stderr,
        )
        remaining = set(graph.keys()) - set(sorted_list)
        sorted_list.extend(list(remaining))

    return sorted_list


def main():
    # --- 1. Determine Git Base ---
    event_name = os.getenv("GITHUB_EVENT_NAME")

    if event_name == "pull_request":
        base_ref = os.getenv("GITHUB_BASE_REF", "main")
        base_sha = f"origin/{base_ref}"
    else:
        # Push event or local run
        base_sha = os.getenv("GITHUB_EVENT_BEFORE", "main")
        if base_sha == "0000000000000000000000000000000000000000":
            base_sha = "HEAD^"

    # Allow manual override via args
    if len(sys.argv) > 1:
        manual_input = sys.argv[1:]
        print(f"📝 Manual input detected: {manual_input}", file=sys.stderr)
        changed_files = set()
    else:
        changed_files = get_changed_files(base_sha)

    # --- 2. Load Configuration ---
    pixi_path = Path("pixi.toml")
    packages_dir = Path("packages")

    # Build Graph (filtered by allowed_pkgs)
    graph, dir_map = get_dependency_graph(packages_dir)

    explicitly_changed_pkgs = set()
    docker_changed = False

    # Check Docker config
    for f in changed_files:
        if f.startswith("Dockerfile") or f in ["pixi.toml", "pixi.lock"]:
            docker_changed = True

    # Map changes to Package Names
    if len(sys.argv) > 1:
        # Manual Mode
        inputs = sys.argv[1:]
        for i in inputs:
            found = False
            for name, d in dir_map.items():
                if d == i or name == i:
                    explicitly_changed_pkgs.add(name)
                    found = True
            if not found:
                # Check if it was ignored due to pixi.toml
                print(
                    f"⚠️ Warning: Package '{i}' not found in {packages_dir} or not listed in pixi.toml",
                    file=sys.stderr,
                )
    else:
        # Auto Mode
        for f in changed_files:
            # We compare the string representation of the path from git
            if f.startswith(f"{packages_dir}/"):
                parts = f.split("/")
                if len(parts) >= 2:
                    dir_name = parts[1]
                    for name, d in dir_map.items():
                        if d == dir_name:
                            explicitly_changed_pkgs.add(name)

    # --- 3. Calculate Transitive Impacts ---
    rev_graph = get_reverse_graph(graph)
    final_target_pkgs = get_transitive_impact(explicitly_changed_pkgs, rev_graph)

    if len(final_target_pkgs) > len(explicitly_changed_pkgs):
        added = final_target_pkgs - explicitly_changed_pkgs
        print(f"🔗 Added downstream dependencies: {', '.join(added)}", file=sys.stderr)

    # --- 4. Sort and Filter ---
    full_order = topological_sort(graph)

    # Filter: Keep only affected packages, but maintain sorted order
    final_build_order_names = [pkg for pkg in full_order if pkg in final_target_pkgs]

    # Convert names back to directories
    final_build_order_dirs = [dir_map[name] for name in final_build_order_names]

    # --- 5. Output ---
    output_string = " ".join(final_build_order_dirs)
    has_changes = "true" if final_build_order_dirs else "false"
    docker_changed_str = "true" if docker_changed or has_changes else "false"

    print(f"✅ Build Order: {output_string}", file=sys.stderr)

    # Stdout for shell capture
    print(output_string)

    # GitHub Actions Output
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with Path(github_output).open("a") as f:
            f.write(f"build_order={output_string}\n")
            f.write(f"has_changes={has_changes}\n")
            f.write(f"docker_changed={docker_changed_str}\n")


if __name__ == "__main__":
    main()
