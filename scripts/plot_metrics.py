#!/usr/bin/env python
"""
HLS metrics CLI — fetch and plot CloudWatch EMF metrics from Batch jobs.

Commands:
    fetch             Query Logs Insights and save to Parquet
    plot scatter      Paired scatter per granule comparing two values of a dimension
    plot timeseries   Stacked total metric over time by task_name
"""

import datetime
import time
from typing import TYPE_CHECKING

import boto3
import click
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from mypy_boto3_logs import CloudWatchLogsClient

# A single CloudWatch Logs Insights result row: field name -> stringified value.
Record = dict[str, str]

METRICS = ["runtime_seconds", "peak_memory_mb", "avg_cpu_percent"]
METRIC_LABELS = {
    "runtime_seconds": "Runtime (s)",
    "peak_memory_mb": "Peak memory (MB)",
    "avg_cpu_percent": "Average CPU (%)",
}


def _build_query(dimension: str | None = None) -> str:
    dim_field = f", {dimension}" if dimension else ""
    dim_filter = f"| filter ispresent({dimension})\n" if dimension else ""
    return f"""
        fields @timestamp, task_name, input_granule_id{dim_field}, workflow,
               runtime_seconds, peak_memory_mb, avg_cpu_percent
        {dim_filter}| filter ispresent(task_name)
        | limit 10000
    """


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


def _run_query(
    client: CloudWatchLogsClient,
    log_group: str,
    start: datetime.datetime,
    end: datetime.datetime,
    poll_interval: float,
    query: str,
) -> list[Record]:
    response = client.start_query(
        logGroupName=log_group,
        startTime=int(start.timestamp()),
        endTime=int(end.timestamp()),
        queryString=query,
    )
    query_id = response["queryId"]

    while True:
        result = client.get_query_results(queryId=query_id)
        status = result["status"]
        if status == "Complete":
            break
        if status in ("Failed", "Cancelled"):
            raise RuntimeError(f"Logs Insights query {status}: {result}")
        time.sleep(poll_interval)

    records: list[Record] = []
    for row in result["results"]:
        record: Record = {}
        for r in row:
            if r["field"] == "@timestamp":
                record["timestamp"] = r["value"]
            elif not r["field"].startswith("@"):
                record[r["field"]] = r["value"]
        records.append(record)

    return records


def _query_all_chunks(
    client: CloudWatchLogsClient,
    log_group: str,
    start_time: datetime.datetime,
    chunk_hours: int,
    poll_interval: float,
    query: str,
) -> list[Record]:
    end_time = datetime.datetime.now(datetime.UTC)
    hours_back = (end_time - start_time).total_seconds() / 3600
    chunk = datetime.timedelta(hours=chunk_hours)
    total_chunks = -(-int(hours_back) // chunk_hours)

    all_records: list[Record] = []
    chunk_start = start_time
    chunk_num = 0

    while chunk_start < end_time:
        chunk_end = min(chunk_start + chunk, end_time)
        chunk_num += 1
        click.echo(
            f"  chunk {chunk_num}/{total_chunks}: "
            f"{chunk_start:%Y/%m/%d:%H:%M} – {chunk_end:%H:%M} UTC",
            nl=False,
        )
        records = _run_query(
            client, log_group, chunk_start, chunk_end, poll_interval, query
        )
        click.echo(f" ({len(records)} records)")
        all_records.extend([rec for rec in records if "max_cpu_percent" not in rec])
        chunk_start = chunk_end

    return all_records


def _build_dataframe(
    records: list[Record], task_groups: set[str] | None = None
) -> pd.DataFrame:
    df = pd.DataFrame(records)
    if df.empty:
        return df

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    for col in METRICS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if task_groups:
        return df[df["task_name"].isin(task_groups)]
    return df


# ---------------------------------------------------------------------------
# Plot implementations
# ---------------------------------------------------------------------------


def _plot_scatter(
    df: pd.DataFrame,
    versions: tuple[str, str],
    dimension: str,
) -> plt.Figure:
    task_groups = sorted(df["task_name"].unique())
    n_rows, n_cols = len(METRICS), len(task_groups)
    v1, v2 = versions

    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=(6 * n_cols, 4.5 * n_rows),
        squeeze=False,
        constrained_layout=True,
    )
    fig.suptitle(f"{dimension}={v1} vs {v2}", fontsize=13)

    for row_idx, metric in enumerate(METRICS):
        for col_idx, task_group in enumerate(task_groups):
            ax = axes[row_idx][col_idx]

            subset = (
                df[df["task_name"] == task_group][
                    ["input_granule_id", dimension, "workflow", "timestamp", metric]
                ]
                .dropna(subset=[metric])
                .sort_values("timestamp")
                .drop_duplicates(subset=["input_granule_id", dimension], keep="last")
            )

            pivot = subset.pivot_table(
                index="input_granule_id",
                columns=dimension,
                values=metric,
                aggfunc="first",
            )

            if v1 not in pivot.columns or v2 not in pivot.columns:
                ax.text(
                    0.5,
                    0.5,
                    f"No paired data\n(need {dimension}={v1} and ={v2})",
                    ha="center",
                    va="center",
                    transform=ax.transAxes,
                    fontsize=9,
                    color="grey",
                )
                ax.set_title(f"{task_group} — {METRIC_LABELS[metric]}")
                continue

            workflow_map = subset.groupby("input_granule_id")["workflow"].first()
            paired = pivot[[v1, v2]].dropna()
            paired["workflow"] = workflow_map
            n = len(paired)

            lo = min(paired[v1].min(), paired[v2].min())
            hi = max(paired[v1].max(), paired[v2].max())
            pad = (hi - lo) * 0.05
            ref = [lo - pad, hi + pad]
            ax.plot(
                ref, ref, color="black", linewidth=0.9, linestyle="--", label="y = x"
            )

            cycle = plt.rcParams["axes.prop_cycle"].by_key()["color"]
            workflow_colors = {
                wf: cycle[i % len(cycle)]
                for i, wf in enumerate(sorted(paired["workflow"].unique()))
            }

            for workflow, wf_data in paired.groupby("workflow"):
                color = workflow_colors[workflow]
                ax.scatter(
                    wf_data[v1], wf_data[v2], alpha=0.55, s=28, zorder=3, color=color
                )

                m, b = np.polyfit(wf_data[v1], wf_data[v2], 1)
                r2 = np.corrcoef(wf_data[v1], wf_data[v2])[0, 1] ** 2
                mean_v1 = wf_data[v1].mean()
                mean_v2 = wf_data[v2].mean()
                pct = (mean_v2 / mean_v1 - 1) * 100
                sign = "+" if pct >= 0 else ""
                ax.plot(
                    np.array(ref),
                    m * np.array(ref) + b,
                    color=color,
                    linewidth=1.2,
                    label=f"{workflow}  μ: {mean_v1:.1f}→{mean_v2:.1f}  {sign}{pct:.1f}%  R²={r2:.2f}",
                )

            ax.set_xlim(ref)
            ax.set_ylim(ref)
            ax.set_aspect("equal", adjustable="box")
            ax.set_xlabel(f"{dimension}={v1}", fontsize=9)
            ax.set_ylabel(f"{dimension}={v2}", fontsize=9)
            ax.set_title(f"{task_group} — {METRIC_LABELS[metric]}", fontsize=9)
            ax.legend(fontsize=8, loc="upper left")
            ax.text(
                0.97,
                0.03,
                f"n = {n}",
                transform=ax.transAxes,
                ha="right",
                va="bottom",
                fontsize=8,
                color="grey",
            )

    return fig


def _plot_timeseries(df: pd.DataFrame, metric: str, freq: str) -> plt.Figure:
    ts = (
        df.groupby(["task_name", pd.Grouper(key="timestamp", freq=freq)])[metric]
        .sum()
        .unstack("task_name")
        .sort_index(axis="columns")
        .fillna(0)
    )

    ts_pct = ts.div(ts.sum(axis=1), axis=0).mul(100).fillna(0)

    fig, (ax_abs, ax_pct) = plt.subplots(
        2,
        1,
        figsize=(12, 7),
        sharex=True,
        constrained_layout=True,
    )
    fig.suptitle(
        f"Total {METRIC_LABELS[metric]} by task  ({freq} buckets)",
        fontsize=13,
    )

    # Bar width: 80% of the bucket duration in matplotlib's day units
    bar_width = (
        pd.Timedelta(pd.tseries.frequencies.to_offset(freq)).total_seconds()
        / 86400
        * 0.8
    )
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    x = ts.index.to_pydatetime()

    for ax, data in [(ax_abs, ts), (ax_pct, ts_pct)]:
        bottom = np.zeros(len(data))
        for i, col in enumerate(data.columns):
            ax.bar(
                x,
                data[col],
                bottom=bottom,
                width=bar_width,
                label=col if ax is ax_abs else None,
                color=colors[i % len(colors)],
                alpha=0.85,
            )
            bottom += data[col].to_numpy()

    ax_abs.set_ylabel(METRIC_LABELS[metric])
    ax_abs.legend(title="task_name", fontsize=8, loc="upper right")

    ax_pct.set_ylabel("% of total")
    ax_pct.set_ylim(0, 100)

    ax_pct.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax_pct.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))

    return fig


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group()
def cli() -> None:
    """HLS metrics: fetch and plot Batch job metrics from CloudWatch."""


@cli.command()
@click.argument("output", default="metrics.parquet")
@click.option(
    "--log-group",
    default="/hls-orchestration/hls-mcp-development-viirs/log-metrics",
    show_default=True,
)
@click.option(
    "--since",
    default=None,
    metavar="DATETIME",
    help="Start datetime in ISO format (e.g. '2026-05-01' or '2026-05-01T12:00'). Defaults to 48 h ago.",
)
@click.option(
    "--chunk-hours", default=4, show_default=True, help="Time window per query"
)
@click.option("--region", default=None, help="AWS region")
@click.option(
    "--tasks",
    default=None,
    help="Comma-separated task_name values to keep (default: keep all tasks)",
)
@click.option(
    "--dimension",
    default=None,
    help="Optional experiment dimension to filter on and include (e.g. lasrc_version)",
)
def fetch(
    output: str,
    log_group: str,
    since: str | None,
    chunk_hours: int,
    region: str | None,
    tasks: str | None,
    dimension: str | None,
) -> None:
    """Query Logs Insights and save to OUTPUT (default: metrics.parquet)."""
    if since is None:
        start_time = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=48)
    else:
        try:
            start_time = datetime.datetime.fromisoformat(since)
        except ValueError:
            raise click.BadParameter(
                f"cannot parse {since!r} — use ISO format, e.g. '2026-05-01' or '2026-05-01T12:00'",
                param_hint="'--since'",
            )
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=datetime.UTC)

    task_groups = set(tasks.split(",")) if tasks else None

    client = boto3.client("logs", region_name=region)

    click.echo(
        f"Querying {log_group!r} — from {start_time:%Y-%m-%d %H:%M UTC} in {chunk_hours}h chunks ..."
    )
    records = _query_all_chunks(
        client,
        log_group,
        start_time,
        chunk_hours,
        poll_interval=2.0,
        query=_build_query(dimension),
    )
    click.echo(f"  {len(records)} records total")

    df = _build_dataframe(records, task_groups)
    click.echo(f"  {len(df)} records after filtering to {task_groups or 'all tasks'}")

    if df.empty:
        raise click.ClickException("No data to save.")

    if dimension and dimension in df.columns:
        breakdown = df.groupby(["task_name", dimension]).size()
        for (task, dim_val), count in breakdown.items():
            click.echo(f"  {task:10s}  {dimension}={dim_val}  {count}")
    else:
        for task, count in df.groupby("task_name").size().items():
            click.echo(f"  {task:10s}  {count}")

    df.to_parquet(output, index=False)
    click.echo(f"Saved → {output}")


@cli.group()
def plot() -> None:
    """Visualization subcommands (load from a Parquet file produced by fetch)."""


@plot.command()
@click.argument("inputs", nargs=-1, default="metrics.parquet")
@click.option(
    "--dimension",
    required=True,
    help="Column to compare across values",
)
@click.option("--x", "x_val", required=True, help="Dimension value for x-axis")
@click.option("--y", "y_val", required=True, help="Dimension value for y-axis")
@click.option(
    "--tasks",
    multiple=True,
    default=None,
    help="Task(s) to plot (repeat for multiple, e.g. --tasks Fmask --tasks LaSRC). Defaults to all tasks in the data.",
)
@click.option("--output", default=None, help="Save figure to file instead of showing")
def scatter(
    inputs: tuple[str, ...],
    dimension: str,
    x_val: str,
    y_val: str,
    tasks: tuple[str, ...],
    output: str | None,
) -> None:
    """Paired scatter: dimension x vs y, one point per granule."""
    dfs: list[pd.DataFrame] = []
    for input_ in inputs:
        dfs.append(pd.read_parquet(input_))
    df = pd.concat(dfs)

    has_granule = df.get("input_granule_id", pd.Series(pd.NA, index=df.index))
    df = df[has_granule.notna() & (has_granule != "")]

    if tasks:
        df = df[df["task_name"].isin(tasks)]

    if df.empty:
        raise click.ClickException("No rows with input_granule_id in the data.")

    fig = _plot_scatter(df, versions=(x_val, y_val), dimension=dimension)
    _save_or_show(fig, output)


@plot.command()
@click.argument("input", default="metrics.parquet")
@click.option(
    "--metric",
    default="runtime_seconds",
    show_default=True,
    type=click.Choice(METRICS),
)
@click.option(
    "--freq",
    default="1h",
    show_default=True,
    help="Resample frequency (pandas offset alias, e.g. 1h, 30min, 1D)",
)
@click.option("--output", default=None, help="Save figure to file instead of showing")
def timeseries(input: str, metric: str, freq: str, output: str | None) -> None:
    """Stacked total metric over time, broken down by task_name."""
    df = pd.read_parquet(input)

    if "timestamp" not in df.columns:
        raise click.ClickException(
            "No timestamp column — re-run fetch to get updated data."
        )

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    fig = _plot_timeseries(df, metric, freq)
    _save_or_show(fig, output)


def _save_or_show(fig: plt.Figure, output: str | None) -> None:
    if output:
        fig.savefig(output, dpi=150, bbox_inches="tight")
        click.echo(f"Saved → {output}")
    else:
        plt.show()


if __name__ == "__main__":
    cli()
