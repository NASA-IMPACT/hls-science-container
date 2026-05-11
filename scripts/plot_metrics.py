#!/usr/bin/env python
"""
HLS metrics CLI — fetch and plot CloudWatch EMF metrics from Batch jobs.

Commands:
    fetch             Query Logs Insights and save to Parquet
    plot scatter      fmask_version 4 vs 5 paired scatter per granule
    plot timeseries   Stacked total metric over time by task_name
"""

import datetime
import time

import boto3
import click
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

METRICS = ["runtime_seconds", "peak_memory_mb", "avg_cpu_percent"]
METRIC_LABELS = {
    "runtime_seconds": "Runtime (s)",
    "peak_memory_mb": "Peak memory (MB)",
    "avg_cpu_percent": "Average CPU (%)",
}


def _build_query(dimension: str) -> str:
    return f"""
        fields @timestamp, task_name, input_granule_id, {dimension}, workflow,
               runtime_seconds, peak_memory_mb, avg_cpu_percent
        | filter ispresent({dimension})
        | filter ispresent(task_name)
        | limit 10000
    """


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


def _run_query(
    client,
    log_group: str,
    start: datetime.datetime,
    end: datetime.datetime,
    poll_interval: float,
    query: str,
) -> list[dict]:
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

    records = []
    for row in result["results"]:
        record = {}
        for r in row:
            if r["field"] == "@timestamp":
                record["timestamp"] = r["value"]
            elif not r["field"].startswith("@"):
                record[r["field"]] = r["value"]
        records.append(record)

    return records


def _query_all_chunks(
    client,
    log_group: str,
    hours_back: int,
    chunk_hours: int,
    poll_interval: float,
    query: str,
) -> list[dict]:
    end_time = datetime.datetime.now(datetime.UTC)
    start_time = end_time - datetime.timedelta(hours=hours_back)
    chunk = datetime.timedelta(hours=chunk_hours)
    total_chunks = -(-hours_back // chunk_hours)

    all_records: list[dict] = []
    chunk_start = start_time
    chunk_num = 0

    while chunk_start < end_time:
        chunk_end = min(chunk_start + chunk, end_time)
        chunk_num += 1
        click.echo(
            f"  chunk {chunk_num}/{total_chunks}: "
            f"{chunk_start:%H:%M} – {chunk_end:%H:%M} UTC",
            nl=False,
        )
        records = _run_query(
            client, log_group, chunk_start, chunk_end, poll_interval, query
        )
        click.echo(f" ({len(records)} records)")
        all_records.extend(records)
        chunk_start = chunk_end

    return all_records


def _build_dataframe(records: list[dict], task_groups: set[str]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    if df.empty:
        return df

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    for col in METRICS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[df["task_name"].isin(task_groups)]


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
                pct = (wf_data[v2].mean() / wf_data[v1].mean() - 1) * 100
                sign = "+" if pct >= 0 else ""
                ax.plot(
                    np.array(ref),
                    m * np.array(ref) + b,
                    color=color,
                    linewidth=1.2,
                    label=f"{workflow}  {sign}{pct:.1f}%  R²={r2:.2f}",
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
def cli():
    """HLS metrics: fetch and plot Batch job metrics from CloudWatch."""


@cli.command()
@click.argument("output", default="metrics.parquet")
@click.option(
    "--log-group",
    default="/hls-orchestration/dev-viirs/log-metrics",
    show_default=True,
)
@click.option(
    "--hours", default=48, show_default=True, help="Hours of history to fetch"
)
@click.option(
    "--chunk-hours", default=4, show_default=True, help="Time window per query"
)
@click.option("--region", default=None, help="AWS region")
@click.option(
    "--tasks",
    default="Fmask,LaSRC",
    show_default=True,
    help="Comma-separated task_name values to keep",
)
@click.option(
    "--dimension",
    default="fmask_version",
    show_default=True,
    help="Experiment dimension to filter and include",
)
def fetch(output, log_group, hours, chunk_hours, region, tasks, dimension):
    """Query Logs Insights and save to OUTPUT (default: metrics.parquet)."""
    task_groups = set(tasks.split(","))

    kwargs = {"region_name": region} if region else {}
    client = boto3.client("logs", **kwargs)

    click.echo(f"Querying {log_group!r} — last {hours}h in {chunk_hours}h chunks ...")
    records = _query_all_chunks(
        client,
        log_group,
        hours,
        chunk_hours,
        poll_interval=2.0,
        query=_build_query(dimension),
    )
    click.echo(f"  {len(records)} records total")

    df = _build_dataframe(records, task_groups)
    click.echo(f"  {len(df)} records after filtering to {task_groups}")

    if df.empty:
        raise click.ClickException("No data to save.")

    for (task, dim_val), count in df.groupby(["task_name", dimension]).size().items():
        click.echo(f"  {task:10s}  {dimension}={dim_val}  {count}")

    df.to_parquet(output, index=False)
    click.echo(f"Saved → {output}")


@cli.group()
def plot():
    """Visualization subcommands (load from a Parquet file produced by fetch)."""


@plot.command()
@click.argument("input", default="metrics.parquet")
@click.option(
    "--dimension",
    default="fmask_version",
    show_default=True,
    help="Column to compare across versions",
)
@click.option(
    "--x", "x_val", default="4", show_default=True, help="Dimension value for x-axis"
)
@click.option(
    "--y", "y_val", default="5", show_default=True, help="Dimension value for y-axis"
)
@click.option(
    "--tasks",
    multiple=True,
    default=None,
    help="Task(s) to plot (repeat for multiple, e.g. --tasks Fmask --tasks LaSRC). Defaults to all tasks in the data.",
)
@click.option("--output", default=None, help="Save figure to file instead of showing")
def scatter(input, dimension, x_val, y_val, tasks, output):
    """Paired scatter: dimension x vs y, one point per granule."""
    df = pd.read_parquet(input)

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
def timeseries(input, metric, freq, output):
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
