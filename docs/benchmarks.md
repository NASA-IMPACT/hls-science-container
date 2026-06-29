# Performance Benchmarks

End-to-end benchmarks for the Sentinel-2 (S30) and Landsat AC (L30-AC) pipelines. Each test runs a full
`construct_pipeline().run()` against real input data and real binaries, recording runtime, peak memory, and CPU from
the pipeline's own metrics sampler.

Results are published to GitHub Pages after every merge to `main` via
[benchmark-action/github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark), giving a
commit-by-commit performance history and automatic regression alerts.

## How it works

Each benchmark test is parametrized over a list of granule IDs, so each granule runs independently. Every metric is
published as a `customSmallerIsBetter` series keyed by `{task} {metric} [{granule}]`, so each granule (and each
instrumented pipeline stage) is tracked independently across commits:

```
sentinel-ac runtime_seconds [S2B_MSIL1C_20260127T160429_N0511_R097_T18UYA_20260127T193526]
sentinel-ac peak_memory_mb  [S2B_MSIL1C_20260127T160429_N0511_R097_T18UYA_20260127T193526]
LaSRC avg_cpu_percent       [S2B_MSIL1C_20260127T160429_N0511_R097_T18UYA_20260127T193526]
```

Granule input data and LaSRC ancillary data are downloaded from S3 in session-scoped fixtures (outside the timed
section) and reused across all tests in a run.

Benchmarks run inside the production Docker image (so fmask, lasrc, and all other binaries are the real ones, not
mocks). A `benchmark` Docker target extends the image with pytest and the test files.

## Runtime, memory, and CPU in one chart group

`github-action-benchmark` uses one parser (`tool`) per publish step, and the `pytest` tool only charts runtime. To
track **runtime, peak memory, and average CPU** together — including the heavy subprocesses (Fmask MCR, LaSRC) where
most of the work happens — all three are emitted into a single `customSmallerIsBetter` JSON and published in one step
(chart group "HLS pipeline benchmarks").

The measurements reuse the same sampler the pipelines use in production: `hls_nextgen_orchestration.metrics`. Its
`_PollingThread` samples the whole process tree's peak RSS and rolls up subprocess CPU via `cpu_times()`. Each benchmark
injects an `InMemorySink` into `construct_pipeline(..., metric_sink=...)`, so the pipeline's existing per-task
(`collect()`, gated by `instrument=True`) and aggregate (`collect_pipeline()`) measurements are captured locally instead
of being shipped to CloudWatch. After the run, the conftest's `resource_metrics` collector writes
`resources.json` into `$BENCHMARK_OUTPUT_DIR` (default `benchmark-output/`).

Per-task series are naturally limited to the `instrument=True` stages (e.g. `LaSRC`, `Fmask`), plus the pipeline
aggregate (`sentinel-ac` / `landsat-ac`).

## GitHub Actions setup

The benchmark workflow (`.github/workflows/benchmark.yml`) triggers on push to `main`. It requires the following to be
configured in the repository's **Settings → Secrets and variables → Actions**:

### Secrets

| Secret                   | Description                                        |
| ------------------------ | -------------------------------------------------- |
| `AWS_ACCESS_KEY_ID`      | Already used by the PR workflow for ECR            |
| `AWS_SECRET_ACCESS_KEY`  | Already used by the PR workflow for ECR            |
| `BENCHMARK_INPUT_BUCKET` | S3 bucket containing pre-staged benchmark granules |
| `BENCHMARK_AUX_BUCKET`   | S3 bucket containing LaSRC ancillary data          |

### Variables

| Variable                   | Description                                         | Example     |
| -------------------------- | --------------------------------------------------- | ----------- |
| `AWS_REGION`               | Already used by the PR workflow                     | `us-west-2` |
| `BENCHMARK_INPUT_PREFIX`   | S3 key prefix for benchmark granules                | `inputs`    |
| `BENCHMARK_AUX_PREFIX`     | S3 key prefix for LaSRC ancillary data              | `lasrc_aux` |
| `BENCHMARK_S2_GRANULE_IDS` | Comma-separated Sentinel-2 granule IDs to benchmark | see below   |
| `BENCHMARK_LS_GRANULE_IDS` | Comma-separated Landsat granule IDs to benchmark    | see below   |

### Thread pinning

`BENCHMARK_NUM_THREADS` (default `2`, clamped to the runner's CPUs) pins `OMP_NUM_THREADS` so LaSRC and Fmask thread
consistently across runs. It is also a `workflow_dispatch` input. The benchmark series key tags the Fmask version as
`[<granule> (v5)]`.

| Env var (workflow_dispatch input) | Description                          | Default |
| --------------------------------- | ------------------------------------ | ------- |
| `BENCHMARK_NUM_THREADS`           | Threads to pin (clamped to CPUs)     | `2`     |

#### Expected S3 layout

```
s3://{BENCHMARK_INPUT_BUCKET}/
  {BENCHMARK_INPUT_PREFIX}/
    {s2_granule_id}.zip          # one zip per S2 granule
    {ls_granule_id}/             # one directory per Landsat granule
      {ls_granule_id}_MTL.txt
      ...

s3://{BENCHMARK_AUX_BUCKET}/
  {BENCHMARK_AUX_PREFIX}/        # synced to LASRC_AUX_DIR at runtime
    CMGDEM.hdf
    LADS/
    LDCMLUT/
    ...
```

### One-time repo setup

Enable GitHub Pages: **Settings → Pages → Source: `gh-pages` branch, root folder**. The first benchmark run will create
the branch and push the initial chart.

## Choosing benchmark granules

Pick granules that cover a range of processing times and geographic diversity. `fast-s2-granules.csv` in the repo root
lists pre-profiled S2 granules with their runtimes and is a good starting point.

Each granule becomes its own chart series, so adding a granule extends the history without affecting existing series.
Removing one leaves its history intact.

## Running locally

Benchmarks require the real fmask/lasrc binaries, so they run inside the Docker container. Build the benchmark image
first:

```bash
docker build --target benchmark -t hls-science-container:benchmark \
  --secret id=aws_access_key_id,env=AWS_ACCESS_KEY_ID \
  --secret id=aws_secret_access_key,env=AWS_SECRET_ACCESS_KEY .
```

If you already have LaSRC ancillary data locally (e.g. `data/lasrc_aux/`), mount it directly and set `LASRC_AUX_DIR` —
the fixture will skip the S3 download:

```bash
mkdir -p /tmp/benchmark-output
docker run --rm \
  -v /tmp/benchmark-output:/output \
  -v "$(pwd)/data/lasrc_aux:/app/data/lasrc_aux" \
  -e LASRC_AUX_DIR=/app/data/lasrc_aux \
  -e BENCHMARK_S2_GRANULE_IDS="S2B_MSIL1C_..." \
  -e BENCHMARK_INPUT_BUCKET="my-bucket" \
  -e BENCHMARK_INPUT_PREFIX="inputs" \
  -e BENCHMARK_OUTPUT_DIR=/output \
  -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  hls-science-container:benchmark \
  "cd /app && pytest src/hls-nextgen-orchestration/tests/benchmarks -v"
```

The metrics (runtime, peak memory, CPU) are written to `$BENCHMARK_OUTPUT_DIR/resources.json`.

Alternatively, let the fixture download ancillary data from S3 by providing `BENCHMARK_AUX_BUCKET` and
`BENCHMARK_AUX_PREFIX` instead of `LASRC_AUX_DIR`.

## Regression alerts

The workflow is configured with `alert-threshold: 150%` — a result more than 50% slower than the stored baseline
triggers a GitHub commit comment. Alerts do not fail the workflow (`fail-on-alert: false`) so they never block a merge.
