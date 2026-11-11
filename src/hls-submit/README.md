# hls-submit

Operator tooling for running HLS science experiments (LaSRC, Fmask) on the AWS
Batch infrastructure deployed by `hls-orchestration`, e.g. for performance
testing or validating algorithm changes. It is not installed in the container
image.

Job queues and job definitions are looked up from the orchestration
CloudFormation stack (`--stack-name` or `HLS_STACK_NAME`) by logical ID, so no
ARNs are hardcoded. Job definitions are always submitted without a revision, so
Batch runs the latest ACTIVE one. Pass `--job-queue` and `--job-definition` to
skip the lookup.

## Usage

```
pixi run -e dev hls-submit --help
# or, outside of pixi
uv run --project src/hls-submit hls-submit --help
```

Every submit command is a dry run unless `--submit` is given:

```
export HLS_STACK_NAME=hls-mcp-development-viirs

# A CSV with several columns: name the one holding L1 input granule IDs
hls-submit fmask Belmanip2_HLS_sample_Jan_L1.csv --granule-column L1_ID --sensor L30

# A plain list of L1 IDs, one per line, with or without a header
hls-submit lasrc Belmanip2_HLS_sample_S2.csv --sensor S30 --lasrc-impl rust --submit

# Tag jobs for metrics (HLS_EXPERIMENT_FMASK_BUILD=baseline) and read
# Sentinel-2 inputs from a requester-pays bucket in another account
hls-submit fmask Belmanip2_HLS_sample_S2.csv --sensor S30 --rows 5 \
    --experiment fmask_build=baseline \
    --input-bucket hls-validation-datasets --input-prefix datasets/belmanip2/S30/ \
    --requester-pays --submit

hls-submit terminate --sensor S30
hls-submit terminate --sensor S30 --terminate
```

`--sensor` filters rows by the L1 ID (`LC08`/`LC09` for L30, `S2A`-`S2D` for
S30), so a CSV mixing both sensors works. `.SAFE` suffixes and `A+B` twin
granules are cleaned up.

`--experiment KEY=VALUE` (repeatable) sets `HLS_EXPERIMENT_<KEY>=VALUE`, which
the workflows record as a CloudWatch metrics dimension; the values are also
added to job names. `lasrc` additionally tags each job with
`HLS_EXPERIMENT_LASRC_IMPL=<c|rust>`.

Metrics go to `/hls-orchestration/<stack>/log-metrics` unless
`--metric-log-group` is given; with neither a stack nor a log group, jobs do
not record metrics.

AWS credentials and profile come from the usual environment (`AWS_PROFILE`,
etc).
