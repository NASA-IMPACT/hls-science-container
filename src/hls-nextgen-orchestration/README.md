# hls-nextgen-orchestration

This repository provides a robust and extensible framework for orchestrating complex tasks and managing data flows
within a containerized environment, specifically tailored for the Harmonized Landsat Sentinel (HLS) project. It defines
a set of reusable and composable components, including `Task`s and `DataSource`s, to manage the execution of
sophisticated processing workflows for atmospheric correction and other HLS product generation steps.

## Key Workflows

This framework currently supports the following workflows for HLS product generation:

### Sentinel-2 (S30)

Defined in `sentinel/workflow.py`. Processes one or more Sentinel-2 L1C granules (twin-granule pairs are supported)
into atmospherically corrected, tiled HLS S30 products. Key stages:

1. **Download / locate** — download granule ZIP from S3 or use a pre-staged local file
2. **Per-granule preprocessing** — solar zenith check, detector footprint, quality mask, angle derivation
3. **Cloud masking** — Fmask v4 (default) or v5 (`FMASK_VERSION=5`)
4. **Atmospheric correction** — LaSRC via ESPA format conversion
5. **Merge** — combine per-granule HDF parts, add Fmask SDS, trim
6. **Post-processing** — resample to 30 m, NBAR, bandpass correction, COG conversion
7. **Output** — metadata, thumbnails, GIBS browse tiles, VI products, S3 upload

### Landsat Atmospheric Correction (L30-AC)

Defined in `landsat_ac/workflow.py`. Processes a Landsat Level-1 path/row scene into an atmospherically
corrected HDF product. Key stages:

1. **Download / locate** — download granule from S3 or use a local file
2. **Preprocessing** — metadata parsing, solar zenith check, scanline conversion, ESPA format conversion
3. **Cloud masking** — Fmask v4 (default) or v5 (`FMASK_VERSION=5`)
4. **Atmospheric correction** — LaSRC
5. **Output** — HDF product with Fmask SDS and angle bands, S3 upload

### Landsat Tiling (L30-Tile)

Defined in `landsat_tile/workflow.py`. Reprojects and tiles Landsat path/row AC outputs into the HLS MGRS
grid. Key stages:

1. **Download / locate** — fetch atmospherically corrected path/row products from S3 or a local directory
2. **Tiling** — `landsat-tile` and `landsat-angle-tile` utilities produce per-MGRS-tile HDF files
3. **NBAR** — nadir BRDF-adjusted reflectance correction
4. **Post-processing** — COG conversion, metadata (CMR XML, STAC JSON), thumbnails, GIBS browse tiles, VI products, S3 upload

## Framework Architecture

The core of `hls-nextgen-orchestration` is built around a declarative and modular design, enabling flexible and robust
workflow definition.

### Tasks

`Task`s are the fundamental units of work within the framework. Each `Task` represents a distinct processing step and
adheres to a clear contract:

- **`requires`**: A tuple of `Asset`s that the `Task` depends on as input.
- **`provides`**: A tuple of `Asset`s that the `Task` produces as output.
- **`run(inputs: AssetBundle) -> AssetBundle`**: The method that encapsulates the actual logic of the processing step.
  It receives an `AssetBundle` containing the required inputs and returns an `AssetBundle` with the generated outputs.

This design promotes reusability, testability, and clear dependency management between processing steps.

### Data Sources

`DataSource`s are specialized `Task`s responsible for providing initial data or configuration to the workflow. They
typically fetch data from external sources (e.g., environment variables, S3 buckets) and make them available as `Asset`s
for subsequent `Task`s.

### Assets and Asset Bundles

- **`Asset`**: Represents a specific piece of data or configuration within the workflow (e.g., a file path, a
  configuration object, a boolean flag). Each `Asset` has a unique identifier.
- **`AssetBundle`**: A dictionary-like structure that holds a collection of `Asset`s, mapping their identifiers to their
  actual values. `AssetBundle`s are used to pass data between `Task`s.

### Orchestration Engine

The framework includes an orchestration engine that is responsible for:

- **Dependency Resolution**: Determining the correct order of `Task` execution based on their `requires` and `provides`
  declarations.
- **Execution Management**: Running `Task`s in the correct sequence, handling input and output passing.
- **Error Handling**: Providing mechanisms for graceful error handling and reporting `TaskFailure`s.

This architecture allows for the construction of complex, multi-stage processing pipelines with clear data flow and
robust error management, making it ideal for the demanding requirements of HLS product generation.

### Task Metrics

Individual tasks can opt into per-execution metrics collection by setting `instrument = True` on their class.
Currently instrumented tasks are `RunFmask`, `RunFmaskV5`, and `RunLaSRC` in all three workflows, plus
`ProcessPathRows` and `RunNbar` in the tiling workflow.

Metrics are collected and emitted in [CloudWatch Embedded Metrics Format (EMF)](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format.html)
so that CloudWatch Logs Insights can query raw log lines while CloudWatch Metrics extracts numeric values automatically.

Three metrics are captured per task execution:

| Metric | Unit | Description |
|--------|------|-------------|
| `runtime_seconds` | Seconds | Wall-clock time for the full task |
| `peak_memory_mb` | Megabytes | Peak RSS across the Python process and all child processes |
| `max_cpu_percent` | Percent | Maximum combined CPU usage across the process tree |

**Enabling metrics** — set the `METRIC_LOG_GROUP_NAME` environment variable to an existing CloudWatch Logs
log group. The log stream is taken from `AWS_BATCH_JOB_ID` (defaulting to `local_job`). The log group and
stream must already exist; the framework will not create them.

**Experiment dimensions** — any environment variable prefixed with `HLS_EXPERIMENT_` is added as a CloudWatch
dimension, enabling side-by-side comparisons in dashboards. For example:

```
HLS_EXPERIMENT_FMASK_VERSION=v5
```

produces a dimension `fmask_version=v5` on every emitted metric.

**Injecting a custom collector** — `PipelineBuilder.build()` accepts an optional `MetricsCollector` instance,
which is useful in tests or when you need to pre-configure the boto3 client:

```python
from hls_nextgen_orchestration.metrics import MetricsCollector
import boto3

collector = MetricsCollector(client=boto3.client("logs", region_name="us-east-1"))
pipeline = builder.build(metrics=collector)
```
