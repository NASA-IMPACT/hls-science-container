# hls-nextgen-orchestration

This repository provides a robust and extensible framework for orchestrating complex tasks and managing data flows
within a containerized environment, specifically tailored for the Harmonized Landsat Sentinel (HLS) project. It defines
a set of reusable and composable components, including `Task`s and `DataSource`s, to manage the execution of
sophisticated processing workflows for atmospheric correction and other HLS product generation steps.

## Key Workflows

This framework currently supports the following critical workflows for HLS product generation:

### Landsat Atmospheric Correction (LaSRC)

This workflow processes Landsat Level-1 data to produce atmospherically corrected Level-2 surface reflectance products.
It involves downloading the raw Landsat granule, parsing its metadata, performing atmospheric correction using the LaSRC
algorithm, and integrating Fmask cloud/shadow/snow detection. The final atmospherically corrected HDF product and
associated angle files are then uploaded to an S3 output bucket.

### Landsat Tiling

This workflow takes atmospherically corrected Landsat path/row granules and transforms them into the HLS tiling scheme.
It involves downloading the necessary path/row data, extracting scene timing information, and running the `landsat-tile`
and `landsat-angle-tile` utilities to create tiled HDF products. Subsequently, NBAR (Nadir Bidirectional Reflectance
Distribution Function Adjusted Reflectance) correction is applied, and the outputs are converted to Cloud Optimized
GeoTIFFs (COGs). Finally, metadata (CMR XML, STAC JSON), thumbnails, GIBS browse tiles, and Vegetation Index (VI)
products are generated and uploaded to their respective S3 locations.

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
