# HLS Atmospheric Correction

> [!NOTE]
> The code in this repository has been consolidated from other codebases including:
>
> - https://github.com/NASA-IMPACT/espa-dockerfiles
> - https://github.com/NASA-IMPACT/hls-base
> - https://github.com/NASA-IMPACT/hls-sentinel
> - https://github.com/NASA-IMPACT/hls-landsat

## Description

This repository builds a Docker container that can run atmospheric correction on Landsat and Sentinel-2 Level-1 data
as part of creating the Level-2 Harmonized Landsat Sentinel (HLS) product.

## Getting Started

This project uses `pixi` to manage dependencies. To install this, please visit the uv
[installation documentation](https://pixi.sh/latest/installation/) for instructions.

This project uses "scripts to rule them all" for common developer tasks that are explained in the following sections.

### Building and running the Docker container

This project's software requires a large number of compilation of 3rd party libraries. This potentially can be done on
your host if it's easier for your workflows, but the source of truth we're concerned about is whether it runs in the
Docker container this repository publishes.

To build the container and open a Bash terminal in the container, run:

```plain
scripts/shell
```

### Development

Install dependencies for resolving references in your favorite IDE:

```plain
pixi install
```

### Testing

Run unit tests,

```plain
scripts/test
```

### Formatting and Linting

Run formatting,

```plain
scripts/format
```

Run linting,

```plain
scripts/lint
```
