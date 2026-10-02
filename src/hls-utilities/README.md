# hls-utilities

Python utilities for HLS data processing, used as a library by `hls-workflows`.

## Usage

| Module                                      | Function                    |
| ------------------------------------------- | --------------------------- |
| `hls_utilities.apply_s2_quality_mask`       | `apply_s2_quality_mask`     |
| `hls_utilities.check_sentinel_clouds`       | `cloud_cover_is_valid`      |
| `hls_utilities.check_solar_zenith_landsat`  | `solar_zenith_is_valid`     |
| `hls_utilities.check_solar_zenith_sentinel` | `solar_zenith_is_valid`     |
| `hls_utilities.create_landsat_sr_hdf_xml`   | `create_landsat_sr_hdf_xml` |
| `hls_utilities.create_sr_hdf_xml`           | `create_sr_hdf_xml`         |
| `hls_utilities.download_landsat`            | `get_landsat`               |

### Tests

From the repository root, using the pixi `dev` environment:

```bash
$ scripts/test src/hls-utilities
```

Or standalone with `uv` from this directory:

```bash
$ uv run pytest
```
