# hls-metadata-validation

Validates the metadata of final HLS products (COG GeoTIFF tags, CMR XML, STAC
Item JSON) against a data-driven JSON spec, and compares freshly produced
outputs against reference production outputs.

Design: `docs/superpowers/specs/2026-11-12-hls-metadata-validation-design.md`.

## Specs

Product specs live in `src/hls_metadata_validation/specs/` (`L30.json`,
`S30.json`, shared fragments under `common/`). A JSON object of the form
`{"$include": "<path>"}` or `{"$include": ["<path>", ...], ...}` is replaced by
the merged contents of the referenced files (relative to the including file);
duplicate keys are an error so each fact lives in one place.

Golden comparison: `default_tolerance` is a relative tolerance applied to every
numeric value; an attribute's `tolerance` is an additional absolute tolerance
(e.g. `1.0` percentage point for `CLOUD_COVERAGE`).

## Usage

```bash
# Validate final outputs (COGs, .cmr.xml, _stac.json, .jpg) in a directory tree
hls-validate-metadata spec PRODUCT_DIR [--product L30|S30] [--json report.json]

# Write normalized COG metadata (.tif.json) plus CMR/STAC/thumbnail copies,
# e.g. to create small test fixtures; `spec` accepts the dumped directory too
hls-validate-metadata dump PRODUCT_DIR OUT_DIR
```

Exit status is 0 when there are no errors; known gaps are reported as warnings.

```bash
# Validate NEW_DIR, then compare it to reference outputs (same layout) in REF_DIR.
# Granules with no reference are reported as skipped (warning).
hls-validate-metadata golden NEW_DIR REF_DIR [--json report.json]
```
