# HLS Metadata Pipeline

This document describes how global metadata attributes flow from the Docker container definition through to STAC items,
and how to add a new attribute.

## Pipeline stages

```text
Dockerfile ENV
    ↓
Shell scripts (src/scripts/)
    ↓
hls-libs C utilities  →  HDF global attribute  (SDsetattr)
                               ↓
                   hls-hdf_to_cog (automatic via rasterio update_tags)
                               ↓
                          COG tags
                               ↓
                   hls-metadata metadata_creator
                               ↓
                          CMR XML  <AdditionalAttribute>
                               ↓
                   hls-cmr_stac cmr_to_stac_item
                               ↓
                        STAC item properties
```

## Existing attributes

| Attribute     | HDF / COG name       | CMR name             | STAC property                                   |
| ------------- | -------------------- | -------------------- | ----------------------------------------------- |
| LaSRC version | `ACCODE`             | `ACCODE`             | `processing:software["Atmospheric Correction"]` |
| Fmask version | `CLOUD_MASKING_CODE` | `CLOUD_MASKING_CODE` | `processing:software["Cloud Masking"]`          |

Both STAC values are set by `process_software_versions()` in `hls-cmr_stac`, which also adds the `processing`
extension to the item.

The LaSRC version value is read from `ACCODE` (C) / `lasrc.ALGORITHM_VERSION` (Rust), and the Fmask version from
`FMASK_VERSION_STRINGS` in `hls_workflows.constants`, for both the Landsat and Sentinel-2 workflows.

## How to add a new global metadata attribute

### 1. Define the value in the container

Add an `ENV` in **`Dockerfile`** (prod stage, near the other metadata ENV vars):

```dockerfile
ENV MY_ATTRIBUTE="value"
```

### 2. Pass it through the processing scripts

In **`src/scripts/sentinel.sh`** and **`src/scripts/landsat.sh`**, add `"$MY_ATTRIBUTE"` as a positional argument to
whichever `hls-libs` C utility is responsible for writing the HDF file at that stage. Follow the existing `$ACCODE`
example.

### 3. Write it as an HDF global attribute in C

In the relevant C file under **`packages/hls-libs/`**:

1. Add a `char my_attribute[LINELEN];` variable.

2. Increment the `argc` check by 1 and update the usage string.

3. `strcpy(my_attribute, argv[N]);` (adjust index to match new position).

4. Write to HDF:

   ```c
   SDsetattr(output.sd_id, "MY_ATTRIBUTE", DFNT_CHAR8,
             strlen(my_attribute), (VOIDP)my_attribute);
   ```

If the attribute is invariant between twin S2 granules, also propagate it in **`sentinel-consolidate/consolidate.c`**
using `SDfindattr` / `SDreadattr` / `SDsetattr` (see the `CLOUD_MASKING_CODE` block for the pattern).

### 4. No change needed for HDF → COG

`hls-hdf_to_cog` calls `rasterio`'s `update_tags(**sub_dst.tags())` which automatically copies **all** HDF global
attributes into COG tags.

### 5. Add to CMR XML templates (hls-metadata)

In **`metadata_creator/templates/S30.json`** and **`L30.json`**, add an entry to `AdditionalAttributes`:

```json
{
  "Name": "MY_ATTRIBUTE",
  "DataType": "STRING",
  "Description": "Human-readable description"
}
```

In **`metadata_creator/templates/S30_attribute_mapping.json`** and **`L30_attribute_mapping.json`**, add:

```json
"MY_ATTRIBUTE": "MY_ATTRIBUTE"
```

The key is the CMR template field name; the value is the HDF attribute name. Use `"NOT_IN_HDF"` if the value is
hardcoded in the template's `Values` field rather than read from the file.

No changes to `metadata_creator.py` are needed — `attribute_handler()` already handles any mapped attribute generically.

### 6. Add to STAC items (hls-cmr_stac)

In **`hls_cmr_stac/hls_cmr_stac.py`**, add a function:

```python
def process_my_attribute(item, granule):
    for attribute in granule.AdditionalAttributes.AdditionalAttribute:
        if attribute.Name == "MY_ATTRIBUTE":
            item.properties["my_attribute"] = attribute.Values.Value.cdata
```

Call it inside `cmr_to_item()` alongside the other `process_*` calls.

After merging, tag a new release and update the `hls-cmr_stac` tag in **`pixi.toml`**.

### 7. Declare it in the validation spec

Add the attribute to the product specs in **`src/hls-metadata-validation/src/hls_metadata_validation/specs/`**: in
`common/attributes.json` if it is identical for L30 and S30, otherwise in `L30.json` / `S30.json`:

```json
"MY_ATTRIBUTE": {
    "required": true,
    "type": "string",
    "cog_tag": "MY_ATTRIBUTE",
    "cmr_additional_attribute": "MY_ATTRIBUTE",
    "stac_property": "my_attribute",
    "golden": "exact"
}
```

Use `null` for a surface the attribute does not appear in. Use `"golden": "volatile"` for values expected to change
between runs (versions, timestamps) and `"approx"` with an absolute `"tolerance"` for floats. Then run
`scripts/test src/hls-metadata-validation`.
