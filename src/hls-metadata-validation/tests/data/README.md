# Test fixtures

Real HLS v2.0 granule metadata, one granule per product:

| Product | Granule | Fetched |
|---|---|---|
| L30 | `HLS.L30.T56NQK.2026309T235813.v2.0` | 2026-11-12 |
| S30 | `HLS.S30.T55GEM.2026309T235229.v2.0` | 2026-11-12 |

- `*.cmr.xml`: the `<Granule>` element of the CMR search API ECHO10 response
  (`https://cmr.earthdata.nasa.gov/search/granules.echo10?short_name=HLSL30&version=2.0&page_size=1&sort_key=-start_date`,
  likewise `HLSS30`). Edit: the `DataGranule/AdditionalFile` elements were
  removed; LP DAAC adds them at ingest, they are not part of the pipeline's
  `.cmr.xml`, and `metadata_creator`'s `Granule.xsd` does not allow them.
- `*_stac.json`: the published STAC Item, unmodified, from
  `https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-public/HLS{L,S}30.020/<granule>/<granule>_stac.json`.
- `*.<asset>.tif.json`: `hls-validate-metadata dump` of the published COGs
  (`lp-prod-protected/HLS{L,S}30.020/<granule>/<granule>.<asset>.tif`, Earthdata
  login required), fetched 2026-11-12.
- `*.jpg`: zero-byte placeholders; only presence is checked.

The prod L30 granule predates the restore of NASA-IMPACT/hls-landsat-tile@60205b0
(#89): its angle COGs have no nodata and an unset scale, so
`hls-validate-metadata spec tests/data/L30` reports exactly 8 errors (nodata and
scale for SZA/SAA/VZA/VAA). `test_runner.py` pins this as a real-world negative
case. A missing add_offset is not detectable on these files because
GDAL reports an unset offset as 0.0, which equals the expected value.
