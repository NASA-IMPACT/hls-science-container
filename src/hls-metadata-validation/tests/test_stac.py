from pathlib import Path

from hls_metadata_validation.spec import ProductSpec
from hls_metadata_validation.stac import check_stac, load_stac


def test_schema_and_spec_violations_fail(l30: ProductSpec, l30_stac_path: Path) -> None:
    doc = load_stac(l30_stac_path)
    doc["properties"]["eo:cloud_cover"] = "lots"
    del doc["properties"]["processing:software"]["Cloud Masking"]
    del doc["assets"]["SZA"]
    found = {
        (f.check, f.target)
        for f in check_stac(doc, l30, granule="G")
        if f.check != "asset.raster"
    }
    assert found == {
        ("schema", "stac.json"),
        ("property.required", "processing:software.Cloud Masking"),
        ("asset.required", "SZA"),
    }
