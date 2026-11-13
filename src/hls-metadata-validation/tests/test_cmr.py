from pathlib import Path

from hls_metadata_validation.cmr import check_cmr, normalize_cmr
from hls_metadata_validation.spec import ProductSpec


def test_missing_empty_and_wrong_values_fail(
    l30: ProductSpec, l30_cmr_path: Path
) -> None:
    doc = normalize_cmr(l30_cmr_path)
    attrs = doc["additional_attributes"]
    del attrs["ACCODE"]
    attrs["FILLVALUE"] = []
    attrs["NCOLS"] = ["abc"]
    attrs["ANG_SCALE_FACTOR"] = ["1.0"]
    attrs["REF_SCALE_FACTOR"] = ["0.000100"]
    found = {(f.check, f.target) for f in check_cmr(doc, l30, granule="G")}
    assert found == {
        ("attr.required", "ACCODE"),
        ("attr.required", "FILLVALUE"),
        ("attr.type", "NCOLS"),
        ("attr.value", "ANG_SCALE_FACTOR"),
    }
