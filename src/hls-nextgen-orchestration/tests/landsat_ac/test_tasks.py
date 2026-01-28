from typing import TYPE_CHECKING
from unittest.mock import PropertyMock, patch

import boto3
import pytest

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from hls_nextgen_orchestration.landsat_ac.assets import (
    CONFIG,
    ESPA_XML,
    FINAL_HDF,
    FMASK_BIN,
    GRANULE_DIR,
    HLS_XML,
    LASRC_DONE,
    METADATA,
    MTL_FILE,
    RENAMED_ANGLES,
    SCANLINE_DONE,
    SOLAR_VALID,
    SR_HDF,
    UPLOAD_COMPLETE,
    EnvConfig,
    ProcessingMetadata,
)
from hls_nextgen_orchestration.landsat_ac.tasks import (
    AddFmaskSds,
    CheckSolarZenith,
    ConvertScanline,
    ConvertToEspa,
    ConvertToHdf,
    CreateHlsXml,
    DownloadGranule,
    EnvSource,
    ParseMetadata,
    RenameAngleBands,
    RunFmask,
    RunLaSRC,
    UploadResults,
)

# Constants
JOB_ID = "job-456"
GRANULE = "LC08_L1TP_025030_20200101_20200114_01_T1"
BUCKET_IN = "in-bucket"
BUCKET_OUT = "out-bucket"


@pytest.fixture
def mock_config(tmp_path):
    with patch(
        "hls_nextgen_orchestration.landsat_ac.tasks.EnvConfig.working_dir",
        new_callable=PropertyMock,
    ) as mock_wd:
        mock_wd.return_value = tmp_path / "scratch" / JOB_ID
        config = EnvConfig(JOB_ID, GRANULE, BUCKET_IN, BUCKET_OUT, "L8", "LaSRC")
        if not config.granule_dir.exists():
            config.granule_dir.mkdir(parents=True)
        yield config


def test_env_source(monkeypatch, tmp_path):
    """Test environment variable parsing."""
    monkeypatch.setenv("AWS_BATCH_JOB_ID", JOB_ID)
    monkeypatch.setenv("GRANULE", GRANULE)
    monkeypatch.setenv("INPUT_BUCKET", BUCKET_IN)
    monkeypatch.setenv("OUTPUT_BUCKET", BUCKET_OUT)
    monkeypatch.setenv("PREFIX", "L8")
    monkeypatch.setenv("ACCODE", "LaSRC")

    # Patch Path inside the class property to avoid /var/scratch issues
    with patch(
        "hls_nextgen_orchestration.landsat_ac.tasks.EnvConfig.working_dir",
        new_callable=PropertyMock,
    ) as mock_wd:
        mock_wd.return_value = tmp_path / JOB_ID

        source = EnvSource("test_source", provides=(CONFIG,))
        result = source.fetch()

        assert CONFIG in result
        cfg = result[CONFIG]
        assert cfg.job_id == JOB_ID
        assert cfg.granule == GRANULE
        assert cfg.granule_dir.exists()


def test_download_granule(mock_binaries, mock_config, monkeypatch):
    monkeypatch.setenv("GRANULE", GRANULE)
    task = DownloadGranule(
        "test_dl", requires=(CONFIG,), provides=(GRANULE_DIR, MTL_FILE)
    )
    outputs = task.run({CONFIG: mock_config})
    assert outputs[MTL_FILE].exists()


def test_parse_metadata(mock_config):
    task = ParseMetadata("test_meta", requires=(CONFIG,), provides=(METADATA,))
    outputs = task.run({CONFIG: mock_config})
    assert outputs[METADATA].output_name == "2020-01-01_025030"


def test_check_solar_zenith(mock_binaries, mock_config):
    (mock_config.granule_dir / f"{GRANULE}_MTL.txt").touch()
    task = CheckSolarZenith("test_solar", requires=(MTL_FILE,), provides=(SOLAR_VALID,))
    outputs = task.run({MTL_FILE: mock_config.granule_dir / f"{GRANULE}_MTL.txt"})
    assert outputs[SOLAR_VALID] is True


def test_run_fmask(mock_binaries, mock_config):
    task = RunFmask("test_fmask", requires=(CONFIG, GRANULE_DIR), provides=(FMASK_BIN,))
    outputs = task.run({CONFIG: mock_config, GRANULE_DIR: mock_config.granule_dir})
    assert outputs[FMASK_BIN].exists()
    assert outputs[FMASK_BIN].name == "fmask.bin"


def test_convert_scanline(mock_binaries, mock_config):
    # Create dummy TIF to convert
    tif = mock_config.granule_dir / "test.TIF"
    tif.touch()

    task = ConvertScanline(
        "test_scanline", requires=(GRANULE_DIR,), provides=(SCANLINE_DONE,)
    )
    outputs = task.run({GRANULE_DIR: mock_config.granule_dir})

    assert outputs[SCANLINE_DONE] is True
    # Verify file was replaced/touched by mock gdal_translate
    assert tif.exists()


def test_convert_to_espa(mock_binaries, mock_config, monkeypatch):
    # Ensure subprocess can find the right granule name
    monkeypatch.setenv("GRANULE", GRANULE)

    mtl = mock_config.granule_dir / f"{GRANULE}_MTL.txt"
    mtl.touch()

    task = ConvertToEspa(
        "test_espa", requires=(CONFIG, MTL_FILE, SCANLINE_DONE), provides=(ESPA_XML,)
    )
    outputs = task.run(
        {
            SCANLINE_DONE: True,
            CONFIG: mock_config,
            MTL_FILE: mtl,
            GRANULE_DIR: mock_config.granule_dir,
        }
    )

    assert outputs[ESPA_XML].exists()
    assert outputs[ESPA_XML].name == f"{GRANULE}.xml"


def test_run_lasrc(mock_binaries, mock_config):
    xml = mock_config.granule_dir / f"{GRANULE}.xml"
    xml.touch()

    task = RunLaSRC(
        "test_lasrc", requires=(ESPA_XML, SOLAR_VALID), provides=(LASRC_DONE,)
    )
    outputs = task.run({SOLAR_VALID: True, ESPA_XML: xml})

    assert outputs[LASRC_DONE] is True


def test_rename_angle_bands(mock_config):
    # Create dummy angle bands
    suffixes = [
        "_VAA.img",
        "_VAA.hdr",
        "_VZA.img",
        "_VZA.hdr",
        "_SAA.img",
        "_SAA.hdr",
        "_SZA.img",
        "_SZA.hdr",
    ]
    for s in suffixes:
        (mock_config.granule_dir / f"{GRANULE}{s}").touch()

    meta = ProcessingMetadata(output_name="NEW_NAME", bucket_key="foo")

    task = RenameAngleBands(
        "test_rename",
        requires=(CONFIG, METADATA, GRANULE_DIR, LASRC_DONE),
        provides=(RENAMED_ANGLES,),
    )
    outputs = task.run(
        {
            LASRC_DONE: True,
            CONFIG: mock_config,
            METADATA: meta,
            GRANULE_DIR: mock_config.granule_dir,
        }
    )

    assert outputs[RENAMED_ANGLES] is True
    assert (mock_config.granule_dir / "NEW_NAME_VAA.img").exists()
    assert not (mock_config.granule_dir / f"{GRANULE}_VAA.img").exists()


def test_create_hls_xml(mock_binaries, mock_config):
    espa_xml = mock_config.granule_dir / f"{GRANULE}.xml"
    espa_xml.touch()

    task = CreateHlsXml(
        "test_hls_xml", requires=(CONFIG, ESPA_XML, RENAMED_ANGLES), provides=(HLS_XML,)
    )
    outputs = task.run({RENAMED_ANGLES: True, CONFIG: mock_config, ESPA_XML: espa_xml})

    assert outputs[HLS_XML].exists()
    assert outputs[HLS_XML].name == f"{GRANULE}_hls.xml"


def test_convert_to_hdf(mock_binaries, mock_config):
    hls_xml = mock_config.granule_dir / f"{GRANULE}_hls.xml"
    hls_xml.touch()

    task = ConvertToHdf("test_hdf", requires=(HLS_XML,), provides=(SR_HDF,))
    outputs = task.run({HLS_XML: hls_xml})

    assert outputs[SR_HDF].exists()
    assert outputs[SR_HDF].name == "sr.hdf"


def test_add_fmask_sds(mock_binaries, mock_config):
    # Setup dependencies
    sr_hdf = mock_config.granule_dir / "sr.hdf"
    sr_hdf.touch()
    fmask_bin = mock_config.granule_dir / "fmask.bin"
    fmask_bin.touch()
    mtl = mock_config.granule_dir / "MTL.txt"
    mtl.touch()

    meta = ProcessingMetadata(output_name="OUTPUT_GRANULE", bucket_key="foo")

    task = AddFmaskSds(
        "test_add_fmask",
        requires=(CONFIG, METADATA, SR_HDF, FMASK_BIN, MTL_FILE),
        provides=(FINAL_HDF,),
    )
    outputs = task.run(
        {
            CONFIG: mock_config,
            METADATA: meta,
            SR_HDF: sr_hdf,
            FMASK_BIN: fmask_bin,
            MTL_FILE: mtl,
        }
    )

    assert outputs[FINAL_HDF].exists()
    assert outputs[FINAL_HDF].name == "OUTPUT_GRANULE.hdf"


def test_upload_results(mock_aws_s3, mock_config):
    s3: S3Client = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET_OUT)

    final_hdf = mock_config.granule_dir / "output.hdf"
    final_hdf.touch()

    meta = ProcessingMetadata(output_name="output", bucket_key="2020/001")

    task = UploadResults(
        "test_upload",
        requires=(CONFIG, METADATA, FINAL_HDF, GRANULE_DIR),
        provides=(UPLOAD_COMPLETE,),
    )
    outputs = task.run(
        {
            CONFIG: mock_config,
            METADATA: meta,
            FINAL_HDF: final_hdf,
            GRANULE_DIR: mock_config.granule_dir,
        }
    )

    assert outputs[UPLOAD_COMPLETE] is True

    objs = s3.list_objects(Bucket=BUCKET_OUT)
    keys = [o["Key"] for o in objs.get("Contents", [])]
    assert "2020/001/output.hdf" in keys
