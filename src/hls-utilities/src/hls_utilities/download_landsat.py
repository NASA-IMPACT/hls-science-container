from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class KeyDoesNotExist(Exception):
    pass


def key_exists(client: S3Client, bucket: str, path: str) -> bool:
    result = client.list_objects_v2(
        Bucket=bucket, Prefix=path, RequestPayer="requester"
    )
    return result.get("KeyCount", 0) > 0


def download_files(
    client: S3Client, bucket: str, path: str, output_directory: Path | str
) -> None:
    result = client.list_objects_v2(
        Bucket=bucket, Prefix=path, RequestPayer="requester"
    )
    for content in result.get("Contents", []):
        key = content["Key"]
        filename = Path(key).name
        output_file = Path(output_directory).joinpath(filename)
        client.download_file(
            bucket, key, str(output_file), ExtraArgs={"RequestPayer": "requester"}
        )


def get_updated_key(client: S3Client, bucket: str, path: str) -> str:
    path_root = Path(path).parent
    result = client.list_objects_v2(
        Bucket=bucket,
        Prefix=str(path_root) + "/",
        RequestPayer="requester",
        Delimiter="/",
    )
    if result.get("KeyCount") == 0:
        raise KeyDoesNotExist
    else:
        updated_key = [
            prefix["Prefix"]
            for prefix in result.get("CommonPrefixes", [])
            if prefix["Prefix"].split("_")[3] == path.split("_")[3]
        ]
        if len(updated_key) == 0:
            raise KeyDoesNotExist
        else:
            return updated_key[0]


def get_landsat(bucket: str, path: str, output_directory: Path | str) -> str:
    """Download a Landsat granule, returning the (possibly updated) granule ID"""
    client = boto3.client("s3")
    if key_exists(client, bucket, path):
        download_files(client, bucket, path, output_directory)
        id = Path(path).parts[-1]
        return id
    else:
        updated_path = get_updated_key(client, bucket, path)
        download_files(client, bucket, updated_path, output_directory)
        updated_id = Path(updated_path).parts[-1]
        return updated_id
