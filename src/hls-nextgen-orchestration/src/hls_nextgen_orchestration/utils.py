from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def s3_client(role_arn: str | None = None) -> "S3Client":
    """Create an S3 client, optionally assuming an IAM role first."""
    if role_arn:
        sts = boto3.client("sts")
        creds = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="hls-upload",
        )["Credentials"]
        return boto3.client(
            "s3",
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
    return boto3.client("s3")


def validate_command(command: str) -> None:
    """
    Checks if a command exists on the PATH.

    Parameters
    ----------
    command : str
        The name of the command executable to check.

    Raises
    ------
    RuntimeError
        If the command is not found on the PATH.
    """
    if not shutil.which(command):
        raise RuntimeError(f"Required command '{command}' not found on PATH.")
