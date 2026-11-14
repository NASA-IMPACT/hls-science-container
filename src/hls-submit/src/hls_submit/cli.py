"""``hls-submit`` command line entry point."""

from __future__ import annotations

from typing import Any

import click
from botocore.exceptions import BotoCoreError, ClientError

from .fmask import fmask
from .lasrc import lasrc
from .terminate import terminate


class _AwsErrorGroup(click.Group):
    """Report AWS errors (expired credentials, missing stack) without a traceback."""

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except (BotoCoreError, ClientError) as err:
            raise click.ClickException(f"AWS error: {err}") from err


@click.group(cls=_AwsErrorGroup)
def cli() -> None:
    """Submit and manage HLS science experiment jobs on AWS Batch."""


cli.add_command(fmask)
cli.add_command(lasrc)
cli.add_command(terminate)
