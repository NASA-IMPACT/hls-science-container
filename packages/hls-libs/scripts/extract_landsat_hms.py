#! /usr/bin/env python
import datetime
import sys
import warnings

import click
import rasterio

warnings.filterwarnings("ignore")


@click.command()
@click.argument(
    "inputhdfile",
    type=click.Path(
        dir_okay=False,
        file_okay=True,
    ),
)
def main(inputhdfile):
    with rasterio.open(inputhdfile) as dataset:
        tags = dataset.tags()
        sensing_time = tags["SENSING_TIME"]
        datedttimepattern = "%Y-%m-%dT%H:%M:%S"
        scene_time = datetime.datetime.strptime(
            sensing_time.split(".")[0], datedttimepattern
        )
        hms = scene_time.strftime("%H%M%S")
        sys.stdout.write(hms)


if __name__ == "__main__":
    main()
