#! /usr/bin/env python
import warnings
warnings.filterwarnings("ignore")

import datetime
import sys
import click
import rasterio


@click.command()
@click.argument(
    "inputhdfile",
    type=click.Path(dir_okay=False, file_okay=True,),
)
def main(inputhdfile):
    with rasterio.open(inputhdfile) as dataset:
        tags = dataset.tags()
        sensing_time = tags["SENSING_TIME"]
        datedttimepattern = "%Y-%m-%dT%H:%M:%S"
        scene_time = datetime.datetime.strptime(
            sensing_time.split(".")[0], datedttimepattern
        )
        hms = "%s%s%s" % (
            str(scene_time.hour).zfill(2),
            str(scene_time.minute).zfill(2),
            str(scene_time.second).zfill(2)
        )
        sys.stdout.write(hms)


if __name__ == '__main__':
    main()
