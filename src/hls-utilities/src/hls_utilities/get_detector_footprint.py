import glob
import os

import click


@click.command()
@click.argument(
    "inputs2dir",
    type=click.Path(
        dir_okay=True,
        file_okay=False,
    ),
)
def main(inputs2dir):
    detfoo = None
    # determine the name of the {product_id} directory under GRANULE
    granule_dir = os.path.join(inputs2dir, "GRANULE")

    # check if it is an old format SAFE directory
    gml_path = glob.glob(f"{granule_dir}/*/QI_DATA/MSK_DETFOO_B06.gml")
    if len(gml_path) > 0:
        detfoo = gml_path[0]

    jp2_path = glob.glob(f"{granule_dir}/*/QI_DATA/MSK_DETFOO_B06.jp2")
    if len(jp2_path) > 0:
        detfoo = jp2_path[0]

    if detfoo is None:
        raise FileNotFoundError

    click.echo(detfoo)


if __name__ == "__main__":
    main()
