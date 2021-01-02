import click
from .config import configuration

import logging
from .jobs import send_to_warehouse


logging.basicConfig(level=logging.INFO)


@click.group()
def boatman():
    """Send SegmentSpec event files to Warehouses"""


@boatman.command()
@click.option("--config-file", "-cf", type=click.Path(exists=True))
@click.option("--source-dir", "-sd", type=click.Path(exists=True))
@click.option("--app", "-a", required=True)
def send(config_file: str, source_dir: str, app: str):
    """Send Segment Files to different warehouses """
    logging.info(f"config_file={config_file}")
    boatman_conf = configuration.from_yaml(config_file)

    job = send_to_warehouse.SendToWarehouseJob(boatman_conf, source_dir, app)
    job.execute()
