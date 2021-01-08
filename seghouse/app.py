import click
from .config import configuration

import logging
from .jobs import send_to_warehouse
from .util import aws_wrapper
import shutil


logging.basicConfig(level=logging.INFO)


@click.group()
def boatman():
    """Send SegmentSpec event files to Warehouses"""


@boatman.command()
@click.option("--config-file", "-cf", type=click.Path(exists=True))
@click.option("--s3-dir", "-s3d", help="S3 Directory. We will look for *.gz files in this directory. Ensure that you have configured aws credentials using aws cli. Make sure that this directory contains files less than 100.")
@click.option("--source-dir", "-sd", type=click.Path(exists=True))
@click.option("--app", "-a", required=True, help="Will be used to create database in warehouse",)
def send(config_file: str, s3_dir:str, source_dir: str, app: str):
    """Send Segment Files to different warehouses """
    logging.info(f"config_file={config_file}")
    boatman_conf = configuration.from_yaml(config_file)

    if s3_dir:
    	source_dir = aws_wrapper.download_gz_files(s3_dir)

    job = send_to_warehouse.SendToWarehouseJob(boatman_conf, source_dir, app)
    job.execute()

    shutil.rmtree(source_dir)

