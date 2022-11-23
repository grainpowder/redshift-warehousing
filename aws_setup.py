import boto3
import os
import typer
import json
import logging
import sys

from configparser import ConfigParser, ExtendedInterpolation
from resources import *
from typer import Typer


CONFIG_FILE_PATH = f"{os.getcwd()}/dwh.cfg"
with open(f"{os.getcwd()}/resources/default_config.json", "r") as file:
    DEFAULT_CONFIG = json.load(file)
app = Typer()


def make_logger(name: str) -> logging.Logger:
    """
    Return a logger that prints log messages in defined format
    """
    # Define log format to be used in each of handler
    formatter = logging.Formatter(
        fmt="%(asctime)s | (%(funcName)s) : %(msg)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Define handler for console printouts
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    # Make logger and attach each of handler to the logger
    logger = logging.getLogger(name)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)

    return logger


@app.command("build-resources")
def build_resources(
    admin_profile: str = typer.Argument(...),
    db_password: str = typer.Argument(...)
):
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    logger = make_logger(__name__)

    DEFAULT_CONFIG["DEFAULT"]["admin_profile"] = admin_profile
    parser = create_config(CONFIG_FILE_PATH, DEFAULT_CONFIG, parser, logger)
    session = boto3.Session(
        profile_name=parser.get("DEFAULT", "admin_profile"), 
        region_name=parser.get("DEFAULT", "region")
    )
    parser = create_vpc(parser, logger, session)
    parser = create_iam_role(parser, logger, session)
    parser = create_cluster(parser, logger, session, db_password)

    with open(CONFIG_FILE_PATH, "w") as file:
        parser.write(file)


@app.command("delete-resources")
def delete_resources():
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    parser.read(CONFIG_FILE_PATH)
    logger = make_logger(__name__)

    session = boto3.Session(
        profile_name=parser.get("DEFAULT", "admin_profile"), 
        region_name=parser.get("DEFAULT", "region")
    )
    delete_cluster(parser, logger, session)
    delete_iam_role(parser, logger, session)
    delete_vpc(parser, logger, session)
    delete_config(CONFIG_FILE_PATH, logger)


if __name__ == "__main__":
    app()
