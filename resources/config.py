import os
import logging

from configparser import ConfigParser


def create_config(
    config_file_path: str,
    default_config: dict, 
    parser: ConfigParser, 
    logger: logging.Logger
) -> ConfigParser:
    logger.info(f"Create configuration file in project folder")
    if os.path.exists(config_file_path):
        os.remove(config_file_path)
    parser.read_dict(default_config)
    with open(config_file_path, "w") as file:
        parser.write(file)
    
    return parser


def delete_config(config_file_path: str, logger: logging.Logger):
    logger.info("Delete configuration file from project folder")
    os.remove(config_file_path)

