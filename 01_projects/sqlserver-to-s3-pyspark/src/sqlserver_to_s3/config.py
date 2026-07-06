"""
config.py

Loads project configuration from a YAML file.

This module keeps environment-specific settings outside the code,
such as database names, table names, Spark settings, and output paths.
"""

from pathlib import Path
from typing import Any

import yaml


def load_config(config_path: str = "configs/dev.yaml") -> dict[str, Any]:
    """
    Load configuration settings from a YAML file.

    Parameters
    ----------
    config_path : str
        Path to the YAML configuration file.

    Returns
    -------
    dict
        Configuration values loaded from YAML.
    """

    # Convert the string path into a Path object.
    path = Path(config_path)

    # Stop the program early if the config file does not exist.
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # Open and read the YAML file.
    with path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    # Validate that the YAML file was not empty.
    if config is None:
        raise ValueError(f"Config file is empty: {config_path}")

    return config


# =====================================================================
# Interview Notes
# =====================================================================
#
# Question:
#
# Why did you create config.py instead of hard-coding values
# inside main.py?
#
# Suggested Answer:
#
# I externalized configuration to make the ETL pipeline easier
# to maintain, reuse, and deploy across multiple environments.
#
# For example, development, QA, and production may use different:
#
# - SQL Server names
# - Database names
# - Table names
# - Output paths
# - Spark settings
# - AWS S3 buckets
#
# By storing these values in dev.yaml instead of hard-coding them,
# I can change the environment without modifying application code.
#
# This improves portability, reduces risk, and follows good
# software engineering and DevOps practices.
#
# =====================================================================