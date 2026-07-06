# ============================================================
# PYTEST SHARED FIXTURES (conftest.py)
# ============================================================
# PURPOSE:
# Provides reusable pytest fixtures for the test suite.
#
# The spark() fixture creates one SparkSession that can be
# reused across tests.
# ============================================================

from pathlib import Path

import pytest

from sqlserver_to_s3.config import load_config
from sqlserver_to_s3.spark_session import create_spark_session


CONFIG_FILE = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "dev.yaml"
)


@pytest.fixture(scope="session")
def spark():
    """
    Create one reusable SparkSession for tests.
    """

    cfg = load_config(CONFIG_FILE)

    spark_session = create_spark_session(
        app_name="pytest-spark-session",
        master=cfg["spark"]["master"],
        jdbc_driver=cfg["spark"]["jdbc_driver"],
    )

    yield spark_session

    spark_session.stop()