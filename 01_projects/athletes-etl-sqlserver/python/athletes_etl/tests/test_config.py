# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: How did you test your extract layer?
#
# A:
# I wrote unit tests for the extraction layer by creating
# temporary CSV files using pytest fixtures. This allowed me
# to validate that the extract function correctly reads data
# into DataFrames without relying on external files.
#
# This ensures reliability and makes the extraction logic
# independently testable.
# ============================================================

from athletes_etl.config import get_sql_password, load_config
import os


def test_get_sql_password():
    os.environ["SQLSERVER_PASSWORD"] = "test123"

    cfg = {
        "sqlserver": {
            "password_env": "SQLSERVER_PASSWORD"
        }
    }

    result = get_sql_password(cfg)

    assert result == "test123"

def test_load_config():
    cfg = load_config("dev")

    assert isinstance(cfg, dict)
    assert "pipeline" in cfg
    assert "extract" in cfg
    assert "sqlserver" in cfg
    assert "load" in cfg
    assert "reject_rules" in cfg
    assert "dedupe" in cfg

# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: What was your first unit test in your ETL project?
#
# A:
# My first unit test validated the config helper responsible for
# retrieving SQL Server credentials from environment variables.
# I started with this function because it is isolated, reusable,
# and critical to secure database connectivity.
#
# This gave me a foundation for expanding test coverage across
# extract and database helper modules.
# ============================================================