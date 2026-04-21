# ============================================================
# TEST DB MODULE (test_db.py)
# ============================================================
# PURPOSE:
# This module contains integration tests for the db module.
#
# FIRST GOAL (learning phase):
# - Validate that database helper functions work correctly
#   and can safely connect to SQL Server.
#
# WHAT SHOULD LIVE HERE:
# - Tests for test_connection()
# - Tests for get_table_count()
# - Validation of database connectivity and read operations
#
# EXAMPLE RESPONSIBILITY:
# This module should answer:
# 👉 "Can the ETL pipeline safely connect to and read from the database?"
#
# BEST PRACTICES:
# - Use environment variables for credentials (no hardcoding)
# - Treat DB tests as integration tests (not pure unit tests)
# - Keep tests read-only (no inserts/updates/deletes initially)
# - Ensure tests fail gracefully or skip when credentials are missing
#
# WHAT SHOULD NOT BE HERE:
# - ❌ Hardcoded passwords or secrets
# - ❌ Data transformation logic
# - ❌ Full ETL orchestration
#
# EXECUTION STEPS (PowerShell):
# $env:SQLSERVER_PASSWORD="your_password_here"
# .\.venv\Scripts\Activate
# pytest tests/test_db.py -v
#
# END RESULT:
# Ensures the database layer is reliable and can establish
# connections and perform basic read operations before
# being used in the full ETL pipeline.
#
# ============================================================

# ============================================================
# TEST DB MODULE
# ============================================================
# Handles:
# - Testing SQL Server connection
# - Validating table row count queries
# ============================================================

# ============================================================
# TEST HELPER FUNCTIONS
# ============================================================
# _get_engine():
# - Builds SQLAlchemy engine using config + environment variables
#
# test_db_connection():
# - Ensures SQL Server connection works and returns DB name
#
# test_db_get_table_count():
# - Ensures row count query executes successfully
#
# DESIGN PRINCIPLE:
# 👉 "Integration tests validate real system behavior, not mocks."
# ============================================================

# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: Why treat database tests as integration tests?
#
# A:
# Database tests involve real external systems such as SQL Server,
# so they are classified as integration tests rather than unit tests.
# I designed these tests to validate connectivity and basic read
# operations while avoiding write operations to ensure safety.
# This approach ensures the database layer is reliable before
# integrating it into the full ETL pipeline.
# ============================================================

# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: How do you handle credentials in tests?
#
# A:
# I avoid hardcoding credentials in test files and instead use
# environment variables to securely inject secrets at runtime.
# For database-related tests, I ensure they only run when required
# environment variables are present, making them safe and portable
# across environments.
# ============================================================

import os
import pytest

from athletes_etl.db import test_connection as db_test_connection, get_table_count, build_engine
from athletes_etl.config import load_config, get_sql_password


def _get_engine():
    cfg = load_config("dev")
    password = get_sql_password(cfg)

    return build_engine(
        cfg["sqlserver"]["dsn"],
        cfg["sqlserver"]["database"],
        cfg["sqlserver"]["username"],
        password
    )


# Skip tests if password is not set
pytestmark = pytest.mark.skipif(
    not os.getenv("SQLSERVER_PASSWORD"),
    reason="SQLSERVER_PASSWORD not set"
)


def test_db_connection():
    engine = _get_engine()

    db_name = db_test_connection(engine)

    assert isinstance(db_name, str)
    assert len(db_name) > 0


def test_db_get_table_count():
    engine = _get_engine()

    count = get_table_count(engine, "dbo.Athletes")

    assert isinstance(count, int)
    assert count >= 0


# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: How do you handle credentials in tests?
#
# A:
# I avoid hardcoding credentials in test files and instead use
# environment variables to securely inject secrets at runtime.
# For database-related tests, I treat them as integration tests
# and ensure they only run when required environment variables
# are present.
# ============================================================