# ============================================================
# DB MODULE (db.py)
# ============================================================
# PURPOSE:
# This module is responsible for database connectivity and
# database helper operations for the ETL pipeline.
#
# FIRST GOAL (learning phase):
# - Move database-related logic out of main.py into this file.
#
# WHAT SHOULD LIVE HERE:
# - build_engine() → create SQLAlchemy engine
# - test_connection() → verify SQL Server connection
# - get_table_count() → return row counts from target tables
# - Later: audit table inserts/updates
# - Later: reject table writes
#
# EXAMPLE RESPONSIBILITY:
# This module should answer:
# 👉 "How do I connect to the database and perform basic DB tasks safely?"
#
# BEST PRACTICES:
# - Keep connection logic in one place
# - Reuse helper functions instead of repeating SQL code
# - Keep transactions explicit when needed
# - Separate DB access from extraction and transformation logic
#
# WHAT SHOULD NOT BE HERE:
# - ❌ CSV file reading
# - ❌ Data cleansing / transformation rules
# - ❌ Full ETL orchestration
#
# END RESULT:
# main.py should NOT build raw DB connections itself.
# Instead, it should call functions from this module like:
#
#     from athletes_etl.db import build_engine, test_connection, get_table_count
#
# DESIGN PRINCIPLE:
# 👉 "db.py handles database access, not pipeline business logic."
#
# ============================================================

# ============================================================
# DB MODULE
# ============================================================
# Handles:
# - Creating SQLAlchemy engine
# - Testing DB connection
# - Basic database helper functions
# ============================================================
# ============================================================
# DB HELPER FUNCTION
# ============================================================
# get_table_count() belongs in db.py because it performs a
# database read operation and returns a SQL table row count.
#
# WHY MOVE IT:
# - Keeps main.py cleaner
# - Centralizes reusable DB helper logic
# - Makes the database module responsible for DB access
#
# main.py should call:
#     get_table_count(engine, "dbo.Athletes")
#
# instead of defining SQL helper functions directly.
# ============================================================


from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd
import json


def build_engine(dsn: str, db: str, user: str, password: str):
    """
    Build and return a SQLAlchemy engine for SQL Server.
    """
    connection_url = URL.create(
        "mssql+pyodbc",
        username=user,
        password=password,
        host=None,  # DSN handles server name
        database=db,
        query={
            "dsn": dsn,
            "TrustServerCertificate": "yes"
        }
    )

    return create_engine(connection_url)

def test_connection(engine) -> str:
    """
    Test the database connection and return the current DB name.
    """
    with engine.begin() as conn:
        return conn.execute(text("SELECT DB_NAME()")).scalar()
    
def get_table_count(engine, table_fullname: str) -> int:
    """
    Return row count for a fully qualified table name.
    Example: dbo.Athletes
    """
    df = pd.read_sql(f"SELECT COUNT(*) AS row_count FROM {table_fullname}", engine)
    return int(df.loc[0, "row_count"])

# -------------------------
# HELPERS => FUNCTION
# -------------------------


def insert_audit_run_start(conn, audit_table: str, run_id: str, pipeline_name: str, source_name: str, target_table: str):
    """
    Inserts a RUNNING row in the audit table at start of pipeline.
    """
    conn.execute(
        text(f"""
            INSERT INTO {audit_table} (
                run_id, pipeline_name, started_at, source_name, target_table,
                status, message
            )
            VALUES (
                :run_id, :pipeline_name, SYSUTCDATETIME(), :source_name, :target_table,
                'RUNNING', 'ETL started'
            )
        """),
        {
            "run_id": run_id,
            "pipeline_name": pipeline_name,
            "source_name": source_name,
            "target_table": target_table,
        }
    )


def update_audit_run_finish(conn, audit_table: str, run_id: str, payload: dict):
    """
    Updates the audit row at the end with metrics + status.
    """
    conn.execute(
        text(f"""
            UPDATE {audit_table}
            SET
                finished_at       = SYSUTCDATETIME(),
                source_row_count  = :source_row_count,
                target_before     = :target_before,
                inserted_rows     = :inserted_rows,
                target_after      = :target_after,
                row_diff          = :row_diff,
                source_dupes      = :source_dupes,
                target_dupes      = :target_dupes,
                rejected_rows     = :rejected_rows,
                status            = :status,
                message           = :message,
                duration_seconds  = :duration_seconds
            WHERE run_id = :run_id
        """),
        {
            "run_id": run_id,
            **payload
        }
    )

# -------------------------
# REJECT FUNCTIONS
# -------------------------
def write_rejects(engine, run_id, rejects_df: pd.DataFrame, reason) -> int:
    """
    Writes rejects to dbo.Athletes_Rejects.
    Stores run_id, athlete_id (if present), reason, and the full row as JSON.
    Returns number of rows written.

    NOTE (important fix):
    - reason can be a STRING (same reason for all rows)
    - OR reason can be a pd.Series (a different reason per row, aligned by index)
    """
    if rejects_df.empty:
        return 0

    out = pd.DataFrame({
        "run_id": run_id,
        "athlete_id": rejects_df["athlete_id"] if "athlete_id" in rejects_df.columns else None,
        "raw_row_json": rejects_df.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1),
    })

    if isinstance(reason, pd.Series):
        # Align reason by index (works for subsets like string-length rejects)
        out["reason"] = reason.loc[rejects_df.index].astype(str).values
    else:
        out["reason"] = str(reason)

    out.to_sql(
        name="Athletes_Rejects",
        con=engine,
        schema="dbo",
        if_exists="append",
        index=False
    )
    return len(out)

# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: Why did you move engine creation to db.py?
#
# A:
# I separated database connection logic into a dedicated module
# to improve reusability, maintainability, and separation of concerns.
# This allows the main pipeline to remain focused on orchestration,
# while the database module handles connectivity.
#
# ------------------------------------------------------------
# ⚠️ COMMON MISTAKE TO AVOID
# ------------------------------------------------------------
# ❌ Do NOT move configuration access (like cfg["sqlserver"]["dsn"])
#    into db.py
#
# WHY:
# - db.py should NOT know about YAML structure
# - db.py should only receive clean, explicit parameters
#   (dsn, db, user, password)
#
# 👉 This keeps modules loosely coupled and easier to reuse/test
#
# ============================================================

# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: Why pass audit_table and pipeline values as parameters
#    instead of using globals inside db.py?
#
# A:
# I pass them as parameters to keep db.py loosely coupled and reusable.
# The database module should execute SQL operations, but it should not
# depend on configuration or global variables defined in main.py.
# This improves maintainability, testability, and separation of concerns.
# ============================================================

# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: After moving write_rejects() into db.py, do you need to
#    change the calls in main.py?
#
# A:
# If the function signature remains the same, the existing calls
# can stay unchanged. The main updates are to import the function
# from db.py and remove the local copy from main.py.
#
# If I want a cleaner design, I can further improve the function
# by passing the reject table name as a parameter instead of
# hardcoding it inside db.py.
# ============================================================