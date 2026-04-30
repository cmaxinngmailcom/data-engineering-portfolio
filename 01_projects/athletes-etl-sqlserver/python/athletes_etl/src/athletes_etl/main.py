# ============================================================
# ✅ WHERE THIS SCRIPT REFERENCES configs/dev.yaml (ALL PLACES)
# ============================================================
#
# 1) Reads the YAML file itself:
#    - load_config() loads:  {project_root}/configs/dev.yaml
#    - cfg = load_config("dev")  ==> this is the in-memory dict from YAML
#
# 2) Pipeline identity (from YAML):
#    - cfg["pipeline"]["name"]        -> PIPELINE_NAME
#    - cfg["pipeline"]["source_name"] -> SOURCE_NAME
#
# 3) Extract settings (from YAML):
#    - cfg["extract"]["csv_path"]     -> CSV_PATH
#
# 4) SQL Server tables + connection pieces (from YAML):
#    - cfg["sqlserver"]["target_table"] -> TARGET_TABLE
#    - cfg["sqlserver"]["audit_table"]  -> AUDIT_TABLE
#    - cfg["sqlserver"]["reject_table"] -> REJECT_TABLE
#    - cfg["sqlserver"]["dsn"]          -> dsn
#    - cfg["sqlserver"]["database"]     -> db
#    - cfg["sqlserver"]["username"]     -> user
#    - cfg["sqlserver"]["password_env"] -> pwd_env  (name of Windows env var)
#
# 5) Load behavior (from YAML):
#    - cfg["load"]["is_chunked_inserts"] -> IS_CHUNKED_INSERTS
#    - cfg["load"]["chunk_size"]         -> CHUNK_SIZE
#
# 6) Reject rules (from YAML):
#    - cfg["reject_rules"]["string_limits"] -> string_limits
#    - cfg["reject_rules"]["numeric_ranges"]["height_cm"] -> height_min/height_max/height_allow_zero
#    - cfg["reject_rules"]["numeric_ranges"]["weight_kg"] -> weight_min/weight_max/weight_allow_zero
#    - cfg["reject_rules"]["date_sanity"]["min_born_date"]         -> min_born
#    - cfg["reject_rules"]["date_sanity"]["default_date"]          -> default_date_str/default_date
#    - cfg["reject_rules"]["date_sanity"]["reject_born_after_today"] -> reject_born_after_today
#    - cfg["reject_rules"]["date_sanity"]["reject_died_before_born"] -> reject_died_before_born
#
# 7) Dedupe behavior (from YAML):
#    - cfg["dedupe"]["enabled"]  -> DEDUPE_ENABLED
#    - cfg["dedupe"]["hash_cols"] -> hash_cols
#
# NOTE:
# - The actual SQL password is NOT in YAML.          *******************VERY IMPORTANT*******************
# - YAML only gives the ENV VAR NAME (password_env). *******************VERY IMPORTANT*******************
# - The real password is fetched from Windows using: *******************VERY IMPORTANT*******************
#   os.getenv(pwd_env)                               *******************VERY IMPORTANT*******************
# ============================================================

# Added: yaml was imported  =>  READS configs/dev.yaml
#

import os
import time
import json
import uuid
# import hashlib
from pathlib import Path
from urllib.parse import quote_plus

import yaml
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from datetime import date  # ✅ Needed for date.today()

from athletes_etl.config import load_config # for config.py file

from athletes_etl.extract import extract_csv # for extract.py file

from athletes_etl.config import get_sql_password # for config.py file

from athletes_etl.db import (
    build_engine,
    test_connection,
    get_table_count,
    insert_audit_run_start,
    update_audit_run_finish,
    write_rejects,
) # for db.py file


from athletes_etl.transform import (
    clean_athletes_data,
    split_rejects,
    dedupe_rows,
    build_string_length_rejects,
    build_numeric_reject_mask,
    build_date_reject_mask,
) # for transform.py

from athletes_etl.logging_utils import setup_logger # for logging_utils.py

logger = setup_logger()

# -------------------------
# To find working directory
# -------------------------

# print("📍 CURRENT WORKING DIRECTORY:", os.getcwd())
# print("📍 SCRIPT LOCATION:", Path(__file__).resolve())

# -------------------------
# CONFIG (NOW READS configs/dev.yaml)
# -------------------------
# ✅ "Production style" change:
# - All settings live in dev.yaml
# - Password is NOT in yaml, it comes from Windows env var SQLSERVER_PASSWORD *******************VERY IMPORTANT*******************
# - Same code can run dev/prod by swapping yaml files   *******************VERY IMPORTANT*******************

# def load_config(env: str = "dev") -> dict:
   
#    Loads configs/{env}.yaml from the project root (python/athletes_etl).
#    This works even when you run from different folders in VS Code.
   
#    project_root = Path(__file__).resolve().parents[2]  # ...\python\athletes_etl
#    cfg_path = project_root / "configs" / f"{env}.yaml"
#    with open(cfg_path, "r", encoding="utf-8") as f:
#        return yaml.safe_load(f)

# cfg = load_config("dev")


cfg = load_config("dev")


# -------------------------
# PROFILING 1: PERFORMANCE LOGGING HELPER
# -------------------------
def log_step_time(step_name, start_time):
    elapsed = time.perf_counter() - start_time
    logger.info(f"{step_name} took {elapsed:.2f} sec")
    return elapsed




PIPELINE_NAME = cfg["pipeline"]["name"]
SOURCE_NAME = cfg["pipeline"]["source_name"]
CSV_PATH = cfg["extract"]["csv_path"]

TARGET_TABLE = cfg["sqlserver"]["target_table"]
AUDIT_TABLE = cfg["sqlserver"]["audit_table"]
REJECT_TABLE = cfg["sqlserver"]["reject_table"]

# ✅ Chunked inserts toggle (from YAML)
IS_CHUNKED_INSERTS = bool(cfg["load"]["is_chunked_inserts"])
CHUNK_SIZE = int(cfg["load"]["chunk_size"])

# ✅ Reject rules from YAML
string_limits = cfg["reject_rules"]["string_limits"]

height_min = int(cfg["reject_rules"]["numeric_ranges"]["height_cm"]["min"])
height_max = int(cfg["reject_rules"]["numeric_ranges"]["height_cm"]["max"])
height_allow_zero = bool(cfg["reject_rules"]["numeric_ranges"]["height_cm"].get("allow_zero", True))

weight_min = int(cfg["reject_rules"]["numeric_ranges"]["weight_kg"]["min"])
weight_max = int(cfg["reject_rules"]["numeric_ranges"]["weight_kg"]["max"])
weight_allow_zero = bool(cfg["reject_rules"]["numeric_ranges"]["weight_kg"].get("allow_zero", True))

min_born = pd.Timestamp(cfg["reject_rules"]["date_sanity"]["min_born_date"])
default_date_str = cfg["reject_rules"]["date_sanity"]["default_date"]
default_date = pd.Timestamp(default_date_str)
reject_born_after_today = bool(cfg["reject_rules"]["date_sanity"]["reject_born_after_today"])
reject_died_before_born = bool(cfg["reject_rules"]["date_sanity"]["reject_died_before_born"])

# ✅ Hash dedupe from YAML
DEDUPE_ENABLED = bool(cfg["dedupe"]["enabled"])
hash_cols = cfg["dedupe"]["hash_cols"]


# This section is used when not using DOCKER

#dsn = cfg["sqlserver"]["dsn"]
#db = cfg["sqlserver"]["database"]
#user = cfg["sqlserver"]["username"]
#password = get_sql_password(cfg)
#engine = build_engine(dsn, db, user, password)
#########################################################

# Used since we're using DOCKER

server = cfg["sqlserver"]["server"]
db = cfg["sqlserver"]["database"]
user = cfg["sqlserver"]["username"]
driver = cfg["sqlserver"]["driver"]

password = get_sql_password(cfg)

engine = build_engine(server, db, user, password, driver)

# ------------------------------------------------------------
# ✅ TEST DATABASE CONNECTION (debug / validation step)
# ------------------------------------------------------------
print("DB_NAME() =", test_connection(engine))


# Quick test: confirm which DB you're connected to
with engine.begin() as conn:
    print("DB_NAME() =", conn.execute(text("SELECT DB_NAME()")).scalar())


def split_rejects(df: pd.DataFrame, mask: pd.Series):
    """Return (good_rows, bad_rows) given a boolean mask where True means reject."""
    bad = df.loc[mask].copy()
    good = df.loc[~mask].copy()
    return good, bad


# -------------------------
# PROFILLING 2: Add TOTAL TIMER (top of ETL)
# -------------------------
total_start = time.perf_counter()

# -------------------------
# START AUDIT RUN
# -------------------------
run_id = str(uuid.uuid4())

with engine.begin() as conn:
    insert_audit_run_start(
        conn,
        AUDIT_TABLE,
        run_id,
        PIPELINE_NAME,
        SOURCE_NAME,
        TARGET_TABLE,
    )

# -------------------------
# EXTRACT
# -------------------------
# athletes = pd.read_csv(CSV_PATH)
# source_row_count = len(athletes)


# ------------------------------
# PROFILLING 3: PROFILE EXTRACT
# ------------------------------

step_start = time.perf_counter()

athletes = extract_csv(CSV_PATH)
source_row_count = len(athletes)

extract_elapsed = log_step_time("Extract CSV", step_start)

# ------------------------------
# PROFILLING 4: PROFILE CLEANING
# ------------------------------

step_start = time.perf_counter()

athletes = clean_athletes_data(athletes, default_date)

clean_elapsed = log_step_time("Clean athletes data", step_start)
logger.info("Data cleaning complete")



# ----------------------------------------------
# 🚫 STRING LENGTH REJECTS (prevent truncation)
# ----------------------------------------------
# Goal:
# If a string is longer than the SQL Server column limit,
# reject the row instead of crashing the load.
#
# Example:
# dbo.Athletes.name is NVARCHAR(200)
# If CSV has 250 chars, SQL Server throws truncation error
# -> we catch it here and send the row to dbo.Athletes_Rejects
###############################
# What you’ll see after running
###############################
# dbo.Athletes loads without truncation crashes
# dbo.Athletes_Rejects will have rows like:
# reason:
# String length exceeds SQL column limit: born_city length 132 > 100; name length 250 > 200;
# ----------------------------------------------

# Build a mask of "bad rows" (any string column exceeds its limit)

# We'll also build a per-row reason string describing exactly what failed


# -----------------------------------------
# PROFILLING 5: PROFILE STRING LENGTH CHECK
# -----------------------------------------
step_start = time.perf_counter()

too_long_mask, reasons = build_string_length_rejects(athletes, string_limits)


# If any rows violate limits, write them to rejects and remove from load DF
if too_long_mask.any():
    bad_rows = athletes.loc[too_long_mask].copy()

    # ✅ Write rejects to SQL (reason is per row, so we pass a Series)
    write_rejects(
        engine,
        run_id,
        bad_rows,
        "String length exceeds SQL column limit: " + reasons
    )

    # Remove bad rows from athletes (so insert won't fail)
    athletes = athletes.loc[~too_long_mask].copy()

    # print(f"🧾 String-length rejects written: {too_long_mask.sum()}")  # ✅ COMMENTED (repeats totals later)
else:
    print("✅ No string-length truncation risks detected")

string_elapsed = log_step_time("String length checks", step_start)

# -------------------------
# REJECT RULES (bad rows) + write to dbo.Athletes_Rejects
# -------------------------
# Stupid-simple: we split rows into:
# - good_rows => load to dbo.Athletes
# - bad_rows  => store in dbo.Athletes_Rejects with a reason

reject_records = []

def add_rejects(mask, reason):
    """
    mask = boolean Series selecting bad rows
    reason = string reason for reject
    """
    bad = athletes.loc[mask].copy()
    if bad.empty:
        return

    for _, row in bad.iterrows():
        reject_records.append({
            "athlete_id": int(row["athlete_id"]) if pd.notna(row.get("athlete_id")) and str(row.get("athlete_id")).isdigit() else None,
            "reason": reason,
            "raw_row_json": json.dumps(row.to_dict(), default=str)
        })

# Rule 1: athlete_id is required
add_rejects(athletes["athlete_id"].isna(), "athlete_id is NULL (required)")

# Rule 2: athlete_id must be numeric
add_rejects(~athletes["athlete_id"].astype("string").str.fullmatch(r"\d+", na=False), "athlete_id is not a valid integer")

# Rule 3: duplicate athlete_id in the CSV batch
# (this prevents PK violations if athlete_id is the PK)
dup_id_mask = athletes["athlete_id"].astype("string").duplicated(keep=False)
add_rejects(dup_id_mask, "Duplicate athlete_id in source file")

# Build rejects_df
rejects_df = pd.DataFrame(reject_records)
rejected_rows = len(rejects_df)

# Remove rejected rows from athletes before load
if rejected_rows > 0:
    reject_mask = (
        athletes["athlete_id"].isna()
        | (~athletes["athlete_id"].astype("string").str.fullmatch(r"\d+", na=False))
        | (athletes["athlete_id"].astype("string").duplicated(keep=False))
    )
    athletes = athletes.loc[~reject_mask].copy()

# Now safe to convert athlete_id to int
athletes["athlete_id"] = athletes["athlete_id"].astype(int)

# Write rejects to SQL Server
if rejected_rows > 0:
    write_rejects(
        engine,
        run_id,
        rejects_df,
        "REJECT_RULES: athlete_id invalid/null/duplicate"
    )

logger.info(f"Rejected rows written: {rejected_rows}")

# -------------------------
# NUMERIC RANGE REJECTS
# -------------------------
# Pick realistic ranges (adjust if you want)
# Height in cm: 90–250
# Weight in kg: 20–300

# --------------------------------
# PROFILLING 12: NUMERIC PROFILING
# --------------------------------

# -------------------------
# PROFILE NUMERIC REJECTS
# -------------------------
step_start = time.perf_counter()

numeric_reject_mask = build_numeric_reject_mask(
    athletes,
    height_min,
    height_max,
    height_allow_zero,
    weight_min,
    weight_max,
    weight_allow_zero,
)

athletes, numeric_rejects = split_rejects(athletes, numeric_reject_mask)

numeric_reason = (
    f"NUMERIC_RANGE: height must be {height_min}-{height_max}cm"
    + (" (or 0)" if height_allow_zero else "")
    + f", weight must be {weight_min}-{weight_max}kg"
    + (" (or 0)" if weight_allow_zero else "")
)

numeric_reject_count = write_rejects(
    engine,
    run_id,
    numeric_rejects,
    numeric_reason
)

numeric_elapsed = log_step_time("Numeric reject checks", step_start)

logger.info(f"Numeric-range rejects written: {numeric_reject_count}")
# -------------------------
# DATE SANITY REJECTS
# -------------------------
today = pd.Timestamp(date.today())  # local date

# Convert to Timestamp for comparisons (your cols may be python date objects)
# Rule: born_date cannot be after today

# ---------------------------------
# PROFILLING 7: PROFILE DATE SANITY
# ---------------------------------

step_start = time.perf_counter()

date_reject_mask = build_date_reject_mask(
    athletes,
    today,
    min_born,
    default_date_str,
    reject_born_after_today,
    reject_died_before_born,
)

athletes, date_rejects = split_rejects(athletes, date_reject_mask)

date_reason = (
    f"DATE_SANITY: born_date must be <= today (if enabled) and >= {min_born.date()}; "
    f"died_date must be >= born_date (unless default {default_date.date()})"
)

date_reject_count = write_rejects(engine, run_id, date_rejects, date_reason)

date_elapsed = log_step_time("Date sanity checks", step_start)

logger.info(f"Date-sanity rejects written: {date_reject_count}")

# -------------------------
# HASH-BASED DUPLICATE DETECTION (within source batch)
# -------------------------
source_dupes = 0

# ----------------------------
# PROFILLING 8: PROFILE DEDUPE
# ----------------------------

step_start = time.perf_counter()

if DEDUPE_ENABLED:
    athletes, source_dupes = dedupe_rows(athletes, hash_cols)
    dedupe_elapsed = log_step_time("Hash dedupe", step_start)
    logger.info(f"Source duplicates removed (hash): {source_dupes}")
else:
    dedupe_elapsed = 0
    logger.info("Source dedupe disabled (dedupe.enabled=false)")



# -------------------------
# BEFORE COUNT
# -------------------------
target_before = get_table_count(engine, TARGET_TABLE)

# -------------------------
# LOAD (INSERT) + TIMING
# -------------------------

# ---------------------------------
# PROFILLING 9: IMPROVE LOAD TIMING
# ---------------------------------

load_start = time.perf_counter()

status = "SUCCESS"
message = "ETL completed"

try:
    # ✅ Build kwargs so we only pass chunksize when enabled
    to_sql_kwargs = {}
    if IS_CHUNKED_INSERTS:
        to_sql_kwargs["chunksize"] = CHUNK_SIZE

    athletes.to_sql(
        name=TARGET_TABLE.split(".")[-1],  # "Athletes"
        con=engine,
        schema=TARGET_TABLE.split(".")[0],  # "dbo"
        if_exists="append",
        index=False,
        **to_sql_kwargs  # ********************  ** unpacks the dictionary => chunksize=5000 (when enabled)
        # **to_sql_kwargs means: “Take all the key=value pairs inside this dictionary
        # and pass them as named arguments to the function.”
    )

except Exception as e:
    status = "FAILED"
    message = str(e)
    logger.error(f"ETL failed: {message}")
    raise
finally:
    load_elapsed = time.perf_counter() - load_start
    logger.info(f"Load to SQL Server took {load_elapsed:.2f} sec")

# -------------------------
# AFTER COUNT + METRICS
# -------------------------
target_after = get_table_count(engine, TARGET_TABLE)
inserted_rows = target_after - target_before
row_diff = source_row_count - inserted_rows  # source vs inserted (note rejects + dedupe affect this)

# Placeholder (not implemented yet)
target_dupes = None  # you can compute later if you add row_hash column in SQL or business key logic

# -------------------------
# FINISH AUDIT RUN
# -------------------------
with engine.begin() as conn:
    update_audit_run_finish(conn, AUDIT_TABLE, run_id, {
        "source_row_count": source_row_count,
        "target_before": target_before,
        "inserted_rows": inserted_rows,
        "target_after": target_after,
        "row_diff": row_diff,
        "source_dupes": source_dupes,
        "target_dupes": target_dupes,
        "rejected_rows": rejected_rows,
        "status": status,
        "message": message,
        "duration_seconds": round(load_elapsed, 2),
    })


# -------------------------
# PROFILLING 10: TOTAL TIME
# -------------------------

total_elapsed = time.perf_counter() - total_start
logger.info(f"Total ETL run took {total_elapsed:.2f} sec")

# -------------------------
# PRINT SUMMARY
# -------------------------
logger.info("\n ETL RUN SUMMARY")
logger.info("=" * 40)
logger.info(f"Run ID: {run_id}")
logger.info(f"Source rows: {source_row_count}")
logger.info(f"Rejected rows (athlete_id rules): {rejected_rows}")
logger.info(f"Source dupes removed (hash): {source_dupes}")
logger.info(f"Target before: {target_before}")
logger.info(f"Inserted rows: {inserted_rows}")
logger.info(f"Target after: {target_after}")
logger.info(f"Source vs Inserted diff: {row_diff}")
logger.info(f"Duration: {total_elapsed:.2f} sec")
logger.info(f"Status: {status}")

# -----------------------------------------
# PROFILLING 11: FINAL PERFORMANCE SUMMARY
# -----------------------------------------

logger.info("\n ETL STEP TIMING SUMMARY")
logger.info("=" * 40)
logger.info(f"Extract CSV: {extract_elapsed:.2f} sec")
logger.info(f"Clean data: {clean_elapsed:.2f} sec")
logger.info(f"String checks: {string_elapsed:.2f} sec")
logger.info(f"Numeric checks: {numeric_elapsed:.2f} sec")
logger.info(f"Date checks: {date_elapsed:.2f} sec")
logger.info(f"Dedupe: {dedupe_elapsed:.2f} sec")
logger.info(f"Load: {load_elapsed:.2f} sec")
logger.info(f"Total: {total_elapsed:.2f} sec")


# ============================================================
# 🧠 INTERVIEW QUESTION & ANSWER
# ============================================================
# Q: How do you identify bottlenecks in your ETL pipeline?
#
# A:
# I implemented step-level performance profiling using
# high-resolution timers (time.perf_counter) and structured logging.
# This allows me to measure execution time for extraction,
# transformation, validation, and loading stages independently,
# making it easy to identify bottlenecks and optimize performance.
# ============================================================

# ============================================================
# Q: How do you profile validation logic in ETL pipelines?
#
# A:
# I wrap validation steps such as numeric range checks with
# high-resolution timers and log execution time. This allows
# me to measure both data processing and downstream operations
# like reject handling, helping identify bottlenecks across
# transformation and database write stages.
# ============================================================


# -------------------------
# PLACEHOLDERS (we’ll add later)
# -------------------------
#
# 7️⃣ Next level (recommended next steps)
#
# 1️⃣ Chunked inserts (chunksize=5000)
# 2️⃣ Explicit SQL schema validation
# 3️⃣ Staging table + MERGE
# 4️⃣ Airflow DAG version
#
