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
import hashlib
from pathlib import Path
from urllib.parse import quote_plus

import yaml
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from datetime import date  # ✅ Needed for date.today()

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

def load_config(env: str = "dev") -> dict:
    """
    Loads configs/{env}.yaml from the project root (python/athletes_etl).
    This works even when you run from different folders in VS Code.
    """
    project_root = Path(__file__).resolve().parents[2]  # ...\python\athletes_etl
    cfg_path = project_root / "configs" / f"{env}.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

cfg = load_config("dev")

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

# ✅ Build SQLAlchemy engine from YAML + ENV password
dsn = cfg["sqlserver"]["dsn"]
db = cfg["sqlserver"]["database"]
user = cfg["sqlserver"]["username"]
pwd_env = cfg["sqlserver"]["password_env"]

password = os.getenv(pwd_env)
if not password:
    raise RuntimeError(f"Missing environment variable: {pwd_env}")

# URL-encode credentials / dsn safely for connection string
# ✅ Force the database (AdventureWorksDW2014) even when using a DSN
connection_url = URL.create(
    "mssql+pyodbc",
    username=user,
    password=password,
    host=None,            # DSN handles the server name
    database=db,          # ✅ forces AdventureWorksDW2014 (not master)
    query={
        "dsn": dsn,
        "TrustServerCertificate": "yes"
    }
)

engine = create_engine(connection_url)

# Quick test: confirm which DB you're connected to
with engine.begin() as conn:
    print("DB_NAME() =", conn.execute(text("SELECT DB_NAME()")).scalar())

# -------------------------
# HELPERS => FUNCTION
# -------------------------
def get_table_count(engine, table_fullname: str) -> int:
    df = pd.read_sql(f"SELECT COUNT(*) AS row_count FROM {table_fullname}", engine)
    return int(df.loc[0, "row_count"])

def insert_audit_run_start(conn, run_id: str):
    """
    Inserts a RUNNING row in dbo.ETL_Audit_Run at start of pipeline.
    """
    conn.execute(
        text(f"""
            INSERT INTO {AUDIT_TABLE} (
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
            "pipeline_name": PIPELINE_NAME,
            "source_name": SOURCE_NAME,
            "target_table": TARGET_TABLE,
        }
    )

def update_audit_run_finish(conn, run_id: str, payload: dict):
    """
    Updates the audit row at the end with metrics + status.
    """
    conn.execute(
        text(f"""
            UPDATE {AUDIT_TABLE}
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

def split_rejects(df: pd.DataFrame, mask: pd.Series):
    """Return (good_rows, bad_rows) given a boolean mask where True means reject."""
    bad = df.loc[mask].copy()
    good = df.loc[~mask].copy()
    return good, bad

# -------------------------
# START AUDIT RUN
# -------------------------
run_id = str(uuid.uuid4())

with engine.begin() as conn:
    insert_audit_run_start(conn, run_id)

# -------------------------
# EXTRACT
# -------------------------
athletes = pd.read_csv(CSV_PATH)
source_row_count = len(athletes)

# -------------------------
# DATA CLEANSING (your rules)
# -------------------------
string_cols = ["name", "born_city", "born_region", "born_country", "NOC"]
for col in string_cols:
    if col in athletes.columns:
        athletes[col] = athletes[col].astype("string").fillna("").str.strip()

date_cols = ["born_date", "died_date"]
for col in date_cols:
    if col in athletes.columns:
        athletes[col] = pd.to_datetime(athletes[col], errors="coerce").fillna(default_date).dt.date

numeric_cols = ["height_cm", "weight_kg"]
for col in numeric_cols:
    if col in athletes.columns:
        athletes[col] = pd.to_numeric(athletes[col], errors="coerce").fillna(0)

print("✅ Data cleaning complete")

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
too_long_mask = pd.Series(False, index=athletes.index)

# We'll also build a per-row reason string describing exactly what failed
reasons = pd.Series("", index=athletes.index, dtype="string")

for col, limit in string_limits.items():
    if col in athletes.columns:
        # Convert to string, treat null as ""
        s = athletes[col].astype("string").fillna("")

        # Length of each value
        lens = s.str.len()

        # Identify which rows violate limit
        col_too_long = lens > int(limit)

        # Update overall bad mask
        too_long_mask = too_long_mask | col_too_long

        # Append details to "reasons" for those rows
        # Example reason: "name length 250 > 200"
        reasons.loc[col_too_long] = (
            reasons.loc[col_too_long].fillna("") +
            f"{col} length " + lens.loc[col_too_long].astype(str) + f" > {int(limit)}; "
        )

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

print(f"🧾 Rejected rows written: {rejected_rows}")

# -------------------------
# NUMERIC RANGE REJECTS
# -------------------------
# Pick realistic ranges (adjust if you want)
# Height in cm: 90–250
# Weight in kg: 20–300

numeric_reject_mask = pd.Series(False, index=athletes.index)

if "height_cm" in athletes.columns:
    if height_allow_zero:
        numeric_reject_mask = numeric_reject_mask | (
            (athletes["height_cm"] != 0) & ((athletes["height_cm"] < height_min) | (athletes["height_cm"] > height_max))
        )
    else:
        numeric_reject_mask = numeric_reject_mask | (
            (athletes["height_cm"] < height_min) | (athletes["height_cm"] > height_max)
        )

if "weight_kg" in athletes.columns:
    if weight_allow_zero:
        numeric_reject_mask = numeric_reject_mask | (
            (athletes["weight_kg"] != 0) & ((athletes["weight_kg"] < weight_min) | (athletes["weight_kg"] > weight_max))
        )
    else:
        numeric_reject_mask = numeric_reject_mask | (
            (athletes["weight_kg"] < weight_min) | (athletes["weight_kg"] > weight_max)
        )

athletes, numeric_rejects = split_rejects(athletes, numeric_reject_mask)

numeric_reason = (
    f"NUMERIC_RANGE: height must be {height_min}-{height_max}cm"
    + (" (or 0)" if height_allow_zero else "")
    + f", weight must be {weight_min}-{weight_max}kg"
    + (" (or 0)" if weight_allow_zero else "")
)

numeric_reject_count = write_rejects(engine, run_id, numeric_rejects, numeric_reason)
print(f"🧾 Numeric-range rejects written: {numeric_reject_count}")

# -------------------------
# DATE SANITY REJECTS
# -------------------------
today = pd.Timestamp(date.today())  # local date

# Convert to Timestamp for comparisons (your cols may be python date objects)
born_ts = pd.to_datetime(athletes["born_date"], errors="coerce") if "born_date" in athletes.columns else pd.Series(pd.NaT, index=athletes.index)
died_ts = pd.to_datetime(athletes["died_date"], errors="coerce") if "died_date" in athletes.columns else pd.Series(pd.NaT, index=athletes.index)

default_died = pd.Timestamp(default_date_str)

date_reject_mask = pd.Series(False, index=athletes.index)

# Rule: born_date cannot be after today
if reject_born_after_today:
    date_reject_mask = date_reject_mask | (born_ts > today)

# Rule: born_date cannot be too old
date_reject_mask = date_reject_mask | (born_ts < min_born)

# Rule: died_date cannot be before born_date (if died_date is not default)
if reject_died_before_born:
    date_reject_mask = date_reject_mask | ((died_ts.notna()) & (died_ts != default_died) & (died_ts < born_ts))

athletes, date_rejects = split_rejects(athletes, date_reject_mask)

date_reason = (
    f"DATE_SANITY: born_date must be <= today (if enabled) and >= {min_born.date()}; "
    f"died_date must be >= born_date (unless default {default_died.date()})"
)

date_reject_count = write_rejects(engine, run_id, date_rejects, date_reason)
print(f"🧾 Date-sanity rejects written: {date_reject_count}")

# -------------------------
# HASH-BASED DUPLICATE DETECTION (within source batch)
# -------------------------
def generate_row_hash(row) -> str:
    combined = "|".join(str(row.get(col, "")) for col in hash_cols)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

source_dupes = 0
if DEDUPE_ENABLED:
    athletes["row_hash"] = athletes.apply(generate_row_hash, axis=1)
    source_dupes = int(athletes.duplicated("row_hash").sum())
    athletes = athletes.drop_duplicates(subset="row_hash").drop(columns=["row_hash"])
    print(f"🔍 Source duplicates removed (hash): {source_dupes}")
else:
    print("🔍 Source dedupe disabled (dedupe.enabled=false)")

# -------------------------
# BEFORE COUNT
# -------------------------
target_before = get_table_count(engine, TARGET_TABLE)

# -------------------------
# LOAD (INSERT) + TIMING
# -------------------------
start = time.perf_counter()

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
    raise
finally:
    elapsed = time.perf_counter() - start

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
    update_audit_run_finish(conn, run_id, {
        "source_row_count": source_row_count,
        "target_before": target_before,
        "inserted_rows": inserted_rows,
        "target_after": target_after,
        "row_diff": row_diff,
        "source_dupes": source_dupes,
        "target_dupes": target_dupes,
        "rejected_rows": rejected_rows,  # NOTE: this counts only the "athlete_id rule" rejects
        "status": status,
        "message": message,
        "duration_seconds": round(elapsed, 2),
    })

# -------------------------
# PRINT SUMMARY
# -------------------------
print("\n✅ ETL RUN SUMMARY")
print("=" * 40)
print(f"Run ID: {run_id}")
print(f"📥 Source rows: {source_row_count}")
print(f"🧾 Rejected rows (athlete_id rules): {rejected_rows}")
print(f"🔁 Source dupes removed (hash): {source_dupes}")
print(f"📊 Target before: {target_before}")
print(f"✅ Inserted rows: {inserted_rows}")
print(f"📊 Target after: {target_after}")
print(f"📏 Source vs Inserted diff: {row_diff}")
print(f"⏱️ Duration: {elapsed:.2f} sec")
print(f"📌 Status: {status}")

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
