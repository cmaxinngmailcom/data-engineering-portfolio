# ADDED: AUDIT LOGGING + REJECT TABLE
# (keeps your current cleansing + hash-based dedupe)

import time
import json
import uuid
import hashlib
import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------
# CONFIG
# -------------------------
PIPELINE_NAME = "CSV_to_SQLServer_Athletes"
SOURCE_NAME = "bios.csv"
CSV_PATH = "Pandas/Complete_Python_Pandas_Data_Science_Tutorial/bios.csv"
TARGET_TABLE = "dbo.Athletes"

AUDIT_TABLE = "dbo.ETL_Audit_Run"
REJECT_TABLE = "dbo.Athletes_Rejects"

engine = create_engine(
    "mssql+pyodbc://etl_user:max007ime@SqlServer%20ODBC/AdventureWorksDW2014"
    "?dsn=SqlServer%20ODBC"
)

# -------------------------
# HELPERS
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

def write_rejects(conn, run_id: str, rejects_df: pd.DataFrame):
    """
    Writes rejected rows into dbo.Athletes_Rejects.
    Expects columns: athlete_id, reason, raw_row_json
    """
    if rejects_df.empty:
        return

    rejects_df = rejects_df.copy()
    rejects_df["run_id"] = run_id

    # Keep only fields that exist in SQL table
    rejects_df = rejects_df[["run_id", "athlete_id", "reason", "raw_row_json"]]

    rejects_df.to_sql(
        name="Athletes_Rejects",
        con=conn,
        schema="dbo",
        if_exists="append",
        index=False
    )

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

default_date = pd.Timestamp("1901-01-01")
date_cols = ["born_date", "died_date"]
for col in date_cols:
    if col in athletes.columns:
        athletes[col] = pd.to_datetime(athletes[col], errors="coerce").fillna(default_date).dt.date

numeric_cols = ["height_cm", "weight_kg"]
for col in numeric_cols:
    if col in athletes.columns:
        athletes[col] = pd.to_numeric(athletes[col], errors="coerce").fillna(0)

print("✅ Data cleaning complete")

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

# Convert athlete_id safely after rejects identified
# (Only safe rows should remain after we remove rejects)
# We'll remove rejects after we build reject_records.

# Rule 3: duplicate athlete_id in the CSV batch
# (this prevents PK violations if athlete_id is the PK)
dup_id_mask = athletes["athlete_id"].astype("string").duplicated(keep=False)
add_rejects(dup_id_mask, "Duplicate athlete_id in source file")

# Build rejects_df
rejects_df = pd.DataFrame(reject_records)
rejected_rows = len(rejects_df)

# Remove rejected rows from athletes before load
if rejected_rows > 0:
    # Rebuild a mask of rows to reject by matching athlete_id duplicates + invalids + nulls
    # Simpler: reject anything that matches the 3 rules again:
    reject_mask = (
        athletes["athlete_id"].isna()
        | (~athletes["athlete_id"].astype("string").str.fullmatch(r"\d+", na=False))
        | (athletes["athlete_id"].astype("string").duplicated(keep=False))
    )
    athletes = athletes.loc[~reject_mask].copy()

# Now safe to convert athlete_id to int
athletes["athlete_id"] = athletes["athlete_id"].astype(int)

# Write rejects to SQL Server
with engine.begin() as conn:
    write_rejects(conn, run_id, rejects_df)

print(f"🧾 Rejected rows written: {rejected_rows}")

# -------------------------
# HASH-BASED DUPLICATE DETECTION (within source batch)
# -------------------------
hash_cols = ["name", "born_date", "born_city", "born_country"]

def generate_row_hash(row) -> str:
    combined = "|".join(str(row[col]) for col in hash_cols)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

athletes["row_hash"] = athletes.apply(generate_row_hash, axis=1)
source_dupes = int(athletes.duplicated("row_hash").sum())

athletes = athletes.drop_duplicates(subset="row_hash").drop(columns=["row_hash"])
print(f"🔍 Source duplicates removed (hash): {source_dupes}")

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
    athletes.to_sql(
        name="Athletes",
        con=engine,
        schema="dbo",
        if_exists="append",
        index=False
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
        "rejected_rows": rejected_rows,
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
print(f"🧾 Rejected rows: {rejected_rows}")
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

