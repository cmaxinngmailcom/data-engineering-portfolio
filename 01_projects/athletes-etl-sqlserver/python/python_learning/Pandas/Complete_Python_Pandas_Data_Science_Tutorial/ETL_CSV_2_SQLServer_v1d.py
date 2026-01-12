# Added DATA CLEANSING  Steps

import time
import pandas as pd
from sqlalchemy import create_engine

# -------------------------
# CONFIG
# -------------------------
CSV_PATH = "Pandas/Complete_Python_Pandas_Data_Science_Tutorial/bios.csv"
TABLE_FULLNAME = "dbo.Athletes"

engine = create_engine(
    "mssql+pyodbc://etl_user:max007ime@SqlServer%20ODBC/AdventureWorksDW2014"
    "?dsn=SqlServer%20ODBC"
)

# -------------------------
# HELPERS
# -------------------------
def get_table_count(engine, table_fullname: str) -> int:
    query = f"SELECT COUNT(*) AS row_count FROM {table_fullname}"
    df = pd.read_sql(query, engine)
    return int(df.loc[0, "row_count"])

# -------------------------
# EXTRACT
# -------------------------
athletes = pd.read_csv(CSV_PATH)
rows_to_insert = len(athletes)

# -------------------------
# Data Cleansing
# -------------------------

# -------------------------
# 1) Strings: NULL -> "", and keep blanks as ""
# -------------------------
string_cols = ["name", "born_city", "born_region", "born_country", "NOC"]

for col in string_cols:
    # Ensure column exists (safe guard)
    if col in athletes.columns:
        # Convert to pandas string dtype, replace <NA>/NaN with ""
        athletes[col] = athletes[col].astype("string").fillna("")
        # Optional: trim spaces so "   " becomes ""
        athletes[col] = athletes[col].str.strip()

# -------------------------
# 2) Dates: NULL/blank -> 1901-01-01
# -------------------------
default_date = pd.Timestamp("1901-01-01")
date_cols = ["born_date", "died_date"]

for col in date_cols:
    if col in athletes.columns:
        # Convert to datetime; invalid/blank -> NaT
        athletes[col] = pd.to_datetime(athletes[col], errors="coerce")
        # Fill NaT with default date
        athletes[col] = athletes[col].fillna(default_date)
        # Convert to date (no time part)
        athletes[col] = athletes[col].dt.date

# -------------------------
# 3) Numerics: height/weight NULL/blank -> 0
# -------------------------
numeric_cols = ["height_cm", "weight_kg"]

for col in numeric_cols:
    if col in athletes.columns:
        athletes[col] = pd.to_numeric(athletes[col], errors="coerce").fillna(0)

# (Optional) If you want these stored as integers instead of decimals:
# athletes["height_cm"] = athletes["height_cm"].astype(int)
# athletes["weight_kg"] = athletes["weight_kg"].astype(int)

print("✅ Data cleaning complete")
# print(athletes.head(5))


# -------------------------
# BEFORE COUNT
# -------------------------
before_count = get_table_count(engine, TABLE_FULLNAME)

# -------------------------
# LOAD (INSERT) + TIMING
# -------------------------
start = time.perf_counter()

athletes.to_sql(
    name="Athletes",
    con=engine,
    schema="dbo",
    if_exists="append",   # production-safe
    index=False
)

elapsed = time.perf_counter() - start

# -------------------------
# AFTER COUNT
# -------------------------
after_count = get_table_count(engine, TABLE_FULLNAME)

# -------------------------
# METRICS
# -------------------------
inserted_rows = after_count - before_count
throughput = inserted_rows / elapsed if elapsed > 0 else 0

# ✅ Source vs Target row diff
# expected_inserted = rows_to_insert
# actual_inserted   = inserted_rows
source_vs_target_diff = rows_to_insert - inserted_rows

# -------------------------
# PRINT RESULTS
# -------------------------
print(f"📊 Rows before insert: {before_count}")
print(f"⬆️ Rows to insert (source): {rows_to_insert}")
print(f"📊 Rows after insert: {after_count}")
print(f"✅ Rows inserted (target delta): {inserted_rows}")
print(f"⏱️ Insert execution time: {elapsed:.2f} seconds")
print(f"🚀 Insert throughput: {throughput:.0f} rows/sec")

if source_vs_target_diff == 0:
    print("✅ Source vs Target diff: 0 (MATCH)")
else:
    print(f"⚠️ Source vs Target diff: {source_vs_target_diff} rows (EXPECTED {rows_to_insert}, ACTUAL {inserted_rows})")

# -------------------------
# PLACEHOLDERS (we’ll add later)
# -------------------------
# TODO: Add hash-based duplicate detection
# TODO: Add logging + audit table
# TODO: Add reject table for bad rows
