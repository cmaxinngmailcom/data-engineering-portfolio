# ETL => Move csv data to SQL Server table Athletes

import pandas as pd
import time

from sqlalchemy import create_engine

# Load CSV into DataFrame
athletes = pd.read_csv(
    'Pandas/Complete_Python_Pandas_Data_Science_Tutorial/bios.csv'
)

# print(athletes.head(20))

# Create SQL Server connection
engine = create_engine(
    "mssql+pyodbc://etl_user:max007ime@SqlServer%20ODBC/AdventureWorksDW2014"
    "?dsn=SqlServer%20ODBC"
)

# Function that returns count of table
def get_table_count(engine, table_name, schema="dbo"):
    query = f"SELECT COUNT(*) AS row_count FROM {schema}.{table_name}"
    df = pd.read_sql(query, engine)
    return df.loc[0, "row_count"]


# ************************************************************************************
# 🧠 What this code does (high level)
#
# ✔ Validates CSV data before inserting into SQL Server
# ✔ Prevents "String or binary data would be truncated" errors
# ✔ Identifies:
#
# Which column
#
# Which rows
#
# Exactly how long the bad values are
#
# This is production-grade ETL validation — excellent practice 👍
#
# 💡 Why this matters in real ETL pipelines
#
# In real data engineering jobs:
#
# Schemas never match data perfectly
#
# Truncation errors break pipelines
#
# This check is often automated as a data quality gate
#
# You just implemented a data contract validation step — a very senior-level habit.
# ************************************************************************************
#
# limits = {
#     "name": 200,
#     "born_city": 100,
#     "born_region": 100,
#     "born_country": 5,
#     "NOC": 50,
# }
#
# for col, lim in limits.items():
#     s = athletes[col].astype("string").fillna("")
#     too_long = s.str.len() > lim
#     if too_long.any():
#         print(f"\n❌ Column '{col}' has values longer than {lim}")
#         print(
#             athletes.loc[too_long, ["athlete_id", col]]
#             .assign(length=s[too_long].str.len())
#             .sort_values("length", ascending=False)
#             .head(10)
#         )
# ************************************************************************************

table_name = "Athletes"

# BEFORE count
before_count = get_table_count(engine, table_name)
print(f"📊 Rows before insert: {before_count}")

# Rows to insert
rows_to_insert = len(athletes)
print(f"⬆️ Rows to insert: {rows_to_insert}")

# ⏱️ START TIMER
start_time = time.time()

# INSERT
# Load data into SQL Server
athletes.to_sql(
    name=table_name,
    con=engine,
    schema="dbo",
    if_exists="append",
    index=False
)

# ⏱️ END TIMER
end_time = time.time()

# AFTER count
after_count = get_table_count(engine, table_name)
print(f"📊 Rows after insert: {after_count}")

# ⏱️ EXECUTION TIME
execution_time = end_time - start_time
print(f"⏱️ Insert execution time: {execution_time:.2f} seconds")

# 📈 OPTIONAL: throughput metric
if execution_time > 0:
    rows_per_second = rows_to_insert / execution_time
    print(f"🚀 Insert throughput: {rows_per_second:,.0f} rows/sec")

# ✅ VALIDATION
expected_after = before_count + rows_to_insert

if after_count == expected_after:
    print("✅ Row count validation PASSED")
else:
    print("❌ Row count validation FAILED")
    print(f"Expected: {expected_after}, Actual: {after_count}")
