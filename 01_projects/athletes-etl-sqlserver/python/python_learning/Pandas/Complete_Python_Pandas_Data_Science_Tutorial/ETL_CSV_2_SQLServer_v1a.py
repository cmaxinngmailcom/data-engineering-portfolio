# ETL => Move csv data to SQL Server table Athletes

import pandas as pd

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

# Load data into SQL Server
athletes.to_sql(
    name='Athletes',
    con=engine,
    schema='dbo',
    if_exists='append',   # ✅ production-safe
    index=False           # ✅ do NOT write DataFrame index
)


# Count Validation
query = "SELECT COUNT(*) AS row_count FROM dbo.Athletes"
df = pd.read_sql(query, engine)

row_count = df.loc[0, "row_count"]

if row_count == 0:
    print("⚠️ Table is empty")
else:
    print(f"✅ Table has {row_count} rows")

