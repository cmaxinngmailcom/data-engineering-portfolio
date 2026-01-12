import pandas as pd
from sqlalchemy import create_engine


engine = create_engine(
    "mssql+pyodbc://etl_user:max007ime@SqlServer%20ODBC/AdventureWorksDW2014"
    "?dsn=SqlServer%20ODBC"
)


query = """
SELECT *
FROM dbo.FactCallCenter
"""

df = pd.read_sql(query, engine)

print(len(df))

print(df.head(120))

