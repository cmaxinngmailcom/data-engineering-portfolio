import pandas as pd

df = pd.read_sql("SELECT TOP 5 name FROM sys.tables ORDER BY name;", engine)
print(df)
