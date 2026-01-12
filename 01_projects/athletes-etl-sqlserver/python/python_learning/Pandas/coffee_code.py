# Loading in DataFrames from Files (CSV, Excel, Parquet, etc.)
import pandas as pd
 
coffee = pd.read_csv('Pandas/Complete_Python_Pandas_Data_Science_Tutorial/coffee.csv')

# print(coffee.head())
print(coffee.tail())

# Pandas does NOT ship with Parquet support by default.
# Pandas tries to use one of these engines:
# pyarrow (recommended)
# fastparquet


results = pd.read_parquet('Pandas/Complete_Python_Pandas_Data_Science_Tutorial/results.parquet')

#print(results.head())

# Excel support requires openpyxl.
# pip install openpyxl

# olympics_data = pd.read_excel('Pandas/Complete_Python_Pandas_Data_Science_Tutorial/olympics-data.xlsx')

# read specific sheets

olympics_data = pd.read_excel('Pandas/Complete_Python_Pandas_Data_Science_Tutorial/olympics-data.xlsx',
                              sheet_name = "results")

#print(olympics_data.head())

bios = pd.read_csv('Pandas/Complete_Python_Pandas_Data_Science_Tutorial/bios.csv')

# read_csv vs to_csv: read_excel vs to_excel

# in pandas when ready files what's the diffrence with: read_csv vs to_csv or read_excel vs to_excel ??????


# Core idea (one sentence)

# read_* = bring data into Pandas

# to_* = write data out of Pandas

# Think: read → DataFrame → transform → write