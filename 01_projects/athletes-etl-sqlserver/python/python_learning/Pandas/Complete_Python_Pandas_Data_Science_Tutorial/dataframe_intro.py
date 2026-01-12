# dataframe Intro
# dataframe = like a table 

import pandas as pd

df = pd.DataFrame([[1,2,3],[4,5,6],[7,8,9]], columns=["A","B","C"])

# to see dataFrame content
print(df.head())
# print(df.head()) SHOW 1st Row
# print(df.tail(2)) SHOW lat 2 rows

# print(df.columns) SHOW Columns Names

# print(df.index.tolist()) SHOW indexes

# print(df.info()) SHOW Infos on DataFrames

# print(df.describe()) SHOW info on Data like count mean std min max etc

# print(df.nunique())  SHOW unique values 

# print(df['A'].nunique()) SHOW unique values in specific Column

# print(df.shape) SHOW shpae of dataframe

# print(df.size) SHOW dataframe size

