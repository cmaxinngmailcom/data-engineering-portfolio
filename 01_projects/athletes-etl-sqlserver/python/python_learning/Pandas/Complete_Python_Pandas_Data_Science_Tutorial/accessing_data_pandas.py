# Loading in DataFrames from Files (CSV, Excel, Parquet, etc.)
import pandas as pd
 
coffee = pd.read_csv('Pandas/Complete_Python_Pandas_Data_Science_Tutorial/coffee.csv')


# Accessing Data with Pandas
# print(coffee.head())
# print(coffee.tail())
# print(len(coffee))

# print(coffee.sample(5))

# Makes sure every time you run the program values does not change
# print(coffee.sample(10, random_state=1))

# Loc [rows, columns]

# print(coffee.loc[0])

# print(coffee.head())

# print(coffee.loc[[0,1,2]])

# print(coffee.loc[0:3]) # slice

# print(coffee.loc[5:12]) # slice

# (coffee.loc[5:12, "Day"]) # slice

# print(coffee.loc[5:12, ["Day", "Units Sold" ]]) # slice

# all rows
# print(coffee.loc[:, ["Day", "Units Sold" ]]) # slice

# iloc similar to loc but uses index location and not labels

# print(coffee.iloc[:, [0,2]]) # slice

# print(coffee.iloc[0:5, [0,2]]) # slice

# coffee.index = coffee["Day"]

# print(coffee.head())

# print(coffee.loc["Monday"])

# print(coffee.loc["Monday": "Wednesday"])

# ********************************* Accessing Data *********************************

# modifying values
# coffee.loc[1:3, "Units Sold" ] = 10

# print(coffee.head()) 

# optimal ways to modify values
# iat and at

# In pandas, .at and .iat are highly optimized accessors used to get or set 
# a single scalar value in a DataFrame. They are faster than their more 
# general counterparts, .loc and .iloc, because they do not perform 
# index alignment or type checking for multiple rows/columns. 

# print(coffee.at[0, "Units Sold" ])

# print(coffee.head()) 

# print(coffee.iat[0,0])

# print(coffee.head()) 

# 21:11 - Accessing Data | Grab Columns, Sort Values, Ascending/Descending

# print(coffee["Units Sold" ])

# print(coffee.Day)

# *************** SORT DATA ***************
# print(coffee.sort_values("Units Sold"))

# DESCENDING ORDER 1 column 
# print(coffee.sort_values("Units Sold", ascending=False))


# DESCENDING ORDER 2 columns same Order = descending
# print(coffee.sort_values(["Units Sold", "Coffee Type"], ascending=False))

# DESCENDING ORDER 2 columns same Order => column 1 = descending, column 2 = ascending
# print(coffee.sort_values(["Units Sold", "Coffee Type"], ascending=[0,1]))


# 23:01 - Iterating over a DataFrame (df) with a For Loop | df.iterrows()
#    print(index)
#    print(row)
#    print("\n\n\n\n\n")

#for index, row in coffee.iterrows():
#    print(index)
#    print(row["Units Sold"])
#    print("\n\n\n\n\n")

# 24:12 - Filtering Data | Syntax Options, Numeric Values, Multiple Conditions

# bios = pd.read_csv('Pandas/Complete_Python_Pandas_Data_Science_Tutorial/bios.csv')

# print(bios.head())
# print(bios.tail())

# print(bios.info())

# print(bios.loc[bios["height_cm"] > 215])

# print(bios.loc[bios["height_cm"] > 215, ["name", "height_cm"]])

# print(bios[bios["height_cm"] > 215])

# print(bios[bios["height_cm"] > 215] [["name", "height_cm"]])


# 27:58 - Filtering Data | String Operations, Regular Expressions (Regex)

# print(bios[(bios["name"].str.contains("Keith"))]) # the string is case sensitive

# print(bios[(bios["name"].str.contains("keith", case=False))]) # the string is NOT case sensitive

# the string is NOT case sensitive AND filter on one name or (| REGEX)the other 
#print(bios[(bios["name"].str.contains("keith | patrick", case=False))]) 

# Here are a few REGEX FILTERS you can apply to DATAFRAME to showcase the POWER OF REGULAR EXPRESSIONS:

# 1. Find athletes born in cities that start with a vowel:
#vowel_cities = bios[(bios["born_city"].str.contains(r'^[AEIOUaeiou]', na=False))]
#print(vowel_cities)

# 2. Find athletes with names that contains exactly two vowels:
# two_vowels = bios[bios['name'].str.contains(r'^[^AEIOUaeiou]*[AEIOUaeiou][^AEIOUaeiou]*[AEIOUaeiou][^AEIOUaeiou]*$', na=False)]
# print(two_vowels)

# 3. Find athletes with names that have repeated consecutive letters (e.g., "Aaron", "Emmett"):
# repeated_letters = bios[bios['name'].str.contains(r'(.)\1', na=False)]
# print(repeated_letters)

# 4. Find athletes with names ending in 'son' or 'sen':
# son_sen_names = bios[bios['name'].str.contains(r'son$|sen$', case=False, na=False)]
# print(son_sen_names)

# 5.  Find athletes born in a year starting with '19':
# born_19xx = bios[bios['born_date'].str.contains(r'^19', na=False)]
# print(born_19xx)

# 6. Find athletes with names that do not contain any vowels:
# no_vowels = bios[bios['name'].str.contains(r'^[^AEIOUaeiou]*$', na=False)]
# print(no_vowels)

# 7. Find athletes whose names contain a hyphen or an apostrophe:
# hyphen_apostrophe = bios[bios['name'].str.contains(r"[-']", na=False)]
# print(hyphen_apostrophe)

# 8.Find athletes with names that start and end with the same letter:
# start_end_same = bios[bios['name'].str.contains(r'^(.).*\1$', na=False, case=False)]
# print(start_end_same)

# 9. Find athletes with a born_city that has exactly 7 characters:
#city_seven_chars = bios[bios['born_city'].str.contains(r'^.{7}$', na=False)]
#print(city_seven_chars)

# 10. Find athletes with names containing three or more vowels:
# three_or_more_vowels = bios[bios['name'].str.contains(r'([AEIOUaeiou].*){3,}', na=False)]
# print(three_or_more_vowels)

# Don't use regex search (exact match)
# bios[bios['name'].str.contains('keith|patrick', case=False, regex=False)]

# print(bios[bios['name'].str.contains('keith|patrick', case=False, regex=False)])

# isin method & startswith
# print(bios[bios['born_country'].isin(["USA", "FRA", "GBR"]) & (bios['name'].str.startswith("Maxime"))])



# 33:09 - Filtering Data | Query Functions

# (bios.query('born_country == "USA"'))

# print(bios.query('born_country == "USA" and born_city == "Seattle"'))

# 34:20 - Adding / Removing Columns | Basics, Conditional Values, Math Operations, Renaming Columns
# Adding / Removing Columns

# print(coffee.head())

# coffee['price'] = 4.99

# print(coffee.head())

import numpy as np 

# print(coffee.head())

# coffee['new_price'] = np.where(coffee['Coffee Type'] == 'Espresso', 3.99, 5.99)

# print(coffee.head())

# coffee.drop(columns=['price'], inplace=True)

# print(coffee.head())

# coffee = coffee[['Day', 'Coffee Type', 'Units Sold', 'new_price']]
# print(coffee.head())

# creates copy of coffee
# coffee_new = coffee.copy()
# coffee_new['price'] = 4.99

# coffee['revenue'] = coffee['Units Sold'] * coffee['new_price']
# print(coffee.head())

# coffee.rename(columns={'new_price': 'price'}, inplace=True)
# print(coffee.head())

# 41:40 - Adding / Removing Columns | String Operations, Datetime (pd.to_datetime) Operations

# bios_new = bios.copy()

# bios_new['first_name'] = bios_new['name'].str.split(' ').str[0]
# print(bios_new.head(10))

# bios_new.query('first_name == "Keith"')

# print(bios_new.head(10))

# bios_new['born_datetime'] = pd.to_datetime(bios_new['born_date']) # errors=coerce; format="%Y-%m-%d"

# print(bios_new.head(10))


# bios_new['born_year'] = bios_new['born_datetime'].dt.year
# print(bios_new[['name','born_year']])



# 46:38 - Saving our Updated DataFrame (df.to_csv, df.to_excel, df.to_parquet, etc)
# bios_new.to_csv('Pandas/Complete_Python_Pandas_Data_Science_Tutorial/bios_new.csv', index=False)

# 47:14 - Adding / Removing Columns | Using Lambda & Custom Functions w/ .apply()
#  LAMBDA FUNCTION USED BELOW
# bios['height_category'] = bios['height_cm'].apply(lambda x: 'Short' if x < 165 else ('Average' if x < 185 else 'Tall'))



# def categorize_athlete(row):
#     if row['height_cm'] < 175 and row['weight_kg'] < 70:
#         return 'Lightweight'
#     elif row['height_cm'] < 185 or row['weight_kg'] <= 80:
#         return 'Middleweight'
    
#     else:
#         return 'Heavyweight'
    
# bios['Category'] = bios.apply(categorize_athlete, axis=1) # 1 is rows, 0 is columns

# print(bios.head())

# 50:42 - Merging & Concatenating Data | pd.merge(), pd.concat(), types of joins
bios = pd.read_csv('Pandas/Complete_Python_Pandas_Data_Science_Tutorial/bios.csv')


# 58:33 - Handling Null Values (NaNs) | .fillna() .interpolate() .dropna() .isna() .notna()


# 1:04:05 - Aggregating Data | value_counts()
# 1:05:47 - Aggregating Data | Using Groupby - groupby() .sum() .mean() .agg()
# 1:08:24 - Aggregating Data | Pivot Tables
# 1:10:28 - Groupby combined with Datetime Operations
# 1:14:38 - Advanced Functionality | .shift() .rank() .cumsum() .rolling()
# 1:22:10 - New Functionality | Pandas 1.0 vs Pandas 2.0 - pyarrow 
# 1:25:29 - New Functionality | GitHub Copilot & OpenAI ChatGPT
# 1:32:05 - What Next?? | Continuing your Python Pandas Learning…