"""
Python ETL Example
"""
import time
import requests
import pandas as pd

from sqlalchemy import create_engine


def extract()-> dict:
    """ This API extracts data from
    http://universities.hipolabs.com
    """
    API_URL = "http://universities.hipolabs.com/search?country=United+States"

    data = requests.get(API_URL).json()
    return data 

def transform(data:dict) -> pd.DataFrame:
    """ Transforms the dataset into desired structure and filters"""
    df = pd.DataFrame(data) # convert dictionary to pandas dataframe => dataframe = CSV in Memory
    print(f"Total Number of universities from API {len(data)}")
    df = df[df["name"].str.contains("California")] # filter universities in Cali
    print(f"Number of universities in california {len(df)}")
    df['domains'] = [','.join(map(str, l)) for l in df['domains']] # list of domains in comma separated format
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains","country","web_pages","name"]] # return needed columns
     
def load(df:pd.DataFrame)-> None:
    """ Loads data into a sqllite database"""
    disk_engine = create_engine('sqlite:///my_lite_store.db') # change to SQL Server databae
    df.to_sql('cal_uni', disk_engine, if_exists='replace')

# %%
data = extract()
df = transform(data)

# load(df)


# %%