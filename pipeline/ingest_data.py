#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd


year=2021
month=1


pd.__file__


# In[4]:

df = pd.read_csv(
    f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    f"yellow_tripdata_{year}-{month:02d}.csv.gz"
)


# In[5]:


df.head


# In[6]:


len(df)


# In[8]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
 f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    f"yellow_tripdata_{year}-{month:02d}.csv.gz",
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[9]:


df.head()


# In[10]:





# In[12]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[13]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[14]:


df.to_sql(
    name='yellow_taxi_data',
    con=engine,
    if_exists='append',
    index=False
)


# In[17]:


df.head(0).to_sql(name='yellow_taxi_data', con=engine,if_exists='replace')


# In[30]:


df_iter = pd.read_csv(
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    "yellow_tripdata_2021-01.csv.gz",

    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000,

)


# In[23]:





# In[24]:


from tqdm.auto import tqdm


# In[ ]:


for df_chunk in tqdm(df_iter):
    print(len(df_chunk))
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[29]:


len(df)


# In[ ]:




