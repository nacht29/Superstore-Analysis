#!/usr/bin/env python
# coding: utf-8

# ## Installing dependencies

# In[1]:


# !pip install xlrd
# !pip install pandas
# !pip install numpy
# !pip install mysql-connector-python
# !pip install sqlalchemy
# !pip install pymysql
# !pip install google-cloud-bigquery
# !pip install db-dtypes
# !pip install db-dtypes

# ---

# ## Set up environment

# In[3]:


import re
import os
import warnings
import pandas as pd
import numpy as np
import mysql.connector as mysql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import *
from google.cloud import bigquery as bq
from google.oauth2 import service_account

warnings.filterwarnings('ignore')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'explore29-33756158108f.json'

# ---

# ## Import data

# In[4]:


file_path = 'data-src/Sample - Superstore.xls'
orders_df = pd.read_excel(file_path, sheet_name='Orders', header=0)
people_df = pd.read_excel(file_path, sheet_name='People', header=0)
returns_df = pd.read_excel(file_path, sheet_name='Returns', header=0)

# ---

# ## Functions

# In[5]:


def snake_case(data:str) -> str:
	return(data.lower().strip().replace(' ','_').replace('-', '_'))

def std_null(data):
	if pd.isna(data):
		return None
	if isinstance(data, str):
		data = data.strip()
		return None if data.lower() in ['null', ''] else data
	return data

# ---

# ## Process ```Orders```

# ### Format headers (columns)

# In[6]:


try:
	orders_df = orders_df.rename(columns={
	'Country/Region': 'country',
	'State/Province': 'province',
	'Postal Code': 'post_code'
})
except Exception as error:
	print(f'Failed to reformat headers for orders table: {error}')
	
orders_df.columns = [snake_case(col) for col in orders_df.columns]

# ### Aggregate rows

# In[7]:


agg_instruction = {col: ('sum' if col in ['sales', 'quantity', 'profit'] else 'last') for col in list(orders_df.columns)}
orders_df = orders_df.groupby(['order_id', 'product_id'], as_index=False).agg(agg_instruction)

# ### Format numerical data

# In[8]:


orders_df['sales'] = orders_df['sales'].astype(float).round(2)
orders_df['quantity'] = orders_df['quantity'].astype(int).round(2)
orders_df['discount'] = orders_df['discount'].astype(float).round(2)
orders_df['profit'] = orders_df['profit'].astype(float).round(2)

# ---

# ## Process ```People```

# ### Format headers

# In[11]:


people_df = people_df.rename(columns={
	'Regional Manager': 'manager',
	'Region': 'region'
})

# ### NULL/Missing values and duplicates

# In[13]:


## handle duplicates
people_df = people_df.drop_duplicates()

## handle null values
people_df = people_df.applymap(std_null)

# ---

# ## Process ```Returns```

# ### Format headers

# In[20]:


returns_df.columns = [snake_case(col) for col in returns_df.columns]

# #### Merge with ```Orders```

# In[21]:


orders_oid_count = orders_df['order_id'].value_counts().reset_index()
orders_oid_count.columns = ['order_id', 'orders_oid_count']
returns_oid_count = returns_df['order_id'].value_counts().reset_index()
returns_oid_count.columns = ['order_id', 'returns_oid_count']
same_id_entries = orders_oid_count.merge(returns_oid_count, on='order_id', how='inner')
mismatch = len(same_id_entries[same_id_entries['orders_oid_count'] != same_id_entries['returns_oid_count']])

if mismatch == 0:
	orders_df['returned'] = orders_df['order_id'].isin(returns_df['order_id']).map({True: 'Yes', False: 'No'})
	orders_df = orders_df.applymap(std_null) # handle null values here for orders_df

# ---

# ## Loading data to Bigquery

# ### Restructure

# In[22]:


orders = orders_df[['order_id', 'order_date', 'ship_date', 'ship_mode', 'customer_id', 'product_id', 'sales', 'quantity', 'discount', 'profit', 'returned']]
customers = orders_df[['customer_id', 'customer_name', 'segment', 'country', 'city', 'province', 'post_code', 'region']].drop_duplicates(subset=['customer_id'])
products = orders_df[['product_id', 'product_name', 'category', 'sub_category']].drop_duplicates(subset=['product_id'])
regions = people_df[['region', 'manager']]

# ### Establish connection

# In[23]:


client = bq.Client()

# ### Load ```orders```

# In[24]:


destination_table = 'explore29.superstore.orders2'

job_config = bq.LoadJobConfig(
	write_disposition='WRITE_APPEND',
	autodetect=True
)

# ### Load ```customers```

# In[ ]:


destination_table = 'explore29.superstore.customers2'

job_config = bq.LoadJobConfig(
	write_disposition='WRITE_APPEND',
	autodetect=True
)

job = client.load_table_from_dataframe(
	customers,
	destination_table,
	job_config=job_config
)
