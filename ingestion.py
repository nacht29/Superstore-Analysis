#!/usr/bin/env python
# coding: utf-8

# ## Installing dependencies

# In[3]:


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

# In[29]:


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
from google.api_core.exceptions import *

warnings.filterwarnings('ignore')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'explore29-33756158108f.json'

# ---

# ## Import data

# In[5]:


file_path = 'data-src/Sample - Superstore.xls'
orders_df = pd.read_excel(file_path, sheet_name='Orders', header=0)
people_df = pd.read_excel(file_path, sheet_name='People', header=0)
returns_df = pd.read_excel(file_path, sheet_name='Returns', header=0)

# ---

# ## Formatting functions

# In[6]:


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

# In[7]:


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

# In[8]:


agg_instruction = {col: ('sum' if col in ['sales', 'quantity', 'profit'] else 'last') for col in list(orders_df.columns)}
orders_df = orders_df.groupby(['order_id', 'product_id'], as_index=False).agg(agg_instruction)

# ### Format numerical data

# In[9]:


orders_df['sales'] = orders_df['sales'].astype(float).round(2)
orders_df['quantity'] = orders_df['quantity'].astype(int).round(2)
orders_df['discount'] = orders_df['discount'].astype(float).round(2)
orders_df['profit'] = orders_df['profit'].astype(float).round(2)

# ---

# ## Process ```People```

# ### Format headers

# In[10]:


people_df = people_df.rename(columns={
	'Regional Manager': 'manager',
	'Region': 'region'
})

# ### NULL/Missing values and duplicates

# In[11]:


## handle duplicates
people_df = people_df.drop_duplicates()

## handle null values
people_df = people_df.applymap(std_null)

# ---

# ## Process ```Returns```

# ### Format headers

# In[12]:


returns_df.columns = [snake_case(col) for col in returns_df.columns]

# #### Merge with ```Orders```

# In[13]:


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

# In[14]:


orders = orders_df[['order_id', 'order_date', 'ship_date', 'ship_mode', 'customer_id', 'product_id', 'sales', 'quantity', 'discount', 'profit', 'returned']]
customers = orders_df[['customer_id', 'customer_name', 'segment', 'country', 'city', 'province', 'post_code', 'region']].drop_duplicates(subset=['customer_id'])
products = orders_df[['product_id', 'product_name', 'category', 'sub_category']].drop_duplicates(subset=['product_id'])
regions = people_df[['region', 'manager']]

# ### Establish connection

# In[15]:


client = bq.Client()

# ### Repetitive functions

# In[ ]:


# loads df into table
# returns the destination of table being loaded
def load_temp_table(df:'dataframe', table_name:str) -> tuple:
	temp_table = f'explore29.superstore.{table_name}_tmp'

	job_config = bq.LoadJobConfig(
		write_disposition='WRITE_TRUNCATE',
		autodetect=True
	)

	job = client.load_table_from_dataframe(
		df,
		temp_table,
		job_config=job_config
	)

	return (job.result(), temp_table)

# gets column names for tables
# adds src. suffix for table columns and return as str
def get_col(df:'dataframe') ->tuple:
	col_lst = list(df.columns)
	col = ', '.join(col_lst)

	src_col = [f'src.{elem}' for elem in col_lst]
	src_col = ', '.join(src_col)

	return(col, src_col)

# ### Load and merge ```orders```

# In[48]:


temp_table = load_temp_table(orders, 'orders')[-1]
columns, src_columns = get_col(orders)


merge_query = f"""
MERGE INTO explore29.superstore.orders  AS target
USING `{temp_table}` AS src ON
	target.order_id = src.order_id
	AND target.product_id = src.product_id
WHEN NOT MATCHED THEN
INSERT ({columns})
VALUES ({src_columns});
DROP TABLE {temp_table};
"""

try:
	merge = client.query(merge_query)
	merge.result()
except BadRequest as error:
	print(f'Failed to load orders. {error}')

# ### Load ```customers```

# In[49]:


temp_table = load_temp_table(customers, 'customers')[-1]
columns, src_columns = get_col(customers)

merge_query = f"""
MERGE INTO explore29.superstore.customers AS target
USING `{temp_table}` AS src ON
	target.customer_id = src.customer_id
WHEN NOT MATCHED THEN
INSERT ({columns})
VALUES ({src_columns});
DROP TABLE {temp_table};
"""


try:
	merge = client.query(merge_query)
	merge.result()
except BadRequest as error:
	print(f'Failed to load customers. {error}')

# ### Load ```products```

# In[50]:


temp_table = load_temp_table(products, 'products')[-1]
columns, src_columns = get_col(products)

merge_query = f"""
MERGE INTO explore29.superstore.products AS target
USING `{temp_table}` AS src ON
	target.product_id = src.product_id
WHEN NOT MATCHED THEN
INSERT ({columns})
VALUES ({src_columns});
DROP TABLE {temp_table};
"""


try:
	merge = client.query(merge_query)
	merge.result()
except BadRequest as error:
	print(f'Failed to load products. {error}')

# ### Load ```regions```

# In[51]:


temp_table = load_temp_table(regions, 'regions')[-1]
columns, src_columns = get_col(regions)

merge_query = f"""
MERGE INTO explore29.superstore.regions AS target
USING `{temp_table}` AS src ON
	target.region = src.region
WHEN NOT MATCHED THEN
INSERT ({columns})
VALUES ({src_columns});
DROP TABLE {temp_table};
"""


try:
	merge = client.query(merge_query)
	merge.result()
except BadRequest as error:
	print(f'Failed to load regions. {error}')