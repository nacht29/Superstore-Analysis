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

# load data into df
file_path = 'data-src/Sample - Superstore.xls'
orders_df = pd.read_excel(file_path, sheet_name='Orders', header=0)
people_df = pd.read_excel(file_path, sheet_name='People', header=0)
returns_df = pd.read_excel(file_path, sheet_name='Returns', header=0)

def snake_case(data:str) -> str:
	return(data.lower().strip().replace(' ','_').replace('-', '_'))

# orders
## rename headers
try:
	orders_df = orders_df.rename(columns={
	'Country/Region': 'country',
	'State/Province': 'province',
	'Postal Code': 'post_code'
})
except Exception as error:
	print(f'Failed to reformat headers for orders table: {error}')

## format header
orders_df.columns = [snake_case(col) for col in orders_df.columns]

## remove duplicates
orders_df = orders_df.drop_duplicates(subset=['order_id', 'product_id', 'quantity'], keep='last')

## merge entries that have similar columns except quantity
merged_df = orders_df.groupby(['order_id', 'product_id'], as_index=False).agg({
	'sales': 'sum',
	'quantity': 'sum',
	'profit': 'sum'
})

## format data to float
orders_df['sales'] = orders_df['sales'].astype(float).round(2)
orders_df['quantity'] = orders_df['quantity'].astype(int).round(2)
orders_df['discount'] = orders_df['discount'].astype(float).round(2)
orders_df['profit'] = orders_df['profit'].astype(float).round(2)

# people
## rename headers
people_df = people_df.rename(columns={
	'Regional Manager': 'manager',
	'Region': 'region'
})

# returns
## rename headers
returns_df.columns = [snake_case(col) for col in returns_df.columns]

## merging orders with returns
orders_df['returned'] = orders_df['order_id'].isin(returns_df['order_id']).map({True: 'Yes', False: 'No'})

# breaking data into dfs
orders = orders_df[['order_id', 'order_date', 'ship_date', 'ship_mode', 'customer_id', 'product_id', 'sales', 'quantity', 'discount', 'profit', 'returned']]
customers = orders_df[['customer_id', 'customer_name', 'segment', 'country', 'city', 'province', 'post_code', 'region']].drop_duplicates(subset=['customer_id'])
products = orders_df[['product_id', 'product_name', 'category', 'sub_category']].drop_duplicates(subset=['product_id'])
regions = people_df[['region', 'manager']]

# load data
client = bq.Client()

## orders
destination_table = 'explore29.superstore.orders'

job_config = bq.LoadJobConfig(
	write_disposition='WRITE_APPEND',
	autodetect=True
)

job = client.load_table_from_dataframe(
	orders,
	destination_table,
	job_config=job_config
)

## customers
destination_table = 'explore29.superstore.customers'

job_config = bq.LoadJobConfig(
	write_disposition='WRITE_APPEND',
	autodetect=True
)

job = client.load_table_from_dataframe(
	customers,
	destination_table,
	job_config=job_config
)

## products
destination_table = 'explore29.superstore.products'

job_config = bq.LoadJobConfig(
	write_disposition='WRITE_APPEND',
	autodetect=True
)

job = client.load_table_from_dataframe(
	products,
	destination_table,
	job_config=job_config
)

## regions
destination_table = 'explore29.superstore.regions'

job_config = bq.LoadJobConfig(
	write_disposition='WRITE_APPEND',
	autodetect=True
)

job = client.load_table_from_dataframe(
	regions,
	destination_table,
	job_config=job_config
)
