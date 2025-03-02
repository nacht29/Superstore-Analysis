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
# show max columns
pd.set_option('display.max_columns', None)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'explore29-33756158108f.json'

file_path = 'data-src/Sample - Superstore.xls'
orders_df = pd.read_excel(file_path, sheet_name='Orders', header=0)
people_df = pd.read_excel(file_path, sheet_name='People', header=0)
returns_df = pd.read_excel(file_path, sheet_name='Returns', header=0)

def snake_case(data:str) -> str:
	return(data.lower().strip().replace(' ','_').replace('-', '_'))

# orders
# rename headers
try:
	orders_df = orders_df.rename(columns={
	'Country/Region': 'country',
    'State/Province': 'province',
    'Postal Code': 'post_code'
})
except Exception as error:
	print(f'Failed to reformat headers for orders table: {error}')

# format header
orders_df.columns = [snake_case(col) for col in orders_df.columns]

# remove duplicates
orders_df = orders_df.drop_duplicates(subset=['order_id', 'product_id', 'quantity'], keep='last')

# merge entries that have similar columns except quantity
merged_df = orders_df.groupby(['order_id', 'product_id'], as_index=False).agg({
	'sales': 'sum',
	'quantity': 'sum',
	'profit': 'sum'
})

# format data to float
orders_df['sales'] = orders_df['sales'].astype(float).round(2)
orders_df['quantity'] = orders_df['quantity'].astype(int).round(2)
orders_df['discount'] = orders_df['discount'].astype(float).round(2)
orders_df['profit'] = orders_df['profit'].astype(float).round(2)

