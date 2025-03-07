from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.log.logging_mixin import LoggingMixin

import re
import os
import warnings
import xlrd
import pandas as pd
import numpy as np
import mysql.connector as mysql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import *
from google.cloud import bigquery as bq
from google.oauth2 import service_account
from google.api_core.exceptions import *

'''
init logging
'''
log = LoggingMixin().log

'''
Bigquery connection
'''
key_path = "explore29-66295ccb78c3.json" 
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bq.Client(credentials=credentials, project=credentials.project_id)

'''
init dataframe
'''
# raw data
orders_df = pd.DataFrame()
people_df = pd.DataFrame()
returns_df = pd.DataFrame()

# target tables
orders = pd.DataFrame()
customers = pd.DataFrame()
products = pd.DataFrame()
regions = pd.DataFrame()

'''
helper functions - transform
'''
# format headers to snake case
def snake_case(data:str) -> str:
	return(data.lower().strip().replace(' ','_').replace('-', '_'))

# standardise null data
def std_null(data):
	if pd.isna(data):
		return None
	if isinstance(data, str):
		data = data.strip()
		return None if data.lower() in ['null', ''] else data
	return data

'''
helper functions - load
'''
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

'''
Extract
'''
def extract(file_path:str):
	global orders_df, people_df, returns_df
	
	# check data src file path
	log.info(f"Reading file from: {file_path}")
	
	# read Excel file
	try:
		orders_df = pd.read_excel(file_path, sheet_name='Orders', header=0)
		people_df = pd.read_excel(file_path, sheet_name='People', header=0)
		returns_df = pd.read_excel(file_path, sheet_name='Returns', header=0)
		log.info("File read successfully.")
	except Exception as error:
		log.error(f"Failed to read file: {error}")
		raise

'''
Transform
'''

def log_orders():
	global orders_df
	log.info(f"Global orders: {orders_df.shape}, columns: {orders_df.columns.tolist()}")


def process_Orders():
	global orders_df

	# format headers
	try:
		orders_df = orders_df.rename(columns={
		'Country/Region': 'country',
		'State/Province': 'province',
		'Postal Code': 'post_code'
	})
	except Exception as error:
		log.error(f'Failed to reformat headers for Orders table: {error}')
	
	orders_df.columns = [snake_case(col) for col in list(orders_df.columns)]

	# aggregate duplicates
	agg_instruction = {col: ('sum' if col in ['sales', 'quantity', 'profit'] else 'last') for col in list(orders_df.columns)}
	orders_df = orders_df.groupby(['order_id', 'product_id'], as_index=False).agg(agg_instruction)

	# format numerical data
	orders_df['sales'] = orders_df['sales'].astype(float).round(2)
	orders_df['quantity'] = orders_df['quantity'].astype(int).round(2)
	orders_df['discount'] = orders_df['discount'].astype(float).round(2)
	orders_df['profit'] = orders_df['profit'].astype(float).round(2)

def process_People():
	global people_df
	try:
		people_df = people_df.rename(columns={
			'Regional Manager': 'manager',
			'Region': 'region'
		})
	except Exception as error:
		log.error(f'Failed to reformat headers for Peoples table: {error}')

	# remove duplicates
	people_df = people_df.drop_duplicates()
	# handle null values
	people_df = people_df.applymap(std_null)

def process_Returns():
	global returns_df

	log.info(orders_df.columns)

	returns_df.columns = [snake_case(col) for col in returns_df.columns]

	# merge Returns with Orders and handle NULL values
	orders_oid_count = orders_df['order_id'].value_counts().reset_index()
	orders_oid_count.columns = ['order_id', 'orders_oid_count']

	returns_oid_count = returns_df['order_id'].value_counts().reset_index()
	returns_oid_count.columns = ['order_id', 'returns_oid_count']

	same_id_entries = orders_oid_count.merge(returns_oid_count, on='order_id', how='inner')
	mismatch = len(same_id_entries[same_id_entries['orders_oid_count'] != same_id_entries['returns_oid_count']])

	if mismatch == 0:
		orders_df['returned'] = orders_df['order_id'].isin(returns_df['order_id']).map({True: 'Yes', False: 'No'})
		orders_df = orders_df.applymap(std_null) # handle null values here for orders_df

'''
Load
'''

def restructure_df(orders_df, people_df, returns_df, client):
	global orders, customers, products, regions

	orders = orders_df[['order_id', 'order_date', 'ship_date', 'ship_mode', 
					'customer_id', 'product_id',
					'sales', 'quantity', 'discount', 'profit', 'returned']]

	customers = orders_df[['customer_id', 'customer_name', 'segment', 
						'country', 'city', 'province', 'post_code', 'region']].drop_duplicates(subset=['customer_id'])

	products = orders_df[['product_id', 'product_name', 'category', 'sub_category']].drop_duplicates(subset=['product_id'])
	regions = people_df[['region', 'manager']]

def load_orders():
	global orders

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
	except Exception as error:
		log.error(f'Failed to load superstore.orders:\n\n{error}')

def load_customers():
	global customers

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
		log.error(f'Failed to load superstore.customers:\n\n{error}')

def load_products():
	global products

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
		print(f'Failed to load superstore.products:\n\n{error}')

def load_regions():
	global regions

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
		print(f'Failed to load superstore.regions:\n\n{error}')

with DAG(
	'superstore_pipeline',
	start_date=datetime(2024, 3, 6),
	schedule=None,
	catchup=False,
	params={
		'file_path': Param(
			default='data-src/sample2.xls',
			type='string',
			title='Excel File Path',
			description='Path to the Excel file to be processed.'
		)
	}
) as dag:

	task_extract = PythonOperator(
		task_id='extract_superstore_data',
		python_callable=extract,
		op_kwargs={
			'file_path': '{{ params.file_path }}'
		},
		provide_context=True
	)

	task_log = PythonOperator(
		task_id='log_global',
		python_callable=log_orders,
		provide_context=True
	)

	task_process_orders = PythonOperator(
		task_id='process_orders',
		python_callable=process_Orders,
		provide_context=True
	)
	
	task_process_people = PythonOperator(
		task_id='process_people',
		python_callable=process_People,
		provide_context=True
	)
	
	task_process_returns = PythonOperator(
		task_id='process_returns',
		python_callable=process_Returns,
		provide_context=True
	)
	
	# Restructure dataframes
	task_restructure = PythonOperator(
		task_id='restructure_dataframes',
		python_callable=restructure_df,
		op_kwargs={
			'orders_df': orders_df,
			'people_df': people_df,
			'returns_df': returns_df,
			'client': client
		},
		provide_context=True
	)
	
	# Load tasks
	task_load_orders = PythonOperator(
		task_id='load_orders',
		python_callable=load_orders,
		provide_context=True
	)
	
	task_load_customers = PythonOperator(
		task_id='load_customers',
		python_callable=load_customers,
		provide_context=True
	)
	
	task_load_products = PythonOperator(
		task_id='load_products',
		python_callable=load_products,
		provide_context=True
	)
	
	task_load_regions = PythonOperator(
		task_id='load_regions',
		python_callable=load_regions,
		provide_context=True
	)

	task_extract >> [task_log, task_process_orders, task_process_people, task_process_returns]
	[task_process_orders, task_process_people, task_process_returns] >> task_restructure
	task_restructure >> [task_load_orders, task_load_customers, task_load_products, task_load_regions]