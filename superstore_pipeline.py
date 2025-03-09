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
# returns temp table name to be used in the query
def load_temp_table(df:'dataframe', table_name:str) -> tuple:
	temp_table = f'explore29.superstore.{table_name}_tmp'

	job_config = bq.LoadJobConfig(
		write_disposition='WRITE_APPEND',
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
# (col:'column name in target table', src_col:'column name in target table')
def get_col(df:'dataframe') ->tuple:
	col_lst = list(df.columns)
	col = ', '.join(col_lst)

	src_col = [f'src.{elem}' for elem in col_lst]
	src_col = ', '.join(src_col)

	return(col, src_col)

'''
Extract
'''
# id: extract_superstore_data
def extract_superstore_data(file_path:str, **kwargs):
	# check data src file path
	log.info(f"Reading file from: {file_path}")

	# read Excel file
	try:
		orders_df = pd.read_excel(file_path, sheet_name='Orders', header=0)
		log.info(f'orders_df -- shape: {orders_df.shape} --columns: {orders_df.columns.tolist()}')

		people_df = pd.read_excel(file_path, sheet_name='People', header=0)
		log.info(f'people_df -- shape: {people_df.shape} --columns: {people_df.columns.tolist()}')

		returns_df = pd.read_excel(file_path, sheet_name='Returns', header=0)
		log.info(f'returns_df -- shape: {returns_df.shape} --columns: {returns_df.columns.tolist()}')

		# export df as JSON
		ti = kwargs['ti']
		ti.xcom_push(key='orders_df', value=orders_df.to_json(orient='split'))
		ti.xcom_push(key='people_df', value=people_df.to_json(orient='split'))
		ti.xcom_push(key='returns_df', value=returns_df.to_json(orient='split'))
		log.info("File read successfully.")
	except Exception as error:
		log.error(f"Failed to read file: {error}")
		raise

'''
Transform
'''

def process_Orders_Returns(**kwargs):
	ti = kwargs['ti']

	# get Orders data from extract as JSON
	try:
		orders_df_json = ti.xcom_pull(task_ids='extract_superstore_data', key='orders_df')
	except Exception as error:
		log.error(f'Error retrieving data from Orders for processing:\n\n{error}')
		raise
	
	# read Orders data from JSON
	orders_df = pd.read_json(orders_df_json, orient='split')

	# rename headers
	try:
		orders_df = orders_df.rename(columns={
		'Country/Region': 'country',
		'State/Province': 'province',
		'Postal Code': 'post_code'
	})
	except Exception as error:
		log.error(f'Error formatiing headers for Orders:\n\n{error}')
		raise
	
	orders_df.columns = [snake_case(col) for col in list(orders_df.columns)]

	# Orders aggregation
	try:
		agg_instruction = {col: ('sum' if col in ['sales', 'quantity', 'profit'] else 'last') for col in list(orders_df.columns)}
		orders_df = orders_df.groupby(['order_id', 'product_id'], as_index=False).agg(agg_instruction)
	except Exception as error:
		log.error(f'Orders aggregation error:\n\n{error}')
		raise

	# format Orders numerical data
	try:
		orders_df['sales'] = orders_df['sales'].astype(float).round(2)
		orders_df['quantity'] = orders_df['quantity'].astype(int).round(2)
		orders_df['discount'] = orders_df['discount'].astype(float).round(2)
		orders_df['profit'] = orders_df['profit'].astype(float).round(2)
	except Exception as error:
		log.error(f'Error formatting numerical data for Orders:\n\n{error}')
		raise

	# get Returns data as JSON
	try:
		returns_df_json = ti.xcom_pull(task_ids='extract_superstore_data', key='returns_df')
	except Exception as error:
		log.error(f'Error retrieving Returns data for processing:\n\n{error}')
		raise

	# read data from Returns JSON
	returns_df = pd.read_json(returns_df_json, orient='split')
	
	# format Returns headers
	try:
		returns_df.columns = [snake_case(col) for col in returns_df.columns]
	except Exception as error:
		log.error(f'Error formatting Returns header:\n\n{error}')
		raise

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

	# update Orders to JSON
	ti.xcom_push(key='orders_df', value=orders_df.to_json(orient='split'))

def process_People(**kwargs):
	ti = kwargs['ti']

	# get data as JSON
	try:
		people_df_json = ti.xcom_pull(task_ids='extract_superstore_data', key='people_df')
	except Exception as error:
		log.error(f'Error retrieving People data for processing:\n\n{error}')
		raise

	# read data from JSON
	people_df = pd.read_json(people_df_json, orient='split')

	# format headers
	try:
		people_df = people_df.rename(columns={
			'Regional Manager': 'manager',
			'Region': 'region'
		})
	except Exception as error:
		log.error(f'Error formmatting headers for People:\n\n{error}')
		raise

	# remove duplicates
	people_df = people_df.drop_duplicates()
	# handle NULL values
	people_df = people_df.applymap(std_null) # apply map as we are dealing with each indvidual element - use apply for columns

	# update df to JSON
	ti.xcom_push(key='people_df', value=people_df.to_json(orient='split'))

def restructure_df(**kwargs):
	ti = kwargs['ti']
	
	# extract data as JSON
	try:
		orders_df_json = ti.xcom_pull(task_ids='process_Orders_Returns', key='orders_df')
		people_df_json = ti.xcom_pull(task_ids='process_People', key='people_df')
	except Exception as error:
		log.error(f'Error retrieving Orders and People for restructuring:\n\n{error}')
		raise

	# read JSON into df
	orders_df = pd.read_json(orders_df_json, orient='split')
	people_df = pd.read_json(people_df_json, orient='split')

	# restructuring
	orders = orders_df[['order_id', 'order_date', 'ship_date', 'ship_mode', 
					'customer_id', 'product_id',
					'sales', 'quantity', 'discount', 'profit', 'returned']]

	customers = orders_df[['customer_id', 'customer_name', 'segment', 
						'country', 'city', 'province', 'post_code', 'region']].drop_duplicates(subset=['customer_id'])

	products = orders_df[['product_id', 'product_name', 'category', 'sub_category']].drop_duplicates(subset=['product_id'])
	regions = people_df[['region', 'manager']]

	# update df as JSON
	ti.xcom_push(key='orders', value=orders.to_json(orient='split'))
	ti.xcom_push(key='customers', value=customers.to_json(orient='split'))
	ti.xcom_push(key='products', value=products.to_json(orient='split'))
	ti.xcom_push(key='regions', value=regions.to_json(orient='split'))

def load_orders(**kwargs):
	ti = kwargs['ti']

	# extract orders data as JSON
	try:
		orders_json = ti.xcom_pull(task_ids='restructure_df', key='orders')
	except Exception as error:
		log.error(f'Error loading superstore.orders:\n\n{error}')

	# read orders data from JSON
	orders = pd.read_json(orders_json, orient='split')
	orders['order_date'] = pd.to_datetime(orders['order_date'])
	orders['ship_date'] = pd.to_datetime(orders['ship_date'])

	# create temp table to hold new orders data
	temp_table = load_temp_table(orders, 'orders')[-1] # get temp table name while loading to temp table
	columns, src_columns = get_col(orders)

	# query to merge new orders data with existing orders data
	merge_query = f"""
	-- merge into table as target
	MERGE INTO explore29.superstore.orders  AS target
	USING `{temp_table}` AS src ON
		target.order_id = src.order_id
		AND target.product_id = src.product_id
	WHEN NOT MATCHED THEN
	-- insert into these columns in target
	INSERT ({columns})
	-- using these values from src
	VALUES ({src_columns});
	DROP TABLE {temp_table};
	"""

	# merge
	try:
		merge = client.query(merge_query)
		log.info(f'Loaded superstore.orders. {merge.result()}')
	except Exception as error:
		log.error(f'Error loading superstore.orders:\n\n{error}')
		raise

def load_customers(**kwargs):
	ti = kwargs['ti']

	# extract customers data as JSON
	try:
		customers_json = ti.xcom_pull(task_ids='restructure_df', key='customers')
	except Exception as error:
		log.error(f'Error loading superstore.customers:\n\n{error}')

	# read customers data from JSON
	customers = pd.read_json(customers_json, orient='split')

	# create temp table to hold new customers data
	temp_table = load_temp_table(customers, 'customers')[-1]
	columns, src_columns = get_col(customers)

	# query to merge new customers data with existing customer data
	merge_query = f"""
	MERGE INTO explore29.superstore.customers AS target
	USING `{temp_table}` AS src ON
		target.customer_id = src.customer_id
	WHEN NOT MATCHED THEN
	INSERT ({columns})
	VALUES ({src_columns});
	DROP TABLE {temp_table};
	"""

	# merge
	try:
		merge = client.query(merge_query)
		log.info(f'Loaded superstore.customers: {merge.result()}')
	except Exception as error:
		log.error(f'Error loading superstore.customers:\n\n{error}')
		raise

def load_products(**kwargs):
	ti = kwargs['ti']

	# extract products data as JSON
	try:
		products_json = ti.xcom_pull(task_ids='restructure_df', key='products')
	except Exception as error:
		log.error(f'Error loading superstore.products:\n\n{error}')

	# read products data from JSON
	products = pd.read_json(products_json, orient='split')

	# create temp table to store new products data
	temp_table = load_temp_table(products, 'products')[-1]
	columns, src_columns = get_col(products)

	# query to merge new products data with existing products data
	merge_query = f"""
	MERGE INTO explore29.superstore.products AS target
	USING `{temp_table}` AS src ON
		target.product_id = src.product_id
	WHEN NOT MATCHED THEN
	INSERT ({columns})
	VALUES ({src_columns});
	DROP TABLE {temp_table};
	"""

	# merge
	try:
		merge = client.query(merge_query)
		log.info(f'Loaded superstore.products: {merge.result()}')
	except BadRequest as error:
		print(f'Failed to load superstore.products:\n\n{error}')


def load_regions(**kwargs):
	ti = kwargs['ti']

	# extract regions data as JSON
	try:
		regions_json = ti.xcom_pull(task_ids='restructure_df', key='regions')
	except Exception as error:
		log.error(f'Error loading superstore.regions:\n\n{error}')

	# read regions data from JSON
	regions = pd.read_json(regions_json, orient='split')

	# create temp table to store new regions data
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
	schedule_interval=None,  # Correct parameter name
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
		python_callable=extract_superstore_data,
		op_kwargs={
			'file_path': '{{ params.file_path }}' 
		}
	)

	task_process_Orders_Returns = PythonOperator(
		task_id='process_Orders_Returns',
		python_callable=process_Orders_Returns,
		provide_context=True
	)

	task_process_People = PythonOperator(
		task_id='process_People',
		python_callable=process_People,
		provide_context=True
	)

	task_restructure_df = PythonOperator(
		task_id='restructure_df',
		python_callable=restructure_df,
		provide_context=True
	)

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

	# task dependencies
	task_extract >> [task_process_Orders_Returns, task_process_People]
	[task_process_Orders_Returns, task_process_People] >> task_restructure_df
	task_restructure_df >> [task_load_orders, task_load_customers, task_load_products, task_load_regions]