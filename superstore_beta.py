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
	people_df = people_df.applymap(std_null)

	# update df to JSON
	ti.xcom_push(key='people_df', value=people_df.to_json(orient='split'))

with DAG(
	'superstore_beta',
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
			'file_path': '{{ params.file_path }}'  # Access params correctly
		}
	)

	task_process_Orders_Returns = PythonOperator(
		task_id='process_Orders_Returns',
		python_callable=process_Orders_Returns  # Renamed function to avoid conflict
	)

	task_process_People = PythonOperator(
		task_id='process_People',
		python_callable=process_People  # Renamed function to avoid conflict
	)

	# Define task dependencies
	task_extract >> [task_process_Orders_Returns, task_process_People]