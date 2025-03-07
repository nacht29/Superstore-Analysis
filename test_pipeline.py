from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance
from airflow.decorators import task
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

def extract(file_path:str, **context):
	global orders_df, people_df, returns_df
	
	# check data src file path
	log.info(f"Reading file from: {file_path}")
	
	# read Excel file
	try:
		orders_df = pd.read_excel(file_path, sheet_name='Orders', header=0)
		people_df = pd.read_excel(file_path, sheet_name='People', header=0)
		returns_df = pd.read_excel(file_path, sheet_name='Returns', header=0)

		ti = context['ti']
		ti.xcom_push(key='orders_df', value=orders_df.to_json(orient='split'))
		ti.xcom_push(key='people_df', value=people_df.to_json(orient='split'))
		ti.xcom_push(key='returns_df', value=returns_df.to_json(orient='split'))
		log.info("File read successfully.")
	except Exception as error:
		log.error(f"Failed to read file: {error}")
		raise

def logging(**context):
	ti = context['ti']
	
	# Pull data from XCom
	orders_json = ti.xcom_pull(task_ids='extract_superstore_data', key='orders_df')
	people_json = ti.xcom_pull(task_ids='extract_superstore_data', key='people_df')

	# Deserialize DataFrames
	orders_df = pd.read_json(orders_json, orient='split')
	people_df = pd.read_json(people_json, orient='split')

	# Log using the task instance's logger
	ti.log.info(f"Orders DataFrame: Shape = {orders_df.shape}, Columns = {orders_df.columns.tolist()}")
	ti.log.info(f"People DataFrame: Shape = {people_df.shape}, Columns = {people_df.columns.tolist()}")

with DAG(
	'test_pipeline',
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
		task_id='log_orders_and_people',
		python_callable=logging,
		provide_context=True
	)

	task_extract >> task_log 
