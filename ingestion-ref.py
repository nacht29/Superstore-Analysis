from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models.param import Param

import os
import warnings
import pandas as pd
import numpy as np
from google.cloud import bigquery as bq

# Airflow Basics Explanation:
# - DAG: A collection of tasks that should be run in a specific order
# - Operators: Predefined ways to perform tasks (like running Python functions)
# - Variables: Airflow's way of storing and retrieving configuration
# - Parameters: Allow passing runtime configurations

def process_superstore_data(
	file_path=None, 
	sheet_names=None, 
	**kwargs
):
	"""
	Flexible data processing function that can handle different file paths and sheet names
	
	Args:
	- file_path: Path to the Excel file (can be local or cloud storage)
	- sheet_names: Dictionary mapping sheet types to sheet names
	"""
	# Use default sheet names if not provided
	if sheet_names is None:
		sheet_names = {
			'orders': 'Orders',
			'people': 'People', 
			'returns': 'Returns'
		}

	# If no file path is provided, try to get from Airflow Variables
	if file_path is None:
		file_path = Variable.get("superstore_file_path", default_var=None)
	
	# Validate file path
	if not file_path:
		raise ValueError("No file path provided. Please set 'superstore_file_path' in Airflow Variables.")
	
	# Rest of the processing logic remains the same as in previous script
	warnings.filterwarnings('ignore')

	# Read Excel with flexible sheet names
	orders_df = pd.read_excel(
		file_path, 
		sheet_name=sheet_names['orders'], 
		header=0
	)
	people_df = pd.read_excel(
		file_path, 
		sheet_name=sheet_names['people'], 
		header=0
	)
	returns_df = pd.read_excel(
		file_path, 
		sheet_name=sheet_names['returns'], 
		header=0
	)
	
	# [... rest of the original data processing code ...]
	
	# Push processed data to XCom
	kwargs['ti'].xcom_push(key='orders', value=orders.to_dict(orient='records'))
	kwargs['ti'].xcom_push(key='customers', value=customers.to_dict(orient='records'))
	kwargs['ti'].xcom_push(key='products', value=products.to_dict(orient='records'))
	kwargs['ti'].xcom_push(key='regions', value=regions.to_dict(orient='records'))

# Create DAG with flexible configuration
with DAG(
	'superstore_data_pipeline',
	start_date=datetime(2024, 1, 1),
	schedule_interval=None,  # Manually triggered
	catchup=False,
	# Define parameters that can be passed at runtime
	params={
		'file_path': Param(
			default='/path/to/default/Sample - Superstore.xls', 
			type='string', 
			title='Excel File Path'
		),
		'orders_sheet': Param(
			default='Orders', 
			type='string', 
			title='Orders Sheet Name'
		),
		'people_sheet': Param(
			default='People', 
			type='string', 
			title='People Sheet Name'
		),
		'returns_sheet': Param(
			default='Returns', 
			type='string', 
			title='Returns Sheet Name'
		)
	}
) as dag:
	# Task to process data with flexible input
	process_data_task = PythonOperator(
		task_id='process_superstore_data',
		python_callable=process_superstore_data,
		op_kwargs={
			'file_path': '{{ params.file_path }}',
			'sheet_names': {
				'orders': '{{ params.orders_sheet }}',
				'people': '{{ params.people_sheet }}',
				'returns': '{{ params.returns_sheet }}'
			}
		},
		provide_context=True
	)

	# Task to load data to BigQuery (from previous example)
	load_bigquery_task = PythonOperator(
		task_id='load_to_bigquery',
		python_callable=load_to_bigquery,
		provide_context=True
	)

	# Define task dependencies
	process_data_task >> load_bigquery_task