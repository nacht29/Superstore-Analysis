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

# Initialize logging
log = LoggingMixin().log

def connect_bq():
	return bq.Client()

def extract(file_path: str):
	warnings.filterwarnings('ignore')
	os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'explore29-33756158108f.json'
	
	# Log the file path for debugging
	log.info(f"Reading file from: {file_path}")
	
	# Read the Excel file
	try:
		orders_df = pd.read_excel(file_path, sheet_name='Orders', header=0)
		people_df = pd.read_excel(file_path, sheet_name='People', header=0)
		returns_df = pd.read_excel(file_path, sheet_name='Returns', header=0)
		log.info("File read successfully.")
	except Exception as e:
		log.error(f"Failed to read file: {e}")
		raise
	
	client = connect_bq()
	transform(orders_df, people_df, returns_df, client)
	load(orders_df, people_df, returns_df, client)

def transform(orders_df, people_df, returns_df, client):
	# Transformation logic here
	pass

def load(orders_df, people_df, returns_df, client):
	# Loading logic here
	pass

with DAG(
	'superstore_pipeline',
	start_date=datetime(2024, 3, 5),
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