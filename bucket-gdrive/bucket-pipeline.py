import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.log.logging_mixin import LoggingMixin

import os
import warnings
import subprocess
import pandas as pd
from datetime import date, datetime
from google.cloud import bigquery as bq
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.api_core.exceptions import Forbidden, NotFound

TIME_ZONE = pendulum.timezone('Asia/Singapore')
START_DATE = datetime(2025, 3, 11, tzinfo=TIME_ZONE)

SQL_SCRIPTS_PATH = 'sql-scripts/'
JSON_KEYS_PATH = 'json-keys/'

# Service Account JSON
# consider secret manager - safekeeping of json keys (ask Ken)
BQ_SERVICE_ACCOUNT = f'{JSON_KEYS_PATH}explore29-5a4f7581e39f.json'
BUCKET_SERVICE_ACCOUNT = f'{JSON_KEYS_PATH}test_bucket_gdrive.json'

# Google Drive auth
SCOPES = ['https://www.googleapis.com/auth/drive']
PARENT_FOLDER_ID = '1OLll_SpLxMhVC5kY3ISu0lf0cdF01_pP'

warnings.filterwarnings('ignore')

# set up BQ credentials to query data
bq_key_path = BQ_SERVICE_ACCOUNT
bq_credentials = service_account.Credentials.from_service_account_file(bq_key_path)
bq_client = bq.Client(credentials=bq_credentials, project=bq_credentials.project_id)

# set up Bucket credentials to load CSV to bucket
bucket_key_path = BUCKET_SERVICE_ACCOUNT
bucket_credentials = service_account.Credentials.from_service_account_file(bucket_key_path)
bucket_client = storage.Client(credentials=bucket_credentials, project=bucket_credentials.project_id)

def file_type_in_dir(dir:str, file_type:str):
	if dir is None:
		files_in_dir = os.listdir()
	else:
		files_in_dir = os.listdir(dir)

	if file_type is None:
		return(files_in_dir)

	files = [file for file in files_in_dir if file.endswith(file_type)]
	return files

def gen_file_name(file:str, file_type:str, replace_type:str):
	file_name = f"{file.replace(file_type, '')}_{date.today()}{replace_type}"

	return file_name

def query_data():
	files_in_dir = os.listdir(SQL_SCRIPTS_PATH)

	sql_scripts = file_type_in_dir(SQL_SCRIPTS_PATH, '.sql')

	for script in sql_scripts:
		with open(f'{SQL_SCRIPTS_PATH}{script}', 'r') as cur_script:
			query = ' '.join([line for line in cur_script])
			out_file = gen_file_name(script, '.sql', '.csv')
			results = bq_client.query(query).to_dataframe()
			results.to_csv(out_file, sep='|', encoding='utf-8', index=False, header=True)

def load_bq():
	bucket = bucket_client.get_bucket('bucket_drive_test')

	load_files = file_type_in_dir(None, '.csv')
	for file in load_files:
		bucket.blob(file).upload_from_filename(file)

def authenticate():
	creds = service_account.Credentials.from_service_account_file(BUCKET_SERVICE_ACCOUNT, scopes=SCOPES)
	return creds

def load_gdrive():
	creds = authenticate()
	service = build('drive', 'v3', credentials=creds)

	# get all existing files in Drive
	query = f"'{PARENT_FOLDER_ID}' in parents and trashed=false"
	response = service.files().list(q=query, fields='files(id, name)').execute()
	files_in_drive = response.get('files_in_drive') # response.get('files', [])

	# get name of all files to be loaded (.csv for now)
	load_files = file_type_in_dir(None, '.csv')

	# check for duplicates
	for load_file in load_files:
		# get list of dup files
		query = f"'{PARENT_FOLDER_ID}' in parents and name='{load_file}' and trashed=false"
		response = service.files().list(q=query, fields='files(id, name)').execute()
		dup_files = response.get('files')

		# delete duplicates if any
		if dup_files:
			for dup_file in dup_files:
				service.files().delete(fileId=dup_file['id']).execute()
	
		# data of file to be loaded
		file_metadata ={
			'name': load_file,
			'parents': [PARENT_FOLDER_ID]
		}

		# load file to drive
		file = service.files().create(
			body=file_metadata,
			media_body=load_file
		).execute()

def remove_outfiles():
	files_in_dir = file_type_in_dir(None, '.csv')

	# remove all output files (in main dir by default)
	for file in files_in_dir:
		os.remove(file)
	print(files_in_dir)

with DAG(
	'bucket_pipeline',
	start_date=START_DATE,
	# runs at 13:51 UTC +8
	schedule="54 11 * * *",
	catchup=True
) as dag:

	task_query_data = PythonOperator(
		task_id='Get_data_from_BigQuery',
		python_callable=query_data,
		provide_context=True
	)

	task_load_bq = PythonOperator(
		task_id='Load_files_to_BQ',
		python_callable=load_bq,
		provide_context=True
	)

	task_load_gdrive = PythonOperator(
		task_id='Load_files_to_Drive',
		python_callable=load_gdrive,
		provide_context=True
	)

	task_remove_outfiles = PythonOperator(
		task_id='Remove_out_files',
		python_callable=remove_outfiles,
		provide_context=True
	)

	task_query_data >> task_load_bq
	task_load_bq >> task_load_gdrive
	task_load_gdrive >> task_remove_outfiles

query_data()
load_bq()
load_gdrive()
remove_outfiles()