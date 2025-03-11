import os
import warnings
import subprocess
import pandas as pd
from datetime import date
from google.cloud import bigquery as bq
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.api_core.exceptions import Forbidden, NotFound

# print("Current working directory:", os.getcwd())
# print("Files in current directory:", os.listdir('sql-scripts'))

# Service Account JSON
BQ_SERVICE_ACCOUNT = 'json-keys/explore29-5a4f7581e39f.json'
BUCKET_SERVICE_ACCOUNT = 'json-keys/test_bucket_gdrive.json'

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

def file_type_in_dir(file_type:str):
	files_in_dir = os.listdir('sql-scripts')
	files = [file for file in files_in_dir if file.endswith(file_type)]
	return files

def gen_file_name(file:str, file_type:str, replace_type:str):
	file_name = f'{file.replace(file_type, '')}_{date.today()}{replace_type}'
	return file_name

def query_data():
	files_in_dir = os.listdir('sql-scripts')

	sql_scripts = file_type_in_dir('.sql')

	for script in sql_scripts:
		with open(script, 'r') as cur_script:
			query = ' '.join([line for line in cur_script])
			out_file = gen_file_name(script, '.sql', '.csv')
			results = bq_client.query(query).to_dataframe()
			results.to_csv(out_file, sep='|', encoding='utf-8', index=False, header=True)

def load_bq():
	bucket = bucket_client.get_bucket('bucket_drive_test')

	load_files = file_type_in_dir('.csv')
	for file in load_files:
		bucket.blob(file).upload_from_filename(file)

def authenticate():
	creds = service_account.Credentials.from_service_account_file(BUCKET_SERVICE_ACCOUNT, scopes=SCOPES)
	return creds

def load_gdrive():
	creds = authenticate()
	service = build('drive', 'v3', credentials=creds)

	query = f"'{PARENT_FOLDER_ID}' in parents and trashed=false"
	response = service.files().list(q=query, fields='files(id, name)').execute()
	files_in_drive = response.get('files_in_drive') # response.get('files', [])

	load_files = file_type_in_dir('.csv')
	for load_file in load_files:
		query = f"'{PARENT_FOLDER_ID}' in parents and name='{load_file}' and trashed=false"
		response = service.files().list(q=query, fields='files(id, name)').execute()
		dup_files = response.get('files')

		if dup_files:
			for dup_file in dup_files:
				service.files().delete(fileId=dup_file['id']).execute()
	
		file_metadata ={
			'name': load_file,
			'parents': [PARENT_FOLDER_ID]
		}

		file = service.files().create(
			body=file_metadata,
			media_body=load_file
		).execute()

def main():
	query_data()
	load_bq()
	load_gdrive()

	rm_files = file_type_in_dir('.csv')
	for file in rm_files:
		os.remove(file)

main()