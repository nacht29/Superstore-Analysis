import pandas as pd
from google.cloud import bigquery as bq
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core.exceptions import Forbidden, NotFound

# log = LoggingMixin().log

key_path = "explore29-66295ccb78c3.json" 
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bq.Client(credentials=credentials, project=credentials.project_id)

def query():
	query = f"""
	SELECT order_id
	FROM explore29.superstore.orders
	LIMIT 20
	"""
	
	results = client.query(query).to_dataframe