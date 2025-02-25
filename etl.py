import re
import warnings
import pandas as pd
import numpy as np

# filter warnings
warnings.filterwarnings('ignore')

def snake_case(data:str) -> str:
	data = data.lower()
	return(data.strip().replace(' ','_'))

# load data
file_path = 'Sample - Superstore.xls'
orders_df = pd.read_excel(file_path, sheet_name=0, header=0)
people_df = pd.read_excel(file_path, sheet_name=1, header=0)
returns_df = pd.read_excel(file_path, sheet_name=2, header=0)

# reformatting headers to snake case (abc_def)
orders_df.columns = [snake_case(col) for col in orders_df.columns]
people_df.columns = [snake_case(col) for col in people_df.columns]
returns_df.columns = [snake_case(col) for col in returns_df.columns]

# remove dup rows (if any)
if orders_df.duplicated().sum() > 0:
	orders_df.drop_duplicates(keep='last', inplace=True)
if people_df.duplicated().sum() > 0:
	people_df.drop_duplicates(keep='last', inplace=True)
if returns_df.duplicated().sum() > 0:
	returns_df.drop_duplicates(keep='last', inplace=True)
