##########################################################################################
"""
project_name = earthquake-project-dataflow
file_name = utility.py
date - 2024-10-21
desc - all key constant variable all dependencies define here
version - 1
date of modification -
"""

###########################################################################################

## importing the libraries and module required

from datetime import datetime


## date str
date_str = datetime.now().strftime('%Y%m%d')


## google service account key
GOOGLE_APPLICATION_CREDENTIALS = r'C:\Users\HP\Desktop\data_projects\earthquake\key\delta-compass-440906-dca1fe6ea297.json'

## url for monthly basis


URL_MONTH = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'

## url for daily basis

URL_DAILY = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'



### GCP storage Bucket variables
PROJECT_NAME = 'all-projects'
BUCKET_NAME = 'earthquake-dataflow'

# Configure Google Cloud options
JOB_NAME_HISTORICAL = f'historical {date_str}'
JOB_NAME_DAILY = f'daily {date_str}'
LOCATION = 'us-central1'
STAGGING = f'gs://{BUCKET_NAME}/stagging'
TEMP = f'gs://{BUCKET_NAME}/temp'







RAW_LAYER_PATH = f'gs://{BUCKET_NAME}/landing_layer/{date_str}'
RAW_LAYER_PATH_READ = f'gs://{BUCKET_NAME}/landing_layer/{date_str}.json'


READ_JSON_FROM_CLOUD = f"landing_layer/{date_str}.json"
READ_JSON_FROM_CLOUD_DAILY = f"landing_layer/daily{date_str}.json"


# for silver layer path


WRITE_PARQUATE = f'gs://{BUCKET_NAME}/silver_layer/{date_str}'

WRITE_PARQUATE_DAILY = f'gs://{BUCKET_NAME}/silver_layer/daily{date_str}.parquet'

## bigquary variables

PROJECT_ID = 'delta-compass-440906'

DATASET_NAME = 'earthquake_project'

TABLE_NAME = 'dataflow_eartheqake'

STAGING_BUCKET = 'earthquake-project-files'
