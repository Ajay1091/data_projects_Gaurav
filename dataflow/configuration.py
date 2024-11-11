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
import os
import logging
# from util import Utils
from datetime import datetime
import requests
import json
import pyarrow


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
JOB_NAME_HISTORICAL = f'historical{date_str}'
JOB_NAME_DAILY = f'daily{date_str}'
LOCATION = 'us-central1'
STAGGING = f'gs://{BUCKET_NAME}/stagging'
TEMP = f'gs://{BUCKET_NAME}/temp'







RAW_LAYER_PATH = f'gs://{BUCKET_NAME}/landing_layer/{date_str}'
RAW_LAYER_PATH_READ = f'gs://{BUCKET_NAME}/landing_layer/{date_str}.json'


RAW_LAYER_PATH_DAILY = f'gs://{BUCKET_NAME}/landing_layer/daily{date_str}'
RAW_LAYER_PATH_READ_DAILY = f'gs://{BUCKET_NAME}/landing_layer/daily{date_str}.json'

READ_JSON_FROM_CLOUD = f"landing_layer/{date_str}.json"
READ_JSON_FROM_CLOUD_DAILY = f"landing_layer/daily{date_str}.json"



CLEAN_DATA_PARQUTE_SCHEMA  = pyarrow.schema([
    ('mag', pyarrow.float64()),
    ('place', pyarrow.string()),
    ('time', pyarrow.string()),        # Timestamp in seconds
    ('updated', pyarrow.string()),     # Timestamp in seconds
    ('tz', pyarrow.string()),
    ('url', pyarrow.string()),
    ('detail', pyarrow.string()),
    ('felt', pyarrow.float64()),
    ('cdi', pyarrow.float64()),
    ('mmi', pyarrow.float64()),
    ('alert', pyarrow.string()),
    ('status', pyarrow.string()),
    ('tsunami', pyarrow.int64()),
    ('sig', pyarrow.int64()),
    ('net', pyarrow.string()),
    ('code', pyarrow.string()),
    ('ids', pyarrow.string()),
    ('sources', pyarrow.string()),
    ('types', pyarrow.string()),
    ('nst', pyarrow.int64()),
    ('dmin', pyarrow.float64()),
    ('rms', pyarrow.float64()),
    ('gap', pyarrow.int64()),
    ('magType', pyarrow.string()),
    ('type', pyarrow.string()),
    ('title', pyarrow.string()),
    ('area', pyarrow.string()),
    ('longitude', pyarrow.float64()),
    ('latitude', pyarrow.float64()),
    ('depth', pyarrow.float64()),
    ('insert_date', pyarrow.string())  # Timestamp in seconds
])

# for silver layer path


WRITE_PARQUATE = f'gs://{BUCKET_NAME}/silver_layer/{date_str}'
READ_PARQUATE = f'gs://{BUCKET_NAME}/silver_layer/{date_str}.parquet'

WRITE_PARQUATE_DAILY = f'gs://{BUCKET_NAME}/silver_layer/daily{date_str}'

## bigquary variables

PROJECT_ID = 'delta-compass-440906'

DATASET_NAME = 'earthquake_project'

TABLE_NAME = 'dataflow_eartheqake'

STAGING_BUCKET = 'earthquake-project-files'

TABLE = f'{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}'

