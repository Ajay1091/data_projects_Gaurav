##########################################################################################
"""
project_name = earthquake-project-dataproc
file_name = configuration.py
date - 2024-10-21
desc - all key constant variable all dependencies define here
version - 1
date of modification - 2024/11/15
"""

###########################################################################################

## importing the libraries and module required
from datetime import datetime

## google service account key
GOOGLE_APPLICATION_CREDENTIALS = r'C:\Users\HP\Desktop\data_projects\earthquake\key\delta-compass-440906-dca1fe6ea297.json'

## url for monthly basis
URL_MONTH = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'

## url for daily basis
URL_DAILY = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'

### GCP storage Bucket variables
PROJECT_NAME = 'all-projects'
BUCKET_NAME = 'earthquake-data-storage-123'

### FILE PATHS TO READ AND WRITE
date_str = datetime.now().strftime('%Y%m%d')

##raw data write path
WRITE_JSON = f"raw_layer_data/{date_str}.json"
WRITE_JSON_DAILY = f'raw_layer_data/daily{date_str}.json'

# Create a unique file name with the current date
READ_JSON_FROM_CLOUD = f"raw_layer_data/{date_str}.json"
READ_JSON_FROM_CLOUD_DAILY = f"raw_layer_data/daily{date_str}.json"

# for silver layer path
WRITE_PARQUATE = f'gs://{BUCKET_NAME}/silver_layer_data/{date_str}.parquet'
WRITE_PARQUATE_DAILY = f'gs://{BUCKET_NAME}/silver_layer_data/daily{date_str}.parquet'

## bigquary variables
PROJECT_ID = 'delta-compass-440906'
DATASET_NAME = 'earthquake_project'
TABLE_NAME = 'dataproc_eartheqake_data'
AUDIT_TABLE = 'dataproc_audit_log'
STAGING_BUCKET = 'earthquake-project-files'




