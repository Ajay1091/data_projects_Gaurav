##########################################################################################
"""
project_name = earthquake-project-dataproc
file_name = historical.py
date - 2024-10-21
desc - all operation give in project task pdf
version -
date of modification -
"""

###########################################################################################

## importing the libraries and module required

from pyspark.sql import SparkSession
import configuration as conf
from utility import ReadDataFromApiJson,UploadtoGCS,GCSFileDownload,EarthquakeDataFrameCreation,Transformation,SilverParquet,UploadToBigquery


### creating spark session

# spark = SparkSession.builder.appName('earthquke-project').getOrCreate()



def main():
    content = ReadDataFromApiJson.reading(conf.URL_MONTH)
    upload = UploadtoGCS.uploadjson(conf.BUCKET_NAME,content)
    json_data = GCSFileDownload(conf.BUCKET_NAME).download_json_as_text(conf.READ_JSON_FROM_CLOUD)
    dataframe = EarthquakeDataFrameCreation(json_data).convert_to_dataframe()
    df = Transformation.process(dataframe)
    parquate_upload = SilverParquet.upload_parquet(df,conf.WRITE_PARQUATE)
    bq_upload = UploadToBigquery(conf.PROJECT_ID,conf.DATASET_NAME,conf.STAGING_BUCKET).to_bigquery(conf.TABLE_NAME,df)



if __name__ == '__main__':
    main()

#gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar



