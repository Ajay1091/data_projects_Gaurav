###########################################################################################################
"""
file_name = historical.py
desc = manual_data ingection by using datflow in bigquary
date = 2024/11/05
version = 1

"""
############################################################################################################


## import library

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import configuration as conf
from utility import ReadDataFromApiJson, feature_flatten, transformation
from apache_beam.io.gcp.gcsio import GcsIO
import pyarrow as pa
import pyarrow.parquet as pq
import io
import json
import os
import pandas as pd

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    try:
        # Set environment variable for Google credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = conf.GOOGLE_APPLICATION_CREDENTIALS
        options = PipelineOptions()

        # Configure Google Cloud options
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = conf.PROJECT_NAME
        google_cloud_options.job_name = conf.JOB_NAME_DAILY
        google_cloud_options.region = conf.LOCATION
        google_cloud_options.staging_location = conf.STAGGING
        google_cloud_options.temp_location = conf.TEMP

        ## data read from api

        content = ReadDataFromApiJson.reading(conf.URL_DAILY)

        content_as_string = json.dumps(content)

        ## load json data to GCS bucket
        with beam.Pipeline(options=options) as p1:
            file_GCS = (
                    p1
                    | 'Create PCollection' >> beam.Create([content_as_string])
                    | 'Write to GCS' >> beam.io.WriteToText(conf.RAW_LAYER_PATH_DAILY, file_name_suffix='.json',
                                                            shard_name_template='', num_shards=1)
            )

        ### load from GCS bucket and flatten and do trnsformation and transfer to GCS
        ## write file to GCS in parqute form and dump to bq

        with beam.Pipeline(options=options) as p2:
            tranasform_data = (
                    p2
                    | 'Read Raw Data' >> beam.io.ReadFromText(conf.RAW_LAYER_PATH_READ_DAILY)
                    | 'Flatten Features' >> beam.Map(feature_flatten)
                    | 'Transform Data' >> beam.FlatMap(transformation)
                # | 'print' >> beam.Map(print)
            )

            parqute_silver = (tranasform_data
                              | 'parqute to GCS' >> beam.io.WriteToParquet(
                        conf.WRITE_PARQUATE_DAILY, schema=conf.CLEAN_DATA_PARQUTE_SCHEMA, file_name_suffix='.parquet',
                        shard_name_template=''
                    )
                              )

            bq_write = (tranasform_data
                        | "write into bigquery" >> beam.io.WriteToBigQuery(

                        table=conf.TABLE,
                        # schema=bq_schema,
                        schema='SCHEMA_AUTODETECT',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED

                    ))





    except Exception as e:
        print(f"Pipeline failed: {e}")


