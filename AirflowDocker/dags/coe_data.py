from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
import time
import pandas as pd
from io import StringIO, BytesIO
import json
import requests
from zipfile import ZipFile

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

@dag(dag_id='coe_retriever', default_args=default_args, schedule_interval='@monthly', catchup=False)
def coe_retriever():
    @task(task_id="retrieve_coe")
    def retrieve_coe():
        url = "https://datamall.lta.gov.sg/content/dam/datamall/datasets/Facts_Figures/Vehicle%20Registration/COE%20Bidding%20Results.zip"
        # Send an HTTP GET request to the URL
        response = requests.get(url)
        # Raise an exception if the request failed
        response.raise_for_status()

        with ZipFile(BytesIO(response.content)) as z:
            print("Files in the ZIP:", z.namelist())
            csv_filename = z.namelist()[1] # Index of correct csv
            with z.open(csv_filename) as csv_file:
                # Read the CSV file into a pandas DataFrame
                dataframe = pd.read_csv(csv_file)
        print(dataframe)
        return dataframe

    @task(task_id="save_coe_csv")
    def save_coe_csv(dataframe):
        gcs_hook = GCSHook(google_cloud_storage_conn_id='google_cloud_default')
        bucket_name = 'is3107-datasets'
        object_name = 'sgCOE/coe.csv'
        csv_data = dataframe.to_csv(index=False).encode()

        gcs_hook.upload(bucket_name=bucket_name, object_name=object_name, data=csv_data)

    @task(task_id="save_to_bq")
    def save_data_to_bq(dataframe):
        hook = BigQueryHook(bigquery_conn_id='google_cloud_default', use_legacy_sql=False)
        project_id = 'your-gcp-project-id'  # replace with your GCP project ID
        dataset_id = 'your_dataset_id'  # replace with your BigQuery dataset ID
        table_id = 'your_table_id'  # replace with your BigQuery table ID
        destination_table = f"{project_id}.{dataset_id}.{table_id}"

        # Load the DataFrame to BigQuery
        hook.insert_rows(dataframe, destination_table, auto_create_table=True)
    
    df = retrieve_coe()
    save_coe_csv(df)

dag = coe_retriever()
