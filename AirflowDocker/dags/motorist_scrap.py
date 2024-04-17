from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
import time

from motorist_operators.extract import get_links, get_details
from motorist_operators.transform import transform 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

@dag(dag_id='motorist_webscraper', default_args=default_args, schedule_interval='@daily', catchup=False)
def webscraper_taskflow():

    @task(task_id='fetch_links')
    def fetch_links():
        return get_links()

    @task(task_id='fetch_details')
    def fetch_details(links):
        return get_details(links)

    @task(task_id='transform_data')
    def transform_data(details):
        return transform(details)

    @task(task_id='save_data_to_bigquery')
    def save_data_to_bigquery(df):
        
    
        hook = BigQueryHook(bigquery_conn_id='gcp_is3107', use_legacy_sql=False)
        client = hook.get_client()
    
        table_id = 'is3107-418903.temp.motorist-temp'
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("model", "STRING"),
                bigquery.SchemaField("price", "FLOAT"),
                bigquery.SchemaField("depreciation", "FLOAT"),
                bigquery.SchemaField("milleage", "FLOAT"),
                bigquery.SchemaField("registration_date", "STRING"),
                bigquery.SchemaField("coe_left", "FLOAT"),
                bigquery.SchemaField("no_of_owner", "INTEGER"),
                bigquery.SchemaField("omv", "FLOAT"),
                bigquery.SchemaField("arf", "FLOAT"),
                bigquery.SchemaField("power", "FLOAT"),
                bigquery.SchemaField("capacity", "FLOAT"),
                bigquery.SchemaField("accessories", "STRING"),
            ],
            write_disposition="WRITE_APPEND", 
        )

        try:
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result() 
            print(f"Loaded {df.shape[0]} rows into {table_id}")
        except Exception as e:
            print(f"An error occurred while loading data to BigQuery: {e}")

    @task(task_id='google_storage_dump')
    def save_to_google_storage(transformed_data):
        gcs_hook = GCSHook(google_cloud_storage_conn_id='google_cloud_default')
        bucket_name = 'is3107-datasets'
        folder_name = 'motoristsg'  # Replace with your desired folder name
        today = datetime.now().strftime("%d%m%Y")
        file_name = f'{today}_motorist.csv'
        destination_blob_name = f'{folder_name}/{file_name}'
        gcs_hook.upload(bucket_name=bucket_name, object_name=destination_blob_name, data=transformed_data.to_csv(index=False).encode())

        
        
    links = fetch_links()
    details = fetch_details(links)
    transformed_data = transform_data(details)
    # save_data_to_bigquery(transformed_data)
    save_to_google_storage(transformed_data)

dag = webscraper_taskflow()

