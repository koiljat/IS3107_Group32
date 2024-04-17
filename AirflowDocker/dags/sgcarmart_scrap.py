from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from sgcarmart_operators.sgcarmart import run_scraper, run_test_scraper, transform_sgcarmart_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

@dag(dag_id='sgcarmart_scraper', default_args=default_args, schedule_interval='@daily', catchup=False)
def sgcarmart_taskflow():

    @task
    def execute_test_scraper(num_pages): # test_scraper used to test out scraping of 10 pages. Change before full test runs
        return run_test_scraper(num_pages)

    @task
    def transform_data(df):
        cleaned_df = transform_sgcarmart_data(df)
        return cleaned_df
    
    @task
    def save_to_google_storage(transformed_data):
        gcs_hook = GCSHook(google_cloud_storage_conn_id='google_cloud_default')
        bucket_name = 'is3107-datasets'
        folder_name = 'sgcarmart'  # Replace with your desired folder name
        today = datetime.now().strftime("%d%m%Y")
        file_name = f'{today}_sgcarmart.csv'
        destination_blob_name = f'{folder_name}/{file_name}'
        gcs_hook.upload(bucket_name=bucket_name, object_name=destination_blob_name, data=transformed_data.to_csv(index=False).encode())


    test_scraper_task = execute_test_scraper(1)
    cleaned_data_task = transform_data(test_scraper_task)
    save_to_google_storage(cleaned_data_task)

dag = sgcarmart_taskflow()