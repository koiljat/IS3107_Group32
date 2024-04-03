from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

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
        cleaned_df.to_csv('/opt/airflow/data/sgcarmart_cleaned.csv', index=True)
        return cleaned_df

    test_scraper_task = execute_test_scraper(10)
    cleaned_data_task = transform_data(test_scraper_task)

dag = sgcarmart_taskflow()