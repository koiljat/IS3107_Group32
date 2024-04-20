from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from modules.extract import get_links, get_details
from modules.transform import transform
from modules.sgcarmart import run_test_scraper, transform_sgcarmart_data
from modules.combine_cars import get_df_from_gcs, append_final_table, transform_car_combined, load_temp_bq_table
from io import StringIO
import pandas as pd


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    
}

@dag(dag_id='daily_dag', default_args=default_args, schedule_interval='@daily', catchup=False)
def webscraper_taskflow():

    @task(task_id='start')
    def initiate_dag():
        return "DAG Initiated"
    
    @task_group(group_id='data_ingestion')
    def web_scraping():

        @task_group(group_id='motoristsg_tasks')
        def motoristsg_group():
            @task(task_id='motoristsg_fetch_links')
            def fetch_links():
                return get_links()

            @task(task_id='motoristsg_fetch_data')
            def fetch_data(links):
                return get_details(links)

            @task(task_id='motoristsg_transform')
            def transform_data(details):
                return transform(details)

            @task(task_id='motoristsg_data_to_gcs')
            def dump_to_gcs(transformed_data):
                gcs_hook = GCSHook(google_cloud_storage_conn_id='google_cloud_default')
                bucket_name = 'is3107-datasets'
                folder_name = 'motoristsg'
                today = datetime.now().strftime("%d%m%Y")
                file_name = f'{today}_motorist.csv'
                destination_blob_name = f'{folder_name}/{file_name}'
                gcs_hook.upload(bucket_name=bucket_name, object_name=destination_blob_name, data=transformed_data.to_csv(index=False).encode())
                return f"Data uploaded to GCS at {destination_blob_name}"

            links = fetch_links()
            data = fetch_data(links)
            transformed = transform_data(data)
            storage = dump_to_gcs(transformed)

        @task_group(group_id='sgcarmart_tasks')
        def sgcarmart_group():
            @task(task_id='sgcarmart_fetch_data')
            def fetch_data(num_pages):
                return run_test_scraper(num_pages)

            @task(task_id='sgcarmart_transform')
            def transform_data(df):
                return transform_sgcarmart_data(df)

            @task(task_id='sgcarmart_data_to_gcs')
            def dump_to_gcs(transformed_data):
                gcs_hook = GCSHook(google_cloud_storage_conn_id='google_cloud_default')
                bucket_name = 'is3107-datasets'
                folder_name = 'sgcarmart'
                today = datetime.now().strftime("%d%m%Y")
                file_name = f'{today}_sgcarmart.csv'
                destination_blob_name = f'{folder_name}/{file_name}'
                gcs_hook.upload(bucket_name=bucket_name, object_name=destination_blob_name, data=transformed_data.to_csv(index=False).encode())
                return f"Data uploaded to GCS at {destination_blob_name}"

            data = fetch_data(10)
            transformed = transform_data(data)
            storage = dump_to_gcs(transformed)
            
        motoristsg_group()
        sgcarmart_group()

    @task_group(group_id='staging_area')
    def merge_data():
        
        @task_group(group_id='big_query_merge')
        def big_query_merge():            
            @task(task_id='fetch_motoristsg_from_gcs')
            def get_motoristsg_from_gcs():
                gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
                
                bucket_name = 'is3107-datasets'
                folder_name = 'motoristsg'
                today = datetime.now().strftime("%d%m%Y")
                file_name = f'{today}_motorist.csv'
                destination_blob_name = f'{folder_name}/{file_name}'

                # Download the content of the file
                file_content = gcs_hook.download(bucket_name=bucket_name, object_name=destination_blob_name)
                file_str = file_content.decode('utf-8')
                dataframe = pd.read_csv(StringIO(file_str))
            
                return dataframe
            
            @task(task_id='fetch_sgcarmart_from_gcs')
            def get_sgcarmart_from_gcs():
                gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        
                bucket_name = 'is3107-datasets'
                folder_name = 'sgcarmart'
                today = datetime.now().strftime("%d%m%Y")
                file_name = f'{today}_sgcarmart.csv'
                destination_blob_name = f'{folder_name}/{file_name}'

                # Download the content of the file
                file_content = gcs_hook.download(bucket_name=bucket_name, object_name=destination_blob_name)
                file_str = file_content.decode('utf-8')
                dataframe = pd.read_csv(StringIO(file_str))
            
                return dataframe
            
            @task(task_id='transform_car_combined')
            def transform_car(df1, df2):
                return transform_car_combined(df1, df2)
                
            @task(task_id='load_temp_bq_table')
            def load_temp(df):
                return load_temp_bq_table(df)
            
            @task(task_id='append_final_table')
            def append_final():
                return append_final_table()
                
            motorist_data = get_motoristsg_from_gcs()
            sgcarmart_data = get_sgcarmart_from_gcs()
            transformed_data = transform_car(motorist_data, sgcarmart_data)
            load_temp_bq_table(transformed_data) >> append_final_table()
        
        @task_group(group_id='query_CarAPI')
        def query_CarAPI():
            @task(task_id='query_sgcarmart')
            def query_sgcarmart():
                pass
            
            @task(task_id='query_motoristsg')
            def query_motoristsg():
                pass
            
            query_sgcarmart()
            query_motoristsg()
        
        big_query_merge()
        query_CarAPI()
    
    
    @task(task_id='end')
    def end():
        print(f"End task received")
        
    
    @task_group(group_id='data_warehouse')
    def BQ_transformation():
        pass

    @task_group(group_id='serving_layer')
    def train_model():
        pass
    
    initiate_dag() >> web_scraping() >> merge_data() >> BQ_transformation() >> train_model() >> end()

dag = webscraper_taskflow()
