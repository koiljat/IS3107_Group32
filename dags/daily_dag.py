from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from modules.extract import get_links, get_details
from modules.transform import transform
from modules.sgcarmart import run_scraper, transform_sgcarmart_data
from modules.combine_cars import append_final_table, transform_car_combined, load_temp_bq_table
from io import StringIO
import pandas as pd
from modules.api_operations import api_calling_logic_motorist, upload_to_BQ, api_calling_logic_sgcarmart
from modules.ml_pipeline import data_encoding, change_reg_date_to_years, drop_cols, drop_highly_correlated_cols, train_evaluate_RF
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,  # Set retries to 3
    'retry_delay': timedelta(minutes=5),  # Delay between retries
    'email': ['koiljatchong@gmail.com', 'gemsonkokjunming@gmail.com', 'isabel.pan1231@gmail.com', 'tansirui2@gmail.com', 'pngwenlong@gmail.com'],
    'email_on_failure': True,  # Send email on task failure
    'email_on_retry': False,  # Optionally, set to True if you want emails on retry as well
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
                return run_scraper()

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

            data = fetch_data(183)
            transformed = transform_data(data)
            storage = dump_to_gcs(transformed)
            
        motoristsg_group()
        sgcarmart_group()

    @task_group(group_id='staging_area')
    def api_and_merge_data():
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
        
        @task_group(group_id='big_query_merge')
        def big_query_merge(motorist_data, sgcarmart_data):            
            
            @task(task_id='transform_car_combined')
            def transform_car(df1, df2):
                return transform_car_combined(df1, df2)
                
            @task(task_id='load_temp_bq_table')
            def load_temp(df):
                return load_temp_bq_table(df)
            
            @task(task_id='append_final_table')
            def append_final():
                return append_final_table()
                
            transformed_data = transform_car(sgcarmart_data, motorist_data)
            load_temp(transformed_data) >> append_final()
        
        @task_group(group_id='query_CarAPI')
        def query_CarAPI(motorist_data, sgcarmart_data):
            @task(task_id="fetch_api_json")
            def fetch_api_json():
                gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
            
                bucket_name = 'is3107-datasets'
                object_name = 'carAPI/api.json'
                data = {}

                try:
                    file_content = gcs_hook.download(bucket_name=bucket_name, object_name=object_name)
                    data = json.loads(file_content)
                
                except Exception as e:
                    data = {}

                return data
            
            @task(task_id="execute_api_calls_for_motorist")
            def execute_api_calls_for_motorist(df, dict):
                return api_calling_logic_motorist(df, dict)
            
            
            @task(task_id="execute_api_calls_for_sgcarmart")
            def execute_api_calls_for_sgcarmart(df, dict):
                return api_calling_logic_sgcarmart(df, dict)
            
            @task(task_id="save_api_json")
            def save_api_json(dict):
                gcs_hook = GCSHook(google_cloud_storage_conn_id='google_cloud_default')
                bucket_name = 'is3107-datasets'
                object_name = 'carAPI/api.json'
                json_data = json.dumps(dict)
                gcs_hook.upload(bucket_name=bucket_name, object_name=object_name, data=json_data.encode(), mime_type='application/json')
                
            @task(task_id='save_api_data_to_bigquery')
            def save_api_data_to_bigquery(dict):
                return upload_to_BQ(dict)
            
            api_json_data = fetch_api_json()
            first_dict = execute_api_calls_for_motorist(motorist_data, api_json_data)
            second_dict = execute_api_calls_for_sgcarmart(sgcarmart_data, first_dict)
            save_api_json(second_dict)
            save_api_data_to_bigquery(second_dict)
            
        motorist_data = get_motoristsg_from_gcs()
        sgcarmart_data = get_sgcarmart_from_gcs()
        big_query_merge(motorist_data, sgcarmart_data)
        query_CarAPI(motorist_data, sgcarmart_data)
    
    
    @task(task_id='end')
    def end():
        print(f"End task received")

    @task(task_id='serving_layer')
    def train_model():
        import joblib
        
        hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
        sql = """
        SELECT * FROM `is3107-418903.final.carsCombinedFinal`
        """
        data = hook.get_pandas_df(sql=sql)
        data = data.dropna()
        data = change_reg_date_to_years(data)
        data = drop_cols(data)
        data = data_encoding(data)

        x = drop_highly_correlated_cols(data)
        y=data['price']

        r2_RF, rf_regressor = train_evaluate_RF(x,y)
        joblib.dump(rf_regressor, 'modelRF.pkl')
        data.to_csv('dataset.csv', index=False)
        

        bucket_name = 'is3107-model'
        object_name_modelRF = 'ml_model/modelRF.pkl'

        object_name_model_dataset = 'ml_model/dataset.csv'

        gcs_hook = GCSHook()
        gcs_hook.upload(bucket_name=bucket_name, object_name=object_name_modelRF, filename='modelRF.pkl')

        gcs_hook.upload(bucket_name=bucket_name, object_name=object_name_model_dataset, filename='dataset.csv')

    
    initiate_dag() >> web_scraping() >> api_and_merge_data() >> train_model() >> end()

dag = webscraper_taskflow()
