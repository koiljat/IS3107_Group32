from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import bigquery
import time
import pandas as pd
from io import StringIO
import json
from api_operators.car_query import get_car_query_trims
from api_operators.car_api import get_trim_details

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

@dag(dag_id='api_calls', default_args=default_args, schedule_interval='@daily', catchup=False)
def api_calls_taskflow():

    @task(task_id="fetch_motorist_csv")
    def fetch_motorist():
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

    @task(task_id="fetch_sgcarmart_csv")
    def fetch_sgcarmart():
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
        for index, row in df.iterrows():
            print(index, len(df))
            name = row['model']
            reg_date = row['registration_date']

            name_split = name.split(' ')
            make = name_split[0]
            model = name_split[1]

            year = pd.to_datetime(reg_date).year if pd.notnull(reg_date) else None
            # Form query_string to check for existence in dict
            query_string = f"({make},{model},{year})"

            if query_string in dict:
                continue
            else:
                query_temp_dict = {'model_make_id': None, 'model_name': None, 'model_trim': None, 'model_year': None, 'model_body': None, 'model_seats': None, 'model_weight_kg': None, 'model_engine_fuel': None, 'model_engine_cyl': None, 'model_drive': None, 'model_transmission_type': None, 'model_fuel_cap_l': None}
                
                #populate dictionary with carAPI first
                trim_info = get_trim_details(make, model, year)
                if trim_info:
                    query_temp_dict['model_make_id'] = trim_info['make_model_id']
                    query_temp_dict['model_trim'] = trim_info['id']
                    query_temp_dict['model_year'] = trim_info['year']
                    query_temp_dict['model_seats'] = trim_info['make_model_trim_body']['seats']
                    query_temp_dict['model_weight_kg'] = trim_info['make_model_trim_body']['curb_weight']
                    query_temp_dict['model_fuel_cap_l'] = trim_info['make_model_trim_mileage']['fuel_tank_capacity']
                    #if theres no hidden data, js assign immediately, if not remain empty
                    if "hidden" not in trim_info['make_model']['name']:
                        query_temp_dict['model_name'] = model
                        query_temp_dict['model_body'] = trim_info['make_model_trim_body']['type']
                        query_temp_dict['model_engine_fuel'] = trim_info['make_model_trim_engine']['fuel_type']
                        query_temp_dict['model_engine_cyl'] = trim_info['make_model_trim_engine']['cylinders']
                        query_temp_dict['model_drive'] = trim_info['make_model_trim_engine']['drive_type']
                        query_temp_dict['model_transmission_type'] = trim_info['make_model_trim_engine']['transmission']
                    
                # call carQuery if there are null values in the row        
                if not all(value is not None for value in query_temp_dict.values()):
                     
                    model_info = get_car_query_trims(make, model, year)
                    if model_info:
                        first_trim = model_info[0]
                        query_temp_dict['model_make_id'] = query_temp_dict.get('model_make_id', first_trim['model_make_id'])
                        query_temp_dict['model_name'] = query_temp_dict.get('model_name', first_trim['model_name'])
                        query_temp_dict['model_trim'] = query_temp_dict.get('model_trim', first_trim['model_trim'])
                        query_temp_dict['model_year'] = query_temp_dict.get('model_year', first_trim['model_year'])
                        query_temp_dict['model_body'] = query_temp_dict.get('model_body', first_trim['model_body'])
                        query_temp_dict['model_seats'] = query_temp_dict.get('model_seats', first_trim['model_seats'])
                        query_temp_dict['model_weight_kg'] = query_temp_dict.get('model_weight_kg', first_trim['model_weight_kg'])
                        query_temp_dict['model_engine_fuel'] = query_temp_dict.get('model_engine_fuel', first_trim['model_engine_fuel'])
                        query_temp_dict['model_engine_cyl'] = query_temp_dict.get('model_engine_cyl', first_trim['model_engine_cyl'])
                        query_temp_dict['model_drive'] = query_temp_dict.get('model_drive', first_trim['model_drive'])
                        query_temp_dict['model_transmission_type'] = query_temp_dict.get('model_transmission_type', first_trim['model_transmission_type'])
                        query_temp_dict['model_fuel_cap_l'] = query_temp_dict.get('model_fuel_cap_l', first_trim['model_fuel_cap_l'])
                        
                # Update dict
                all_values_none = all(value is None for value in query_temp_dict.values()) # if both APIS failed
                if all_values_none:
                    dict[query_string] = None
                else:
                    dict[query_string] = query_temp_dict
            
        return dict
        


    @task(task_id="execute_api_calls_for_sgcarmart")
    def execute_api_calls_for_sgcarmart(df, dict):
        for index, row in df.iterrows():
            print(index, len(df))
            name = row['name']
            reg_date = row['reg_date']

            name_split = name.split(' ')
            make = name_split[0]
            model = name_split[1]

            year = pd.to_datetime(reg_date).year if pd.notnull(reg_date) else None
            # Form query_string to check for existence in dict
            query_string = f"({make},{model},{year})"

            if query_string in dict:
                continue
            else:
                query_temp_dict = {'model_make_id': None, 'model_name': None, 'model_trim': None, 'model_year': None, 'model_body': None, 'model_seats': None, 'model_weight_kg': None, 'model_engine_fuel': None, 'model_engine_cyl': None, 'model_drive': None, 'model_transmission_type': None, 'model_fuel_cap_l': None}
                
                #populate dictionary with carAPI first
                trim_info = get_trim_details(make, model, year)
                if trim_info:
                    query_temp_dict['model_make_id'] = trim_info['make_model_id']
                    query_temp_dict['model_trim'] = trim_info['id']
                    query_temp_dict['model_year'] = trim_info['year']
                    query_temp_dict['model_seats'] = trim_info['make_model_trim_body']['seats']
                    query_temp_dict['model_weight_kg'] = trim_info['make_model_trim_body']['curb_weight']
                    query_temp_dict['model_fuel_cap_l'] = trim_info['make_model_trim_mileage']['fuel_tank_capacity']
                    #if theres no hidden data, js assign immediately, if not remain empty
                    if "hidden" not in trim_info['make_model']['name']:
                        query_temp_dict['model_name'] = model
                        query_temp_dict['model_body'] = trim_info['make_model_trim_body']['type']
                        query_temp_dict['model_engine_fuel'] = trim_info['make_model_trim_engine']['fuel_type']
                        query_temp_dict['model_engine_cyl'] = trim_info['make_model_trim_engine']['cylinders']
                        query_temp_dict['model_drive'] = trim_info['make_model_trim_engine']['drive_type']
                        query_temp_dict['model_transmission_type'] = trim_info['make_model_trim_engine']['transmission']
                    
                # call carQuery if there are null values in the row        
                if not all(value is not None for value in query_temp_dict.values()):
                     
                    model_info = get_car_query_trims(make, model, year)
                    if model_info:
                        first_trim = model_info[0]
                        query_temp_dict['model_make_id'] = query_temp_dict.get('model_make_id', first_trim['model_make_id'])
                        query_temp_dict['model_name'] = query_temp_dict.get('model_name', first_trim['model_name'])
                        query_temp_dict['model_trim'] = query_temp_dict.get('model_trim', first_trim['model_trim'])
                        query_temp_dict['model_year'] = query_temp_dict.get('model_year', first_trim['model_year'])
                        query_temp_dict['model_body'] = query_temp_dict.get('model_body', first_trim['model_body'])
                        query_temp_dict['model_seats'] = query_temp_dict.get('model_seats', first_trim['model_seats'])
                        query_temp_dict['model_weight_kg'] = query_temp_dict.get('model_weight_kg', first_trim['model_weight_kg'])
                        query_temp_dict['model_engine_fuel'] = query_temp_dict.get('model_engine_fuel', first_trim['model_engine_fuel'])
                        query_temp_dict['model_engine_cyl'] = query_temp_dict.get('model_engine_cyl', first_trim['model_engine_cyl'])
                        query_temp_dict['model_drive'] = query_temp_dict.get('model_drive', first_trim['model_drive'])
                        query_temp_dict['model_transmission_type'] = query_temp_dict.get('model_transmission_type', first_trim['model_transmission_type'])
                        query_temp_dict['model_fuel_cap_l'] = query_temp_dict.get('model_fuel_cap_l', first_trim['model_fuel_cap_l'])
                        
                # Update dict
                all_values_none = all(value is None for value in query_temp_dict.values()) # if both APIS failed
                if all_values_none:
                    dict[query_string] = None
                else:
                    dict[query_string] = query_temp_dict
            
        return dict
        

    @task(task_id="save_api_json")
    def save_api_json(dict):
        gcs_hook = GCSHook(google_cloud_storage_conn_id='google_cloud_default')
        bucket_name = 'is3107-datasets'
        object_name = 'carAPI/api.json'
        json_data = json.dumps(dict)
        gcs_hook.upload(bucket_name=bucket_name, object_name=object_name, data=json_data.encode(), mime_type='application/json')
        
    @task(task_id="save_api_csv")
    def save_api_csv(dict):
        gcs_hook = GCSHook(google_cloud_storage_conn_id='google_cloud_default')
        bucket_name = 'is3107-datasets'
        object_name = 'carAPI/api.csv'
        rows = []
        for key, values in dict.items():
            make, model, year = key.replace('(', '').replace(')', '').split(',')
            row = {'make': make, 'model': model, 'year': year}
            if values is not None:
                row.update(values)
                rows.append(row)
        df = pd.DataFrame(rows)
        csv_data = df.to_csv(index=False).encode()
    
        gcs_hook.upload(bucket_name=bucket_name, object_name=object_name, data=csv_data)
        
        
    motorist_df = fetch_motorist()
    sgcarmart_df = fetch_sgcarmart()
    api_dict = fetch_api_json()
    first_dict = execute_api_calls_for_motorist(motorist_df, api_dict)
    second_dict = execute_api_calls_for_sgcarmart(sgcarmart_df, first_dict)
    save_api_json(second_dict)
    save_api_csv(second_dict)
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bigquery',
        bucket='is3107-datasets',
        source_objects=['carAPI/api.csv'],
        destination_project_dataset_table='api.api',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='google_cloud_default',
        autodetect=True
    )
    
    save_api_csv(second_dict) >> load_csv_to_bigquery
    

dag = api_calls_taskflow()
