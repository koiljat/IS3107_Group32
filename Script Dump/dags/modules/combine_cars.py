import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime, timedelta
import re
from google.cloud import storage
from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

"----------------------------------------- helper functions ---------------------------------------------------------------------------------"
'''
Retrieve latest sgcarmart and motorist data from GCS as df
'''
def get_df_from_gcs(folder_path):
    # Define your bucket name
    bucket_name = 'is3107-datasets'
    storage_client = storage.Client()
    
    # Get the bucket objects
    bucket = storage_client.get_bucket(bucket_name)
    
    # List all objects in the bucket and sort by creation time
    blobs = list(bucket.list_blobs(prefix=folder_path))
    blobs.sort(key=lambda blob: blob.time_created, reverse=True)
    
    # Get the latest object
    latest_blob = blobs[0] if blobs else None
    
    # Download the latest object data if it exists
    if latest_blob:
        print(f"Latest object: {latest_blob.name}")
        data = latest_blob.download_as_bytes()
        return pd.read_csv(BytesIO(data))
    else:
        print("No objects found in the bucket.")


'''
Concat and clean the combined df to ensure data consistency
'''
def transform_car_combined(sgcarmart_df, motorist_df):
    # rename motorist_df to match sgcarmart_df
    motorist_renamed = motorist_df.rename(columns={"model":"name", "milleage":"mileage", "registration_date":"reg_date", "no_of_owner":"owners", "capacity":"eng_cap"})
    # concat both dfs 
    combined_df = pd.concat([sgcarmart_df, motorist_renamed], ignore_index=True)
    # drop any duplicates that have same price, mileage and registration date
    combined_df = combined_df.drop_duplicates(subset=["price","mileage","reg_date"], ignore_index=True)
    # add the date of listing
    combined_df["date_listed"] = (datetime.today() - timedelta(days=1)).strftime('%d-%b-%Y')

    # remove \n as csv reads it as a delimiter
    combined_df["accessories"] = combined_df["accessories"].apply(lambda x : x.replace("\n", ""))
    
    combined_df.dropna(inplace=True)
    
    print(combined_df.columns)
    
    return combined_df


'''
Load the combined df to a temp table in BQ
'''
def load_temp_bq_table(df):
    hook = BigQueryHook(bigquery_conn_id='gcp_is3107', use_legacy_sql=False)
    client = hook.get_client()
    
    table_id = 'is3107-418903.temp.cars-temp'

    schema = [
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("depreciation", "FLOAT"),
        bigquery.SchemaField("mileage", "FLOAT"),
        bigquery.SchemaField("eng_cap", "FLOAT"),
        bigquery.SchemaField("power", "FLOAT"),
        bigquery.SchemaField("reg_date", "STRING"),
        bigquery.SchemaField("coe_left", "FLOAT"),
        bigquery.SchemaField("owners", "INTEGER"),
        bigquery.SchemaField("omv", "FLOAT"),
        bigquery.SchemaField("arf", "FLOAT"),
        bigquery.SchemaField("accessories", "STRING"),
        bigquery.SchemaField("date_listed", "STRING")
    ]

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema)

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() 
        print(f"Loaded {df.shape[0]} rows into {table_id}")
    except Exception as e:
        print(f"An error occurred while loading data to BigQuery: {e}")


'''
Merge the temp table with the full table in BQ
'''
def append_final_table():   
    hook = BigQueryHook(bigquery_conn_id='gcp_is3107', use_legacy_sql=False)
    client = hook.get_client()
    
    project_id = 'is3107-418903'
    target_dataset_id = "sgVehicles"
    target_table_id = "cars-listings"
    temp_dataset_id = "temp"
    temp_table_id = "cars-temp"

    # SQL to merge data from a temporary table into the final table based on specified conditions
    merge_query = f"""
    MERGE `{project_id}.{target_dataset_id}.{target_table_id}` final
    USING `{project_id}.{temp_dataset_id}.{temp_table_id}` temp
    ON final.`price` = temp.`price` AND final.`reg_date` = temp.`reg_date` AND final.`mileage` = temp.`mileage`
    WHEN NOT MATCHED THEN
    INSERT ROW
    """

    # Execute the merge query and wait for it to complete
    try:
        merge_job = client.query(merge_query)
        merge_job.result()  # Waits for the query to finish
        print("Merge operation successful.")
    except Exception as e:
        print(f"An error occurred during the merge operation: {e}")

"----------------------------------------- combine cars and load into BQ -------------------------------------------------------------------------"