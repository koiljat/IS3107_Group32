import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime, timedelta
import re


"----------------------------------------- helper functions ---------------------------------------------------------------------------------"
'''
Retrieve latest sgcarmart and motorist data from GCS as df
'''
def get_df_from_gcs(folder_path):
    # Define your bucket name
    bucket_name = 'is3107-datasets'
    
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
    
    return combined_df


'''
Load the combined df to a temp table in BQ
'''
def load_temp_bq_table(df, project_id, dataset_id, table_id, schema):
    job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    schema=schema)

    # Load the DataFrame to the temporary table
    job = bigquery_client.load_table_from_dataframe(
        df,
        f"{project_id}.{dataset_id}.{table_id}",
        job_config=job_config)
    
    job.result()  # Wait for the job to complete


'''
Merge the temp table with the full table in BQ
'''
def append_final_table(target_dataset_id, target_table_id, temp_dataset_id, temp_table_id):   
    project_id = 'is3107-418903'
    merge_query = f"""
    MERGE `{project_id}.{target_dataset_id}.{target_table_id}` final
    USING `{project_id}.{temp_dataset_id}.{temp_table_id}` temp
    ON final.`price` = temp.`price` and final.`reg_date` = temp.`reg_date` and final.`mileage` = temp.`mileage`
    WHEN NOT MATCHED THEN
    INSERT ROW
    """
    
    # Run the merge query
    merge_job = bigquery_client.query(merge_query)
    merge_job.result() 

"----------------------------------------- combine cars and load into BQ -------------------------------------------------------------------------"
from google.cloud import storage
from google.cloud import bigquery

# Path to your service account key file
path_to_private_key = 'is3107-418903-a6116d294003.json'

# Authenticate the sessions
storage_client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)
bigquery_client = bigquery.Client.from_service_account_json(json_credentials_path=path_to_private_key)


sgcarmart_df = get_df_from_gcs("sgcarmart/")
motorist_df = get_df_from_gcs("motoristsg/")
combined_df = transform_car_combined(sgcarmart_df, motorist_df)

project_id = 'is3107-418903'
dataset_id = 'temp'
table_id = 'cars-temp'
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

# Overwrite the BigQuery temp table with the CSV data
load_temp_bq_table(combined_df, project_id, dataset_id, table_id, schema)
# Merge the temp table into the full table
append_final_table("sgVehicles", "cars-listings", "temp", "cars-temp")
