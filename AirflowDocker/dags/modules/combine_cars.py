import pandas as pd
import numpy as np
from io import BytesIO
import datetime
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
    latest_blob = blobs[1] if blobs else None
    
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
    motorist_df.rename(columns={"model":"name", "milleage":"mileage", "registration_date":"reg_date", "no_of_owner":"owners", "capacity":"eng_cap"}, inplace=True)
    # concat both dfs 
    combined_df = pd.concat([sgcarmart_df, motorist_df], ignore_index=True)
    # drop any duplicates that have same price, mileage and registration date
    combined_df = combined_df.drop_duplicates(subset=["price","mileage","reg_date"], ignore_index=True)
    # remove \n as csv reads it as a delimiter
    combined_df["accessories"] = combined_df["accessories"].apply(lambda x : x.replace("\n", ""))
    print(combined_df["date_listed"].head())
    combined_df['date_listed'] = pd.to_datetime(combined_df['date_listed'])
    print(combined_df['date_listed'].head())
     # Apply the function to determine the correct month and bidding no based on 'datelisted'
    combined_df['coe_bidding_month'], combined_df['coe_bidding_no'] = zip(*combined_df['date_listed'].apply(check_date_position_and_bidding_no))
    
    return combined_df

'''
Returns the 1st, 2nd, and 3rd Wednesdays of the specified month and year. (Helper for transform)
'''
def find_wednesdays(year, month):
    wednesdays = []
    print(year, month)
    date = datetime.date(year, month, 1)
    while date.weekday() != 2:
        date += datetime.timedelta(days=1)
    for _ in range(3):
        wednesdays.append(date)
        date += datetime.timedelta(days=7)
    return wednesdays

'''
Check the position of the date relative to the first three Wednesdays of its month.
'''
def check_date_position_and_bidding_no(input_date):
    
    print(input_date)
    input_date = input_date.date() if hasattr(input_date, 'date') else input_date
    first_wednesday, second_wednesday, third_wednesday = find_wednesdays(input_date.year, input_date.month)
    if input_date < first_wednesday:
        if input_date.month == 1:
            return datetime.date(input_date.year - 1, 12, 1), 2
        else:
            return datetime.date(input_date.year, input_date.month - 1, 1), 2
    elif first_wednesday <= input_date < third_wednesday:
        return datetime.date(input_date.year, input_date.month, 1), 1
    else:
        return datetime.date(input_date.year, input_date.month, 1), 2
    
'''
Load the combined df to a temp table in BQ
'''
def load_temp_bq_table(df):
    df.to_csv("testing.csv")
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
        bigquery.SchemaField("date_listed", "STRING"),
        bigquery.SchemaField("coe_bidding_month", "STRING"),
        bigquery.SchemaField("coe_bidding_no", "INTEGER"),
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

    # First, clear all existing data in the target table
    delete_query = f"""
    DELETE FROM `{project_id}.{target_dataset_id}.{target_table_id}` WHERE TRUE
    """

    # Then, insert all data from the temporary table into the final table
    insert_query = f"""
    INSERT INTO `{project_id}.{target_dataset_id}.{target_table_id}`
    SELECT * FROM `{project_id}.{temp_dataset_id}.{temp_table_id}`
    """

    # Execute the delete query and wait for it to complete
    try:
        delete_job = client.query(delete_query)
        delete_job.result()  # Waits for the delete to finish
        print("All data deleted from the target table.")
    except Exception as e:
        print(f"An error occurred during the delete operation: {e}")

    # Execute the insert query and wait for it to complete
    try:
        insert_job = client.query(insert_query)
        insert_job.result()  # Waits for the insert to finish
        print("New data inserted into the target table.")
    except Exception as e:
        print(f"An error occurred during the insert operation: {e}")

"----------------------------------------- combine cars and load into BQ -------------------------------------------------------------------------"