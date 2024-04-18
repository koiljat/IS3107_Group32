from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

@dag(dag_id='transform2', start_date=datetime(2024,1,1), schedule=None, catchup=False, tags=['transform2'])

def transform2():
    @task(task_id="merge_datasets")
    def merge_datasets():

        #rename columns
        hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
        client = hook.get_client()
        
        # # combine with api
        sql_api_join = """
        CREATE OR REPLACE TABLE `is3107-418903.factTable.cars-listingsAPI` AS
        SELECT a.owners, a.eng_cap, a.price, a.depreciation, a.mileage, a.power, a.coe_left, a.omv, a.arf, a.accessories,
            b.*, 
        FROM (
            SELECT 
                *, 
                SPLIT(name, ' ')[OFFSET(0)] AS make,
                SPLIT(name, ' ')[OFFSET(1)] AS model,
                EXTRACT(YEAR FROM PARSE_DATE('%d-%b-%Y', reg_date)) AS year 
            FROM `is3107-418903.sgVehicles.cars-listings`
        ) a
        JOIN `is3107-418903.api.api` b
        ON a.make = b.make AND a.model = b.model AND a.year = b.year;
        """
        sql_api_join = client.query(sql_api_join)
        sql_api_join.result()


        # # calculate vehicle class
        sql_vehicle_class = """
        CREATE OR REPLACE TABLE `is3107-418903.factTable.cars-listingsAPI_class` AS
        SELECT *,
            CASE
                WHEN eng_cap <= 1600 AND power <= 97 THEN 'A'
                ELSE 'B'
            END AS vehicle_class
        FROM `is3107-418903.factTable.cars-listingsAPI`;


        """
        sql_vehicle_class = client.query(sql_vehicle_class)
        sql_vehicle_class.result()

        # fetch coe price, this will be the factTable.cars-listingsCOE
        # sql_coe_price = """

        # """
        # hook.run(sql_coe_price)

    @task(task_id="clean_data")
    def clean_data():
        hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
        client = hook.get_client()

        # drop make model_name year as cleanup
        sql_drop_columns = """
        CREATE OR REPLACE TABLE `is3107-418903.factTable.carsCombinedFinal` AS
        SELECT
        owners, eng_cap, price, depreciation, mileage, power, coe_left, omv, arf, accessories, model_make_id, make, model_trim, model_year, model_body, model_seats, model_weight_kg, model_engine_fuel, model_engine_cyl, model_drive, model_transmission_type, model_fuel_cap_l, vehicle_class
        FROM `is3107-418903.factTable.cars-listingsAPI_class`; -- this should be is3107-418903.factTable.cars-listingsCOE
        """
        sql_drop_columns = client.query(sql_drop_columns)
        sql_drop_columns.result()

        # drop tables
        client.delete_table('is3107-418903.factTable.cars-listingsAPI', not_found_ok=False)
        client.delete_table('is3107-418903.factTable.cars-listingsAPI_class', not_found_ok=False)
        # client.delete_table('is3107-418903.factTable.cars-listingsCOE', not_found_ok=False)

        # remove rows with null values
        table_id = 'is3107-418903.factTable.carsCombinedFinal'
        table = client.get_table(table_id) 
        columns = [field.name for field in table.schema]

        conditions = ' OR '.join([f"{col} IS NULL" for col in columns])

        sql_remove_null = f"""
        DELETE FROM `{table_id}`
        WHERE {conditions}
        """
        sql_remove_null = client.query(sql_remove_null)
        sql_remove_null.result()


    
    merge_datasets() >> clean_data()
    

transform2_dag = transform2()




        
