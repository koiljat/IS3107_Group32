import pandas as pd
from modules.car_query import get_car_query_trims
from modules.car_api import get_trim_details
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery


def api_calling_logic_sgcarmart(df, dict):
    for index, row in df.iterrows():
        print(index, len(df))
        name = row['name']
        reg_date = row['reg_date']

        name_split = name.split(' ')
        make = name_split[0].lower()
        model = name_split[1].lower()

        year = pd.to_datetime(reg_date).year if pd.notnull(reg_date) else None
        # Form query_string to check for existence in dict
        query_string = f"({make},{model},{year})"

        if query_string in dict:
            continue
        else:
            query_temp_dict = {'model_make_id': None, 'model_name': None, 'model_trim': None, 'model_year': None, 'model_body': None, 'model_seats': None,
                               'model_weight_kg': None, 'model_engine_fuel': None, 'model_engine_cyl': None, 'model_drive': None, 'model_transmission_type': None, 'model_fuel_cap_l': None}

            # populate dictionary with carAPI first
            trim_info = get_trim_details(make, model, year)
            if trim_info:
                query_temp_dict['model_make_id'] = str(
                    trim_info['make_model_id']) if trim_info['make_model_id'] is not None else None
                query_temp_dict['model_trim'] = str(
                    trim_info['id']) if trim_info['id'] is not None else None
                query_temp_dict['model_year'] = int(
                    trim_info['year']) if trim_info['year'] is not None else None
                query_temp_dict['model_seats'] = int(
                    trim_info['make_model_trim_body']['seats']) if trim_info['make_model_trim_body']['seats'] is not None else None
                query_temp_dict['model_weight_kg'] = trim_info['make_model_trim_body']['curb_weight']
                query_temp_dict['model_fuel_cap_l'] = trim_info['make_model_trim_mileage']['fuel_tank_capacity']
                # if theres no hidden data, js assign immediately, if not remain empty
                if "hidden" not in trim_info['make_model']['name']:
                    query_temp_dict['model_name'] = str(model)
                    query_temp_dict['model_body'] = str(
                        trim_info['make_model_trim_body']['type']) if trim_info['make_model_trim_body']['type'] is not None else None
                    query_temp_dict['model_engine_fuel'] = str(
                        trim_info['make_model_trim_engine']['fuel_type']) if trim_info['make_model_trim_engine']['fuel_type'] is not None else None
                    query_temp_dict['model_engine_cyl'] = str(
                        trim_info['make_model_trim_engine']['cylinders']) if trim_info['make_model_trim_engine']['cylinders'] is not None else None
                    query_temp_dict['model_drive'] = str(
                        trim_info['make_model_trim_engine']['drive_type']) if trim_info['make_model_trim_engine']['drive_type'] is not None else None
                    query_temp_dict['model_transmission_type'] = str(
                        trim_info['make_model_trim_engine']['transmission']) if trim_info['make_model_trim_engine']['transmission'] is not None else None

            # call carQuery if there are null values in the row
            if not all(value is not None for value in query_temp_dict.values()):

                model_info = get_car_query_trims(make, model, year)
                if model_info:
                    first_trim = model_info[0]

                    if query_temp_dict['model_make_id'] is None:
                        query_temp_dict['model_make_id'] = str(
                            first_trim['model_make_id']) if first_trim['model_make_id'] is not None else None
                    if query_temp_dict['model_name'] is None:
                        query_temp_dict['model_name'] = str(
                            first_trim['model_name']) if first_trim['model_name'] is not None else None
                    if query_temp_dict['model_trim'] is None:
                        query_temp_dict['model_trim'] = str(
                            first_trim['model_trim']) if first_trim['model_trim'] is not None else None
                    if query_temp_dict['model_year'] is None:
                        query_temp_dict['model_year'] = int(
                            first_trim['model_year']) if first_trim['model_year'] is not None else None
                    if query_temp_dict['model_body'] is None:
                        query_temp_dict['model_body'] = str(
                            first_trim['model_body']) if first_trim['model_body'] is not None else None
                    if query_temp_dict['model_seats'] is None:
                        query_temp_dict['model_seats'] = int(
                            first_trim['model_seats']) if first_trim['model_seats'] is not None else None
                    if query_temp_dict['model_weight_kg'] is None:
                        query_temp_dict['model_weight_kg'] = float(
                            first_trim['model_weight_kg']) if first_trim['model_weight_kg'] is not None else None
                    if query_temp_dict['model_engine_fuel'] is None:
                        query_temp_dict['model_engine_fuel'] = str(
                            first_trim['model_engine_fuel']) if first_trim['model_engine_fuel'] is not None else None
                    if query_temp_dict['model_engine_cyl'] is None:
                        query_temp_dict['model_engine_cyl'] = str(
                            first_trim['model_engine_cyl']) if first_trim['model_engine_cyl'] is not None else None
                    if query_temp_dict['model_drive'] is None:
                        query_temp_dict['model_drive'] = str(
                            first_trim['model_drive']) if first_trim['model_drive'] is not None else None
                    if query_temp_dict['model_transmission_type'] is None:
                        query_temp_dict['model_transmission_type'] = str(
                            first_trim['model_transmission_type']) if first_trim['model_transmission_type'] is not None else None
                    if query_temp_dict['model_fuel_cap_l'] is None:
                        query_temp_dict['model_fuel_cap_l'] = float(
                            first_trim['model_fuel_cap_l']) if first_trim['model_fuel_cap_l'] is not None else None

            # Update dict
            # if both APIS failed
            all_values_none = all(
                value is None for value in query_temp_dict.values())
            if all_values_none:
                dict[query_string] = None
            else:
                dict[query_string] = query_temp_dict
    return dict


def api_calling_logic_motorist(df, dict):
    for index, row in df.iterrows():
        print(index, len(df))
        name = row['model']
        reg_date = row['registration_date']

        name_split = name.split(' ')
        make = name_split[0].lower()
        model = name_split[1].lower()

        year = pd.to_datetime(reg_date).year if pd.notnull(reg_date) else None
        # Form query_string to check for existence in dict
        query_string = f"({make},{model},{year})"

        if query_string in dict:
            continue
        else:
            query_temp_dict = {'model_make_id': None, 'model_name': None, 'model_trim': None, 'model_year': None, 'model_body': None, 'model_seats': None,
                               'model_weight_kg': None, 'model_engine_fuel': None, 'model_engine_cyl': None, 'model_drive': None, 'model_transmission_type': None, 'model_fuel_cap_l': None}

            # populate dictionary with carAPI first
            trim_info = get_trim_details(make, model, year)
            if trim_info:

                query_temp_dict['model_make_id'] = str(
                    trim_info['make_model_id']) if trim_info['make_model_id'] is not None else None
                query_temp_dict['model_trim'] = str(
                    trim_info['id']) if trim_info['id'] is not None else None
                query_temp_dict['model_year'] = int(
                    trim_info['year']) if trim_info['year'] is not None else None
                query_temp_dict['model_seats'] = int(
                    trim_info['make_model_trim_body']['seats']) if trim_info['make_model_trim_body']['seats'] is not None else None
                query_temp_dict['model_weight_kg'] = trim_info['make_model_trim_body']['curb_weight']
                query_temp_dict['model_fuel_cap_l'] = trim_info['make_model_trim_mileage']['fuel_tank_capacity']
                # if theres no hidden data, js assign immediately, if not remain empty
                if "hidden" not in trim_info['make_model']['name']:
                    query_temp_dict['model_name'] = str(model)
                    query_temp_dict['model_body'] = str(
                        trim_info['make_model_trim_body']['type']) if trim_info['make_model_trim_body']['type'] is not None else None
                    query_temp_dict['model_engine_fuel'] = str(
                        trim_info['make_model_trim_engine']['fuel_type']) if trim_info['make_model_trim_engine']['fuel_type'] is not None else None
                    query_temp_dict['model_engine_cyl'] = str(
                        trim_info['make_model_trim_engine']['cylinders']) if trim_info['make_model_trim_engine']['cylinders'] is not None else None
                    query_temp_dict['model_drive'] = str(
                        trim_info['make_model_trim_engine']['drive_type']) if trim_info['make_model_trim_engine']['drive_type'] is not None else None
                    query_temp_dict['model_transmission_type'] = str(
                        trim_info['make_model_trim_engine']['transmission']) if trim_info['make_model_trim_engine']['transmission'] is not None else None

            # call carQuery if there are null values in the row
            if not all(value is not None for value in query_temp_dict.values()):

                model_info = get_car_query_trims(make, model, year)
                if model_info:
                    first_trim = model_info[0]

                    if query_temp_dict['model_make_id'] is None:
                        query_temp_dict['model_make_id'] = str(
                            first_trim['model_make_id']) if first_trim['model_make_id'] is not None else None
                    if query_temp_dict['model_name'] is None:
                        query_temp_dict['model_name'] = str(
                            first_trim['model_name']) if first_trim['model_name'] is not None else None
                    if query_temp_dict['model_trim'] is None:
                        query_temp_dict['model_trim'] = str(
                            first_trim['model_trim']) if first_trim['model_trim'] is not None else None
                    if query_temp_dict['model_year'] is None:
                        query_temp_dict['model_year'] = int(
                            first_trim['model_year']) if first_trim['model_year'] is not None else None
                    if query_temp_dict['model_body'] is None:
                        query_temp_dict['model_body'] = str(
                            first_trim['model_body']) if first_trim['model_body'] is not None else None
                    if query_temp_dict['model_seats'] is None:
                        query_temp_dict['model_seats'] = int(
                            first_trim['model_seats']) if first_trim['model_seats'] is not None else None
                    if query_temp_dict['model_weight_kg'] is None:
                        query_temp_dict['model_weight_kg'] = float(
                            first_trim['model_weight_kg']) if first_trim['model_weight_kg'] is not None else None
                    if query_temp_dict['model_engine_fuel'] is None:
                        query_temp_dict['model_engine_fuel'] = str(
                            first_trim['model_engine_fuel']) if first_trim['model_engine_fuel'] is not None else None
                    if query_temp_dict['model_engine_cyl'] is None:
                        query_temp_dict['model_engine_cyl'] = str(
                            first_trim['model_engine_cyl']) if first_trim['model_engine_cyl'] is not None else None
                    if query_temp_dict['model_drive'] is None:
                        query_temp_dict['model_drive'] = str(
                            first_trim['model_drive']) if first_trim['model_drive'] is not None else None
                    if query_temp_dict['model_transmission_type'] is None:
                        query_temp_dict['model_transmission_type'] = str(
                            first_trim['model_transmission_type']) if first_trim['model_transmission_type'] is not None else None
                    if query_temp_dict['model_fuel_cap_l'] is None:
                        query_temp_dict['model_fuel_cap_l'] = float(
                            first_trim['model_fuel_cap_l']) if first_trim['model_fuel_cap_l'] is not None else None

            # Update dict
            # if both APIS failed
            all_values_none = all(
                value is None for value in query_temp_dict.values())
            if all_values_none:
                dict[query_string] = None
            else:
                dict[query_string] = query_temp_dict

    return dict


def upload_to_BQ(dict):
    hook = BigQueryHook(
        bigquery_conn_id='google_cloud_default', use_legacy_sql=False)
    client = hook.get_client()
    rows = []
    for key, values in dict.items():
        make, model, year = key.replace(
            '(', '').replace(')', '').split(',')
        row = {'make': make, 'model': model, 'year': int(year)}
        if values is not None:
            row.update(values)
            rows.append(row)
    df = pd.DataFrame(rows)        
    df = df.drop(columns=['model_make_id', 'model_name', 'model_year'])

    table_id = 'is3107-418903.car_info.car_info'

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("make", "STRING"),
            bigquery.SchemaField("model", "STRING"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("model_trim", "STRING"),
            bigquery.SchemaField("model_body", "STRING"),
            bigquery.SchemaField("model_seats", "INTEGER"),
            bigquery.SchemaField("model_weight_kg", "FLOAT"),
            bigquery.SchemaField("model_engine_fuel", "STRING"),
            bigquery.SchemaField("model_engine_cyl", "STRING"),
            bigquery.SchemaField("model_drive", "STRING"),
            bigquery.SchemaField("model_transmission_type", "STRING"),
            bigquery.SchemaField("model_fuel_cap_l", "FLOAT"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    try:
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {df.shape[0]} rows into {table_id}")
    except Exception as e:
        print(f"An error occurred while loading data to BigQuery: {e}")
