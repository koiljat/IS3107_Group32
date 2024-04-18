from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


from ml_functions.ml_pipeline import data_encoding, change_reg_date_to_years, drop_cols, drop_highly_correlated_cols, train_evaluate_GB, train_evaluate_DT



@dag(dag_id='ml_taskflow', start_date=datetime(2024,1,1), schedule=None, catchup=False, tags=['ml_pipeline'])

def ml_taskflow():
    @task(task_id="read_dataset")
    def read_dataset():
        
        hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
        sql = """
        SELECT * FROM `is3107-418903.factTable.carsCombinedFinal`
        """
        data = hook.get_pandas_df(sql=sql)

        return data
    
    @task(task_id="train_evaluate_model")
    def train_evaluate_model(data):
        import joblib
        data = change_reg_date_to_years(data)
        data = drop_cols(data)
        data = data_encoding(data)

        x = drop_highly_correlated_cols(data)
        y=data['price']

        r2_GB, gb_regressor = train_evaluate_GB(x,y)
        joblib.dump(gb_regressor, 'modelGB.pkl')
        r2_DT, dt_regressor = train_evaluate_DT(x,y)
        joblib.dump(dt_regressor, 'modelDT.pkl')

        data.to_csv('dataset.csv', index=False)

        bucket_name = 'is3107-model'
        object_name_modelGB = 'ml_model_test/modelGB.pkl'
        object_name_modelDT = 'ml_model_test/modelDT.pkl'
        object_name_model_dataset = 'ml_model_test/dataset.csv'

        gcs_hook = GCSHook()
        gcs_hook.upload(bucket_name=bucket_name, object_name=object_name_modelGB, filename='modelGB.pkl')
        gcs_hook.upload(bucket_name=bucket_name, object_name=object_name_modelDT, filename='modelDT.pkl')

        gcs_hook.upload(bucket_name=bucket_name, object_name=object_name_model_dataset, filename='dataset.csv')

        return r2_GB
    
    dataset = read_dataset()
    train_evaluate_model(dataset)
    

    
ml_tasks_dag = ml_taskflow()