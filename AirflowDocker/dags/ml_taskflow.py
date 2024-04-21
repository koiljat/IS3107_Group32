from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


from ml_functions.ml_pipeline import data_encoding, change_reg_date_to_years, drop_cols, drop_highly_correlated_cols, train_evaluate_RF



@dag(dag_id='run_ml_taskflow', start_date=datetime(2024,1,1), schedule=None, catchup=False, tags=['ml_pipeline'])

def run_ml_taskflow():
    @task(task_id="run_ml_taskflow")
    def run_ml_taskflow():
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

        return r2_RF
    
    run_ml_taskflow()
    

    
ml_tasks_dag = run_ml_taskflow()