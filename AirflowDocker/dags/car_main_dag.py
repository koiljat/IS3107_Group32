from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

@dag(dag_id='main_taskflow', default_args=default_args, schedule_interval=None, catchup=False)
def main_taskflow():
    
    @task(task_id='start_motorist_dag')
    def start_motorist_dag():
        # Trigger the motorist_webscraper DAG
        return TriggerDagRunOperator(task_id='trigger_motorist_dag', trigger_dag_id='motorist_webscraper')

    @task(task_id='start_sgcarmart_dag')
    def start_sgcarmart_dag():
        # Trigger the sgcarmart_scraper DAG
        return TriggerDagRunOperator(task_id='trigger_sgcarmart_dag', trigger_dag_id='sgcarmart_scraper')

    start_motorist_task = start_motorist_dag()
    start_sgcarmart_task = start_sgcarmart_dag()

dag = main_taskflow()
