import json
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

url = 'https://jsonplaceholder.typicode.com/posts/1'

def get_testing_increase(ti):
    res = requests.get(url)
    testing_increase = json.loads(res.text)['title']
    ti.xcom_push(key='testing_increase', value=testing_increase)

def analyze_testing_increases(ti):
    testing_increases=ti.xcom_pull(key='testing_increase', task_ids='get_testing_increase_data')
    print('Testing increases for : ', testing_increases)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('xcom_dag',start_date=datetime(2021, 1,1 ), max_active_runs=2,schedule=timedelta(minutes=30),default_args=default_args,catchup=False) as dag:
    opr_get_covid_data = PythonOperator(task_id = 'get_testing_increase_data',python_callable=get_testing_increase)
    opr_analyze_testing_data = PythonOperator(task_id = 'analyze_data', python_callable=analyze_testing_increases)

    opr_get_covid_data >> opr_analyze_testing_data