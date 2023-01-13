from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def add_numbers(ti,**kwargs):
    c = kwargs['dag_run'].conf['a'] + kwargs['dag_run'].conf['b']
    obj={"result":c}
    print(obj)
    ti.xcom_push(key='obj', value=obj)

def postprocess(ti):
    postprocess_res = ti.xcom_pull(key='obj', task_ids='add_numbers')
    print('Testing increases for : ', postprocess_res)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('my_xcom3_addnum_dag',start_date=datetime(2021, 1,1 ), max_active_runs=2,schedule=timedelta(minutes=30),default_args=default_args,catchup=False) as dag:
    add_num = PythonOperator(task_id = 'get_testing_increase_data',python_callable=add_numbers)
    postprocess = PythonOperator(task_id = 'analyze_data', python_callable=postprocess)

    add_num >> postprocess()