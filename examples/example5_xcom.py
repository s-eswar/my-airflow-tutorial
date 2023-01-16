from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

def preprocess(ti,**kwargs):
    output={"valid":False}
    if(isinstance(kwargs['dag_run'].conf['a'],int) and isinstance(kwargs['dag_run'].conf['b'],int)):
        output['valid']=True
        output['values']={"a":kwargs['dag_run'].conf['a'],"b":kwargs['dag_run'].conf['b']}
    else:
        output['valid']=False
    ti.xcom_push(key='output', value=output)

def add_numbers(ti):
    input_values = ti.xcom_pull(key='output', task_ids='preprocess')
    if input_values['valid']:
        obj={"valid":True,"result":input_values['values']['a']+input_values['values']['b']}
        ti.xcom_push(key='obj', value=obj)
    else:
        obj=input_values
        ti.xcom_push(key='obj', value=obj)

def choose_branch(ti):
    c = ti.xcom_pull(key='output', task_ids='preprocess')
    print(c)
    if c['valid']:
        return 'add_numbers'
    else:
        return 'Invalid'

def postprocess(ti):
    postprocess_res = ti.xcom_pull(key='obj', task_ids='add_numbers')
    if not postprocess_res['valid']:
        print('Postprocess output: Invalid input')
    else:
        print('Postprocess output : ', postprocess_res)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG('my_xcom5_3tasks_dag',start_date=datetime(2021, 1,1 ), max_active_runs=2,schedule=timedelta(minutes=30),default_args=default_args,catchup=False) as dag:
    preprocess = PythonOperator(task_id='preprocess', python_callable=preprocess)
    add_num = PythonOperator(task_id = 'add_numbers',python_callable=add_numbers)
    postprocess = PythonOperator(task_id = 'postprocess', python_callable=postprocess)
    invalid = EmptyOperator(task_id='Invalid')
    end = EmptyOperator(task_id='end')
    start =  EmptyOperator(task_id='start')
    choose_branch = BranchPythonOperator(task_id='choose_branch',python_callable=choose_branch)

    start >> preprocess >> choose_branch >> add_num >> postprocess >> end
    start >> preprocess >> choose_branch >> invalid >> end
