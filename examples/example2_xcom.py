from airflow.decorators import dag, task
from datetime import datetime
import requests
import json

url = 'https://jsonplaceholder.typicode.com/posts/1'

default_args = {
                   'start_date': datetime(2021, 1, 1)
               }

@dag('my_xcom_taskflow_dag', schedule='@daily', default_args=default_args, catchup=False)
def taskflow():

    @task
    def get_testing_increase():
        res = requests.get(url)
        return{'testing_increase': json.loads(res.text)['title']}

    @task
    def analyze_testing_increases(testing_increase: int):
        print('Testing increases for :', testing_increase)

    analyze_testing_increases(get_testing_increase())

dag = taskflow()