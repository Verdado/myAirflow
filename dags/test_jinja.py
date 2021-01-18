from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 9),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('test_jinja',
    default_args = default_args,
    max_active_runs = 1,
    catchup = True,
    schedule_interval = '0 22 * * *'
)

def test_print(input):
    print('Testing')
    print(input)

test_print = PythonOperator(
        task_id='test_print',
        python_callable=test_print,
        op_args=["{{ (next_execution_date + macros.timedelta(hours=9)).strftime('%Y%m%d') }}"],
        dag=dag
)
