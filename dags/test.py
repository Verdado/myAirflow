from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.models import DAG
import sys

batch_date = datetime(2020, 11, 15)

def getPipelineDefaultArgs():
    return {
        'owner': 'SuperDB team',
        'email': ['dev-spdb-pipeline-notifications-prod@mail.rakuten.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }

def dummyFunc(**context):
    #return context['execution_date'].strftime('%Y-%m-%d')
    return "{{ macros.ds_add(ds, 1) }}"

def getPyFunc(dag):
    print("{{ macros.ds_add(ds, 1) }}")
    return PythonOperator(task_id='t2',trigger_rule='none_failed', provide_context=True,python_callable=dummyFunc, dag=dag)

dagArgs = getPipelineDefaultArgs()
dag = DAG(
    dag_id = 'dummy_dag',
    default_args = dagArgs,
    schedule_interval = '0 16 * * *',
    catchup = True,
    start_date = batch_date,
    max_active_runs = 1,
    tags=['redbasket'],
)

t1 = PythonOperator(task_id='t1',trigger_rule='none_failed', provide_context=True,python_callable=dummyFunc, dag=dag)
t2 = getPyFunc(dag)

t1 >> t2


if __name__ == "__main__":
    dag.cli()
