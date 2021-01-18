from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.models import DAG
from mart.functions import *

batch_date = datetime(2020, 12, 1)
dagArgs = getPipelineDefaultArgs()
jobName = 'redbasket'

dag = DAG(
    dag_id = 'SPDB_REDBASKET_2.0',
    default_args = dagArgs,
    schedule_interval = '0 16 * * *',
    catchup = True,
    start_date = batch_date,
    max_active_runs = 1,
    tags=['redbasket'],
)

''' Data Sources '''
sense_ICHIBA = getExternalSensorTask(dag, moduleConfig = {"name": "ICHIBA__ORACLE__spdb_user_at_sv_spdb_dot_rise",
                                                          "parentDagId": "ICHIBA__ORACLE__spdb_user_at_sv_spdb_dot_rise",
                                                          "execution_date_fn": lambda dt: dt})

sense_ICHIBA_Daily = getExternalSensorTask(dag, moduleConfig = {"name": "ICHIBA__ORACLE__spdb_user_at_sv_spdb_dot_rise__Daily",
                                                                "parentDagId": "ICHIBA__ORACLE__spdb_user_at_sv_spdb_dot_rise__Daily",
                                                                "execution_date_fn": lambda dt: dt})

sense_MergeUserRank = getExternalSensorTask(dag, moduleConfig = {"name": "POINT__MergeUserRank",
                                                        "parentDagId": "POINT__MergeUserRank",
                                                        "execution_date_fn": lambda dt: dt})

sense_all_sources = DummyOperator(task_id='sense_all_sources_completion', trigger_rule='none_failed', dag=dag)

sense_ICHIBA >> sense_all_sources
sense_ICHIBA_Daily >> sense_all_sources
sense_MergeUserRank >> sense_all_sources

''' Module Tasks '''
module_tasks={}
moduleConfigs = getMartJobConfigs(jobName)
for module in moduleConfigs:
    module_tasks[module['name']] = getSparkTask(dag, module, jobName)

for module in moduleConfigs:
    if 'upStream' not in module:
        sense_all_sources >> module_tasks[module['name']]
        continue
    for upStream in module['upStream']:
        module_tasks[upStream] >> module_tasks[module['name']]


if __name__ == "__main__":
    dag.cli()
