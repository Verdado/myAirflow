from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timezone
from airflow.models import DAG
from mart.functions import *

start_date = datetime(2020, 12, 1)
dagArgs = getPipelineDefaultArgs()
jobName = 'POINT__MergeUserRank'

dag = DAG(
    dag_id = jobName,
    default_args = dagArgs,
    schedule_interval = '0 16 1 * *',
    catchup = True,
    start_date = start_date,
    max_active_runs = 1,
    tags=['ingestion','MergeUserRank'],
)

''' Data Sources '''
sensePOINT = getExternalSensorTask(dag, moduleConfig = {"name": "sense_POINT__ORACLE__PTADMIN_at_SV_EPNT001_JPE1",
                                                         "parentDagId": "POINT__ORACLE__PTADMIN_at_SV_EPNT001_JPE1",
                                                         "execution_date_fn": lambda dt: dt})

''' MergeUserRank Job '''
module_tasks={}
moduleConfigs = getMartJobConfigs(jobName)
for module in moduleConfigs:
    module['batch_date'] = "{{ (execution_date + macros.timedelta(hours=9)).strftime('%Y-%m-%d') }}"
    module_tasks[module['name']] = getSparkTask(dag, module, jobName)

for module in moduleConfigs:
    if 'upStream' not in module:
        sensePOINT >> module_tasks[module['name']]
        continue
    for upStream in module['upStream']:
        module_tasks[upStream] >> module_tasks[module['name']]


if __name__ == "__main__":
    dag.cli()
