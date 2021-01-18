from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from nfs_to_s3.spdb_filebased_functions import *
databaseConfig = {
   'dataSourceQn': 'master_point',
   'hiveDatabase': 'spdb__master_point'
}
dagArgs = getPipelineDefaultArgs()
dagArgs['email'].append('dev-spdb-pipeline@mail.rakuten.com')
dagArgs['sla'] = timedelta(minutes = 50000)
dagArgs['start_date'] = datetime(2019, 4, 1)
dagArgs['end_date'] = datetime(9999, 4, 1)
dag = DAG(dag_id = 'master_point',
  default_args = dagArgs,
  max_active_runs = 1,
  catchup = True,
  schedule_interval = '0 22 * * *',
  tags=['ingestion', databaseConfig['dataSourceQn']]
)
for tableConfig in getTableConfigs(databaseConfig['dataSourceQn']):
  if not tableConfig['isEnabled']:
    continue
  check_nfs_file_existence = getCheckNfsFileExistenceTask(dag, tableConfig)
  copy_file_from_nfs_to_s3 = getNfsToS3Task(dag, tableConfig)
  SparkJob = getFileBasedSparkTask(dag, tableConfig, databaseConfig)
  update_metadata_task = get_update_metadata_task(
      task_id=f"{tableConfig['hiveTableName']}__UpdateQueryTableMetadata",
      database=databaseConfig['hiveDatabase'].lower(),
      table=tableConfig['hiveTableName'],
      dag=dag
    )
  check_nfs_file_existence >> copy_file_from_nfs_to_s3 >> SparkJob >> update_metadata_task
if __name__ == "__main__":
  dag.cli()
