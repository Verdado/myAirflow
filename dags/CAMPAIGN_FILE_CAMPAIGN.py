from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from nfs_to_s3.spdb_filebased_functions import *

databaseConfig = {
    'dataSourceQn': 'CAMPAIGN_FILE_CAMPAIGN',
    'hiveDatabase': 'spdb__CAMPAIGN__FILE__CAMPAIGN'
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 22),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(databaseConfig['dataSourceQn']+ "__test",
    default_args = default_args,
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
