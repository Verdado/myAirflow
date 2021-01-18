from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago
from pipeline.functions import *

databaseConfig = {
    'dataSourceQn': 'CAMPAIGN_FILE_CAMPAIGN',
    'hiveDatabase': 'spdb__CAMPAIGN__FILE__CAMPAIGN'
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'nfs_to_s3',
    default_args = default_args,
    max_active_runs = 1,
    catchup = True,
    schedule_interval = timedelta(days=1)
)

for tableConfig in getTableConfigs(databaseConfig['dataSourceQn']):
    if not tableConfig['isEnabled']:
        continue

    file_pattern  = replace_date_pattern(tableConfig['file_pattern'])
    target_bucket = replace_date_pattern(tableConfig['target_bucket'])
    poke_interval = str(tableConfig['poke_interval'])
    no_of_retries = str(tableConfig['no_of_retries'])
    file_count    = str(tableConfig.get('file_count',''))
    pod_env_vars  = {
    'NFS_HOST_NAME': tableConfig['nfs_host_name'],
    'ENDPOINT_URL': tableConfig['s3endPointLoc']
    }
    file_check_args = [
    	"/usr/src/app/nfs_file_existence_check.py", "-source_dir", tableConfig['source_dir'], "-file_pattern", file_pattern, "-poke_interval", poke_interval , "-no_of_retries", no_of_retries
    ]
    if file_count:
        file_check_args.extend(["-file_count", file_count])

    file_copy_args = [
    	"/usr/src/app/copy_file_nfs_to_s3.py", "-source_dir", tableConfig['source_dir'], "-file_pattern", file_pattern, "-target_bucket", target_bucket
    ]


    check_nfs_file_existence = KubernetesPodOperator(
    task_id= tableConfig['name'] + "__check_nfs_file_existence",
    namespace="spdb-airflow",
    image="asia.gcr.io/gdo-jp-data-prod/dps/spdb2-nfs-lib:20.12.01-67",
    image_pull_policy="IfNotPresent",
    image_pull_secrets="gcr-json-key",
    secrets = [
                Secret(deploy_type='env', deploy_target='KEY',  secret='staas-values', key='access_key'),
                Secret(deploy_type='env', deploy_target='SECRET', secret='staas-values', key='access_secret')
                ],
    cmds=["python"],
    arguments=file_check_args,
    name="spdb2-nfs-lib",
    env_vars=pod_env_vars,
    resources=None,
    service_account_name="default",
    in_cluster=True,
    get_logs=True,
    is_delete_operator_pod=True,
    tolerations=None,
    node_selectors=None,
    retries=2,
    dag=dag
    )

    copy_file_from_nfs_to_s3 = KubernetesPodOperator(
    task_id=tableConfig['name'] + "__copy_file_from_nfs_to_s3",
    namespace="spdb-airflow",
    image="asia.gcr.io/gdo-jp-data-prod/dps/spdb2-nfs-lib:20.12.01-67",
    image_pull_policy="IfNotPresent",
    image_pull_secrets="gcr-json-key",
    secrets = [
                Secret(deploy_type='env', deploy_target='KEY',  secret='staas-values', key='access_key'),
                Secret(deploy_type='env', deploy_target='SECRET', secret='staas-values', key='access_secret')
                ],
    cmds=["python"],
    arguments=file_copy_args,
    name="spdb2-nfs-lib",
    env_vars=pod_env_vars,
    resources=None,
    service_account_name="default",
    in_cluster=True,
    get_logs=True,
    is_delete_operator_pod=True,
    tolerations=None,
    node_selectors=None,
    retries=3,
    dag=dag
    )

    HiveTableCreation = getUpdateQueryTableDataTaskFile(tableConfig, databaseConfig, dag)
    check_nfs_file_existence >> copy_file_from_nfs_to_s3 >> HiveTableCreation

if __name__ == "__main__":
    dag.cli()
