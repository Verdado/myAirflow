from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from pipeline.operators.update_hive_partition_operator import UpdateHivePartitionOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from pipeline.functions import *
from airflow.models import Variable
import os, json

def _nfsToS3Params(tableConfig):
    return {
        'file_pattern': replace_date_pattern(tableConfig['file_pattern']),
        'target_bucket' : replace_date_pattern(tableConfig['target_bucket']),
        'poke_interval': str(tableConfig['poke_interval']),
        'no_of_retries' : str(tableConfig['no_of_retries']),
        'pod_env_vars' : {
            'NFS_HOST_NAME': tableConfig['nfs_host_name'],
            'ENDPOINT_URL': tableConfig['s3endPointLoc']
        }
    }

def getJarUrl() -> dict:
    with open("/usr/local/airflow/dags/git/nfs_to_s3/_global.json") as json_data:
        return json.load(json_data)

def getCheckNfsFileExistenceTask(dag, tableConfig):
    file_check_args = ["/usr/src/app/nfs_file_existence_check.py",
                       "-source_dir", tableConfig['source_dir'],
                       "-file_pattern", _nfsToS3Params(tableConfig)['file_pattern'],
                       "-poke_interval", _nfsToS3Params(tableConfig)['poke_interval'],
                       "-no_of_retries", _nfsToS3Params(tableConfig)['no_of_retries']]
    if tableConfig.get('file_count'):
        file_check_args.extend(["-file_count",tableConfig['file_count']])
    return KubernetesPodOperator(
        task_id= tableConfig['name'] + "__check_nfs_file_existence",
        namespace=tableConfig['namespace'],
        image=tableConfig['image'],
        image_pull_policy="IfNotPresent",
        image_pull_secrets="gcr-json-key",
        secrets = [
                    Secret(deploy_type='env', deploy_target='KEY',  secret='staas-values', key='access_key'),
                    Secret(deploy_type='env', deploy_target='SECRET', secret='staas-values', key='access_secret')
                    ],
        cmds=["python"],
        arguments=file_check_args,
        name="spdb2-nfs-lib",
        env_vars=_nfsToS3Params(tableConfig)['pod_env_vars'],
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

def getNfsToS3Task(dag, tableConfig):
    file_copy_args = ["/usr/src/app/copy_file_nfs_to_s3.py",
                      "-source_dir", tableConfig['source_dir'],
                      "-file_pattern", _nfsToS3Params(tableConfig)['file_pattern'],
                      "-target_bucket", _nfsToS3Params(tableConfig)['target_bucket']]
    return KubernetesPodOperator(
    task_id=tableConfig['name'] + "__copy_file_from_nfs_to_s3",
    namespace=tableConfig['namespace'],
    image=tableConfig['image'],
    image_pull_policy="IfNotPresent",
    image_pull_secrets="gcr-json-key",
    secrets = [
                Secret(deploy_type='env', deploy_target='KEY',  secret='staas-values', key='access_key'),
                Secret(deploy_type='env', deploy_target='SECRET', secret='staas-values', key='access_secret')
                ],
    cmds=["python"],
    arguments=file_copy_args,
    name="spdb2-nfs-lib",
    env_vars=_nfsToS3Params(tableConfig)['pod_env_vars'],
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

def getFileBasedSparkTask(dag, tableConfig, databaseConfig, **context):
    appArgs = ["--s3secretKeyAws", Variable.get("s3_secret_key"),
               "--s3accessKeyAws", Variable.get("s3_secret_accesskey"),
               "--preprocessings", json.dumps(tableConfig.get('preprocessings',[])),
               "--delimiter", tableConfig.get('delimiter'),
               "--hiveDatabaseName", databaseConfig['hiveDatabase'],
               "--processingDate", "{{ (execution_date + macros.timedelta(hours=9)).strftime('%Y%m%d') }}"]

    config = {
        'name': tableConfig['name']+'__UpdateQueryTableData',
        'java_class': 'com.rakuten.dps.dataplatform.spark.job.SpdbFileBasedJob',
        'application': getJarUrl()['sparkAppJar'],
        'conn_id': 'spark_cluster2',
        'pool': 'query_table_spark_job_pool',
        'conf': {
            'spark.driver.memory': tableConfig['driverMemory'],
            'spark.sql.session.timeZone': 'UTC',
            'spark.sql.orc.enabled': 'true',
            'spark.sql.parquet.filterPushdown': 'true',
            'spark.sql.orc.filterPushdown': 'true',
            'spark.sql.hive.convertMetastoreOrc': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.sources.bucketing.enabled': 'true',
            'spark.sql.csv.parser.columnPruning.enabled': 'false',
            'spark.sql.files.ignoreCorruptFiles': 'true'
        },
        'trigger_rule': 'none_failed'
    }

    sparkConf = {
        "driverMemory": "driverMemory",
        "totalExecutorCores": "total_executor_cores",
        "executorCores": "executor_cores",
        "executorMemory": "executor_memory",
    }
    excludedFields = ('isEnabled', 'nfs_host_name', 'file_pattern', 'source_dir','target_bucket',
                      'poke_interval','no_of_retries','file_count','name','preprocessings','delimiter',
                      'namespace','image')

    ''' Spark configurations executors etc. '''
    for key, rename in sparkConf.items():
        if tableConfig.get(key) is not None:
            config[rename] = tableConfig.get(key)

    ''' Application configurations '''
    for key, value in tableConfig.items():
        if key in excludedFields or key in sparkConf:
            continue
        appArgs.extend(["--"+key , str(value)])

    config['application_args'] = appArgs
    return SparkSubmitOperator(task_id=config['name'], provide_context=True, dag=dag, **config)

def get_update_metadata_task(task_id, database, table,dag):
    return UpdateHivePartitionOperator(
        task_id=task_id,
        database=database,
        table=table,
        metastore_conn_id='metastore_default',
        webhdfs_conn_id='webhdfs_default',
        dag=dag
    )
