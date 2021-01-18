from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import json, os
import glob

def getPipelineDefaultArgs():
    return {
        'owner': 'SuperDB team',
        'email': ['dev-spdb-pipeline-notifications-prod@mail.rakuten.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    }

def getMartJobConfigs(jobName):
    tableConfigs = []
    for tableConfigFile in glob.glob("/usr/local/airflow/dags/git/mart/"+ jobName +"/configs/*.json"):
        with open(tableConfigFile) as json_data:
            try:
                tableConfigs.append(json.load(json_data))
            except Exception as e:
                raise Exception(f"Failed to read {tableConfigFile}. {str(e)}")
    return tableConfigs

'''Separate JAR JSONFile for multiple Mart Jobs'''
def getJarUrl(jobName: str) -> dict:
    with open("/usr/local/airflow/dags/git/mart/"+ jobName +"/_global.json") as json_data:
        return json.load(json_data)

def getSparkTask(dag, moduleConfig, jobName):
    appArgs = []
    appArgs.append(os.path.join('config', moduleConfig['configName']))
    config = {
        'name': moduleConfig['configName'].split('.')[0],
        'java_class': 'com.rakuten.dps.dataplatform.spark.driver.DriverFactory',
        'conn_id': 'spark_cluster2',
        'pool': 'query_table_spark_job_pool',
        'conf': {
            'spark.driver.memory': moduleConfig['driverMemory'],
            'spark.sql.session.timeZone': 'UTC',
            'spark.sql.orc.enabled': 'true',
            'spark.sql.parquet.filterPushdown': 'true',
            'spark.sql.orc.filterPushdown': 'true',
            'spark.sql.hive.convertMetastoreOrc': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.sources.bucketing.enabled': 'true'
        },
        'trigger_rule': 'none_failed'
    }


    remapConfig = {
        "driverMemory": "driverMemory",
        "totalExecutorCores": "total_executor_cores",
        "executorCores": "executor_cores",
        "executorMemory": "executor_memory",
    }
    for key, rename in remapConfig.items():
        if moduleConfig.get(key) is not None:
            config[rename] = moduleConfig.get(key)

    if 'needDailyData' in moduleConfig:
        # Batch date day-1
        appArgs.extend(['--start_date', '{{ macros.ds_add(ds, -1) }}'])
        appArgs.extend(['--end_date', '{{ macros.ds_add(ds, -1) }}'])

    # If extra spark configuration required on a module level
    if 'extraConf' in moduleConfig:
        config['conf'].update(moduleConfig['extraConf'])

    if 'configName' in moduleConfig:
        config['application'] = getJarUrl(jobName)['sparkAppJar']

    ''' Job Arguments'''
    for key, value in moduleConfig['jobArgs'].items():
        appArgs.extend(['--'+key, str(value)])
        
    config['application_args'] = appArgs
    return SparkSubmitOperator(task_id=moduleConfig['name'], dag=dag, **config)

def getExternalSensorTask(dag, moduleConfig):
    config = {
        "external_dag_id": moduleConfig['parentDagId'],
        "mode": 'reschedule',
        "retries": 5,
        "timeout": 300
    }
    if 'parentTaskId' in moduleConfig:
        config['external_task_id'] = moduleConfig['parentTaskId']
    if 'execution_date_fn' in moduleConfig:
        config['execution_date_fn'] = moduleConfig['execution_date_fn']
    else:
        config['execution_date_fn'] = lambda dt: dt - timedelta(days=1)
    return ExternalTaskSensor(task_id='sense_'+ moduleConfig['name'], dag=dag, **config)
