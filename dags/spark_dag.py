
from datetime import timedelta, datetime

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0
}

# configmaps = [
#     k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="kubernetes-admin")),
#     k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="test-configmap-2")),
# ]

# [END default_args]

# [START instantiate_dag]

dag = DAG(
    'spark_pi',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=['example']
)

# spark_file = open(
#     "hive_conn_spark.yaml").read()

submit = SparkKubernetesOperator(
    task_id='spark_submit',
    namespace="spark",
    application_file="sparkapplication/basic_spark.yaml",
    kubernetes_conn_id="microk8s",
    do_xcom_push=True,
    dag=dag,
)

sensor = SparkKubernetesSensor(
    task_id='spark_monitor',
    namespace="airflow",
    application_name="{{ task_instance.xcom_pull(task_ids='spark_submit')['metadata']['name'] }}",
    kubernetes_conn_id="microk8s",
    dag=dag,
)

submit >> sensor