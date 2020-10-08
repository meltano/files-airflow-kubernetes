# If you want to define a custom DAG, create
# a new file under orchestrate/dags/ and Airflow
# will pick it up automatically.

### INSTRUCTIONS ###
# To use this KubernetePodOperator, you must specify the Kubernetes optional package
# in the meltano.yml orchestrators configuration, e.g. airflow[kubernetes]==1.10.12

# Populate the NAMESPACE and IMAGE variables below

# The Volume imports and configuration are optional, but allow
# tasks to persists Meltano logs beyond their lifecycle

# For more information: https://kubernetes.io/docs/concepts/storage/volumes/

import os
import logging
import subprocess
import json

from airflow import DAG
# from airflow.kubernetes.volume import Volume
# from airflow.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from datetime import timedelta


# VOLUME_NAME = "your volume name goes here"
# CLAIM_NAME = "your volume claim name goes here"

# volume_mount = VolumeMount(
#     VOLUME_NAME,
#     mount_path='code/.meltano/logs/elt',
#     sub_path=None,
#     read_only=False
# )

# volume_config= {
#     'persistentVolumeClaim':
#       {
#         'claimName': CLAIM_NAME
#       }
#     }

# volume = Volume(name=VOLUME_NAME, configs=volume_config)

NAMESPACE = 'your namespace here'
IMAGE = 'your Docker image identifier here'

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "concurrency": 1,
}

project_root = os.getenv("MELTANO_PROJECT_ROOT", os.getcwd())

result = subprocess.run(
    [".meltano/run/bin", "schedule", "list", "--format=json"],
    cwd=project_root,
    stdout=subprocess.PIPE,
    universal_newlines=True,
    check=True,
)
schedules = json.loads(result.stdout)

for schedule in schedules:
    logging.info(f"Considering schedule '{schedule['name']}': {schedule}")

    if not schedule["cron_interval"]:
        logging.info(
            f"No DAG created for schedule '{schedule['name']}' because its interval is set to `@once`."
        )
        continue

    args = DEFAULT_ARGS.copy()
    if schedule["start_date"]:
        args["start_date"] = schedule["start_date"]

    dag_id = f"meltano_{schedule['name']}"

    # from https://airflow.apache.org/docs/stable/scheduler.html#backfill-and-catchup
    #
    # It is crucial to set `catchup` to False so that Airflow only create a single job
    # at the tail end of date window we want to extract data.
    #
    # Because our extractors do not support date-window extraction, it serves no
    # purpose to enqueue date-chunked jobs for complete extraction window.
    dag = DAG(
        dag_id, catchup=False, default_args=args, schedule_interval=schedule["interval"], max_active_runs=1
    )

    k8_elt = KubernetesPodOperator(
        namespace=NAMESPACE,
        image=IMAGE,
        arguments=["meltano", "elt"] + schedule['elt_args'],
        labels={"app": "meltano-pipeline-job"},
        # volumes=[volume],
        # volume_mounts=[volume_mount],
        name=schedule['name'],
        task_id="extract_load",
        is_delete_operator_pod=True,
        # Specifying resources is optional, configure based on your requirements
        # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
        # resources={
        #     'request_cpu': '1',
        #     'request_memory': '4096Mi',
        #     'limit_cpu': '2',
        #     'limit_memory': '8192Mi'
        # },
        env_vars={
            # inherit the current env
            **os.environ,
            **schedule["env"],
        },
        dag=dag,
    )

    # register the dag
    globals()[dag_id] = dag

    logging.info(f"DAG created for schedule '{schedule['name']}'")
