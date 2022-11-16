"""
Overwatch
This runs daily

Source code is  [overwatch repository](https://github.com/mozilla/overwatch/).
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable

from operators.gcp_container_operator import GKEPodOperator


default_args = {
    "owner": "gleonard@mozilla.com",
    "depends_on_past": False,
    "start_date": datetime(2022, 9, 1),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

# TODO GLE temp for finding dag easily.
tags = ["overwatch"]
image = "gcr.io/automated-analysis-dev/overwatch:" + Variable.get("overwatch_image_version")

with DAG(
    "overwatch",
    default_args=default_args,
    schedule_interval="@daily",
    doc_md=__doc__,
    tags=tags,
) as dag:
    run_analysis = GKEPodOperator(
        task_id="run_analysis",
        name="run_analysis",
        image=image,
        email=["gleonard@mozilla.com"],
        dag=dag,
        env_vars={"SLACK_BOT_TOKEN": Variable.get("overwatch_slack_token")},
        arguments=["run-analysis", "--date={{ ds }}", "./config_files/"],
        # Temp setting for dev testing.
        gcp_conn_id="google_cloud_gke_sandbox",
        project_id="moz-fx-data-gke-sandbox",
        cluster_name="gleonard-gke-sandbox",
        location="us-west1",
    )
