"""
#### Getting Started

This pipeline requires a connection to dbt Cloud as well as an Airflow Variable for the ``job_id`` to be
triggered.

To create a connection to dbt Cloud, navigate to `Admin -> Connections` in the Airflow UI and select the
"dbt Cloud" connection type. An API token is required, however, the Account ID is not. You may provide an
Account ID and this will be used by the dbt Cloud tasks, but you can also override or supply a specific
Account ID at the task level using the `account_id` parameter if you wish.

#### Provider Details
For reference, the following provider version was used when intially authoring this pipeline:

```
    apache-airflow-providers-dbt-cloud==3.2.2
```
This provider version uses dbt Cloud API.
"""

from pendulum import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator



DBT_CLOUD_CONN_ID = "dbt_airflow_conn"
JOB_ID = "383473"



@dag(
    dag_id="dag_dbt_teams_brazil",
    start_date=datetime(2023, 7, 18),
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
    doc_md=__doc__,
)
def run_dbt_teams_brazil():
    begin, end = [EmptyOperator(task_id=id) for id in ["begin", "end"]]

    teams_brazil_task = DbtCloudRunJobOperator(
        task_id="teams_brazil_task",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=JOB_ID,
        check_interval=600,
        timeout=3600,
    )

    begin >> teams_brazil_task >> end


dag = run_dbt_teams_brazil()


