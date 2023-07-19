
from pendulum import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator



DBT_CLOUD_CONN_ID = "dbt_airflow_conn"
JOB_ID = "382782"



@dag(
    dag_id="dag_dbt_teams_fixtures_statistics",
    start_date=datetime(2023, 7, 18),
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
    doc_md=__doc__,
)
def run_dbt_teams_fixtures_statistics():
    begin, end = [EmptyOperator(task_id=id) for id in ["begin", "end"]]

    teams_fixtures_statistics_task = DbtCloudRunJobOperator(
        task_id="teams_fixtures_statistics_task",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=JOB_ID,
        check_interval=600,
        timeout=3600,
    )

    begin >> teams_fixtures_statistics_task >> end


dag = run_dbt_teams_fixtures_statistics()


