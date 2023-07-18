from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.append("../utils/")
from utils.main import process_fixtures_odds

default_args = {
    "owner": "Jess",
    "retries": 1,
    "retry_delay": 0 
}

with DAG(
    dag_id="dag_process_fixtures_odds_pre", 
    start_date=datetime(2023, 7, 18),
    schedule_interval="@daily",
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
    tags=['project', 'etl', 's3']
) as dag:
    
    process_fixtures_odds_task = PythonOperator(
        task_id='process_fixtures_odds_task',
        python_callable= process_fixtures_odds
    )

    process_fixtures_odds_task



