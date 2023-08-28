from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.append("../streaming/")
from streaming.streaming_processor import process_fixtures_odds_in_match

default_args = {
    "owner": "Jess",
    "retries": 1,
    "retry_delay": 0 
}

with DAG(
    dag_id="dag_process_fixtures_odds_in_match", 
    start_date=datetime(2023, 8, 15),
    schedule_interval="0/30 * * * *",
    max_active_runs=10,
    default_args=default_args,
    catchup=False,
    tags=['streaming','odds-in-match' 'etl', 's3']
) as dag:
    
    process_fixtures_odds_in_match_task = PythonOperator(
        task_id='process_fixtures_odds_in_match_task',
        python_callable= process_fixtures_odds_in_match
    )

    process_fixtures_odds_in_match_task



