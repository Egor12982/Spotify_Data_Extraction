from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from Spotify_Data_Extraction import run_spotify_etl

# fill datetime () by start_date of your choise in format: year, month, day
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "spotify_dag",
    default_args=default_args,
    description="DAG for Spotify_Data.",
    schedule_interval='40 12 * * *',
)

run_etl = PythonOperator(
    task_id="whole_spotify_etl",
    python_callable=run_spotify_etl,
    dag=dag,
)

run_etl
