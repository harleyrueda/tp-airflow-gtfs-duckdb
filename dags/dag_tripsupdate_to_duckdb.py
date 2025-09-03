from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os

DATA_DIR = "/opt/airflow/data"
PB_FILE = os.path.join(DATA_DIR, "trip_updates.pb")


def download_trip_updates():
    url = "https://ara-api.enroute.mobi/rla/gtfs/trip-updates"
    response = requests.get(url)

    os.makedirs(DATA_DIR, exist_ok=True)

    with open(PB_FILE, "wb") as f:
        f.write(response.content)


with DAG(
    dag_id="trip_updates_to_duckdb",
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "realtime"],
) as dag:

    t1 = PythonOperator(
        task_id="download_trip_updates",
        python_callable=download_trip_updates,
    )
