from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os


def trips_plus_routes():
    trips_path = "/opt/airflow/data/trips.txt"
    routes_path = "/opt/airflow/data/routes.txt"
    output_path = "/opt/airflow/exports/trips_plus_routes.csv"

    trips_df = pd.read_csv(trips_path)
    routes_df = pd.read_csv(routes_path)

    merged = trips_df.merge(
        routes_df[["route_id", "route_short_name", "route_long_name"]],
        on="route_id",
        how="left",
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    merged.to_csv(output_path, index=False)
    print(f"Exported {len(merged)} trips with route names to {output_path}")


with DAG(
    dag_id="gtfs_trips",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs"],
) as dag:

    task_trips_plus_routes = PythonOperator(
        task_id="trips_plus_routes",
        python_callable=trips_plus_routes,
    )
