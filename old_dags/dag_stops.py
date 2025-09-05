from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import geopandas as gpd
import os


def stops():
    input_path = "/opt/airflow/data/stops.txt"
    output_path = "/opt/airflow/exports/stops_with_geom.csv"

    df = pd.read_csv(input_path)
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.stop_lon, df.stop_lat), crs="EPSG:4326"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    gdf.to_csv(output_path, index=False)
    print(f"Exported {len(gdf)} stops to {output_path}")


with DAG(
    dag_id="gtfs_stops",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "geopandas"],
) as dag:

    task_process_stops = PythonOperator(
        task_id="stops_file",
        python_callable=stops,
    )
