from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime


# ---- IMPORT DES FONCTIONS --------

from fonctions.statiques import (
    download_static_zip,
    unzip_static_files,
    load_static_to_duckdb,
)


def duckdb_transformation():
    print("--------------- DUCKDB TRANSFORMATION ------------------------")

    print("--------------- END DUCKDB TRANSFORMATION --------------------")


# -- DAG GENERAL ---

with DAG(
    dag_id="dag_gtfs_general",
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "realtime", "static"],
) as dag:

    with TaskGroup("gtfs_static") as TGdag_static:
        s1 = PythonOperator(
            task_id="download_static_zip", python_callable=download_static_zip
        )
        s2 = PythonOperator(
            task_id="unzip_static_files", python_callable=unzip_static_files
        )
        s3 = PythonOperator(
            task_id="load_static_to_duckdb", python_callable=load_static_to_duckdb
        )

        s1 >> s2 >> s3

    with TaskGroup("duckdb_transformation") as TGdag_duckdb:
        transform = PythonOperator(
            task_id="data_transformation_with_duckdb",
            python_callable=duckdb_transformation,
        )

    [TGdag_static] >> TGdag_duckdb
