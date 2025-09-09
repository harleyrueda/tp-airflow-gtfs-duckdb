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

from fonctions.trips_updates import (
    download_trip_updates,
    parse_trip_updates,
    export_trip_updates_csv,
    load_trips_updates_db,
)

from fonctions.vehicle_positions import (
    download_vehicle_positions,
    parse_vehicle_positions,
    export_vehicle_positions_csv,
    load_vehicle_positions_db,
)

from fonctions.duckdb_transformation import duckdb_transformation

# Selon la documentation duck db ne permert pas ecrire plusiers tables en meme temps, donc creation de init_duckdb ensuite reorganisation de taches dans taskgroups:


def init_duckdb():
    import duckdb, os

    path = "/opt/airflow/warehouse/gtfs.duckdb"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        os.remove(path)
    duckdb.connect(path).close()


# -- DAG GENERAL ---

with DAG(
    dag_id="dag_gtfs_general",
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["gtfs", "realtime", "static"],
) as dag:

    # -------------------- extraction  parsing  export ------------------------------------------
    with TaskGroup("download_parse_export") as TGdag_download:
        # statiques
        s1 = PythonOperator(
            task_id="download_static_zip", python_callable=download_static_zip
        )
        s2 = PythonOperator(
            task_id="unzip_static_files", python_callable=unzip_static_files
        )

        # trips updates
        t1 = PythonOperator(
            task_id="download_trip_updates", python_callable=download_trip_updates
        )
        t2 = PythonOperator(
            task_id="parse_trip_updates", python_callable=parse_trip_updates
        )
        t3 = PythonOperator(
            task_id="export_trips_updates_csv", python_callable=export_trip_updates_csv
        )

        # vehicle positions
        v1 = PythonOperator(
            task_id="download_vehicle_positions",
            python_callable=download_vehicle_positions,
        )
        v2 = PythonOperator(
            task_id="parse_vehicle_positions", python_callable=parse_vehicle_positions
        )
        v3 = PythonOperator(
            task_id="export_vehicle_positions_csv",
            python_callable=export_vehicle_positions_csv,
        )

        s1 >> s2
        t1 >> t2 >> t3
        v1 >> v2 >> v3

    # ------------------------- init creation duckdb warehouse--------------------------------------------------
    init = PythonOperator(
        task_id="init_duckdb",
        python_callable=init_duckdb,
    )

    # ----------------------chargement vers DuckDB ----------------------------------------

    with TaskGroup("load_to_duckdb") as TGdag_load:
        s3 = PythonOperator(
            task_id="load_static_to_duckdb", python_callable=load_static_to_duckdb
        )
        t4 = PythonOperator(
            task_id="load_trips_updates_db", python_callable=load_trips_updates_db
        )
        v4 = PythonOperator(
            task_id="load_vehicle_positions_db",
            python_callable=load_vehicle_positions_db,
        )

    # ------------------ Transformation en DuckDB ---------------------
    with TaskGroup("duckdb_transformation") as TGdag_transform:
        transform = PythonOperator(
            task_id="data_transformation",
            python_callable=duckdb_transformation,
        )

    TGdag_download >> init >> TGdag_load >> TGdag_transform
