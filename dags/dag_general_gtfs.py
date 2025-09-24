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

from fonctions.delay_analysis import average_delay_by_minute, punctuality_rate
from fonctions.routes_analysis import position_bus_vehicles, average_delay_by_route

from fonctions.view_support_delay import create_delays_with_support_columns
from fonctions.stops_cartes_analysis import average_delay_by_stop
from fonctions.heatmap_delay_day import heatmap_delay_day
from fonctions.evolution_delay_stop_analysis_q7 import evolution_delay_by_stop

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
    schedule_interval="0 10,15,16 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["gtfs", "realtime", "static"],
) as dag:

    # -------------------- extraction  parsing  export ------------------------------------------
    with TaskGroup("extraction_download_parsing_export") as TGdag_download:
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

    # ----------------- creation table view support -delay + colonnes -------------
    with TaskGroup("canonical_view_delay_min") as TGdag_canonical_view:
        create_view = PythonOperator(
            task_id="create_delays_with_support_columns",
            python_callable=create_delays_with_support_columns,
        )

    # ------------------ Transformation en DuckDB ---------------------
    with TaskGroup("duckdb_transformation_questions_chargement") as TGdag_transform:
        average_delay = PythonOperator(
            task_id="average_delay_by_minute",
            python_callable=average_delay_by_minute,
        )
        carte_bus = PythonOperator(
            task_id="position_bus_vehicles",
            python_callable=position_bus_vehicles,
        )
        stop_delay = PythonOperator(
            task_id="average_delay_by_stop",
            python_callable=average_delay_by_stop,
        )
        delay_by_route = PythonOperator(
            task_id="average_delay_by_route",
            python_callable=average_delay_by_route,
        )
        punctuality = PythonOperator(
            task_id="punctuality_rate",
            python_callable=punctuality_rate,
        )
        heatmap_q5 = PythonOperator(
            task_id="heatmap_delay_day",
            python_callable=heatmap_delay_day,
        )
        evolution_q7 = PythonOperator(
            task_id="evolution_delay_by_stop",
            python_callable=evolution_delay_by_stop,
        )

        average_delay >> heatmap_q5
        stop_delay >> evolution_q7

    TGdag_download >> init >> TGdag_load >> TGdag_canonical_view >> TGdag_transform
