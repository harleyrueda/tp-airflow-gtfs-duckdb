from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os
import zipfile
import duckdb

DATA_DIR = "/opt/airflow/data"
GTFS_ZIP = os.path.join(DATA_DIR, "gtfs_static.zip")

# --- tache 1 download zip statiques ---


def download_static_zip():
    url = "https://chouette.enroute.mobi/api/v1/datas/OpendataRLA/gtfs.zip"
    response = requests.get(url)

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(GTFS_ZIP, "wb") as f:
        f.write(response.content)

    print(f"zip telechargement ok - {GTFS_ZIP}")


# -- tache 2 extraction de zip --


def unzip_static_files():
    with zipfile.ZipFile(GTFS_ZIP, "r") as zip_ref:
        zip_ref.extractall(DATA_DIR)

    print(f" extraction zip fichiers ok {DATA_DIR}")

    # -- tache 3 chargement dans DuckDB --


def load_static_to_duckdb():
    print("--------------- START ------------------------")

    # stops table duckDB

    duckdb.sql(
        f"""
        CREATE OR REPLACE TABLE stops AS
        SELECT * FROM read_csv_auto('{os.path.join(DATA_DIR, "stops.txt")}', HEADER=TRUE)
    """
    )
    print("aperçu de stops (10 ligns) :")
    duckdb.sql("SELECT * FROM stops LIMIT 10").show()

    # routes table duckDB


def load_static_to_duckdb():
    print("--------------- START ------------------------")

    # stops.txt
    duckdb.sql(
        f"""
        CREATE OR REPLACE TABLE stops AS
        SELECT * FROM read_csv_auto('{os.path.join(DATA_DIR, "stops.txt")}', HEADER=TRUE)
    """
    )
    print("aperçu de stops (10 lignes) :")
    duckdb.sql("SELECT * FROM stops LIMIT 10").show()

    # routes.txt
    duckdb.sql(
        f"""
        CREATE OR REPLACE TABLE routes AS
        SELECT * FROM read_csv_auto('{os.path.join(DATA_DIR, "routes.txt")}', HEADER=TRUE)
    """
    )
    print("aperçu de routes (10 lignes) :")
    duckdb.sql("SELECT * FROM routes LIMIT 10").show()

    # trips.txt
    duckdb.sql(
        f"""
        CREATE OR REPLACE TABLE trips AS
        SELECT * FROM read_csv_auto('{os.path.join(DATA_DIR, "trips.txt")}', HEADER=TRUE)
    """
    )
    print("aperçu de trips (10 lignes) :")
    duckdb.sql("SELECT * FROM trips LIMIT 10").show()

    print("--------------- END ------------------------")


# -- DAG --

with DAG(
    dag_id="gtfs_static_to_duckdb",
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "static"],
) as dag:

    t1 = PythonOperator(
        task_id="download_static_zip",
        python_callable=download_static_zip,
    )
    t2 = PythonOperator(
        task_id="unzip_static_files",
        python_callable=unzip_static_files,
    )
    t3 = PythonOperator(
        task_id="load_static_to_duckdb",
        python_callable=load_static_to_duckdb,
    )

    t1 >> t2 >> t3
