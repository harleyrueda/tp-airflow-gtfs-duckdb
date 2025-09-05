import os
import requests
import zipfile
import duckdb

DATA_DIR = "/opt/airflow/data"
GTFS_ZIP = os.path.join(DATA_DIR, "gtfs_static.zip")


# --------------------------------- TACHES gtfs statiques-------------
# -------------------------------------------------------------------------

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

    # stop times
    duckdb.sql(
        f"""
        CREATE OR REPLACE TABLE stop_times AS
        SELECT * FROM read_csv_auto(
            '{os.path.join(DATA_DIR, "stop_times.txt")}', HEADER=TRUE,
            SAMPLE_SIZE=-1,
            ALL_VARCHAR=TRUE
        )
        """
    )
    print("aperçu de stop_times (10 lignes) :")
    duckdb.sql("SELECT * FROM stop_times LIMIT 10").show()

    print("--------------- END ------------------------")
