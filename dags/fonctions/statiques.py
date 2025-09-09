import os
import requests
import zipfile
import duckdb
import pandas as pd

DATA_DIR = "/opt/airflow/data"
GTFS_ZIP = os.path.join(DATA_DIR, "gtfs_static.zip")

WAREHOUSE = "/opt/airflow/warehouse/gtfs.duckdb"


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


def format_hhmmss_to_seconds(t):
    try:
        h, m, s = map(int, t.split(":"))
        return h * 3600 + m * 60 + s
    except:
        return None


def load_static_to_duckdb():
    print("--------------- START ------------------------")

    con = duckdb.connect(WAREHOUSE)

    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".txt"):
            table_name = filename.replace(".txt", "")
            file_path = os.path.join(DATA_DIR, filename)

            print(f"Lecture {filename}  table {table_name}")
            df = pd.read_csv(file_path, dtype=str)

            # correction  stop_times.txt
            if filename == "stop_times.txt":
                df["arrival_time_sec"] = df["arrival_time"].apply(
                    format_hhmmss_to_seconds
                )
                df["departure_time_sec"] = df["departure_time"].apply(
                    format_hhmmss_to_seconds
                )

            con.register("df_view", df)
            con.sql(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_view")
            con.sql(f"SELECT * FROM {table_name} LIMIT 5").show()

    con.close()

    print(f"Tables sauvegardes dans {WAREHOUSE}")

    print("--------------- END ------------------------")

    # stops_df = pd.read_csv(os.path.join(DATA_DIR, "stops.txt"))
    # routes_df = pd.read_csv(os.path.join(DATA_DIR, "routes.txt"))
    # trips_df = pd.read_csv(os.path.join(DATA_DIR, "trips.txt"))
    # stop_times_df = pd.read_csv(os.path.join(DATA_DIR, "stop_times.txt"), dtype=str)

    # stops.txt

    # duckdb.sql("CREATE OR REPLACE TABLE stops AS SELECT * FROM stops_df")
    # print("Aperçu de stops (10 lignes) :")
    # duckdb.sql("SELECT * FROM stops LIMIT 10").show()

    # routes.txt

    # duckdb.sql("CREATE OR REPLACE TABLE routes AS SELECT * FROM routes_df")
    # print("Aperçu de routes (10 lignes) :")
    # duckdb.sql("SELECT * FROM routes LIMIT 10").show()

    # trips.txt

    # duckdb.sql("CREATE OR REPLACE TABLE trips AS SELECT * FROM trips_df")
    # print("Aperçu de trips (10 lignes) :")
    # duckdb.sql("SELECT * FROM trips LIMIT 10").show()

    # stop times

    # duckdb.sql("CREATE OR REPLACE TABLE stop_times AS SELECT * FROM stop_times_df")
    # print("Aperçu de stop_times (10 lignes) :")
    # duckdb.sql("SELECT * FROM stop_times LIMIT 10").show()
