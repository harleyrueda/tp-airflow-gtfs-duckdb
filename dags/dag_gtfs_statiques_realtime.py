from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from google.transit import gtfs_realtime_pb2
from datetime import datetime
import pandas as pd
import requests
import os
import pickle
import duckdb
import zipfile

# Variables communes

DATA_DIR = "/opt/airflow/data"

# --------- Variables trips_update --------------

PB_FILE = os.path.join(DATA_DIR, "trip_updates.pb")
PARSED_FILE = os.path.join(DATA_DIR, "trip_updates_parsed.pkl")
CSV_FILE = os.path.join(DATA_DIR, "trip_updates.csv")

# --------- variables vehicle positions -------------

PB_FILE_VEHICLE = os.path.join(DATA_DIR, "vehicle_positions.pb")
PARSED_FILE_VEHICLE = os.path.join(DATA_DIR, "vehicle_positions_parsed.pkl")
CSV_FILE_VEHICLE = os.path.join(DATA_DIR, "vehicle_positions.csv")

# --------- variables Gtfs statiques -------------

GTFS_ZIP = os.path.join(DATA_DIR, "gtfs_static.zip")

# ----------------------------------- TACHES trips updates---------------
# -----------------------------------------------------------------------


# --- tache 1 download trip updates ---
def download_trip_updates():
    url = "https://ara-api.enroute.mobi/rla/gtfs/trip-updates"
    response = requests.get(url)

    os.makedirs(DATA_DIR, exist_ok=True)

    with open(PB_FILE, "wb") as f:
        f.write(response.content)

    print(f"donnees trips_update telecharges - {PB_FILE }")


# --- tache 2 "parsing" -
def parse_trip_updates():
    feed = gtfs_realtime_pb2.FeedMessage()

    with open(PB_FILE, "rb") as f:
        feed.ParseFromString(f.read())

    trips_update_list = []

    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip = entity.trip_update.trip

            trip_id = trip.trip_id
            route_id = trip.route_id
            direction_id = trip.direction_id if trip.HasField("direction_id") else None

            for stop_time_update in entity.trip_update.stop_time_update:
                stop_id = stop_time_update.stop_id
                stop_sequence = stop_time_update.stop_sequence

                if stop_time_update.HasField(
                    "arrival"
                ) and stop_time_update.arrival.HasField("time"):
                    arrival_time = stop_time_update.arrival.time
                else:
                    arrival_time = None

                if stop_time_update.HasField(
                    "departure"
                ) and stop_time_update.departure.HasField("time"):
                    departure_time = stop_time_update.departure.time
                else:
                    departure_time = None

                trips_update_list.append(
                    {
                        "trip_id": trip_id,
                        "route_id": route_id,
                        "direction_id": direction_id,
                        "stop_id": stop_id,
                        "stop_sequence": stop_sequence,
                        "arrival_time": arrival_time,
                        "departure_time": departure_time,
                    }
                )

    with open(PARSED_FILE, "wb") as f:
        pickle.dump(trips_update_list, f)
    print(f"parsing ok{PARSED_FILE}")


# -- tache 3 exporter CSV ---
def export_trip_updates_csv():
    with open(PARSED_FILE, "rb") as f:
        trips_update_list = pickle.load(f)

    df = pd.DataFrame(trips_update_list)
    df.to_csv(CSV_FILE, index=False)

    print(f"export trips_update en CSV OK {CSV_FILE}")


# --- tache 4. aprés comprehension de DUCKDB - travail a partir de memoire dans DUCKDB ---
def load_trips_updates_db():
    print("--------------- START ------------------------")

    my_df = pd.read_csv(CSV_FILE)

    # en mémoire avec DuckDB
    duckdb.sql("CREATE OR REPLACE TABLE trips_updates AS SELECT * FROM my_df")

    print("---- Aperçu en mémoire (logs) ----")
    duckdb.sql("SELECT * FROM trips_updates LIMIT 10").show()

    print("--------------- END ------------------------")

    # -----------------------------TACHES vehicle positions ------------
    # -----------------------------------------------------------------------

    # -- tache 1 download vehicle positions


def download_vehicle_positions():
    url = "https://ara-api.enroute.mobi/rla/gtfs/vehicle-positions"
    response = requests.get(url)

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PB_FILE_VEHICLE, "wb") as f:
        f.write(response.content)

    print(f"fichier vehicle_positions telecharge - {PB_FILE_VEHICLE}")


# -- tache 2 parsing en utilisant HasField


def parse_vehicle_positions():
    feed = gtfs_realtime_pb2.FeedMessage()

    with open(PB_FILE_VEHICLE, "rb") as f:
        feed.ParseFromString(f.read())

    vehicles_list = []

    for entity in feed.entity:
        if entity.HasField("vehicle"):
            veh = entity.vehicle

            trip_id = veh.trip.trip_id if veh.trip.HasField("trip_id") else None
            route_id = veh.trip.route_id if veh.trip.HasField("route_id") else None
            stop_id = veh.stop_id if veh.HasField("stop_id") else None

            latitude = (
                veh.position.latitude if veh.position.HasField("latitude") else None
            )
            longitude = (
                veh.position.longitude if veh.position.HasField("longitude") else None
            )
            bearing = veh.position.bearing if veh.position.HasField("bearing") else None
            speed = veh.position.speed if veh.position.HasField("speed") else None

            timestamp = veh.timestamp if veh.HasField("timestamp") else None
            vehicle_id = veh.vehicle.id if veh.HasField("vehicle") else None

            vehicles_list.append(
                {
                    "trip_id": trip_id,
                    "route_id": route_id,
                    "stop_id": stop_id,
                    "latitude": latitude,
                    "longitude": longitude,
                    "bearing": bearing,
                    "speed": speed,
                    "timestamp": timestamp,
                    "vehicle_id": vehicle_id,
                }
            )

    with open(PARSED_FILE_VEHICLE, "wb") as f:
        pickle.dump(vehicles_list, f)

    print(f"parsing ok {PARSED_FILE_VEHICLE}")


# --- tache 3 exporter CSV


def export_vehicle_positions_csv():
    with open(PARSED_FILE_VEHICLE, "rb") as f:
        vehicles_list = pickle.load(f)

    df = pd.DataFrame(vehicles_list)
    df.to_csv(CSV_FILE_VEHICLE, index=False)

    print(f"export vehicle_positions en CSV OK {CSV_FILE_VEHICLE}")


# --- tache 4 charge et visualisation memoire DuckDB


def load_vehicle_positions_db():
    print("--------------- START ------------------------")

    my_df = pd.read_csv(CSV_FILE_VEHICLE)

    duckdb.sql("CREATE OR REPLACE TABLE vehicle_positions AS SELECT * FROM my_df")

    print("---- Aperçu en mémoire (logs) ----")
    duckdb.sql("SELECT * FROM vehicle_positions LIMIT 10").show()

    print("--------------- END ------------------------")


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


# --------------------------------- DuckDB TR-------------
# -------------------------------------------------------------------------


def duckdb_transformation():
    print("--------------- DUCKDB TRANSFORMATION ------------------------")

    print("--------------- END DUCKDB TRANSFORMATION --------------------")


# -- DAG GENERAL - TaskGroup pour statiques, trips updates, vehicle_positions --

with DAG(
    dag_id="dag_gtfs_global",
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "realtime", "static"],
) as dag:

    with TaskGroup("trips_updates") as TGdag_trips_updates:
        t1 = PythonOperator(
            task_id="download_trip_updates",
            python_callable=download_trip_updates,
        )
        t2 = PythonOperator(
            task_id="parse_trip_updates",
            python_callable=parse_trip_updates,
        )
        t3 = PythonOperator(
            task_id="export_trips_updates_csv",
            python_callable=export_trip_updates_csv,
        )
        t4 = PythonOperator(
            task_id="load_trips_updates_db",
            python_callable=load_trips_updates_db,
        )

        t1 >> t2 >> t3 >> t4

    with TaskGroup("vehicle_positions") as TGdag_vehicle:
        v1 = PythonOperator(
            task_id="download_vehicle_positions",
            python_callable=download_vehicle_positions,
        )
        v2 = PythonOperator(
            task_id="parse_vehicle_positions",
            python_callable=parse_vehicle_positions,
        )
        v3 = PythonOperator(
            task_id="export_vehicle_positions_csv",
            python_callable=export_vehicle_positions_csv,
        )
        v4 = PythonOperator(
            task_id="load_vehicle_positions_db",
            python_callable=load_vehicle_positions_db,
        )

        v1 >> v2 >> v3 >> v4

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

    [TGdag_trips_updates, TGdag_vehicle, TGdag_static] >> TGdag_duckdb
