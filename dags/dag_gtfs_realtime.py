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


# -- DAG GENERAL - TaskGroup pour trips updates et vehicle_positions --

with DAG(
    dag_id="dag_gtfs_realtime",
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "realtime"],
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

    TGdag_trips_updates >> TGdag_vehicle
