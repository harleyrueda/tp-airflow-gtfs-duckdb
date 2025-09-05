from airflow import DAG
from airflow.operators.python import PythonOperator
from google.transit import gtfs_realtime_pb2
from datetime import datetime
import pandas as pd
import requests
import os
import pickle
import duckdb

DATA_DIR = "/opt/airflow/data"
PB_FILE = os.path.join(DATA_DIR, "trip_updates.pb")
PARSED_FILE = os.path.join(DATA_DIR, "trip_updates_parsed.pkl")
CSV_FILE = os.path.join(DATA_DIR, "trip_updates.csv")

# definition de table general dans DUCKDB
DUCKDB_FILE = "/opt/airflow/warehouse/warehouse.duckdb"
TABLE_NAME = "trips_updates"


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


# --- tache 4.1 Telechargement dans DUCKDB ---
# def load_trip_updates_duckdb():
# os.makedirs(os.path.dirname(DUCKDB_FILE), exist_ok=True)
# con = duckdb.connect(DUCKDB_FILE)
# con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
# con.execute(
# f"""
# CREATE TABLE {TABLE_NAME} AS
# SELECT * FROM read_csv_auto('{CSV_FILE}', HEADER=TRUE)
# """
# )
# con.close()

# -- DAG - TACHES --
with DAG(
    dag_id="trip_updates_to_duckdb",
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "realtime"],
) as dag:

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
