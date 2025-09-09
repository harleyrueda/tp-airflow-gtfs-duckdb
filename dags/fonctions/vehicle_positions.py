import os
import requests
import pickle
import pandas as pd
import duckdb
from google.transit import gtfs_realtime_pb2
from datetime import datetime
from zoneinfo import ZoneInfo


DATA_DIR = "/opt/airflow/data"
WAREHOUSE = "/opt/airflow/warehouse/gtfs.duckdb"

PB_FILE_VEHICLE = os.path.join(DATA_DIR, "vehicle_positions.pb")
PARSED_FILE_VEHICLE = os.path.join(DATA_DIR, "vehicle_positions_parsed.pkl")
CSV_FILE_VEHICLE = os.path.join(DATA_DIR, "vehicle_positions.csv")

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


# --- tache 4 charge et visualisation memoire et warehouse DuckDB


def load_vehicle_positions_db():
    print("--------------- START ------------------------")

    my_df = pd.read_csv(CSV_FILE_VEHICLE)

    def epoch_to_paris_datetime(epoch):
        try:
            return datetime.fromtimestamp(int(epoch), tz=ZoneInfo("Europe/Paris"))
        except:
            return None

    my_df["timestamp_paris"] = my_df["timestamp"].apply(epoch_to_paris_datetime)

    con = duckdb.connect(WAREHOUSE)
    con.register("df_view", my_df)
    con.sql("CREATE OR REPLACE TABLE vehicle_positions AS SELECT * FROM df_view")

    # Apercu
    con.sql("SELECT * FROM vehicle_positions LIMIT 10").show()

    con.close()
    print(f"Table vehicle_positions sauvegarde dans {WAREHOUSE}")

    print("--------------- END ------------------------")
