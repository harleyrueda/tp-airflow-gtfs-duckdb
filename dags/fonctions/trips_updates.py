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

PB_FILE = os.path.join(DATA_DIR, "trip_updates.pb")
PARSED_FILE = os.path.join(DATA_DIR, "trip_updates_parsed.pkl")
CSV_FILE = os.path.join(DATA_DIR, "trip_updates.csv")

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


# --- tache 4. aprés comprehension de DUCKDB - travail a partir de memoire et warehouse dans DUCKDB ---
def load_trips_updates_db():
    print("--------------- START ------------------------")

    my_df = pd.read_csv(CSV_FILE)

    # correction epoch a datetime Europe Paris
    def epoch_to_paris_datetime(epoch):
        try:
            return datetime.fromtimestamp(int(epoch), tz=ZoneInfo("Europe/Paris"))
        except:
            return None

    my_df["arrival_dt_paris"] = my_df["arrival_time"].apply(epoch_to_paris_datetime)
    my_df["departure_dt_paris"] = my_df["departure_time"].apply(epoch_to_paris_datetime)

    con = duckdb.connect(WAREHOUSE)
    con.register("df_view", my_df)
    con.sql("CREATE OR REPLACE TABLE trips_updates AS SELECT * FROM df_view")

    # Apercu
    con.sql("SELECT * FROM trips_updates LIMIT 10").show()

    con.close()
    print(f"table trips_updates sauvegardee dans {WAREHOUSE}")

    print("--------------- END ------------------------")
