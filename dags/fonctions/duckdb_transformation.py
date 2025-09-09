import duckdb
import os
from datetime import datetime
from zoneinfo import ZoneInfo

DATA_DIR = "/opt/airflow/data"
WAREHOUSE = "/opt/airflow/warehouse/gtfs.duckdb"
EXPORT_DIR = "/opt/airflow/exports"

os.makedirs(EXPORT_DIR, exist_ok=True)


def current_timestamp_string():
    return datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y%m%d_%H%M%S")


def duckdb_transformation():
    print("--------------- START DUCKDB TRANSFORMATION ------------------------")
    con = duckdb.connect(WAREHOUSE)
    try:
        ts = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y%m%d_%H%M%S")
        export_path = os.path.join(EXPORT_DIR, f"trips_updates_check_{ts}.csv")
        escaped_path = export_path.replace("'", "''")

        con.sql(
            f"""
            COPY (
                SELECT
                    trip_id, route_id, stop_id,
                    arrival_dt_paris,
                    departure_dt_paris
                FROM trips_updates
                WHERE arrival_dt_paris IS NOT NULL
                ORDER BY arrival_dt_paris
                LIMIT 50
            ) TO '{escaped_path}'
            (HEADER, DELIMITER ',');
        """
        )

        print(f"CSV exporte : {export_path}")
        print("--------------- END DUCKDB TRANSFORMATION --------------------")
    finally:
        con.close()
