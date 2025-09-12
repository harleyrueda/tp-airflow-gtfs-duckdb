import duckdb
import os
from datetime import datetime
from zoneinfo import ZoneInfo

DATA_DIR = "/opt/airflow/data"
WAREHOUSE = "/opt/airflow/warehouse/gtfs.duckdb"
EXPORT_DIR = "/opt/airflow/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)


# ----------- Carte de bus en temps reel  QUESTION 3-----


def current_timestamp_string():
    return datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y%m%d_%H%M%S")


def position_bus_vehicles():
    ts = current_timestamp_string()
    out_path = os.path.join(EXPORT_DIR, f"position_bus_vehicles_{ts}.csv")
    out_path_esc = out_path.replace("'", "''")

    con = duckdb.connect(WAREHOUSE)
    try:
        con.sql(
            f"""
            COPY (
              WITH vp AS (
                SELECT
                  trip_id,
                  route_id,
                  latitude,
                  longitude,
                  to_timestamp("timestamp") AS ts_utc
                FROM vehicle_positions
              ),
              bus_with_delay AS (
                SELECT
                  v.trip_id,
                  v.route_id,
                  v.latitude,
                  v.longitude,
                  v.ts_utc,
                  pe.delay_min
                FROM vp v
                LEFT JOIN delays_with_support_columns pe
                  ON v.trip_id = pe.trip_id
              )
              SELECT
                trip_id,
                route_id,
                latitude,
                longitude,
                ts_utc AT TIME ZONE 'Europe/Paris' AS timestamp_paris,
                ROUND(delay_min, 2) AS delay_min
              FROM bus_with_delay
              ORDER BY ts_utc DESC
            ) TO '{out_path_esc}' (HEADER, DELIMITER ',');
        """
        )
        print(f"[Q2] CSV exporte: {out_path}")
    finally:
        con.close()


# ------------------ retard par route - QUESTION 4 -------------


def average_delay_by_route():
    ts = current_timestamp_string()
    out_path = os.path.join(EXPORT_DIR, f"avg_delay_by_route_{ts}.csv")
    out_path_esc = out_path.replace("'", "''")

    con = duckdb.connect(WAREHOUSE)
    try:
        con.sql(
            f"""
            COPY (
              SELECT
                t.route_id,
                ROUND(AVG(pe.delay_min), 2) AS avg_delay_min
              FROM delays_with_support_columns pe
              JOIN trips t ON pe.trip_id = t.trip_id
              GROUP BY t.route_id
              ORDER BY avg_delay_min DESC
            ) TO '{out_path_esc}' (HEADER, DELIMITER ',');
            """
        )
        print(f"[Q4] CSV exporte: {out_path}")
    finally:
        con.close()
