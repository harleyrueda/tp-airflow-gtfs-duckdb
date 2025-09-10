import duckdb
import os
from datetime import datetime
from zoneinfo import ZoneInfo

DATA_DIR = "/opt/airflow/data"
WAREHOUSE = "/opt/airflow/warehouse/gtfs.duckdb"
EXPORT_DIR = "/opt/airflow/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)


# ----------- Carte de bus en temps reel-----


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
              WITH delay AS (
                SELECT
                  tu.trip_id,
                  tu.stop_sequence,
                  CAST(tu.arrival_dt AS TIMESTAMP WITH TIME ZONE) AS arrival_dt_utc,
                  CAST(tu.arrival_dt AT TIME ZONE 'Europe/Paris' AS DATE) AS local_service_day,
                  st.arrival_time_sec
                FROM trips_updates tu
                JOIN stop_times st
                  ON tu.trip_id = st.trip_id
                 AND tu.stop_sequence = CAST(st.stop_sequence AS BIGINT)
                WHERE tu.arrival_dt IS NOT NULL
                  AND st.arrival_time_sec IS NOT NULL
              ),
              per_event AS (
                SELECT
                  d.trip_id,
                  (
                    (local_service_day + d.arrival_time_sec * INTERVAL '1 second')
                    AT TIME ZONE 'Europe/Paris'
                  ) AS scheduled_ts_utc,
                  EXTRACT(
                    EPOCH FROM (
                      d.arrival_dt_utc - (
                        (local_service_day + d.arrival_time_sec * INTERVAL '1 second')
                        AT TIME ZONE 'Europe/Paris'
                      )
                    )
                  ) / 60.0 AS delay_min
                FROM delay d
              ),
              joined AS (
                SELECT
                  vp.trip_id,
                  vp.route_id,
                  vp.latitude,
                  vp.longitude,
                  to_timestamp(vp.timestamp) AS ts_utc,
                  ROUND(pe.delay_min, 2) AS delay_min
                FROM vehicle_positions vp
                LEFT JOIN per_event pe
                  ON vp.trip_id = pe.trip_id
              )
              SELECT
                trip_id,
                route_id,
                latitude,
                longitude,
                ts_utc AT TIME ZONE 'Europe/Paris' AS timestamp_paris,
                delay_min
              FROM joined
              ORDER BY ts_utc DESC
            ) TO '{out_path_esc}' (HEADER, DELIMITER ',');
        """
        )
        print(f"[Q2] CSV exporte: {out_path}")
    finally:
        con.close()
