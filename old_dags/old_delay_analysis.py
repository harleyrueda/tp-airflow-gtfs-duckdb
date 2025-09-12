import duckdb
import os
from datetime import datetime
from zoneinfo import ZoneInfo

DATA_DIR = "/opt/airflow/data"
WAREHOUSE = "/opt/airflow/warehouse/gtfs.duckdb"
EXPORT_DIR = "/opt/airflow/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)


# ----------- Retards moyens dans le temps - voyages journée -----


def current_timestamp_string():
    return datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y%m%d_%H%M%S")


def average_delay_by_minute():
    ts = current_timestamp_string()
    out_path = os.path.join(EXPORT_DIR, f"avg_delay_by_minute_{ts}.csv")
    out_path_esc = out_path.replace("'", "''")

    con = duckdb.connect(WAREHOUSE)
    try:
        con.sql(
            f"""
            COPY (
              WITH actual AS (
                SELECT
                  tu.trip_id,
                  tu.stop_sequence,
                  CAST(tu.arrival_dt AS TIMESTAMP WITH TIME ZONE) AS arrival_dt_utc
                FROM trips_updates tu
                WHERE tu.arrival_dt IS NOT NULL
              ),
              sched AS (
                SELECT
                  st.trip_id,
                  CAST(st.stop_sequence AS BIGINT) AS stop_sequence,
                  st.arrival_time_sec
                FROM stop_times st
                WHERE st.arrival_time_sec IS NOT NULL
              ),
              joined AS (
                SELECT
                  a.arrival_dt_utc,
                  CAST(a.arrival_dt_utc AT TIME ZONE 'Europe/Paris' AS DATE) AS local_service_day,
                  s.arrival_time_sec
                FROM actual a
                JOIN sched s
                  ON a.trip_id = s.trip_id
                 AND a.stop_sequence = s.stop_sequence
              ),
              per_event AS (
                SELECT
                  arrival_dt_utc,
                  (
                    (local_service_day + arrival_time_sec * INTERVAL '1 second')
                    AT TIME ZONE 'Europe/Paris'
                  ) AS scheduled_ts_utc,
                  EXTRACT(
                    EPOCH FROM (
                      arrival_dt_utc
                      - ((local_service_day + arrival_time_sec * INTERVAL '1 second') AT TIME ZONE 'Europe/Paris')
                    )
                  ) / 60.0 AS delay_min
                FROM joined
              )
              SELECT
                DATE_TRUNC('minute', arrival_dt_utc AT TIME ZONE 'Europe/Paris') AS minute_ts,
                ROUND(AVG(delay_min), 2) AS avg_delay_min
              FROM per_event
              GROUP BY 1
              ORDER BY 1
            ) TO '{out_path_esc}' (HEADER, DELIMITER ',');
        """
        )
        print(f"[P1] CSV exportado: {out_path}")
    finally:
        con.close()
