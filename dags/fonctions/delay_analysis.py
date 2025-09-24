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
                WITH first_stop AS (
                    SELECT trip_id, MIN(stop_sequence) AS first_seq
                    FROM trips_updates
                    GROUP BY trip_id
                ),
                trip_start AS (
                    SELECT
                        tu.trip_id,
                        tu.arrival_dt AS trip_start_utc,
                        st.arrival_time_sec AS trip_start_sec
                    FROM trips_updates tu
                    JOIN first_stop fs 
                        ON tu.trip_id = fs.trip_id AND tu.stop_sequence = fs.first_seq
                    JOIN stop_times st 
                        ON tu.trip_id = st.trip_id AND tu.stop_sequence = st.stop_sequence
                    WHERE tu.arrival_dt IS NOT NULL AND st.arrival_time_sec IS NOT NULL
                ),
                full_data AS (
                    SELECT
                        tu.trip_id,
                        tu.stop_sequence,
                        st.stop_id,
                        CAST(tu.arrival_dt AS TIMESTAMP WITH TIME ZONE) AS arrival_dt_utc,
                        st.arrival_time_sec,
                        ts.trip_start_utc,
                        ts.trip_start_sec
                    FROM trips_updates tu
                    JOIN stop_times st 
                        ON tu.trip_id = st.trip_id AND tu.stop_sequence = st.stop_sequence
                    JOIN trip_start ts 
                        ON tu.trip_id = ts.trip_id
                )
                SELECT
                    DATE_TRUNC('minute', arrival_dt_utc AT TIME ZONE 'Europe/Paris') AS minute_ts,
                    ROUND(AVG(
                        EXTRACT(EPOCH FROM (
                            arrival_dt_utc - (
                                trip_start_utc + (arrival_time_sec - trip_start_sec) * INTERVAL '1 second'
                            )
                        )) / 60.0
                    ), 2) AS avg_delay_min
                FROM full_data
                WHERE arrival_time_sec IS NOT NULL
                GROUP BY 1
                ORDER BY 1
            ) TO '{out_path_esc}' (HEADER, DELIMITER ',');
            """
        )
        print(f"[Q1] CSV exporte: {out_path}")
    finally:
        con.close()


# ----------- Taux  de punctualite QUESTION 6 -----


def punctuality_rate():
    ts = current_timestamp_string()
    out_path = os.path.join(EXPORT_DIR, f"punctuality_rate_{ts}.csv")
    out_path_esc = out_path.replace("'", "''")

    con = duckdb.connect(WAREHOUSE)
    try:
        con.sql(
            f"""
            COPY (
                SELECT
                    COUNT(*) AS total_events,
                    COUNT_IF(delay_min <= 5) AS on_time_events,
                    ROUND(100.0 * COUNT_IF(delay_min <= 5) / COUNT(*), 2) AS pct_on_time
                FROM delays_with_support_columns
            ) TO '{out_path_esc}' (HEADER, DELIMITER ',');
            """
        )
        print(f"[Q6] CSV exporte: {out_path}")
    finally:
        con.close()
