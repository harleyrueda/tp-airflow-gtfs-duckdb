import duckdb
import os
from datetime import datetime
from zoneinfo import ZoneInfo

DATA_DIR = "/opt/airflow/data"
WAREHOUSE = "/opt/airflow/warehouse/gtfs.duckdb"
EXPORT_DIR = "/opt/airflow/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)


# ----------- Retards le plus frequents par jour -----


def current_timestamp_string():
    return datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y%m%d_%H%M%S")


def heatmap_delay_day():
    ts = current_timestamp_string()
    out_path = os.path.join(EXPORT_DIR, f"avg_delay_heatmap_Q5_{ts}.csv")
    out_path_esc = out_path.replace("'", "''")

    con = duckdb.connect(WAREHOUSE)
    try:
        con.sql(
            """
            CREATE OR REPLACE VIEW delays_csv AS
            SELECT * FROM read_csv_auto('./exports/avg_delay_by_minute_*.csv', header=True)
        """
        )
        con.sql(
            f"""
            COPY (
                SELECT
                    strftime(minute_ts, '%Y-%m-%d') AS jour_date,
                    strftime(minute_ts, '%H') AS heure,
                    ROUND(AVG(avg_delay_min), 2) AS avg_delay_min,
                    COUNT(*) AS nb_events
                FROM delays_csv
                WHERE minute_ts IS NOT NULL
                AND minute_ts <= now()
                GROUP BY jour_date, heure
                ORDER BY jour_date, heure
            )
            TO '{out_path_esc}' (HEADER, DELIMITER ',');
        """
        )
        print(f"[Q5] CSV exporte: {out_path}")
    finally:
        con.close()
