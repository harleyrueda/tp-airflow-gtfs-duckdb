import duckdb
import os
from datetime import datetime
from zoneinfo import ZoneInfo

DATA_DIR = "/opt/airflow/data"
WAREHOUSE = "/opt/airflow/warehouse/gtfs.duckdb"
EXPORT_DIR = "/opt/airflow/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)


# ----------- Carte de arrets -  retard average arret -----


def current_timestamp_string():
    return datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y%m%d_%H%M%S")


def average_delay_by_stop():
    ts = current_timestamp_string()
    out_path = os.path.join(EXPORT_DIR, f"avg_delay_by_stop_{ts}.csv")
    out_path_esc = out_path.replace("'", "''")

    con = duckdb.connect(WAREHOUSE)
    try:
        con.sql(
            f"""
            COPY (
              SELECT
                s.stop_id,
                s.stop_name,
                s.stop_lat,
                s.stop_lon,
                ROUND(AVG(pe.delay_min), 2) AS avg_delay_min
              FROM stops s
              LEFT JOIN delays_with_support_columns pe
                ON s.stop_id = pe.stop_id
              GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon
              ORDER BY avg_delay_min DESC
            ) TO '{out_path_esc}' (HEADER, DELIMITER ',');
            """
        )
        print(f"[Q3] CSV exporte: {out_path}")
    finally:
        con.close()
