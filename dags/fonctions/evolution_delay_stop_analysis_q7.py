import duckdb
import os
from datetime import datetime
from zoneinfo import ZoneInfo

DATA_DIR = "/opt/airflow/data"
WAREHOUSE = "/opt/airflow/warehouse/gtfs.duckdb"
EXPORT_DIR = "/opt/airflow/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)


# ----------- evolution retard par arret QUESTION 7-----


def current_timestamp_string():
    return datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y%m%d_%H%M%S")


def evolution_delay_by_stop():
    ts = current_timestamp_string()
    out_path = os.path.join(EXPORT_DIR, f"evolution_delay_by_stop_Q7_{ts}.csv")
    out_path_esc = out_path.replace("'", "''")

    con = duckdb.connect(WAREHOUSE)
    try:
        con.sql(
            """
            CREATE OR REPLACE VIEW delays_stop_snapshot AS
            SELECT *,
                   STRPTIME(
                       regexp_extract(filename, '([0-9]{8}_[0-9]{6})'),
                       '%Y%m%d_%H%M%S'
                   ) AS snapshot_ts_temps
            FROM read_csv_auto(
                './exports/avg_delay_by_stop_*.csv',
                HEADER=TRUE,
                AUTO_DETECT=TRUE,
                union_by_name=TRUE,
                SAMPLE_SIZE=-1,
                FILENAME=TRUE
            );
        """
        )

        con.sql(
            f"""
            COPY (
                SELECT 
                    CAST(stop_id AS VARCHAR) AS stop_id,
                    stop_name,
                    stop_lat,
                    stop_lon,
                    snapshot_ts_temps,
                    avg_delay_min
                FROM delays_stop_snapshot
                WHERE stop_id IN ('21265','8064','8077','1183','619')
                ORDER BY stop_id, snapshot_ts_temps
            )
            TO '{out_path_esc}' (HEADER, DELIMITER ',');
        """
        )
        print(f"[Q7] CSV exporte: {out_path}")
    finally:
        con.close()
