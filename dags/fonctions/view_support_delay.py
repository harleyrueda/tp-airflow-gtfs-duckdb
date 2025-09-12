import duckdb

WAREHOUSE = "/opt/airflow/warehouse/gtfs.duckdb"


def create_delays_with_support_columns():
    con = duckdb.connect(WAREHOUSE)

    con.execute(
        """
        CREATE OR REPLACE VIEW delays_with_support_columns AS
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
            trip_id,
            stop_sequence,
            stop_id,
            arrival_dt_utc,
            trip_start_utc + (arrival_time_sec - trip_start_sec) * INTERVAL '1 second' AS scheduled_ts_utc,
            EXTRACT(EPOCH FROM (
                arrival_dt_utc - (
                    trip_start_utc + (arrival_time_sec - trip_start_sec) * INTERVAL '1 second'
                )
            )) / 60.0 AS delay_min
        FROM full_data
        WHERE arrival_time_sec IS NOT NULL;
        """
    )

    con.close()
