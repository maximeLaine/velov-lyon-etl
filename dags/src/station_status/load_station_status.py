from utils_gcp.load_to_bq_gcp import load_to_bq
from datetime import datetime


def load_bq_station_status():

    # Get the last refresh data
    now = datetime.now()
    date_format = now.strftime('%Y-%m-%d-%H')
    #date_format_bq = now.strftime('%Y%m%d')

    table_id = f"velov-etl-project.ods.station-status"

    uri = f"gs://bucket-velov-etl/station-status/{date_format}.json"
    schema_table_path = f"/home/airflow/gcs/dags/src/station_status/schema.json"

    load_to_bq(table_id, uri, schema_table_path)

if __name__ == "__main__":
    load_bq_station_status()
