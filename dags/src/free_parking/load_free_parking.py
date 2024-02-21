from utils_gcp.load_csv_to_bq_gcp import load_csv_to_bq
from datetime import datetime


def load_bq_station_information():

    # Get the last refresh data
    now = datetime.now()
    date_format = now.strftime('%Y-%m-%d')

    table_id = f"velov-etl-project.ods.free_parking_velo"

    uri = f"gs://bucket-velov-etl/free_parking_velo/{date_format}.csv"
    schema_table_path = "/home/airflow/gcs/dags/src/free_parking_velo/schema.json"

    load_csv_to_bq(table_id, uri, schema_table_path)

if __name__ == "__main__":
    load_bq_station_information()
