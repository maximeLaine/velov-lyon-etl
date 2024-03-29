from utils_gcp.load_json_to_bq_gcp import load_json_to_bq
from datetime import datetime


def load_bq_station_information():

    # Get the last refresh data
    now = datetime.now()
    date_format = now.strftime('%Y-%m-%d-%H')

    table_id = f"velov-etl-project.ods.station-information"

    uri = f"gs://bucket-velov-etl/station-information/{date_format}.json"
    schema_table_path = "/home/airflow/gcs/dags/src/station_information/schema.json"

    load_json_to_bq(table_id, uri, schema_table_path)

if __name__ == "__main__":
    load_bq_station_information()
