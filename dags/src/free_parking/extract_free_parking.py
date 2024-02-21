import os
import pandas as pd

from utils_gcp.upload_bucket_gcp import upload_blob
from datetime import datetime


def extract_csv_free_parking():
    # Read JSON file station_information
   # df = pd.read_json("https://download.data.grandlyon.com/files/rdata/jcd_jcdecaux.jcdvelov/station_information.json")

    df = pd.read_csv ("https://www.data.gouv.fr/fr/datasets/r/3a1390e8-9ea1-47b5-b9a8-9196005ce207")


    # Get the last refresh data
    now = datetime.now()
    date_format = now.strftime('%Y-%m-%d')
    # date = datetime.fromtimestamp(df['last_updated']['stations'] / 1e3)
    # date_format = date.strftime('%Y-%m-%d-%H')

    filename = f'free-parking-{date_format}.csv'

    # Save in local JSON file the transformations done
    df.to_csv(f'/home/airflow/gcs/data/{filename}')

    # Upload the file into a bucket GCP
    bucket_filename = f'free-parking/{date_format}.json'
    upload_blob('bucket-velov-etl', f'/home/airflow/gcs/data/{filename}', bucket_filename)

    # Remove the local file
    os.remove(f'/home/airflow/gcs/data/{filename}')


if __name__ == "__main__":
    extract_csv_free_parking()
