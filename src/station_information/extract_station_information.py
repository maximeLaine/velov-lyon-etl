import os
import pandas as pd
import json

from utils_gcp.upload_bucket_gcp import upload_blob
from datetime import datetime
from io import StringIO



def extract_json_station_information():
    # Read JSON file station_information
    df = pd.read_json("https://download.data.grandlyon.com/files/rdata/jcd_jcdecaux.jcdvelov/station_information.json")

    # Get the last refresh data
    now = datetime.now()
    date_format = now.strftime('%Y-%m-%d-%H')
    # date = datetime.fromtimestamp(df['last_updated']['stations'] / 1e3)
    # date_format = date.strftime('%Y-%m-%d-%H')

    # print(type(data))
    filename = f'station_information-{date_format}.json'

    # Get the data in the JSON file
    json_string = json.dumps(df['data'][0], indent=3)
    in_json = StringIO(json_string)
    json_load = json.load(in_json)

    cleaned = []
    for record in json_load:
        if "capacity" not in record:
            record["capacity"] = 0
        cleaned.append(record)


    # Transform json in newline json
    limit_json = [json.dumps(record) for record in cleaned]
    json_newline = "\n".join(limit_json)

    # Save in local JSON file the transformations done
    f = open(f'./data/{filename}', 'w')
    f.write(json_newline)
    f.close()

    # Upload the file into a bucket GCP
    bucket_filename = f'station-information/{date_format}.json'
    upload_blob('bucket-velov-etl', f'./data/{filename}', bucket_filename)

    # Remove the local file
    os.remove(f'./data/{filename}')


if __name__ == "__main__":
    extract_json_station_information()

