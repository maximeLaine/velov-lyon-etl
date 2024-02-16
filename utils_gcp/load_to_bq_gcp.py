from google.cloud import bigquery


def load_to_bq(table_id, uri, schema_table_path):
#def load_to_bq():
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # variable test
    # table_id ="velov-lyon-etl.ods.station-information"
    # uri = f"gs://velov-bucket-etl/station-information/2024-02-15-16.json"
    # schema_table_path = "src/station_information/schema.json"

    schema_dict = client.schema_from_json(schema_table_path)


    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job_config.schema = schema_dict

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="EU",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

