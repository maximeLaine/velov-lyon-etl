from google.cloud import bigquery


def load_json_to_bq(table_id, uri, schema_table_path):

    # Construct a BigQuery client object.
    client = bigquery.Client()

    schema_dict = client.schema_from_json(schema_table_path)

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    job_config.schema = schema_dict

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))