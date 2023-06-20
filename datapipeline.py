import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery



# Define your GCP project ID, bucket, and BigQuery dataset and table names
project_id = 'autonomous-tube-363006'
bucket_name = 'meetupxebia'
dataset_name = 'sample'
table_name = 'venues'

# Define the JSON file pattern in the GCP bucket
json_file_pattern = 'gs://meetupxebia/events.json'.format(bucket_name)

# Define the Beam pipeline
def run():
    with beam.Pipeline() as pipeline:
        # Read the JSON file from the GCP bucket
        json_data = pipeline | 'Read JSON' >> beam.io.ReadFromText(json_file_pattern)

        # Load the JSON data into BigQuery with auto-detected schema
        json_data | 'Write to BigQuery' >> WriteToBigQuery(
            table='{}.{}'.format(dataset_name, table_name),
            dataset=dataset_name,
            project=project_id,
            schema='AUTO_DETECT'
        )

# Run the pipeline
if __name__ == '__main__':
    run()