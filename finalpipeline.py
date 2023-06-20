import argparse
import datetime
import json
import logging
import sys

import apache_beam as beam
import apache_beam.transforms.window as window

from apache_beam.options.pipeline_options import PipelineOptions
from bigquery_schema_generator.generate_schema import SchemaGenerator
from google.cloud import storage, bigquery


class JobOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--project_id",
            type=str,
            help="project ID of GCP project",
            default="autonomous-tube-363006"
        )
       
        parser.add_argument(
            "--input",
            type=str,
            help="GCS Path of the directory. Must end with /",
            default="gs://meetupxebia/events.json"
        )       
        parser.add_argument(
            "--bigquery_dataset",
            type=str,
            help="Bigquery Dataset to write raw  data",
            default="sample"
        )
        parser.add_argument(
            "--bigquery_table",
            type=str,
            help="Bigquery Table to write raw salesforce data",
            default="venues"
        )


class WriteDataframeToBQ(beam.DoFn):

    def __init__(self, bq_dataset, bq_table, project_id):
        self.bq_dataset = bq_dataset
        self.bq_table = bq_table
        self.project_id = project_id

    def start_bundle(self):
        self.client = bigquery.Client()

    def process(self, df):
        # table where we're going to store the data
        table_id = f"{self.bq_dataset}.{self.bq_table}"

        # function to help with the json -> bq schema transformations
        generator = SchemaGenerator(input_format='dict', quoted_values_are_strings=True, keep_nulls=True)

        # Get original schema to assist the deduce_schema function. If the table doesn't exist
        # proceed with empty original_schema_map
        try:
            table = self.client.get_table(table_id)
            original_schema = table.schema
            self.client.schema_to_json(original_schema, "original_schema.json")
            with open("original_schema.json") as f:
                original_schema = json.load(f)
                original_schema_map, original_schema_error_logs = generator.deduce_schema(input_data=original_schema)
        except Exception:
            logging.info(f"{table_id} table not exists. Proceed without getting schema")
            original_schema_map = {}

        # convert dataframe to dict
        json_text = df.to_dict('records')

        # generate the new schema, we need to write it to a file because schema_from_json only accepts json file as input
        schema_map, error_logs = generator.deduce_schema(input_data=json_text, schema_map=original_schema_map)
        schema = generator.flatten_schema(schema_map)

        schema_file_name = "schema_map.json"
        with open(schema_file_name, "w") as output_file:
            json.dump(schema, output_file)

        # convert the generated schema to a version that BQ understands
        bq_schema = self.client.schema_from_json(schema_file_name)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=bq_schema
        )
        job_config.schema = bq_schema

        try:
            load_job = self.client.load_table_from_json(
                json_text,
                table_id,
                job_config=job_config,
            )  # Make an API request.

            load_job.result()  # Waits for the job to complete.
            if load_job.errors:
                logging.info(f"error_result =  {load_job.error_result}")
                logging.info(f"errors =  {load_job.errors}")
            else:
                logging.info(f'Loaded {len(df)} rows.')

        except Exception as error:
            logging.info(f'Error: {error} with loading dataframe')

            if load_job and load_job.errors:
                logging.info(f"error_result =  {load_job.error_result}")
                logging.info(f"errors =  {load_job.errors}")


def run(argv):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    options = pipeline_options.view_as(JobOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            
            | "Write Raw Data to Big Query" >> beam.ParDo(WriteDataframeToBQ(project_id=options.project_id, bq_dataset=options.bigquery_dataset, bq_table=options.bigquery_table))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv)