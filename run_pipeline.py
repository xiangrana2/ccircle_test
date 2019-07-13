# -*- coding: utf-8 -*-
"""
Created on Jul 13th, 2019

@author: xiangrana2
Main ETL function
"""


def run(bigquery_dataset, bigquery_table, storage_bucket="coffeecircle"):
    """
    Extracts the data from the provided file data.csv with an ecommerce sales information

    :param bigquery_dataset: name of the dataset that is going to be use to load the data
    :param bigquery_table: Name of table used to load the data
    :param storage_bucket: Name of the bucket use to store DataFlow logs
    """

    # DataFlow requires to have the imports inside the function or context where the pipeline
    # definition is even though it goes against Python best practices.
    import csv

    import apache_beam as beam
    from apache_beam.io import ReadFromText
    from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
    from datetime import datetime

    class Split(beam.DoFn):

        def process(self, element):
            reader = csv.reader(element.split('\n'), delimiter=',')
            try:
                for row in reader:
                    return [{
                        'invoice_no': str(row[0]),
                        'stock_code': str(row[1]),
                        'description': str(row[2]),
                        'quantity': int(row[3]),
                        'invoice_date': str(datetime.strptime(row[4], '%m/%d/%Y %H:%M')),
                        'unit_price': float(row[5]),
                        'customer_id': int(row[6]) if row[6] else None,
                        'country': str(row[7])
                    }]
            except Exception as exc:
                print(exc)

    # Gets BigQuery schema definition
    bigquery_schema = 'invoice_no:STRING,stock_code:STRING,description:STRING,\
                       quantity:INTEGER,invoice_date:TIMESTAMP,unit_price:FLOAT,\
                       customer_id:INTEGER,country:STRING'
    # Storage bucket for logs
    storage_bucket = "gs://{}/dataflow".format(storage_bucket)

    # Retrieve project Id and append to PROJECT form GoogleCloudOptions
    global PROJECT
    PROJECT = PipelineOptions().view_as(GoogleCloudOptions).project

    # Create and set your PipelineOptions.
    options = PipelineOptions()

    # For Cloud execution, set the Cloud Platform project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'coffeecircle'
    google_cloud_options.job_name = "test-" + \
        str(bigquery_table).replace('_', '-')
    google_cloud_options.staging_location = (
        "%s/staging_location" % storage_bucket)
    google_cloud_options.temp_location = ("%s/temp" % storage_bucket)
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = beam.Pipeline(options=options)

    # Transformation and loading steps
    rows = (
        p |
        ReadFromText('gs://coffeecircle/data.csv', skip_header_lines=1) |
        beam.ParDo(Split())
    )
    rows | 'Write data into Bigquery' >> beam.io.WriteToBigQuery(
        table=bigquery_table,
        dataset=bigquery_dataset,
        project='coffeecircle',
        schema=bigquery_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run('ecommerce', 'sales')
