import apache_beam as beam

with beam.Pipeline() as pipeline:
    (pipeline
     | 'Create Data' >> beam.Create(['Hello', 'Apache Beam', 'Windows'])
     | 'Print' >> beam.Map(print))