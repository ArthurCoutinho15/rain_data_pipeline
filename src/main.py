import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText

pipeline_options = PipelineOptions(argc=None)
pipeline = beam.Pipeline(options=pipeline_options)

dengue = (
    pipeline
    | 'Leitura do dataset de dengue' >> ReadFromText('D:\\Projetos\\Python\\Data_Engineering\\beam_pipeline\\data\\casos_dengue.txt', skip_header_lines=1)

)
 