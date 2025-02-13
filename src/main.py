import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText

pipeline_options = PipelineOptions(argc=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = ['id',
                  'data_iniSE',
                  'casos',
                  'ibge_code',
                  'cidade',
                  'uf',
                  'cep',
                  'latitude',
                  'longitude']

def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(sep=delimitador)

def lista_para_dicionario(elemento, colunas):
    """
    Recebe 2 listas
    Retorna um dicionário
    """
    return dict(zip(colunas, elemento))


dengue = (
    pipeline
    | 'Leitura do dataset de dengue' >> ReadFromText('D:\\Projetos\\Python\\Data_Engineering\\beam_pipeline\\data\\casos_dengue.txt', skip_header_lines=1)
    | 'De texto para lista' >> beam.Map(texto_para_lista)
    | 'De lista para dicionario' >> beam.Map(lista_para_dicionario, colunas_dengue)
    | 'Mostrar resultados' >> beam.Map(print)
    

)

pipeline.run()
