import re
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

def tratamento_data(elemento):
    """
    Recebe um dicionário
    Retorna dicionário com novo campo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    """
    Receber um dicionário
    Retornar uma tupla com o estado (UF) elemento (UF, dicionario)
    """
    chave = elemento['uf']
    return (chave, elemento) 

def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS',[{},{}])
    Retorna uma tupla ('RS-2025-12', 8.0)
    """   
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r"\d", registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)


dengue = (
    pipeline
    | 'Leitura do dataset de dengue' >> ReadFromText('D:\\Projetos\\Python\\Data_Engineering\\beam_pipeline\\data\\casos_dengue.txt', skip_header_lines=1)
    | 'De texto para lista' >> beam.Map(texto_para_lista)
    | 'De lista para dicionario' >> beam.Map(lista_para_dicionario, colunas_dengue)
    | 'Adiciona campo ano_mes' >> beam.Map(tratamento_data)
    | 'Cria chave pelo estado' >> beam.Map(chave_uf)
    | 'Agrupar pelo estado' >> beam.GroupByKey()
    | 'Descompactar casos de dengue' >> beam.FlatMap(casos_dengue)
    | 'Soma dos casos pela chave' >> beam.CombinePerKey(sum)
    # | 'Mostrar resultados' >> beam.Map(print)
    
)

chuvas = (
    pipeline
    | 'Leitura do dataset de chuvas' >> ReadFromText('D:\\Projetos\\Python\\Data_Engineering\\beam_pipeline\\data\\chuvas.csv', skip_header_lines=1)
    | 'De csv para lista' >> beam.Map(texto_para_lista, delimitador=',')
    | 'De lista para dicionario' >> beam.Map(lista_para_dicionario, colunas_dengue)
    | 'Mostrar resultados' >> beam.Map(print)
    
)



pipeline.run()
