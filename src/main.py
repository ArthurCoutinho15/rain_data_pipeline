import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText


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

def chave_uf_ano_mes_de_lista(elemento):
    """
    Receber uma lista de elementos
    Retornar uma tupla contendo chave e o valor de chuva em mm
    ('UF-ANO-MES', 1.3)
    """
    data, mm, uf = elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, float(mm)

def arredonda(elemento):
    """
    Recebe uma tupla e retorna a mesma com o valor arredondado
    """
    chave, mm = elemento
    return (chave, round(mm, 1))

def filtra_campos_vazios(elemento):
    """
    Remove elementos que tenham chaves vazias
    """
    chave, dados = elemento
    if all([
            dados['chuvas'],
            dados['dengue']
        ]):
        return True
    return False


def descompactar_elementos(elemento):
    """
    Recebe tupla ('CE-2015-12', {'chuvas': [7.6], 'dengue': [29.0]})
    Retorna ('CE','2015', '12', '7.6' ,'29.0')
    """
    chave, dados  = elemento
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, ano, mes, str(chuva), str(dengue)

def preparar_csv(elemento, delimitador=';'):
    """
    Recebe tupla ('CE',2015, 12, 7.6 ,29.0)
    Retorna string delimitada por vírgula ('CE;2015; 12; 7.6 ;29.0')
    """
    return f'{delimitador}'.join(elemento)
    
    


dengue = (
    pipeline
    | 'Leitura do dataset de dengue' >> ReadFromText('D:\\Projetos\\Python\\Data_Engineering\\beam_pipeline\\data\\casos_dengue.txt', skip_header_lines=1)
    # | 'Leitura do dataset de dengue' >> ReadFromText('D:\\Projetos\\Python\\Data_Engineering\\beam_pipeline\\data\\sample_casos_dengue.txt', skip_header_lines=1)
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
    # | 'Leitura do dataset de chuvas' >> ReadFromText('D:\\Projetos\\Python\\Data_Engineering\\beam_pipeline\\data\\sample_chuvas.csv', skip_header_lines=1)
    | 'De csv para lista' >> beam.Map(texto_para_lista, delimitador=',')
    | 'Criando chave UF-ANO_MES-MM' >> beam.Map(chave_uf_ano_mes_de_lista)
    | 'Agrupando por chave' >> beam.CombinePerKey(sum)
    | 'Arredondar resultados de chuvas' >> beam.Map(arredonda)
    # | 'Mostrar resultados chuvas' >> beam.Map(print)
    
)

resultado = (
    # (chuvas, dengue)
    # | 'Empilha Pcollections' >> beam.Flatten()
    # | 'Agrupar valores pela chave' >> beam.GroupByKey()
    ({'chuvas': chuvas, 'dengue': dengue})
    | 'Merge de pcolls' >> beam.CoGroupByKey()
    | 'Filtrar dados nulos' >> beam.Filter(filtra_campos_vazios)
    | 'Descompactar elementos' >> beam.Map(descompactar_elementos)
    | 'Preparar para csv' >> beam.Map(preparar_csv)
    # | 'Mostrar resultados junção' >> beam.Map(print)
)

header = 'UF;ANO;MES;CHUVA;DENGUE'

resultado | 'Criar arquivo csv' >>  WriteToText('D:\\Projetos\\Python\\Data_Engineering\\beam_pipeline\\data\\casos_dengue_chuva'\
    , file_name_suffix='csv', header=header)


pipeline.run()
