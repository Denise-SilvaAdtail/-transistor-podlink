# doc - https://developers.transistor.fm/#analytics


# import libs
import requests, json, pandas_gbq
import stalse_functions as sf
import pandas as pd
import datetime as dt


# obtendo report de episódios
def report_apisodes(id_episode, start_date=0, end_date=0):
    """Função que recupera análises de downloads por data para cada podcast

    Args:
        id_episode (int): ID do epsódio
        start_date (str, opcional): Data de início para análise. Defaults: últimos 14 dias.
        end_date (str, opcional): Data de término para análise. Defaults: data atual.

    Returns:
        json: Informações de downloads/data
    """
    
    # utilizando a api-key do KMS
    apikey = sf.keygcp('msc-apikey-transistor')
    
    # requisição dos últimos 14 dias por padrão
    if start_date == 0 and end_date == 0:
        url = "https://api.transistor.fm/v1/analytics/episodes/"+str(id_episode)
        
    else:
        url = "https://api.transistor.fm/v1/analytics/episodes/"+str(id_episode)+"?start_date="+str(start_date)+"&end_date="+str(end_date)
        
    payload={}
    headers = {
    'x-api-key': apikey
    }

    response = requests.get(url, headers=headers, data=payload)
    
    # transformando a resposta da requisição em json
    res = response.text
    res = json.loads(res)
    return res


# criando stringquery para trazer todos os id's dos AD's
query_str = '''SELECT distinct id FROM `mosaic-fertilizantes.podcast.episodes`'''


# consultando os id's da tabela "podcast.episodes" para inserir no df df_query_ids
query_ads_ids = pandas_gbq.read_gbq(query_str, project_id="mosaic-fertilizantes")


# criando df
df_query_ids = pd.DataFrame()
df_query_ids = df_query_ids.append(query_ads_ids, ignore_index=True)


# tratando os id's para futuramente e inserindo na variael "total_valores"
query_ads_ids = list(query_ads_ids['id'])
total_valores = len(query_ads_ids)


# buscando apenas as colunas necessárias
df_cols = ['id']
df_inner_cols = ['date', 'downloads']


# contabilizando datas para exluir 5 últimos dias do BQ
end_date = dt.date.today()
start_date = end_date - dt.timedelta(days=5)
start_date = start_date.strftime('%d-%m-%Y')
end_date = end_date.strftime('%d-%m-%Y')


# percorrendo os níveis do json para gerar dados e popular no df futuramete
episodes_data = []

for id in query_ads_ids:
    atual_value=report_apisodes(id, start_date, str(end_date))
    
    # validando a qtde de downloads/dia
    for epsode in atual_value['data']['attributes']['downloads']:
        _c = []
        
        # nível 1 do json
        for col in df_cols:
            _c.append(atual_value['data'][col])            
            
        # nível 2 do json
        for inner_col in df_inner_cols:
            _c.append(epsode[inner_col])
        
        episodes_data.append(_c)


# jutando as colunas de diferentes níveis do json 
ndf = df_cols + df_inner_cols

# criando df com as informações obtidas acima
df_episodes_report = pd.DataFrame(data=episodes_data, columns=ndf)


# apagando valores zerados
drop_zeros=df_episodes_report[df_episodes_report["downloads"]==0].index
df_episodes_report=df_episodes_report.drop(drop_zeros)


def result(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    
    # deletando datas, neste caso 5 dias
    id_tabela = "`mosaic-fertilizantes.podcast.episodes_report`"
    sf.deleta_datas(df_episodes_report, 'date', id_tabela, 'mosaic-fertilizantes')


    # criando e consumindo csv para importação no BigQuery
    csv = sf.csv_bucket(df_episodes_report, 'episodes_report')
    
    # criando a tabela no BigQuery
    res = sf.cria_bq(df_episodes_report, id_tabela, 'append', 'mosaic-fertilizantes', csv)
    
    if request_json and 'episodes_report' in request_json:
        episodes_report = request_json['episodes_report']
    elif request_args and 'episodes_report' in request_args:
        episodes_report = request_args['episodes_report']
    else:
        return res