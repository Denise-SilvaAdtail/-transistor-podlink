# doc - https://developers.transistor.fm/#episodes


# import libs
import requests, json, functions_framework
import stalse_functions as sf
import pandas as pd
import flask


#app = flask.Flask(__name__)

# inicializando o functions_framework
@functions_framework.http

# obtendo episódios
def req_apisodes(page=0):
    """Função que recupera uma lista paginada de episódios em ordem decrescente por data publicada

    Args:
        per (int, optional): Quantidade de resultados por página. Defaults to 10.
        page (int, optional): Número da página. Defaults to 0.

    Returns:
        json: Informações descritivas de cada episódio
    """
    
    # utilizando a api-key do KMS
    apikey = sf.keygcp('msc-apikey-transistor')
    
    # tratando paginação
    if page==0:
        url = "https://api.transistor.fm/v1/episodes?pagination[per]=10&pagination[page]=1"
    
    else:
        url = "https://api.transistor.fm/v1/episodes?pagination[per]=10&pagination[page]="+str(page)

    payload={}
    headers = {
    'x-api-key': apikey
    }

    response = requests.get(url, headers=headers, data=payload)
    
    # transformando a resposta da requisição em json
    res = response.text
    res = json.loads(res)
    return res


# totalizando episódios para usar no 'for' futuramente e com este número tratar a paginação
total_valores = req_apisodes()['meta']['totalCount']


# buscando apenas as colunas necessárias
df_cols = ['id']
df_inner_cols = ['title', 'number', 'season', 'status', 'published_at', 'duration', 'explicit', 'keywords', 'alternate_url', 'media_url', 
                 'image_url', 'author', 'summary', 'created_at', 'updated_at', 'duration_in_mmss', 
                 'share_url', 'audio_processing', 'type', 'email_notifications']


# percorrendo os níveis do json para gerar dados e popular no df futuramete
epsodes_data = []

for i in range(1, total_valores):
    atual_value=req_apisodes(i)
    
    # nível 1 do json
    for epsode in atual_value['data']:
        _c = []
        for col in df_cols:
            try:
                _c.append(epsode[col])
            except KeyError:
                _c.append(None)
        
        # nível 2 do json            
        for innercol in df_inner_cols:
            try:
                _c.append(epsode['attributes'][innercol])
            except KeyError:
                _c.append(None)
        
        # informação aglutinada dos dois níveis do json
        epsodes_data.append(_c)


# jutando as colunas de diferentes níveis do json 
ndf = df_cols + df_inner_cols

# criando df com as informações obtidas acima
df_episodes = pd.DataFrame(data=epsodes_data, columns=ndf)

def result(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    
    
    # obtendo id's do BigQuery
    id_tabela = "`mosaic-fertilizantes.podcast.episodes`"
    project = id_tabela.split('.')[0]


    # deletando SOMENTE os ID's que a API retornou
    sf.deleta_ids(df_episodes, 'id', id_tabela, project)


    # criando e consumindo csv para importação no BigQuery
    csv = sf.csv_bucket(df_episodes, 'episodes')
    
    
    # criando a tabela no BigQuery
    res = sf.cria_bq(df_episodes, id_tabela, 'append', project, csv)
    
    if request_json and 'episodes' in request_json:
        episodes = request_json['episodes']
    elif request_args and 'episodes' in request_args:
        episodes = request_args['episodes']
    else:
        return res