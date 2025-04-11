import json
import logging
import os
import requests
from dotenv import load_dotenv
from tqdm import tqdm
from sankhya_api.request import SankhyaClient

# Carrega variáveis do .env
load_dotenv()

# Configuração de log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_query(nome, **params):
    caminho = os.path.join("queries", f"{nome}.sql")
    with open(caminho, "r", encoding="utf-8") as file:
        query = file.read()
        return query.format(**params)


def consulta_sankhya(nome_query, item=None, tempo=-1):
    service = "DbExplorerSP.executeQuery"
    endpoint_sankhya = f"https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName={service}&outputType=json"
    client = SankhyaClient(service, endpoint_sankhya, 5)

    if "JSON" not in nome_query:
        query = load_query(nome_query, tempo=tempo)
    else:
        query = load_query(nome_query, item=item)
    result = client.execute_query(query)
    rows = result.get("responseBody", {}).get("rows", []) if result else []
    items = [item[0] for item in rows]
    return items


def envia_cs(dados, endpoint_cs):
    url = f"https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/{endpoint_cs}?In_Tenant_ID=288"
    headers = {
        "Content-Type": "application/json"
    }

    if not dados:
        logging.error("Lista de dados está vazia.")
        return None

    try:
        # dados_parceiro[0] é a string JSON que está dentro da lista
        lista_de_dicts = json.loads(dados[0])  # transforma a string JSON em objeto Python
        response = requests.post(url, json=lista_de_dicts, headers=headers)
        # logging.info(f"Status code: {response.status_code} | Resposta: {response.text}")
        return response

    except json.JSONDecodeError as e:
        logging.error("Erro ao decodificar JSON:", e)
        return None
    except Exception as e:
        logging.error("Erro ao enviar requisição:", e)
        return None


def executa_atualizacoes(query_codigos, query_dados, endpoint_cs, descricao):
    codigos = consulta_sankhya(query_codigos)
    logging.info(f'Iniciando cadastro|atualização de {descricao}')
    for codigo in tqdm(codigos, desc=descricao.capitalize()+'s', unit=descricao):
        dados_consulta = consulta_sankhya(query_dados, codigo)
        envia_cs(dados_consulta, endpoint_cs)
    logging.info(f'Finalizando cadastro|atualização de {descricao}')


if __name__ == "__main__":
    executa_atualizacoes('PARCEIROS', 'JSON_PARCEIRO', 'Cliente', 'parceiro')
    executa_atualizacoes('PRODUTOS', 'JSON_PRODUTO', 'ProdutoUpdate', 'produto')
    executa_atualizacoes('PRODUTOS', 'JSON_PRODUTO', 'Saldos_Atualiza', 'estoque')
