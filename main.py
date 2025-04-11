"""
Script principal para execução da integração entre a API Sankhya e o sistema CS.

Este módulo coordena o carregamento de queries, consulta de dados via Sankhya,
e envio das informações para o endpoint da CS50Integração.
"""

import json
import logging
import os
import requests
import requests.exceptions
from dotenv import load_dotenv
from tqdm import tqdm
from sankhya_api.request import SankhyaClient

# Carrega variáveis do .env
load_dotenv()

# Configuração de log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_query(nome, **params):
    """
    Carrega uma query SQL de um arquivo no diretório 'queries' e aplica os parâmetros fornecidos.

    Args:
        nome (str): Nome do arquivo SQL (sem extensão .sql).
        **params: Parâmetros que serão interpolados na query.

    Returns:
        str: A query SQL formatada com os parâmetros fornecidos.
    """
    caminho = os.path.join("queries", f"{nome}.sql")
    with open(caminho, "r", encoding="utf-8") as file:
        query = file.read()
        return query.format(**params)


_sankhya_clients = {}


def consulta_sankhya(nome_query, service, item=None, tempo=-15):
    """
    Executa uma consulta na API Sankhya reutilizando a mesma instância de SankhyaClient
    por serviço, evitando nova autenticação desnecessária.
    """
    if service not in _sankhya_clients:
        base_url_sankhya = 'https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName='
        endpoint_sankhya = f"{base_url_sankhya}{service}&outputType=json"
        _sankhya_clients[service] = SankhyaClient(service, endpoint_sankhya, 5)

    client = _sankhya_clients[service]

    if "JSON" not in nome_query:
        query = load_query(nome_query, tempo=tempo)
    else:
        query = load_query(nome_query, item=item)

    result = client.execute_query(query)
    rows = result.get("responseBody", {}).get("rows", []) if result else []
    items = [item[0] for item in rows]
    return items


def envia_cs(dados, endpoint_cs):
    """
    Envia uma lista de dados em formato JSON para o endpoint da API CS50 Integração.

    A função espera que `dados` seja uma lista contendo uma única string JSON,
    que será convertida para objeto Python e enviada via POST para o endpoint fornecido.

    Args:
        dados (list): Lista contendo uma string JSON como primeiro elemento.
        Exemplo ['[{"chave": "valor"}]']
        endpoint_cs (str): Parte final da URL do endpoint da CS50Integração
        que será usado na requisição.

    Returns:
        Response | None: Retorna o objeto `requests.Response` se a requisição for bem-sucedida,
                         ou None em caso de erro na conversão do JSON ou falha na requisição.
    """
    baseurl = 'https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/'
    url = f"{baseurl}{endpoint_cs}?In_Tenant_ID=288"
    headers = {
        "Content-Type": "application/json"
    }

    if not dados:
        logging.error("Lista de dados está vazia.")
        return None

    try:
        # dados_parceiro[0] é a string JSON que está dentro da lista
        lista_de_dicts = json.loads(dados[0])  # transforma a string JSON em objeto Python
        response = requests.post(url, json=lista_de_dicts, headers=headers, timeout=15)
        # logging.info(f"Status code: {response.status_code} | Resposta: {response.text}")
        return response

    except json.JSONDecodeError as e:
        logging.info("Erro ao decodificar JSON: %s", e)
        return None

    except requests.exceptions.RequestException as e:
        logging.info("Erro ao enviar requisição: %s", e)
        return None


def executa_atualizacoes(query_codigos, query_dados, endpoint_cs, descricao):
    """
    Executa o processo de sincronização de dados entre a API Sankhya e a API iCorp.

    A função consulta códigos na Sankhya com uma primeira query (`query_codigos`), itera sobre cada
    código e utiliza outra query (`query_dados`) para obter os dados específicos daquele item. Esses
    dados são então enviados para um endpoint específico da iCorp.

    Durante o processo, logs são gerados para indicar o início e fim da execução, e uma barra de
    progresso é exibida com a biblioteca `tqdm`.

    Args:
        Nomes de query sempre sem extensão

        query_codigos (str): Nome da query SQL usada para obter os códigos base.
        query_dados (str): Nome da query SQL usada para obter os dados completos de cada código.
        endpoint_cs (str): Identificador do endpoint iCorp para onde os dados serão enviados.
        descricao (str): Descrição textual do tipo de dado sendo processado
                         (ex: "produto", "cliente"), usada nos logs e na barra de progresso.
    """
    sankhya_service = "DbExplorerSP.executeQuery"

    codigos = consulta_sankhya(query_codigos, sankhya_service)
    logging.info("Iniciando cadastro|atualização de %s", descricao)
    for codigo in tqdm(codigos, desc=descricao.capitalize() + 's', unit=descricao):
        dados_consulta = consulta_sankhya(query_dados, sankhya_service, codigo)
        envia_cs(dados_consulta, endpoint_cs)
    logging.info("Finalizando cadastro|atualização de %s\n", descricao)


if __name__ == "__main__":
    executa_atualizacoes('PARCEIROS', 'JSON_PARCEIRO', 'Cliente', 'parceiro')
    executa_atualizacoes('PRODUTOS', 'JSON_PRODUTO', 'ProdutoUpdate', 'produto')
    executa_atualizacoes('PRODUTOS', 'JSON_PRODUTO', 'Saldos_Atualiza', 'estoque')
