import os
import time
from tqdm import tqdm
import logging
import requests
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ------------------- Configurações e utilitários -------------------

def load_credentials(config_file='config.json'):
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.critical(f"Erro ao carregar config: {e}")
        return None


credentials = load_credentials()
if not credentials:
    raise SystemExit("Credenciais inválidas ou inexistentes")

url_auth = "https://api.sankhya.com.br/login"
url_query = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=DbExplorerSP.executeQuery&outputType=json"

token_cache = {"token": None}


def auth(max_retries=3, delay=3):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(url_auth, headers=credentials)
            if response.status_code == 200:
                token = response.json().get("bearerToken")
                if token:
                    token_cache["token"] = token
                    return token
            logger.warning(f"[{attempt}] Erro de autenticação: {response.status_code} - {response.text}")
        except requests.RequestException as e:
            logger.error(f"[{attempt}] Exceção: {e}")
        time.sleep(delay)
    raise SystemExit("Falha ao autenticar após várias tentativas")


# ------------------- Requisições à API -------------------

def get_data(query, max_attempts=5):
    if not token_cache["token"]:
        auth()

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token_cache['token']}"
    }
    payload = {
        "serviceName": "DbExplorerSP.executeQuery",
        "requestBody": {"sql": query}
    }

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url_query, headers=headers, json=payload, timeout=10)

            if response.status_code == 200:
                return response.json()

            elif response.status_code in [401, 403]:
                logger.warning(f"[{attempt}] Token inválido/expirado, renovando...")
                auth()  # força renovação do token
                headers["Authorization"] = f"Bearer {token_cache['token']}"

            else:
                logger.warning(f"[{attempt}] Erro HTTP {response.status_code} - {response.text}")
        except requests.exceptions.Timeout:
            logger.warning(f"[{attempt}] Timeout. Tentando novamente...")
        except requests.RequestException as e:
            logger.error(f"[{attempt}] Erro de requisição: {e}")
        time.sleep(7)
    return None


# ------------------- Execução principal -------------------


def load_query(nome, **params):
    caminho = os.path.join("queries", f"{nome}.sql")
    with open(caminho, "r", encoding="utf-8") as file:
        query = file.read()
        return query.format(**params)


def consulta_sankhya(nome_query, item=None, tempo=-1):
    if "JSON" not in nome_query:
        query = load_query(nome_query, tempo=tempo)
    else:
        query = load_query(nome_query, item=item)
    result = get_data(query, 5)
    rows = result.get("responseBody", {}).get("rows", []) if result else []
    items = [item[0] for item in rows]
    return items


def envia_cs(dados, endpoint):
    url = f"https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/{endpoint}?In_Tenant_ID=288"
    headers = {
        "Content-Type": "application/json"
    }

    if not dados:
        print("Lista de dados está vazia.")
        return None

    try:
        # dados_parceiro[0] é a string JSON que está dentro da lista
        lista_de_dicts = json.loads(dados[0])  # transforma a string JSON em objeto Python

        # Faz a requisição POST corretamente com json= em vez de data=
        response = requests.post(url, json=lista_de_dicts, headers=headers)

        # logging.info(f"Status code: {response.status_code} | Resposta: {response.text}")

        return response

    except json.JSONDecodeError as e:
        print("Erro ao decodificar JSON:", e)
        return None
    except Exception as e:
        print("Erro ao enviar requisição:", e)
        return None


def processa_envio(nome_query, identificador, endpoint):
    try:
        dados = consulta_sankhya(nome_query, identificador)
        if not dados or not isinstance(dados, list) or not dados[0]:
            logging.warning(f"Nenhum dado retornado para {nome_query} ({identificador})")
            return
        resposta = envia_cs(dados, endpoint)
        logging.info(f"{endpoint} - {identificador} - Status: {resposta.status_code}")
    except Exception as e:
        logging.error(f"Erro ao processar {endpoint} ({identificador}): {e}")


def executa_atualizazoes():
    parceiros = consulta_sankhya('PARCEIROS')
    logging.info('Iniciando cadastro|atualização de parceiros')
    for parceiro in tqdm(parceiros, desc="Parceiros", unit="parceiro"):
        dados_parceiro = consulta_sankhya('JSON_PARCEIRO', parceiro)
        envia_cs(dados_parceiro, 'Cliente')
    logging.info('Finalizando cadastro|atualização de parceiros')

    produtos = consulta_sankhya('PRODUTOS')
    logging.info('Iniciando cadastro|atualização de produtos')
    for produto in tqdm(produtos, desc="Produtos", unit="produto"):
        dados_produto = consulta_sankhya('JSON_PRODUTO', produto)
        envia_cs(dados_produto, 'ProdutoUpdate')
    logging.info('Finalizando cadastro|atualização de produtos\n')

    logging.info('Iniciando cadastro|atualização de estoque')
    for produto in tqdm(produtos, desc="Produtos", unit="produto"):
        dados_estoque = consulta_sankhya('JSON_ESTOQUE', produto)
        envia_cs(dados_estoque, 'Saldos_Atualiza')
    logging.info('Finalizando cadastro|atualização de produtos|estoque')


if __name__ == "__main__":
    executa_atualizazoes()
