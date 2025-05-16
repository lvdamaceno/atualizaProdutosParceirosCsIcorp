import logging
import os
import time

import requests
from requests import RequestException, Timeout
from snk_auth import SankhyaClient
from utils import util_query_name

snk = SankhyaClient()


def snk_fetch_data(sql):
    url = f"{os.getenv('SANKHYA_BASE_URL')}/gateway/v1/mge/service.sbr"
    token = snk.gerar_token()

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    params = {
        "serviceName": "DbExplorerSP.executeQuery",
        "outputType": "json"
    }

    payload = {
        "serviceName": "DbExplorerSP.executeQuery",
        "requestBody": {
            "sql": sql
        }
    }

    tentativas = 5
    for tentativa in range(1, tentativas + 1):
        try:
            response = requests.get(url, headers=headers, params=params, json=payload, timeout=30)

            if response.status_code != 200:
                raise Exception(f"Erro ao consultar parceiros: {response.status_code} - {response.text}")

            data = response.json()
            return data['responseBody']['rows']

        except Timeout:
            logging.warning(f"⏱️ Timeout na tentativa {tentativa}/{tentativas}")
        except RequestException as e:
            logging.warning(f"⚠️ Erro de requisição na tentativa {tentativa}/{tentativas}: {e}")

        time.sleep(tentativa * 2)

    raise Exception(f"❌ Todas as {tentativas} tentativas de consulta falharam.")


def snk_fetch_json(codigo: int, tipo: str) -> str:
    # Define que tipo de consulta será feito no banco
    query = util_query_name(tipo)
    logging.debug(f"🔍 Buscando dados do {tipo} {codigo}")
    sql = f"SELECT sankhya.CC_CS_JSON_{query}({codigo})"
    try:
        data = snk_fetch_data(sql)

        if not data or not data[0]:
            raise ValueError(f"Nenhum dado retornado para o {tipo} {codigo}")
        row = data[0][0]
        logging.debug(f"🔹 Json do {tipo} {codigo}: {row}")
        return row
    except Exception as e:
        logging.error(f"❌ Erro ao buscar JSON do {tipo} {codigo}: {e}")
        return ""
