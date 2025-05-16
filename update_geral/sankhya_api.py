import logging
import os
import time

import requests
from requests import RequestException, Timeout


def snk_fetch_data(token, sql):
    url = f"{os.getenv('SANKHYA_BASE_URL')}/gateway/v1/mge/service.sbr"

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
            response = requests.get(url, headers=headers, params=params, json=payload, timeout=60)

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