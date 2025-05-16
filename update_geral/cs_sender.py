import logging
import os
import time

import requests


def cs_enviar_cliente(dados: list[dict]) -> dict:
    """
    Envia dados de cliente para a API CS50Integração.
    Tenta até 5 vezes em caso de falhas.

    :param dados: Lista de dicionários com dados do(s) cliente(s)
    :return: Resposta da API (dict)
    """
    tenant_id = os.getenv("CS_TENANT")
    url = f"https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/Cliente?In_Tenant_ID={tenant_id}"
    headers = {
        "Content-Type": "application/json"
    }

    tentativas = 5
    for tentativa in range(1, tentativas + 1):
        try:
            logging.info(f"📤 Enviando cliente(s) para CS (tentativa {tentativa}/{tentativas})...")
            response = requests.post(url, headers=headers, json=dados, timeout=120)

            if response.status_code == 200:
                logging.info("✅ Cliente(s) enviado com sucesso.")
                return response.json()
            else:
                logging.warning(f"⚠️ Erro HTTP {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Erro na tentativa {tentativa}: {e}")

        time.sleep(tentativa * 2)  # espera incremental: 2s, 4s, 6s...

    return {"erro": f"Falha após {tentativas} tentativas ao enviar cliente para CS."}


def cs_enviar_produto(dados: list[dict]) -> dict:
    """
    Envia dados de cliente para a API CS50Integração.
    Tenta até 5 vezes em caso de falhas.

    :param dados: Lista de dicionários com dados do(s) cliente(s)
    :return: Resposta da API (dict)
    """
    tenant_id = os.getenv("CS_TENANT")
    url = f"https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/ProdutoUpdate?In_Tenant_ID={tenant_id}"
    headers = {
        "Content-Type": "application/json"
    }

    tentativas = 5
    for tentativa in range(1, tentativas + 1):
        try:
            logging.info(f"📤 Enviando produto(s) para CS (tentativa {tentativa}/{tentativas})...")
            response = requests.post(url, headers=headers, json=dados, timeout=120)

            if response.status_code == 200:
                logging.info("✅ Produto(s) enviado com sucesso.")
                return response.json()
            else:
                logging.warning(f"⚠️ Erro HTTP {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Erro na tentativa {tentativa}: {e}")

        time.sleep(tentativa * 2)  # espera incremental: 2s, 4s, 6s...

    return {"erro": f"Falha após {tentativas} tentativas ao enviar produto para CS."}
