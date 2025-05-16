import logging
import os
import time

import requests

from update_geral.utils import util_cs_enpoint

def cs_enviar(dados: list[dict], tipo) -> dict:
    endpoint = util_cs_enpoint(tipo)
    tenant_id = os.getenv("CS_TENANT")
    url = f"https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/{endpoint}?In_Tenant_ID={tenant_id}"
    headers = {
        "Content-Type": "application/json"
    }

    tentativas = 5
    for tentativa in range(1, tentativas + 1):
        try:
            logging.info(f"📤 Enviando {tipo}(s) para CS (tentativa {tentativa}/{tentativas})...")
            response = requests.post(url, headers=headers, json=dados, timeout=120)

            if response.status_code == 200:
                logging.info(f"✅ {tipo}(s) enviado com sucesso.")
                return response.json()
            else:
                logging.warning(f"⚠️ Erro HTTP {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Erro na tentativa {tentativa}: {e}")

        time.sleep(tentativa * 2)  # espera incremental: 2s, 4s, 6s...

    return {"erro": f"Falha após {tentativas} tentativas ao enviar {tipo} para CS."}
