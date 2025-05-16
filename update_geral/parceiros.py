import json
import logging
import os
import time

import requests
from dotenv import load_dotenv
from requests.exceptions import Timeout, RequestException

from update_geral.utils import util_remove_brackets

load_dotenv()
env = os.getenv('DEBUG_LOGS')

if env == '1':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
else:
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s')

logging.debug(f"Valor da variável de ambiente APP_ENV: '{env}'")

# 🔐 Cache do token
TOKEN_SNK = None


def snk_gerar_token():
    global TOKEN_SNK
    if TOKEN_SNK:
        return TOKEN_SNK

    url = f"{os.getenv('SANKHYA_BASE_URL')}/login"

    headers = {
        "token": os.getenv("SANKHYA_TOKEN"),
        "appkey": os.getenv("SANKHYA_APPKEY"),
        "username": os.getenv("SANKHYA_USERNAME"),
        "password": os.getenv("SANKHYA_PASSWORD"),
    }

    response = requests.post(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Erro ao autenticar: {response.status_code} - {response.text}")

    data = response.json()
    TOKEN_SNK = data['bearerToken']
    logging.debug("🔐 Token gerado com sucesso.")
    return TOKEN_SNK


def snk_fetch_data(sql):
    url = f"{os.getenv('SANKHYA_BASE_URL')}/gateway/v1/mge/service.sbr"
    token = snk_gerar_token()

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


def snk_fetch_lista_parceiros(sql) -> list[int]:
    try:
        data = snk_fetch_data(sql)
        lista_parceiros = [row[0] for row in data]
        logging.debug(f"🔸 Lista de parceiros: {lista_parceiros}")
        return lista_parceiros
    except Exception as e:
        logging.error(f"❌ Falha ao buscar lista de parceiros: {e}")
        return []


def snk_fetch_json_parceiro(codparc: int) -> str:
    """
    Retorna o JSON bruto (string) do parceiro, chamando a função CC_CS_JSON_PARCEIRO.
    """
    sql = f"SELECT sankhya.CC_CS_JSON_PARCEIRO({codparc})"
    try:
        data = snk_fetch_data(sql)
        if not data or not data[0]:
            raise ValueError(f"Nenhum dado retornado para parceiro {codparc}")
        row = data[0][0]
        logging.debug(f"🔹 Json do parceiro {codparc}: {row}")
        return row
    except Exception as e:
        logging.error(f"❌ Erro ao buscar JSON do parceiro {codparc}: {e}")
        return ""


def snk_fetch_json_completo(sql):
    parceiros = snk_fetch_lista_parceiros(sql)

    json_parceiros = []
    for parceiro in parceiros:
        json_parceiro = snk_fetch_json_parceiro(parceiro)
        json_parceiro_limpo = util_remove_brackets(json_parceiro)
        try:
            dicionario = json.loads(json_parceiro_limpo)
            json_parceiros.append(dicionario)
        except json.JSONDecodeError as e:
            logging.error(f"❌ Erro ao decodificar JSON do parceiro {parceiro}: {e}")
            continue

    # Exibe o JSON final em lista
    logging.debug("📝 Json pronto para enviar para o CS")
    logging.debug(json.dumps(json_parceiros, indent=2, ensure_ascii=False))

    return json_parceiros


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

def cs_processar_envio_parceiro(sql, tamanho_lote: int = 100):
    codparcs = snk_fetch_data(sql)
    total = len(codparcs)
    inicio_total = time.time()

    for i in range(0, total, tamanho_lote):
        lote = codparcs[i:i + tamanho_lote]
        json_lote = []
        inicio_lote = time.time()

        for row in lote:
            codparc = row[0]
            json_raw = snk_fetch_json_parceiro(codparc)
            json_limpo = util_remove_brackets(json_raw)
            try:
                json_dict = json.loads(json_limpo)
                json_lote.append(json_dict)
            except Exception as e:
                logging.warning(f"❌ Erro ao decodificar JSON do parceiro {codparc}: {e}")
                continue

        logging.info(f"📦 Enviando lote {i // tamanho_lote + 1} com {len(json_lote)} registros...")
        resposta = cs_enviar_cliente(json_lote)
        logging.debug(f"⚠️ Resposta da CS: {resposta}")

        # Tempo estimado restante
        duracao_lote = time.time() - inicio_lote
        restantes = total - (i + len(lote))
        estimativa_restante = (duracao_lote / len(lote)) * restantes if lote else 0
        logging.info(f"✅ Lote enviado - ⏱️ Estimativa restante: {estimativa_restante:.1f} segundos")

    duracao_total = time.time() - inicio_total
    logging.info(f"🏁 Processamento completo em {duracao_total:.1f} segundos.")

if __name__ == "__main__":
    lista_parceiros_para_enviar = "SELECT TOP 5 CODPARC FROM TGFPAR WHERE CGC_CPF IN (SELECT CPF FROM TFPFUN)"
    cs_processar_envio_parceiro(lista_parceiros_para_enviar, tamanho_lote=50)
