import json
import logging
import os
import time

import requests
from dotenv import load_dotenv

from sankhya_fetch import snk_fetch_data, snk_fetch_json
from utils import util_cs_enpoint, util_remove_brackets, tempo_restante, logging_config

load_dotenv()
logging_config()

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


def cs_processar_envio_parceiro(sql, tamanho_lote: int = 100):
    logging.info("=" * 60)
    logging.info(f"👤 INICIANDO PROCESSAMENTO DE PARCEIRO...")
    logging.info("=" * 60)
    codparcs = snk_fetch_data(sql)
    total = len(codparcs)
    inicio_total = time.time()

    for i in range(0, total, tamanho_lote):
        lote = codparcs[i:i + tamanho_lote]
        json_lote = []
        inicio_lote = time.time()

        for row in lote:
            codparc = row[0]
            json_raw = snk_fetch_json(codparc, "parceiro")
            json_limpo = util_remove_brackets(json_raw)
            try:
                json_dict = json.loads(json_limpo)
                json_lote.append(json_dict)
            except Exception as e:
                logging.warning(f"❌ Erro ao decodificar JSON do parceiro {codparc}: {e}")
                continue

        logging.info(f"📦 Enviando lote {i // tamanho_lote + 1} com {len(json_lote)} registros...")
        resposta = cs_enviar(json_lote, "parceiro")

        tempo_restante(i, lote, inicio_lote, total, resposta)

    duracao_total = time.time() - inicio_total
    logging.info(f"🏁 Processamento completo em {duracao_total:.1f} segundos.")


def cs_processar_envio(tipo, sql, tamanho_lote: int = 100):
    logging.info("=" * 60)
    logging.info(f"🟢 INICIANDO PROCESSAMENTO DE {tipo.upper()}...")
    logging.info("=" * 60)
    codprods = snk_fetch_data(sql)
    total = len(codprods)
    inicio_total = time.time()

    for i in range(0, total, tamanho_lote):
        lote = codprods[i:i + tamanho_lote]
        json_lote = []
        inicio_lote = time.time()

        for row in lote:
            codprod = row[0]
            json_raw = snk_fetch_json(codprod, tipo)
            try:
                json_corrigido = f"[{json_raw}]".replace("}{", "},{")
                lista = json.loads(json_corrigido)
                json_lote.extend(lista)
            except Exception as e:
                logging.warning(f"❌ Erro ao decodificar JSON do {tipo} {codprod}: {e}")
                continue

        # Json completo
        # logging.debug(json.dumps(json_lote, indent=2, ensure_ascii=False))

        logging.info(f"📦 Enviando lote {i // tamanho_lote + 1} com {len(json_lote)} registros...")
        resposta = cs_enviar(json_lote, tipo)

        tempo_restante(i, lote, inicio_lote, total, resposta)

    duracao_total = time.time() - inicio_total
    logging.info(f"🏁 Processamento completo de {tipo} em {duracao_total:.1f} segundos.")