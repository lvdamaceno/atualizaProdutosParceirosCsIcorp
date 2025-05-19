import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from dotenv import load_dotenv

from sankhya_api.sankhya_fetch import snk_fetch_data, snk_fetch_json
from utils import logging_config, util_cs_enpoint, util_remove_brackets

logging_config()
load_dotenv()


def cs_enviar(dados: list[dict], tipo) -> dict:
    endpoint = util_cs_enpoint(tipo)
    tenant_id = os.getenv("CS_TENANT")
    url = f"https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/{endpoint}?In_Tenant_ID={tenant_id}"
    headers = {
        "Content-Type": "application/json"
    }

    tentativas = 5
    timeout = [120, 150, 180, 210, 240]
    for tentativa in range(1, tentativas + 1):
        try:
            logging.info(f"📤 Enviando {tipo}(s) para CS ({tentativa}/{tentativas})...")
            response = requests.post(url, headers=headers, json=dados, timeout=timeout[tentativa-1])

            if response.status_code == 200:
                logging.info(f"✅ {tipo}(s) enviado com sucesso.")
                return response.json()
            else:
                logging.warning(f"⚠️ Erro HTTP {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Erro na tentativa {tentativa}: {e}")

        time.sleep(tentativa * 2)  # espera incremental: 2s, 4s, 6s...

    return {"erro": f"Falha após {tentativas} tentativas ao enviar {tipo} para CS."}


def cs_processar_lote_parceiro(lote, indice_lote, total, tamanho_lote):
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

    logging.info(f"📦 Lote {indice_lote+1} | {len(lote)} registros...")
    resposta = cs_enviar(json_lote, "parceiro")
    logging.info(f"⚠️ Resposta da CS: {resposta}")


def cs_processar_envio_parceiro(sql, tamanho_lote: int = 100, max_workers: int = 5):
    inicio = time.time()
    logging.info("=" * 42)
    logging.info(f"👤 INICIANDO PROCESSAMENTO DE PARCEIRO...")

    codparcs = snk_fetch_data(sql)
    total = len(codparcs)
    inicio_total = time.time()

    lotes = [
        codparcs[i:i + tamanho_lote]
        for i in range(0, total, tamanho_lote)
    ]

    logging.info(f"🔢 {total} registros, {len(lotes)} lotes de {tamanho_lote}.")
    logging.info("=" * 42)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futuros = {
            executor.submit(cs_processar_lote_parceiro, lote, i, total, tamanho_lote): i
            for i, lote in enumerate(lotes)
        }

        for future in as_completed(futuros):
            indice = futuros[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"❌ Erro no lote {indice + 1}: {e}")

    duracao_total = time.time() - inicio_total
    minutos = int(duracao_total // 42)
    segundos = int(duracao_total % 42)
    logging.info(f"🏁 Processamento de parceiro completo.")
    logging.info(f"⏱️ Finalizado em {minutos}m{segundos:02d}s")


def processar_lote_generico(lote, indice_lote, total, tamanho_lote, tipo):
    json_lote = []
    inicio_lote = time.time()

    for row in lote:
        cod = row[0]
        json_raw = snk_fetch_json(cod, tipo)
        try:
            json_corrigido = f"[{json_raw}]".replace("}{", "},{")
            lista = json.loads(json_corrigido)
            json_lote.extend(lista)
        except Exception as e:
            logging.warning(f"❌ Erro ao decodificar JSON do {tipo} {cod}: {e}")
            continue

    logging.info(f"📦 Lote {indice_lote+1} | {len(lote)} registros...")
    resposta = cs_enviar(json_lote, tipo)
    logging.info(f"⚠️ Resposta da CS: {resposta}")


def cs_processar_envio_generico(tipo, sql, tamanho_lote: int = 100, max_workers: int = 5):
    logging.info("=" * 42)
    logging.info(f"🟢 INICIANDO PROCESSAMENTO DE {tipo.upper()}...")


    cods = snk_fetch_data(sql)
    total = len(cods)
    inicio_total = time.time()

    lotes = [
        cods[i:i + tamanho_lote]
        for i in range(0, total, tamanho_lote)
    ]

    logging.info(f"🔢 {total} registros, {len(lotes)} lotes de {tamanho_lote}.")
    logging.info("=" * 42)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futuros = {
            executor.submit(processar_lote_generico, lote, i, total, tamanho_lote, tipo): i
            for i, lote in enumerate(lotes)
        }

        for future in as_completed(futuros):
            indice = futuros[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"❌ Erro no lote {indice + 1} do tipo {tipo}: {e}")

    duracao_total = time.time() - inicio_total
    minutos = int(duracao_total // 60)
    segundos = int(duracao_total % 60)
    logging.info(f"🏁 Processamento de {tipo} completo.")
    logging.info(f"⏱️ Finalizado em {minutos}m{segundos:02d}s")
