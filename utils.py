import logging
import os
import time


def logging_config():
    env = os.getenv('DEBUG_LOGS')

    if env == '1':
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s')

    logging.debug(f"Valor da variável de ambiente APP_ENV: '{env}'")

def util_remove_brackets(json_parc: str):
    return json_parc.strip("[]")


def util_query_name(tipo: str) -> str:
    mapa = {
        "parceiro": "PARCEIRO",
        "produto": "PRODUTO",
        "estoque": "ESTOQUE",
    }
    try:
        return mapa[tipo]
    except KeyError:
        raise ValueError(f"❌ Tipo de query inválido: '{tipo}'")

def tempo_restante(i, lote, inicio_lote, total, resposta):
    logging.debug(f"⚠️ Resposta da CS: {resposta}")
    # Tempo estimado restante
    duracao_lote = time.time() - inicio_lote
    restantes = total - (i + len(lote))
    estimativa_restante = (duracao_lote / len(lote)) * restantes if lote else 0
    logging.info(f"✅ Lote enviado - ⏱️ Estimativa restante: {estimativa_restante:.1f} segundos")


def util_cs_enpoint(tipo: str) -> str:
    mapa = {
        "parceiro": "Cliente",
        "produto": "ProdutoUpdate",
        "estoque": "Saldos_Atualiza",
    }
    try:
        return mapa[tipo]
    except KeyError:
        raise ValueError(f"❌ Endpoint inválido: '{tipo}'")