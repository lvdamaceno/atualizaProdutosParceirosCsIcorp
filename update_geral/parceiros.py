import json
import logging
import time

from dotenv import load_dotenv

from update_geral.cs_sender import cs_enviar_cliente
from update_geral.sankhya_fetch import snk_fetch_data, snk_fetch_json
from update_geral.utils import util_remove_brackets, logging_config

load_dotenv()
logging_config()

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
            json_raw = snk_fetch_json(codparc, "parceiro")
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
