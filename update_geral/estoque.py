import json
import logging
import time
from dotenv import load_dotenv

from update_geral.cs_sender import cs_enviar
from update_geral.sankhya_fetch import snk_fetch_data, snk_fetch_json
from update_geral.utils import logging_config, tempo_restante

load_dotenv()
logging_config()

def cs_processar_envio_estoque(sql, tamanho_lote: int = 100):
    codprods = snk_fetch_data(sql)
    total = len(codprods)
    inicio_total = time.time()

    for i in range(0, total, tamanho_lote):
        lote = codprods[i:i + tamanho_lote]
        json_lote = []
        inicio_lote = time.time()

        for row in lote:
            codprod = row[0]
            json_raw = snk_fetch_json(codprod, "estoque")
            try:
                json_corrigido = f"[{json_raw}]".replace("}{", "},{")
                lista = json.loads(json_corrigido)
                json_lote.extend(lista)  # pois cada produto tem vários JSONs colados
            except Exception as e:
                logging.warning(f"❌ Erro ao decodificar JSON do produto {codprod}: {e}")
                continue

        logging.debug(json.dumps(json_lote, indent=2, ensure_ascii=False))

        logging.info(f"📦 Enviando lote {i // tamanho_lote + 1} com {len(json_lote)} registros...")
        resposta = cs_enviar(json_lote, "estoque")

        tempo_restante(i, lote, inicio_lote, total, resposta)

    duracao_total = time.time() - inicio_total
    logging.info(f"🏁 Processamento completo em {duracao_total:.1f} segundos.")

if __name__ == "__main__":
    lista_produtos_para_enviar = """
                                  SELECT TOP 5 CODPROD 
                                  FROM TGFPRO 
                                  WHERE CODGRUPOPROD <= '1159999' 
                                    AND USOPROD = 'R' 
                                    AND ATIVO = 'S'
                                  """
    cs_processar_envio_estoque(lista_produtos_para_enviar, tamanho_lote=50)
