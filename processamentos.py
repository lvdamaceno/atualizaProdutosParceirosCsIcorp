import logging
import time

from icorp_api.cs_sender import cs_processar_envio_parceiro, cs_processar_envio_generico
from sankhya_api.sankhya_fetch import snk_fetch_data

from utils import logging_config

logging_config()

def processar_parceiros(step, lote, workers):
    query_total_parceiros = """ SELECT COUNT(CODPARC) TOTAL FROM TGFPAR WHERE ATIVO = 'S'
    AND CGC_CPF IS NOT NULL AND CLIENTE = 'S'
    """

    total_parceiros = snk_fetch_data(query_total_parceiros)[0][0]
    logging.info(f"ℹ️ Total de Parceiros: {total_parceiros}")
    j = 1
    inicio = time.time()

    # Processamento de parceiros
    for i in range(0, total_parceiros, step):
        logging.info("=" * 42)
        decorrido = (time.time() - inicio) / 60  # em minutos
        logging.info(f"🔄 Tempo decorrido: {decorrido:.2f} minutos")
        logging.info(f"ℹ️ Lote: {j} de {(total_parceiros-i)//step}")
        logging.info(f"ℹ️ Faltam: {total_parceiros - i} parceiros")
        query_detalhe_parceiro = f"""SELECT CODPARC FROM TGFPAR WHERE ATIVO = 'S' AND CGC_CPF IS NOT NULL 
        AND CLIENTE = 'S' ORDER BY CODPARC OFFSET {i} ROWS FETCH NEXT {step} ROWS ONLY
    """
        cs_processar_envio_parceiro(query_detalhe_parceiro, tamanho_lote=lote, max_workers=workers)
        j += 1

def processar_produtos(step, lote, workers):
    query_total_produtos = """
        SELECT COUNT(DISTINCT ITE.CODPROD)
        FROM TGFITE ITE
        INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
        INNER JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD
        WHERE PRO.ATIVO = 'S' AND PRO.USOPROD = 'R'
    """

    total_produtos = snk_fetch_data(query_total_produtos)[0][0]
    logging.info(f"ℹ️ Total de Produtos: {total_produtos}")
    j = 1
    inicio = time.time()

    for i in range(0, total_produtos, step):
        logging.info("=" * 42)
        logging.info(f"ℹ️ Lote: {j} de {(total_produtos - i) // step}")
        decorrido = (time.time() - inicio) / 60  # em minutos
        logging.info(f"🔄 Tempo decorrido: {decorrido:.2f} minutos")
        logging.info(f"ℹ️ Faltam: {total_produtos - i} produtos")
        query_detalhe_produto = f"""
            SELECT DISTINCT ITE.CODPROD FROM TGFITE ITE
            INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
            INNER JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD
            WHERE PRO.ATIVO = 'S' AND PRO.USOPROD = 'R'
            ORDER BY ITE.CODPROD
            OFFSET {i} ROWS FETCH NEXT {step} ROWS ONLY
        """
        # Processamento de produtos
        cs_processar_envio_generico("produto", query_detalhe_produto,
                                    tamanho_lote= lote, max_workers=workers)
        # Processamento de estoque dos mesmos produtos
        cs_processar_envio_generico("estoque", query_detalhe_produto,
                                    tamanho_lote= lote, max_workers=workers)
        j += 1