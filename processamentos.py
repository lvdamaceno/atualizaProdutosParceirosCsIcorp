import logging
import time
import math

from icorp_api.cs_sender import cs_processar_envio_parceiro, cs_processar_envio_generico
from sankhya_api.sankhya_fetch import snk_fetch_data
from utils import logging_config

logging_config()

def process_batches(
    query_total: str,
    query_base: str,
    step: int,
    lote: int,
    workers: int,
    tipos: list[str]
) -> None:
    """
    Processa registros em lotes baseado em queries, executando envios CS para cada tipo.
    Se não houver registros ou ocorrer falha na consulta, retorna sem erro.
    """
    # Obtém total de registros
    try:
        result = snk_fetch_data(query_total)
        total = result[0][0] if result and result[0] else 0
    except Exception as e:
        logging.warning(f"⚠️ Falha ao obter total de registros: {e}")
        return

    if total == 0:
        logging.info("ℹ️ Nenhuma atualização encontrada. Nada a processar.")
        return

    n_batches = math.ceil(total / step)
    logging.info(f"ℹ️ Total de registros: {total}, dividido em {n_batches} lotes de {step}")

    start = time.perf_counter()

    for idx in range(n_batches):
        offset = idx * step
        elapsed = time.perf_counter() - start
        mins, secs = divmod(int(elapsed), 60)
        remaining = max(total - offset, 0)

        logging.info("=" * 42)
        logging.info(
            f"🔄 Lote {idx + 1}/{n_batches} | "
            f"Tempo decorrido: {mins}m{secs:02d}s | Faltam: {remaining}"
        )

        paged_query = f"{query_base.strip()}\nOFFSET {offset} ROWS FETCH NEXT {step} ROWS ONLY"

        for tipo in tipos:
            logging.info(f"➡️ Processando tipo '{tipo}'")
            # Verifica códigos antes de chamar o sender
            try:
                codes = snk_fetch_data(paged_query)
            except Exception as e:
                logging.warning(f"⚠️ Falha ao buscar códigos de '{tipo}': {e}")
                continue

            if not codes:
                logging.info(f"ℹ️ Sem códigos de '{tipo}' no lote {idx+1}. Pulando.")
                continue

            # Envia lote
            try:
                if tipo == "parceiro":
                    cs_processar_envio_parceiro(
                        paged_query,
                        tamanho_lote=lote,
                        max_workers=workers
                    )
                else:
                    cs_processar_envio_generico(
                        tipo,
                        paged_query,
                        tamanho_lote=lote,
                        max_workers=workers
                    )
            except Exception as e:
                logging.error(f"❌ Erro no envio de '{tipo}' no lote {idx+1}: {e}", exc_info=True)
                continue

    total_elapsed = time.perf_counter() - start
    mins, secs = divmod(int(total_elapsed), 60)
    logging.info(f"🏁 Processamento completo em {mins}m{secs:02d}s")


def processar_parceiros(
    step: int,
    lote: int,
    workers: int,
    query_total: str = None,
    query_base: str = None
) -> None:
    """
    Atualização de parceiros.
    Se query_total e query_base forem fornecidos, executa envio fragmentado,
    senão usa queries padrão semanal.
    """
    if not query_total or not query_base:
        query_total = (
            "SELECT COUNT(CODPARC) "
            "FROM TGFPAR "
            "WHERE ATIVO = 'S' "
            "AND CGC_CPF IS NOT NULL "
            "AND CLIENTE = 'S'"
        )
        query_base = (
            "SELECT CODPARC "
            "FROM TGFPAR "
            "WHERE ATIVO = 'S' "
            "AND CGC_CPF IS NOT NULL "
            "AND CLIENTE = 'S' "
            "ORDER BY CODPARC"
        )

    process_batches(
        query_total=query_total,
        query_base=query_base,
        step=step,
        lote=lote,
        workers=workers,
        tipos=['parceiro']
    )


def processar_produtos(
    step: int,
    lote: int,
    workers: int,
    query_total: str = None,
    query_base: str = None
) -> None:
    """
    Atualização de produtos.
    Se query_total e query_base forem fornecidos, executa envio fragmentado,
    senão usa queries padrão semanal para dados e estoque.
    """
    if not query_total or not query_base:
        query_total = (
            "SELECT COUNT(DISTINCT ITE.CODPROD) "
            "FROM TGFITE ITE "
            "INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA "
            "INNER JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD "
            "WHERE PRO.ATIVO = 'S' "
            "AND PRO.USOPROD = 'R'"
        )
        query_base = (
            "SELECT DISTINCT ITE.CODPROD "
            "FROM TGFITE ITE "
            "INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA "
            "INNER JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD "
            "WHERE PRO.ATIVO = 'S' "
            "AND PRO.USOPROD = 'R' "
            "ORDER BY ITE.CODPROD"
        )

    process_batches(
        query_total=query_total,
        query_base=query_base,
        step=step,
        lote=lote,
        workers=workers,
        tipos=['produto', 'estoque']
    )
