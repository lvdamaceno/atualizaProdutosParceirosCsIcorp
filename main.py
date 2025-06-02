import argparse
import time
import logging
import re

from processamentos import processar_parceiros, processar_produtos
from utils import logging_config
from telegram_notification import enviar_notificacao_telegram

logging_config()


def build_time_window_sql(query: str, tempo: int) -> str:
    """
    Adiciona filtro de janela de tempo em minutos à query base,
    posicionando o WHERE antes do ORDER BY (se houver) e evitando typos.
    """
    time_filter = f"DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())"

    # procura por ORDER BY
    order_by_match = re.search(r'\bORDER\s+BY\b', query, flags=re.IGNORECASE)
    if order_by_match:
        idx = order_by_match.start()
        before = query[:idx].rstrip()
        after = query[idx:].lstrip()

        # se já existe WHERE, insere AND; senão, adiciona WHERE
        if re.search(r'\bWHERE\b', before, flags=re.IGNORECASE):
            before = re.sub(
                r'\bWHERE\b',
                f"WHERE {time_filter} AND",
                before,
                count=1,
                flags=re.IGNORECASE
            )
        else:
            before = f"{before} WHERE {time_filter}"

        return f"{before} {after}"

    # não há ORDER BY
    if re.search(r'\bWHERE\b', query, flags=re.IGNORECASE):
        return re.sub(
            r'\bWHERE\b',
            f"WHERE {time_filter} AND",
            query,
            count=1,
            flags=re.IGNORECASE
        )

    return f"{query.rstrip()} WHERE {time_filter}"


def gerar_query_parceiros(tempo: int) -> tuple[str, str]:
    """
    Retorna tupla (query_total, query_detalhe) para parceiros filtrados por tempo.
    """
    base_total = "SELECT COUNT(CODPARC) AS TOTAL FROM TGFPAR"
    base_detalhe = "SELECT CODPARC FROM TGFPAR ORDER BY CODPARC"
    return (
        build_time_window_sql(base_total, tempo),
        build_time_window_sql(base_detalhe, tempo)
    )


def gerar_query_produtos(tempo: int) -> tuple[str, str]:
    """
    Retorna tupla (query_total, query_detalhe) para produtos filtrados por tempo.
    """
    total_sql = (
        "SELECT COUNT(CODPROD) AS TOTAL FROM ("
        "    SELECT CODPROD FROM TGFPRO "
        "    UNION "
        "    SELECT CODPROD FROM TGFITE ITE INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA "
        "    UNION "
        "    SELECT LTRIM(SUBSTRING(CHAVE, CHARINDEX('CODPROD=', CHAVE)+8,"
        "           CHARINDEX('CODLOCAL=', CHAVE)-(CHARINDEX('CODPROD=', CHAVE)+8))) AS CODPROD "
        "    FROM TSILGT WHERE NOMETAB='TGFEXC'"
        ") AS P"
    )
    detalhe_sql = (
        "SELECT CODPROD FROM TGFPRO "
        "UNION "
        "SELECT CODPROD FROM TGFITE ITE INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA "
        "UNION "
        "SELECT LTRIM(SUBSTRING(CHAVE, CHARINDEX('CODPROD=', CHAVE)+8,"
        "           CHARINDEX('CODLOCAL=', CHAVE)-(CHARINDEX('CODPROD=', CHAVE)+8))) AS CODPROD "
        "FROM TSILGT WHERE NOMETAB='TGFEXC' "
        "ORDER BY CODPROD"
    )
    return (
        build_time_window_sql(total_sql, tempo),
        build_time_window_sql(detalhe_sql, tempo)
    )


def envio_fragmentado(
    step: int,
    lote: int,
    workers: int,
    tempo: int
) -> None:
    """
    Executa o processamento fragmentado de parceiros e produtos
    dentro de uma janela de tempo especificada.
    """
    start = time.perf_counter()
    msg = f"🚀 Início envio fragmentado últimos {tempo}m"
    logging.info(msg)
    enviar_notificacao_telegram(msg)

    # Processa parceiros
    total_sql, detalhe_sql = gerar_query_parceiros(tempo)
    logging.debug(f"SQL Total Parceiros: {total_sql}")
    logging.debug(f"SQL Detalhe Parceiros: {detalhe_sql}")
    processar_parceiros(step, lote, workers, total_sql, detalhe_sql)
    logging.info("✅ Parceiros fragmentados completos")

    # Processa produtos
    total_sql, detalhe_sql = gerar_query_produtos(tempo)
    logging.debug(f"SQL Total Produtos: {total_sql}")
    logging.debug(f"SQL Detalhe Produtos: {detalhe_sql}")
    processar_produtos(step, lote, workers, total_sql, detalhe_sql)
    logging.info("✅ Produtos fragmentados completos")

    # Tempo total
    elapsed = time.perf_counter() - start
    mins, secs = divmod(int(elapsed), 60)
    msg = f"🏁 Fragmentado concluído em {mins}m{secs:02d}s"
    logging.info(msg)
    enviar_notificacao_telegram(msg)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", type=int, default=100)
    parser.add_argument("--lote", type=int, default=100)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--tempo", type=int, default=15)
    args = parser.parse_args()

    envio_fragmentado(
        step=args.step,
        lote=args.lote,
        workers=args.workers,
        tempo=args.tempo
    )
