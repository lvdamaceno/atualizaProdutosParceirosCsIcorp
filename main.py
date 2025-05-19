from processamentos import processar_parceiros_fragmentos_hora, processar_produtos_fragmentos_hora



def gerar_query_parceiros_total(tempo: int) -> str:
    return f"""
        SELECT COUNT(CODPARC) AS TOTAL 
        FROM TGFPAR 
        WHERE ATIVO = 'S'
          AND CGC_CPF IS NOT NULL 
          AND CLIENTE = 'S'
          AND DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
    """


def gerar_query_parceiros_detalhe(tempo: int) -> str:
    return f"""
        SELECT CODPARC 
        FROM TGFPAR 
        WHERE ATIVO = 'S' 
          AND CGC_CPF IS NOT NULL 
          AND CLIENTE = 'S'
          AND DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
        ORDER BY CODPARC
    """


def gerar_query_produtos_total(tempo: int) -> str:
    return f"""
        SELECT COUNT(CODPROD) AS TOTAL 
        FROM (
            SELECT CODPROD FROM TGFPRO
            WHERE DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
            UNION
            SELECT CODPROD FROM TGFITE ITE
            INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
            WHERE ITE.DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
            UNION
            SELECT LTRIM(SUBSTRING(CHAVE, CHARINDEX('CODPROD=', CHAVE) + 8,
                   CHARINDEX('CODLOCAL=', CHAVE) - (CHARINDEX('CODPROD=', CHAVE) + 8))) AS CODPROD
            FROM TSILGT
            WHERE NOMETAB = 'TGFEXC'
              AND DHACAO >= DATEADD(MINUTE, -{tempo}, GETDATE())
        ) AS PRODUTOS
    """


def gerar_query_produtos_detalhe(tempo: int) -> str:
    return f"""
        SELECT CODPROD FROM TGFPRO
        WHERE DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
        UNION
        SELECT CODPROD FROM TGFITE ITE
        INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
        WHERE ITE.DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
        UNION
        SELECT LTRIM(SUBSTRING(CHAVE, CHARINDEX('CODPROD=', CHAVE) + 8,
               CHARINDEX('CODLOCAL=', CHAVE) - (CHARINDEX('CODPROD=', CHAVE) + 8))) AS CODPROD
        FROM TSILGT
        WHERE NOMETAB = 'TGFEXC'
          AND DHACAO >= DATEADD(MINUTE, -{tempo}, GETDATE())
        ORDER BY CODPROD
    """


def envio_fragmentado(step: int, lote: int, workers: int, tempo: int):
    """
    Executa o processamento fragmentado de parceiros e produtos com base na alteração
    nos últimos 'tempo' minutos.
    """
    # Parceiros
    processar_parceiros_fragmentos_hora(
        step, lote, workers,
        gerar_query_parceiros_total(tempo),
        gerar_query_parceiros_detalhe(tempo)
    )

    # Produtos
    processar_produtos_fragmentos_hora(
        step, lote, workers,
        gerar_query_produtos_total(tempo),
        gerar_query_produtos_detalhe(tempo)
    )


if __name__ == "__main__":
    envio_fragmentado(step=10, lote=10, workers=20, tempo=15)
