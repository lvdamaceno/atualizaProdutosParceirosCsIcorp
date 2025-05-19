from processamentos import *


def envio_fragmentado(step, lote, workers, tempo):
    query_total_parceiros = f""" 
        SELECT COUNT(CODPARC) TOTAL 
        FROM TGFPAR WHERE ATIVO = 'S'
        AND CGC_CPF IS NOT NULL AND CLIENTE = 'S'
        AND DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
    """

    query_detalhe_parceiros = f""" 
        SELECT CODPARC 
        FROM TGFPAR WHERE ATIVO = 'S' 
        AND CGC_CPF IS NOT NULL AND CLIENTE = 'S'
        AND DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
        ORDER BY CODPARC 
    """

    processar_parceiros_fragmentos_hora(step, lote, workers, query_total_parceiros,query_detalhe_parceiros)

    query_total_produtos = f"""
        SELECT COUNT(CODPROD) TOTAL FROM (
        SELECT CODPROD FROM TGFPRO
        WHERE DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
        UNION
        SELECT CODPROD FROM TGFITE ITE
        INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
        WHERE ITE.DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
        UNION
        SELECT LTRIM(SUBSTRING(CHAVE,CHARINDEX('CODPROD=', CHAVE) + 8,
        CHARINDEX('CODLOCAL=', CHAVE) - (CHARINDEX('CODPROD=', CHAVE) + 8))) AS CODPROD
        FROM TSILGT
        WHERE NOMETAB = 'TGFEXC'
        AND DHACAO >= DATEADD(MINUTE, -{tempo}, GETDATE())
        ) AS D
    """

    query_detalhe_produtos = f"""
        SELECT CODPROD FROM TGFPRO
        WHERE DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
        UNION
        SELECT CODPROD FROM TGFITE ITE
        INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
        WHERE ITE.DTALTER >= DATEADD(MINUTE, -{tempo}, GETDATE())
        UNION
        SELECT LTRIM(SUBSTRING(CHAVE,CHARINDEX('CODPROD=', CHAVE) + 8,
        CHARINDEX('CODLOCAL=', CHAVE) - (CHARINDEX('CODPROD=', CHAVE) + 8))) AS CODPROD
        FROM TSILGT
        WHERE NOMETAB = 'TGFEXC'
        AND DHACAO >= DATEADD(MINUTE, -{tempo}, GETDATE())
        ORDER BY CODPROD
    """
    processar_produtos_fragmentos_hora(step, lote, workers, query_total_produtos, query_detalhe_produtos)


if __name__ == "__main__":
    envio_fragmentado(10, 10, 20, 3000)