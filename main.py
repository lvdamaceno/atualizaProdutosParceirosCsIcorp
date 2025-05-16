from icorp_api.cs_sender import cs_processar_envio, cs_processar_envio_parceiro

def main():
    # parceiros_sql = """
    #     SELECT CODPARC FROM TGFPAR
    #     WHERE TIPPESSOA = 'F' AND DTALTER >= '12/05/2025'
    #     AND CODPARC NOT IN (193) AND ATIVO = 'S'
    #     UNION
    #     SELECT CODPARC FROM TGFPAR
    #     WHERE CGC_CPF IN (SELECT CPF FROM TFPFUN)
    # """
    #
    # produtos_sql = """
    #     SELECT PRO.CODPROD FROM TGFPRO PRO
    #     WHERE PRO.DTALTER >= '12/05/2025'
    #     UNION
    #     SELECT CODPROD FROM TGFITE ITE
    #     INNER JOIN TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
    #     WHERE ITE.DTALTER >= '12/05/2025'
    #     UNION
    #     SELECT LTRIM(SUBSTRING(CHAVE,CHARINDEX('CODPROD=', CHAVE) + 8,CHARINDEX('CODLOCAL=', CHAVE) - (CHARINDEX('CODPROD=', CHAVE) + 8))) AS CODPROD
    #     FROM TSILGT
    #     WHERE NOMETAB = 'TGFEXC' AND DHACAO >= '12/05/2025'
    # """

    parceiros_sql = """
            SELECT CODPARC FROM TGFPAR WHERE CODPARC = 137
        """

    produtos_sql = """
        SELECT TOP 1 CODPROD FROM TGFPRO WHERE CODGRUPOPROD <= '1159999' AND ATIVO = 'S' AND CODPROD > 0
    """

    lote = 50

    # Processamento de parceiros
    cs_processar_envio_parceiro(parceiros_sql, tamanho_lote=lote)

    # Processamento de produtos
    cs_processar_envio("produto", produtos_sql, tamanho_lote=lote)

    # Processamento de estoque dos mesmos produtos
    cs_processar_envio("estoque", produtos_sql, tamanho_lote=lote)

if __name__ == "__main__":
    main()
