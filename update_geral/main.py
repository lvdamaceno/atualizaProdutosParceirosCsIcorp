from update_geral.cs_sender import cs_processar_envio, cs_processar_envio_parceiro

if __name__ == "__main__":
    lista_parceiros_para_enviar = "SELECT TOP 5 CODPARC FROM TGFPAR WHERE CGC_CPF IN (SELECT CPF FROM TFPFUN)"
    lista_produtos_para_enviar = """
                                  SELECT TOP 1 CODPROD 
                                  FROM TGFPRO 
                                  WHERE CODGRUPOPROD <= '1159999' 
                                    AND USOPROD = 'R' 
                                    AND ATIVO = 'S'
                                  """

    cs_processar_envio_parceiro(lista_parceiros_para_enviar, tamanho_lote=50)
    cs_processar_envio("produto", lista_produtos_para_enviar, tamanho_lote=50)
    cs_processar_envio("estoque", lista_produtos_para_enviar, tamanho_lote=50)
