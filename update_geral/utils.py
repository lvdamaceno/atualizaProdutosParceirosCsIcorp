import logging
import os


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
