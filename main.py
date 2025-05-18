from processamentos import *


def envio_geral(step, lote, workers):
    processar_produtos(step, lote, workers)
    processar_parceiros(step, lote, workers)


if __name__ == "__main__":
    envio_geral(10, 10, 50)
