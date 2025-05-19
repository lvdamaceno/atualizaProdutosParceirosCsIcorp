from processamentos import *
from telegram_notification import enviar_notificacao_telegram


def envio_geral(step, lote, workers):
    processar_produtos(step, lote, workers)
    processar_parceiros(step, lote, workers)
    enviar_notificacao_telegram("🏁 Atualização geral Sankhya-Icorp finalizada")


if __name__ == "__main__":
    enviar_notificacao_telegram("🚀 Atualização geral Sankhya-Icorp iniciada")
    envio_geral(50, 10, 35)
