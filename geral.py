import time
from processamentos import *
from telegram_notification import enviar_notificacao_telegram


def envio_geral(step, lote, workers):
    inicio = time.time()

    processar_produtos(step, lote, workers)
    processar_parceiros(step, lote, workers)

    fim = time.time()
    duracao = fim - inicio

    minutos = int(duracao // 60)
    segundos = int(duracao % 60)
    tempo_formatado = f"{minutos}m {segundos}s" if minutos else f"{segundos}s"

    mensagem = f"🏁 Atualização geral Sankhya-Icorp finalizada\n⏱️ Tempo total: {tempo_formatado}"
    enviar_notificacao_telegram(mensagem)


if __name__ == "__main__":
    enviar_notificacao_telegram("🚀 Atualização geral Sankhya-Icorp iniciada")
    envio_geral(50, 10, 35)
