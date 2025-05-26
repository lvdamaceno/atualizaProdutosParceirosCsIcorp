import time
import logging
from processamentos import processar_produtos, processar_parceiros
from telegram_notification import enviar_notificacao_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(Y-%m-%d %H:%M:%S) - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def envio_geral(step: int, lote: int, workers: int) -> None:
    """
    Executa o processamento de produtos e parceiros,
    mede a duração total e envia notificações via Telegram.
    """
    start_time = time.perf_counter()
    logging.info("🚀 Iniciando atualização geral Sankhya-Icorp...")
    enviar_notificacao_telegram("🚀 Atualização geral iniciada")

    # Processar produtos
    try:
        processar_produtos(step, lote, workers)
    except Exception as e:
        logging.error(f"❌ Erro no processamento de PRODUTOS: {e}", exc_info=True)
        enviar_notificacao_telegram(f"❌ Falha no processo de produtos: {e}")

    # Processar parceiros
    try:
        processar_parceiros(step, lote, workers)
    except Exception as e:
        logging.error(f"❌ Erro no processamento de PARCEIROS: {e}", exc_info=True)
        enviar_notificacao_telegram(f"❌ Falha no processo de parceiros: {e}")

    # Finalizar e notificar
    elapsed = time.perf_counter() - start_time
    mins, secs = divmod(int(elapsed), 60)
    tempo_formatado = f"{mins}m{secs:02d}s" if mins else f"{secs}s"

    mensagem = (
        f"🏁 Atualização geral finalizada\n"
        f"⏱️ Duração total: {tempo_formatado}"
    )
    logging.info(mensagem)
    enviar_notificacao_telegram(mensagem)


if __name__ == "__main__":
    # parâmetros padrão ou via variáveis de ambiente
    STEP = 50
    LOTE = 10
    WORKERS = 35
    envio_geral(STEP, LOTE, WORKERS)
