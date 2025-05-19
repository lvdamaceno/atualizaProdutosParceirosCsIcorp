import logging

import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()


# Função para enviar notificação para o Telegram
def enviar_notificacao_telegram(mensagem):
    token = os.getenv('BOTTOKEN')  # Seu Token do Bot
    chat_id = os.getenv('CHATID')  # O chat_id do destinatário
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    # Dados a serem enviados
    payload = {
        'chat_id': chat_id,
        'text': mensagem,
        "parse_mode": "HTML"
    }

    # Enviar a requisição para o Telegram
    try:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            logging.info("Notificação enviada com sucesso!")
        else:
            logging.warning(f"Falha ao enviar notificação: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Ocorreu um erro: {e}")


def converter_data_iso_para_br(data_iso):
    # Remove excesso de zeros para ficar compatível com o fromisoformat
    if '.' in data_iso:
        parte_data, parte_fuso = data_iso.split('+')
        parte_data = parte_data[:26]  # Cortar para até microssegundos (6 casas)
        data_iso_corrigido = f"{parte_data}+{parte_fuso}"
    else:
        data_iso_corrigido = data_iso

    # Converte a string ISO corrigida para datetime
    dt = datetime.fromisoformat(data_iso_corrigido)

    # Ajusta para horário de Brasília (UTC-3)
    dt_brasilia = dt.astimezone(timezone(timedelta(hours=-3)))

    # Formata para o padrão brasileiro
    return dt_brasilia.strftime('%d/%m/%Y %H:%M:%S')

