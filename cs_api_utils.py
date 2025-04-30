import requests
import time
import logging
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry

from utils import *

# Carrega variáveis do .env
load_dotenv()

# Configuração de log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Controle de taxa de requisições
MAX_REQUESTS_PER_MINUTE = 2000
requests_in_current_minute = 0
minute_start_time = time.time()


@sleep_and_retry
@limits(calls=30, period=1)  # 30 requisições por segundo
def envia_cs(payload, endpoint):
    """Envia dados JSON para a API da CS com controle de taxa e tentativas."""
    global requests_in_current_minute, minute_start_time

    baseurl = 'https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1/'
    url = f"{baseurl}{endpoint}?In_Tenant_ID=288"
    headers = {"Content-Type": "application/json"}
    max_retries = 5

    with requests.Session() as session:
        session.headers.update(headers)

        current_time = time.time()
        if current_time - minute_start_time >= 60:
            requests_in_current_minute = 0
            minute_start_time = current_time

        if requests_in_current_minute >= MAX_REQUESTS_PER_MINUTE:
            wait_time = 60 - (current_time - minute_start_time)
            logging.info(f"Limite de requisições por minuto atingido. Esperando {wait_time:.2f} segundos.")
            time.sleep(wait_time)
            requests_in_current_minute = 0
            minute_start_time = time.time()

        for tentativa in range(1, max_retries + 1):
            try:
                response = session.post(url, data=payload, timeout=(30, 180))
                response.raise_for_status()
                logging.info(f"{endpoint} enviado com sucesso. Status: {response.status_code}")
                requests_in_current_minute += 1
                break
            except requests.exceptions.RequestException as e:
                logging.warning(f"Tentativa {tentativa}/{max_retries} - Erro: {e}")
                if tentativa < max_retries:
                    time.sleep(10)
                else:
                    logging.error(f"Falha após {max_retries} tentativas. Dados não enviados.")
