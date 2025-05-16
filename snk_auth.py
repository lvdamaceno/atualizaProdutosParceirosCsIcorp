import os
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

class SankhyaClient:
    def __init__(self):
        self.token = None

    def gerar_token(self):
        if self.token:
            return self.token

        url = f"{os.getenv('SANKHYA_BASE_URL')}/login"

        headers = {
            "token": os.getenv("SANKHYA_TOKEN"),
            "appkey": os.getenv("SANKHYA_APPKEY"),
            "username": os.getenv("SANKHYA_USERNAME"),
            "password": os.getenv("SANKHYA_PASSWORD"),
        }

        response = requests.post(url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Erro ao autenticar: {response.status_code} - {response.text}")

        data = response.json()
        self.token = data['bearerToken']
        logging.debug("🔐 Token gerado com sucesso.")
        return self.token