from .auth import SankhyaAuth
import time
import requests
import logging
from requests.exceptions import ReadTimeout, ConnectionError


class SankhyaClient:
    def __init__(self, servicename, endpoint, retries, timeout=30):
        self.auth = SankhyaAuth()
        self.token = self.auth.authenticate()
        self.endpoint = endpoint
        self.timeout = timeout
        self.retries = retries
        self.servicename = servicename

    def execute_query(self, sql: str):
        payload = {
            "serviceName": self.servicename,
            "requestBody": {"sql": sql}
        }
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        for attempt in range(self.retries):
            try:
                response = requests.get(self.endpoint, headers=headers, json=payload, timeout=self.timeout)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in (401, 403):
                    self.token = self.auth.authenticate()
                    headers["Authorization"] = f"Bearer {self.token}"
                    continue  # Tenta de novo com novo token
                else:
                    raise Exception(f"Erro HTTP {response.status_code}: {response.text}")
            except (ReadTimeout, ConnectionError) as e:
                logging.info(f"[{attempt + 1}/{self.retries}] Timeout ou erro de conexão: {e}")
                time.sleep(5)  # espera antes de tentar de novo
            except Exception as e:
                raise Exception(f"Erro geral de conexão: {e}")

        raise Exception("Falha após múltiplas tentativas de conexão.")
