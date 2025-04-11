import os
import requests
from dotenv import load_dotenv

load_dotenv()


class AuthError(Exception):
    pass


class SankhyaAuth:
    def __init__(self):
        self.url_auth = "https://api.sankhya.com.br/login"
        self.token = None

    def authenticate(self):
        headers = {
            "token": os.getenv("SANKHYA_TOKEN"),
            "appkey": os.getenv("SANKHYA_APPKEY"),
            "username": os.getenv("SANKHYA_USERNAME"),
            "password": os.getenv("SANKHYA_PASSWORD")
        }
        try:
            response = requests.post(self.url_auth, headers=headers, timeout=10)
            if response.status_code == 200:
                self.token = response.json().get("bearerToken")
                if not self.token:
                    raise AuthError("Token não encontrado no corpo da resposta.")
                return self.token
            else:
                raise AuthError(f"Erro na autenticação: {response.status_code} - {response.text}")
        except requests.RequestException as e:
            raise AuthError(f"Erro de conexão: {e}")
