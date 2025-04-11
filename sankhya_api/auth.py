"""
Módulo responsável por autenticar na API da Sankhya.

Contém a classe SankhyaAuth, que realiza login e obtém o token de autenticação
utilizando credenciais armazenadas em variáveis de ambiente.
"""

import os
import requests
from dotenv import load_dotenv
from sankhya_api.exceptions import AuthError

load_dotenv()


class SankhyaAuth:  # pylint: disable=too-few-public-methods
    """Classe responsável por autenticar na API da Sankhya e obter o token de acesso."""

    def __init__(self):
        """Inicializa a URL de autenticação e o token como None."""
        self.url_auth = "https://api.sankhya.com.br/login"
        self.token = None

    def authenticate(self):
        """
        Realiza a autenticação na API da Sankhya usando variáveis de ambiente.

        Retorna:
           str: Token de autenticação Bearer.

        Levanta:
           AuthError: Se ocorrer um erro de autenticação ou conexão.
        """
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

            raise AuthError(f"Erro na autenticação: {response.status_code} - {response.text}")

        except requests.RequestException as e:
            raise AuthError(f"Erro de conexão: {e}") from e
