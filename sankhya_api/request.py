"""
Módulo responsável por consultar a API Sankhya com autenticação e retentativas automáticas.

Contém a classe SankhyaClient, que realiza autenticação via token Bearer e executa consultas SQL
através da API REST da Sankhya. Inclui mecanismos de revalidação do token em caso de expiração e
tratamento de erros de conexão com múltiplas tentativas controladas.

Dependências:
    - requests
    - os
    - time
    - logging
    - dotenv
    - SankhyaAuth (auth.py)
    - RequestError (exceção personalizada)
"""

import logging
import time
from requests.exceptions import ReadTimeout, ConnectionError as RequestsConnectionError
import requests
from .auth import SankhyaAuth
from .exceptions import RequestError, SankhyaHTTPError


class SankhyaClient:  # pylint: disable=too-few-public-methods
    """
    Cliente para interação com a API Sankhya usando autenticação Bearer.

    Esta classe realiza requisições à API Sankhya para execução de comandos SQL,
    gerenciando automaticamente autenticação e tentativas de reconexão em caso de falhas.

    Atributos:
        auth (SankhyaAuth): Instância responsável pela autenticação e obtenção do token.
        token (str): Token de autenticação Bearer.
        endpoint (str): URL do endpoint da API.
        timeout (int): Tempo limite da requisição em segundos.
        retries (int): Número máximo de tentativas em caso de falhas.
        servicename (str): Nome do serviço a ser executado via API.

    Métodos:
        execute_query(sql): Executa uma query SQL via API, com tratamento de erro e retentativas.
    """
    def __init__(self, servicename, endpoint, retries, timeout=30):
        self.auth = SankhyaAuth()
        self.token = self.auth.authenticate()
        self.endpoint = endpoint
        self.timeout = timeout
        self.retries = retries
        self.servicename = servicename

    def execute_query(self, sql: str):
        """
        Executa uma consulta SQL no serviço Sankhya utilizando o endpoint REST autenticado.

        Realiza tentativas de reconexão em caso de falha, com revalidação do token.

        Parâmetros:
            sql (str): A string contendo a consulta SQL a ser executada.

        Retorna:
            dict: O JSON de resposta da API, caso a consulta seja bem-sucedida.

        Levanta:
            RequestError: Se todas as tentativas de requisição falharem ou ocorrer erro.
        """
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
                response = requests.get(self.endpoint, headers=headers,
                                        json=payload, timeout=self.timeout)
                if response.status_code == 200:
                    return response.json()
                if response.status_code in (401, 403):
                    self.token = self.auth.authenticate()
                    headers["Authorization"] = f"Bearer {self.token}"
                    continue  # Tenta de novo com novo token
                raise SankhyaHTTPError(f"Erro HTTP {response.status_code}: {response.text}")
            except (ReadTimeout, RequestsConnectionError) as e:
                logging.info("[%d/%d] Timeout ou erro de conexão: %s", attempt + 1, self.retries, e)
                time.sleep(5)  # espera antes de tentar de novo
            except Exception as e:
                raise RequestError(f"Erro de conexão geral: {e}") from e

        raise RequestError("Falha após múltiplas tentativas de conexão.")
