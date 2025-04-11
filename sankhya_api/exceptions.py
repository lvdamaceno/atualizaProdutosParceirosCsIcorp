class AuthError(Exception):
    """Exceção personalizada para erros de autenticação com a API Sankhya."""
    pass


class SankhyaConnectionError(Exception):
    """Erro ao conectar com os serviços Sankhya."""
    pass


class CSIntegrationError(Exception):
    """Erro ao enviar ou processar dados na integração com o CS."""
    pass


class RequestError(Exception):
    """Exceção para erros gerais de requisições."""
    pass


class SankhyaHTTPError(Exception):
    """Erro ao processar resposta HTTP da API Sankhya."""
    pass
