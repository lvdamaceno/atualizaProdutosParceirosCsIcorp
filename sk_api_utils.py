import os
from sankhya_api.request import SankhyaClient

_sankhya_clients = {}


def load_query(nome, **params):
    caminho = os.path.join("queries", f"{nome}.sql")
    with open(caminho, "r", encoding="utf-8") as file:
        return file.read().format(**params)


def get_sankhya_client(service):
    if service not in _sankhya_clients:
        base_url = 'https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName='
        endpoint = f"{base_url}{service}&outputType=json"
        _sankhya_clients[service] = SankhyaClient(service, endpoint, 5)
    return _sankhya_clients[service]


def consulta_sankhya(nome_query, service, offset=None, item=None):
    client = get_sankhya_client(service)
    query = load_query(nome_query, offset=offset) if "JSON" not in nome_query else load_query(nome_query, item=item)
    result = client.execute_query(query)
    rows = result.get("responseBody", {}).get("rows", []) if result else []
    return [item[0] for item in rows]


def count_prods():
    client = get_sankhya_client("DbExplorerSP.executeQuery")
    query = load_query('COUNT')
    result = client.execute_query(query)
    rows = result.get("responseBody", {}).get("rows", []) if result else []
    return rows[0][0]
