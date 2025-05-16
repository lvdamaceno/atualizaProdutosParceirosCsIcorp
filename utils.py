import json
import re
from threading import Lock
import logging
import os

from cs_api_utils import *
from sk_api_utils import *

# --- Variáveis globais e controle de concorrência ---
lotes_concluidos = []
lotes_com_erro = []
json_lock = Lock()


# --- Funções utilitárias ---
def substituir_aspas_simples(json_obj):
    """
    Substitui aspas simples por duplas nas chaves do JSON (se necessário).
    """
    json_str = json.dumps(json_obj)
    return re.sub(r"(\w+):", lambda m: m.group(0).replace("'", '"'), json_str)


def obter_proximo_indice(arquivo="lotes_sucesso.json"):
    """
    Retorna o maior índice presente no arquivo JSON, ou 0 se vazio/inexistente.
    """
    if os.path.exists(arquivo):
        try:
            with open(arquivo, "r", encoding="utf-8") as f:
                dados = json.load(f)
                indices = [item["indice"] for item in dados if "indice" in item]
                return max(indices) if indices else 0
        except (json.JSONDecodeError, ValueError) as e:
            logging.warning(f"Erro ao ler o JSON {arquivo}: {e}")
    return 0


def atualizar_arquivo_json(nome_arquivo, novo_registro):
    """
    Adiciona um novo registro ao final do arquivo JSON, com controle de concorrência.
    """
    with json_lock:
        dados = []
        if os.path.exists(nome_arquivo):
            try:
                with open(nome_arquivo, 'r', encoding='utf-8') as f:
                    dados = json.load(f)
            except json.JSONDecodeError as e:
                logging.warning(f"Erro ao ler {nome_arquivo}, substituindo conteúdo. Erro: {e}")

        dados.append(novo_registro)

        try:
            with open(nome_arquivo, 'w', encoding='utf-8') as f:
                json.dump(dados, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logging.error(f"Erro ao escrever no arquivo {nome_arquivo}: {e}")


# --- Funções principais de integração ---

def json_pronto(offset, data):
    """
    Executa consultas no Sankhya e retorna lista de objetos JSON formatados.
    """
    sankhya_service = "DbExplorerSP.executeQuery"
    lista = []
    produtos = consulta_sankhya('ALL', sankhya_service, offset)

    for produto in produtos:
        json_data = consulta_sankhya(data, sankhya_service, 0, produto)
        if not json_data:
            continue

        try:
            objetos = json.loads(json_data[0])
            if isinstance(objetos, list):
                lista.extend(objetos)
                if os.getenv("DEBUG_LOGS") == "1":
                    logging.info(f"JSON bruto: {json_data[0][:50]} ... {json_data[0][-10:]}")
            elif isinstance(objetos, dict):
                lista.append(objetos)
                if os.getenv("DEBUG_LOGS") == "1":
                    logging.info(f"JSON bruto (dicionário): {json_data[0]}")
            else:
                logging.warning(f"Produto {produto} retornou formato inesperado.")
        except json.JSONDecodeError:
            logging.warning(f"Erro ao decodificar JSON do produto {produto}")

    return lista


def enviar_json(indice: int, tipo_json: str, endpoint: str):
    """
    Prepara e envia o JSON corrigido ao endpoint especificado.
    """
    json_raw = json_pronto(indice, tipo_json)
    json_corrigido = substituir_aspas_simples(json_raw)

    if os.getenv("DEBUG_LOGS") == "1":
        logging.info(f"JSON pronto: {json_corrigido[:10]} ... {json_corrigido[-10:]}")

    envia_cs(json_corrigido, endpoint)


def tarefa(indice, tipo_json, endpoint):
    """
    Executa a tarefa de envio de JSON e retorna o status.
    """
    try:
        enviar_json(indice, tipo_json, endpoint)
        return {"indice": indice, "status": "sucesso"}
    except Exception as e:
        return {"indice": indice, "status": "erro", "erro": str(e)}
