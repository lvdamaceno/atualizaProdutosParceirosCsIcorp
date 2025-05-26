import os
import time
import json
import logging
from typing import (
    List, Dict, Any, Optional, Callable, Iterable
)
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

from sankhya_api.sankhya_fetch import snk_fetch_data, snk_fetch_json
from utils import logging_config, util_cs_enpoint, util_remove_brackets

logging_config()
load_dotenv()


class CSClient:
    """
    Cliente HTTP para a API CS, com Session reutilizável
    e retry automático em erros 5xx.
    """
    def __init__(
        self,
        max_retries: int = 5,
        backoff_factor: float = 0.5,
        timeout: int = 120
    ):
        self.tenant_id = os.getenv("CS_TENANT", "")
        base = "https://cc01.csicorpnet.com.br/CS50Integracao_API/rest/CS_IntegracaoV1"
        self.session = self._init_session(max_retries, backoff_factor)
        self.base_url = base
        self.timeout = timeout

    def _init_session(self, max_retries: int, backoff_factor: float) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def send(
        self,
        payload: List[Dict[str, Any]],
        tipo: str
    ) -> Dict[str, Any]:
        endpoint = util_cs_enpoint(tipo)
        url = f"{self.base_url}/{endpoint}"
        params = {"In_Tenant_ID": self.tenant_id}
        headers = {"Content-Type": "application/json"}

        logging.info(f"📤 Enviando {len(payload)} registros de '{tipo}'")
        try:
            resp = self.session.post(
                url,
                headers=headers,
                params=params,
                json=payload,
                timeout=self.timeout
            )
            resp.raise_for_status()
            logging.info(f"✅ {tipo.capitalize()} enviado (HTTP {resp.status_code})")
            return resp.json()
        except requests.RequestException as e:
            logging.error(f"❌ Falha ao enviar '{tipo}': {e}")
            return {"erro": str(e)}


def chunked(iterable: Iterable, size: int) -> Iterable[List]:
    """Divide um iterable em chunks de tamanho fixo."""
    lst = list(iterable)
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def process_lotes(
    *,
    tipo: str,
    sql: str,
    fetch_json_fn: Callable[[Any, str], str],
    clean_json_fn: Optional[Callable[[str], str]],
    cs_client: CSClient,
    lote_size: int = 100,
    max_workers: int = 5
) -> None:
    registros = snk_fetch_data(sql)
    total = len(registros)
    logging.info(f"🔢 {total} registros para '{tipo}', em lotes de {lote_size}")

    start = time.time()

    def _worker(lote: List, idx: int):
        batch: List[Dict[str, Any]] = []
        for row in lote:
            key = row[0]
            raw = fetch_json_fn(key, tipo)
            raw = clean_json_fn(raw) if clean_json_fn else raw
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                logging.warning(f"⚠️ JSON inválido em {tipo}='{key}': {e}")
                continue

            if isinstance(data, dict):
                batch.append(data)
            else:
                batch.extend(data)

        # SUBSTITUIR esta linha:
        # logging.info(f"📦 Lote {idx+1}/{(total + lote_size -1)//lote_size}: {len(batch)} items")

        # POR esta:
        logging.info(
            f"📦 Lote {idx + 1}/{(total + lote_size - 1) // lote_size}: "
            f"{len(lote)} códigos → {len(batch)} registros JSON"
        )

        resp = cs_client.send(batch, tipo)
        logging.debug(f"↩️ Resposta CS (lote {idx + 1}): {resp}")

    # Paraleliza os lotes
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_worker, lote, i)
            for i, lote in enumerate(chunked(registros, lote_size))
        ]
        for f in as_completed(futures):
            if exc := f.exception():
                logging.error(f"❌ Erro em lote: {exc}")

    elapsed = time.time() - start
    mins, secs = divmod(int(elapsed), 60)
    logging.info(f"🏁 '{tipo}' completo em {mins}m{secs:02d}s")


def cs_processar_envio_parceiro(
    sql: str,
    tamanho_lote: int = 100,
    max_workers: int = 5
):
    """
    Processa os parceiros em lotes, limpando os colchetes
    extraídos via snk_fetch_json.
    """
    cs = CSClient()
    process_lotes(
        tipo="parceiro",
        sql=sql,
        fetch_json_fn=snk_fetch_json,
        clean_json_fn=util_remove_brackets,
        cs_client=cs,
        lote_size=tamanho_lote,
        max_workers=max_workers
    )


def cs_processar_envio_generico(
    tipo: str,
    sql: str,
    tamanho_lote: int = 100,
    max_workers: int = 5
):
    """
    Processa qualquer tipo genérico, encapsulando em lista JSON válida.
    """
    cs = CSClient()
    process_lotes(
        tipo=tipo,
        sql=sql,
        fetch_json_fn=snk_fetch_json,
        clean_json_fn=lambda raw: f"[{raw}]".replace("}{", "},{"),
        cs_client=cs,
        lote_size=tamanho_lote,
        max_workers=max_workers
    )
