import time
import logging
from utils import *
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuração de log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def processar_envio(inicio: int, fim: int, passo: int = 50, worker: int = 10):
    tempos_por_lote = []
    start_time = time.time()
    tasks = []

    for i in range(inicio, fim, passo):
        final_intervalo = min(i + passo, fim)

        logging.info(f'Enfileirando envio de cadastros de {i} a {final_intervalo}...')
        tasks.append(("ProdutoUpdate", i))

        logging.info(f'Enfileirando envio de estoque de {i} a {final_intervalo}...')
        tasks.append(("Saldos_Atualiza", i))

    total_tarefas = len(tasks)

    with ThreadPoolExecutor(max_workers=worker) as executor:
        future_to_task = {
            executor.submit(
                enviar_json,
                i,
                'JSON_PRODUTO' if endpoint == 'ProdutoUpdate' else 'JSON_ESTOQUE',
                endpoint
            ): (endpoint, i)
            for endpoint, i in tasks
        }

        with tqdm(total=total_tarefas, desc="Enviando", unit="tarefa", dynamic_ncols=True) as pbar:
            for future in as_completed(future_to_task):
                endpoint, indice = future_to_task[future]
                elapsed = time.time() - start_time

                tempos_por_lote.append(elapsed)
                if len(tempos_por_lote) > 5:
                    tempos_por_lote = tempos_por_lote[-5:]

                media_tempo = sum(tempos_por_lote) / len(tempos_por_lote)
                restantes = total_tarefas - pbar.n
                estimado_restante = media_tempo * restantes
                estimado_restante_horas = estimado_restante / 3600

                try:
                    future.result()
                    logging.info(f"Tarefa concluída com sucesso: {endpoint}, lote {indice}")
                    atualizar_arquivo_json("lotes_sucesso.json", {"endpoint": endpoint, "indice": indice})
                except Exception as exc:
                    logging.error(f"Erro no envio para {endpoint} (lote {indice}): {exc}")
                    atualizar_arquivo_json("lotes_erro.json", {
                        "endpoint": endpoint,
                        "indice": indice,
                        "erro": str(exc)
                    })

                pbar.set_postfix({
                    "Tempo Real": f"{elapsed:.0f}s",
                    "ETAh": f"{estimado_restante_horas:.2f} h",
                    "ETAm": f"{(estimado_restante / 60):.2f} m"
                })
                pbar.update(1)

    logging.info("Processamento finalizado.")


def main():
    max_count = count_prods()
    with logging_redirect_tqdm():
        inicio = obter_proximo_indice()
        fim = max_count  # você pode ajustar a lógica de definição do fim se necessário
        processar_envio(inicio, fim, passo=100, worker=20)


if __name__ == "__main__":
    main()
