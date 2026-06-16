# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Fabric Notebook — nb_main (OPCIONAL)
# Propósito: orquestrar a execução sequencial Bronze → Silver → Gold.
# Como rodar: execute apenas este notebook. Ele chama os demais em ordem.
# Portabilidade: funciona no Fabric e no Databricks (mssparkutils é compatível).

import json
from datetime import datetime

log = []

def executar(nome_notebook, timeout=3600):
    inicio = datetime.now()
    print(f"[{inicio:%H:%M:%S}] Iniciando {nome_notebook}...")
    try:
        resultado = notebookutils.notebook.run(
            nome_notebook,
            timeout,
            {"useRootDefaultLakehouse": True}  # <-- corrige o erro de lakehouse
        )
        fim = datetime.now()
        duracao = (fim - inicio).seconds
        print(f"[{fim:%H:%M:%S}] {nome_notebook} concluído em {duracao}s")
        log.append({"notebook": nome_notebook, "status": "ok", "duracao_s": duracao})
        return resultado
    except Exception as e:
        fim = datetime.now()
        log.append({"notebook": nome_notebook, "status": "erro", "mensagem": str(e)})
        print(f"[{fim:%H:%M:%S}] {nome_notebook} falhou: {e}")
        raise

# ── Execução ───────────────────────────────────────────────────────────────────
print("=" * 60)
print("PIPELINE MEDALLION — início")
print("=" * 60)

executar("nb_bronze")
executar("nb_silver")
executar("nb_gold")

print("=" * 60)
print("PIPELINE MEDALLION — concluído")
print(json.dumps(log, indent=2, ensure_ascii=False))
print("=" * 60)

notebookutils.notebook.exit(json.dumps({"status": "ok", "etapas": log}))

# ── Execução sequencial ────────────────────────────────────────────────────────
print("=" * 60)
print("PIPELINE MEDALLION — início")
print("=" * 60)

executar("nb_bronze")   # 1. ingestão (API → lk_bronze)
executar("nb_silver")   # 2. tratamento (lk_bronze → lk_silver)
executar("nb_gold")     # 3. modelagem (lk_silver → lk_gold)

print("=" * 60)
print("PIPELINE MEDALLION — concluído")
print(json.dumps(log, indent=2, ensure_ascii=False))
print("=" * 60)

# Exporta o resumo para quem chamar este notebook de fora (ex: um pipeline)
mssparkutils.notebook.exit(json.dumps({"status": "ok", "etapas": log}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
