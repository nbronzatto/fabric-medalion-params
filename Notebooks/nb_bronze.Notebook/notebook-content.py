# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9a2a818e-a550-4cb3-95e0-a9e899e81016",
# META       "default_lakehouse_name": "lk_bronze",
# META       "default_lakehouse_workspace_id": "f731a4fe-d2cc-4dee-b197-529502163bb6",
# META       "known_lakehouses": [
# META         {
# META           "id": "9a2a818e-a550-4cb3-95e0-a9e899e81016"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook — nb_bronze
# Propósito: Ingestao de dados brutos da PokeAPI para o Lakehouse Bronze.
#            Cada tabela configurada em nb_parametros e processada aqui.
# Dependencias: nb_parametros, nb_funcoes
# Autor: nbb
# Atualizado: 2026-06

# ── Execucao ──────────────────────────────────────────────────────────────────
# Este notebook e o ponto de entrada da pipeline. Execute-o manualmente ou
# agende-o em um pipeline do Fabric. Ele chama os demais notebooks internamente.
# ==============================================================================


# ==============================================================================
# SETUP — carregar parametros e funcoes
# ==============================================================================

# Celula 1: Executar notebooks de suporte
# ------------------------------------------------------------------------------

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Para mostrar uma imagem salva em Files do lakehouse
import base64
with open("/lakehouse/default/Files/ordem_execucao_medallion_fabric.png", "rb") as f:
    b64 = base64.b64encode(f.read()).decode()
displayHTML(f'<img src="data:image/png;base64,{b64}" width="1500"/>')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

%run nb_funcoes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_parametros

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# nb_parametros retorna JSON com todas as configs
import json
_params_raw = mssparkutils.notebook.run("nb_parametros")
params = json.loads(_params_raw)

# Desempacotar as variaveis que serao usadas neste notebook
API_BASE_URL   = params["API_BASE_URL"]
API_LIMIT      = params["API_LIMIT"]
API_TIMEOUT    = params["API_TIMEOUT"]
API_RETRIES    = params["API_RETRIES"]
LAKEHOUSE      = params["LAKEHOUSE_BRONZE"]
SCHEMA         = params["SCHEMA_BRONZE"]
PARTITION_COL  = params["PARTITION_COL"]
BRONZE_TABLES  = params["BRONZE_TABLES"]

log = get_logger("nb_bronze")
log.info(f"Bronze iniciado — ambiente: {params['AMBIENTE']}")


# ==============================================================================
# INGESTAO
# ==============================================================================

# Celula 2: Loop principal de ingestao
# ------------------------------------------------------------------------------
# Para cada tabela configurada em BRONZE_TABLES:
#   1. Busca todos os registros da API (com paginacao)
#   2. Converte para DataFrame PySpark
#   3. Adiciona dt_carga
#   4. Salva como Delta no Lakehouse Bronze

erros = []

for cfg in BRONZE_TABLES:
    endpoint    = cfg["endpoint"]
    tabela      = cfg["tabela"]
    modo        = cfg["modo_escrita"]

    log.info(f"--- Iniciando ingestao: {tabela} ---")

    try:
        # Passo 1: buscar dados da API
        registros = paginar_endpoint(
            base_url=API_BASE_URL,
            endpoint=endpoint,
            limit=API_LIMIT,
            timeout=API_TIMEOUT,
        )

        if not registros:
            log.warning(f"{tabela}: nenhum registro retornado pela API. Pulando.")
            continue

        # Passo 2: converter para DataFrame
        df = json_para_dataframe(registros)

        # Passo 3: adicionar coluna de carga
        df = adicionar_dt_carga(df)

        # Passo 4: salvar no Bronze
        escrever_delta(
            df=df,
            lakehouse=LAKEHOUSE,
            schema=SCHEMA,
            tabela=tabela,
            modo=modo,
            particionar_por=[PARTITION_COL],
        )

        log.info(f"{tabela}: ingestao concluida com {df.count()} registros.")

    except Exception as e:
        log.error(f"{tabela}: FALHA na ingestao — {e}")
        erros.append({"tabela": tabela, "erro": str(e)})


# ==============================================================================
# RESULTADO
# ==============================================================================

# Celula 3: Sumario final
# ------------------------------------------------------------------------------
if erros:
    log.error(f"Bronze finalizado COM ERROS em {len(erros)} tabela(s):")
    for err in erros:
        log.error(f"  - {err['tabela']}: {err['erro']}")
    raise RuntimeError(f"Falhas na ingestao Bronze: {erros}")
else:
    log.info("Bronze finalizado com sucesso. Todas as tabelas ingeridas.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n=== Contagem Bronze ===")
for cfg in BRONZE_TABLES:
    tabela = cfg["tabela"]
    lh = notebookutils.lakehouse.get(LAKEHOUSE)
    caminho = f"{lh['properties']['abfsPath']}/Tables/{SCHEMA}_{tabela}"
    try:
        n = spark.read.format("delta").load(caminho).count()
        print(f"  {tabela:20s}: {n:>6} linhas")
    except Exception as e:
        print(f"  {tabela:20s}: ERRO — {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
