# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "290a53ff-4375-42b3-8e1f-466072027a32",
# META       "default_lakehouse_name": "lk_silver",
# META       "default_lakehouse_workspace_id": "f731a4fe-d2cc-4dee-b197-529502163bb6",
# META       "known_lakehouses": [
# META         {
# META           "id": "290a53ff-4375-42b3-8e1f-466072027a32"
# META         },
# META         {
# META           "id": "9a2a818e-a550-4cb3-95e0-a9e899e81016"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook — nb_silver
# Propósito: Transformacao Bronze -> Silver.
#            Aplica tipagem, selecao de colunas e normalizacao basica.
# Dependencias: nb_parametros, nb_funcoes
# Pre-requisito: nb_bronze deve ter sido executado com sucesso.
# Autor: nbb
# Atualizado: 2026-06

# ==============================================================================
# SETUP
# ==============================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_funcoes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
_params_raw = mssparkutils.notebook.run("nb_parametros")
params = json.loads(_params_raw)

LAKEHOUSE_SRC = params["LAKEHOUSE_BRONZE"]
SCHEMA_SRC    = params["SCHEMA_BRONZE"]
LAKEHOUSE_DST = params["LAKEHOUSE_SILVER"]
SCHEMA_DST    = params["SCHEMA_SILVER"]
PARTITION_COL = params["PARTITION_COL"]
SILVER_TABLES = params["SILVER_TABLES"]

log = get_logger("nb_silver")
log.info(f"Silver iniciado — {len(SILVER_TABLES)} tabelas a processar")


# ==============================================================================
# NORMALIZACOES ESPECIFICAS POR TABELA
# ==============================================================================

# Celula 2: Funcoes de normalizacao
# ------------------------------------------------------------------------------
# Cada funcao recebe um DataFrame Bronze (com campos aninhados/arrays)
# e retorna um DataFrame com as colunas extras ja extraidas e prontas para cast.

def normalizar_pokemon(df):
    return (
        df
        .withColumn(
            "type_name",
            F.when(F.size(F.col("types")) > 0, F.col("types")[0]["type"]["name"])
             .otherwise(F.lit(None))
        )
        .withColumn(
            "hp",
            F.expr("""
                CAST(aggregate(
                    filter(stats, s -> s.stat.name = 'hp'),
                    0L, (acc, s) -> acc + s.base_stat
                ) AS INT)
            """)
        )
        .withColumn(
            "attack",
            F.expr("""
                CAST(aggregate(
                    filter(stats, s -> s.stat.name = 'attack'),
                    0L, (acc, s) -> acc + s.base_stat
                ) AS INT)
            """)
        )
        .withColumn(
            "defense",
            F.expr("""
                CAST(aggregate(
                    filter(stats, s -> s.stat.name = 'defense'),
                    0L, (acc, s) -> acc + s.base_stat
                ) AS INT)
            """)
        )
    )
    


def normalizar_abilities(df):
    """Extrai nome da geracao a partir da struct aninhada."""
    return df.withColumn("generation_name", F.col("generation")["name"])


def normalizar_moves(df):
    """Extrai nome do tipo do move a partir da struct type."""
    return df.withColumn("type_name", F.col("type")["name"])


def normalizar_species(df):
    """Extrai habitat name a partir da struct habitat (pode ser null)."""
    return df.withColumn(
        "habitat_name",
        F.when(F.col("habitat").isNotNull(), F.col("habitat")["name"])
         .otherwise(F.lit(None))
    )


# Mapa: nome da tabela -> funcao de normalizacao (None = sem normalizacao adicional)
NORMALIZACOES = {
    "pokemon":   normalizar_pokemon,
    "abilities": normalizar_abilities,
    "moves":     normalizar_moves,
    "species":   normalizar_species,
    "types":     None,
}


# ==============================================================================
# LOOP PRINCIPAL — BRONZE -> SILVER
# ==============================================================================

# Celula 3: Resolver caminhos base uma unica vez
# ------------------------------------------------------------------------------
lh_src = notebookutils.lakehouse.get(LAKEHOUSE_SRC)
lh_dst = notebookutils.lakehouse.get(LAKEHOUSE_DST)
caminho_src_base = lh_src["properties"]["abfsPath"]
caminho_dst_base = lh_dst["properties"]["abfsPath"]

log.info(f"Bronze : {caminho_src_base}")
log.info(f"Silver : {caminho_dst_base}")


# Celula 4: Processar cada tabela
# ------------------------------------------------------------------------------
erros = []

for cfg in SILVER_TABLES:
    tabela_orig = cfg["tabela_origem"]
    tabela_dest = cfg["tabela_destino"]
    colunas     = cfg["colunas"]
    filtros     = cfg["filtros"]

    log.info(f"--- Silver: {tabela_orig} -> {tabela_dest} ---")

    try:
        # 1. Ler Bronze pelo caminho abfss
        caminho_src = f"{caminho_src_base}/Tables/{SCHEMA_SRC}_{tabela_orig}"
        df = spark.read.format("delta").load(caminho_src)
        log.info(f"{tabela_orig}: {df.count()} linhas lidas do Bronze.")

        # 2. Normalizacao especifica (explode de arrays, extracao de structs)
        fn_normalizar = NORMALIZACOES.get(tabela_orig)
        if fn_normalizar:
            df = fn_normalizar(df)

        # 3. Aplicar filtros SQL definidos nos parametros
        for filtro in filtros:
            df = df.filter(filtro)

        # 4. Selecionar e fazer cast das colunas conforme schema Silver
        df = aplicar_schema_silver(df, colunas)

        # 5. Relatorio de nulos (diagnostico)
        relatorio_nulos(df, tabela_dest)

        # 6. Escrever Silver pelo caminho abfss
        caminho_dst = f"{caminho_dst_base}/Tables/{SCHEMA_DST}_{tabela_dest}"
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(PARTITION_COL)
            .save(caminho_dst)
        )

        log.info(f"{tabela_dest}: Silver escrito — {df.count()} linhas.")

    except Exception as e:
        log.error(f"{tabela_dest}: FALHA — {e}")
        erros.append({"tabela": tabela_dest, "erro": str(e)})


# ==============================================================================
# RESULTADO
# ==============================================================================

# Celula 5: Sumario
# ------------------------------------------------------------------------------
if erros:
    log.error(f"Silver finalizado COM ERROS: {erros}")
    raise RuntimeError(f"Falhas Silver: {erros}")
else:
    log.info("Silver finalizado com sucesso.")


# Celula 6: Contagem final (leitura por caminho)
# ------------------------------------------------------------------------------
print("\n=== Contagem Silver ===")
for cfg in SILVER_TABLES:
    tabela = cfg["tabela_destino"]
    caminho = f"{caminho_dst_base}/Tables/{SCHEMA_DST}_{tabela}"
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
