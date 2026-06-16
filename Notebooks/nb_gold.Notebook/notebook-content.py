# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e72f1778-2b40-4f40-a4cf-dc96dfbae63b",
# META       "default_lakehouse_name": "lk_gold",
# META       "default_lakehouse_workspace_id": "f731a4fe-d2cc-4dee-b197-529502163bb6",
# META       "known_lakehouses": [
# META         {
# META           "id": "e72f1778-2b40-4f40-a4cf-dc96dfbae63b"
# META         },
# META         {
# META           "id": "290a53ff-4375-42b3-8e1f-466072027a32"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook — nb_gold
# Propósito: Producao das tabelas analiticas (Gold) a partir do Silver.
#            Entrega visoes prontas para consumo em dashboards e relatórios.
# Dependencias: nb_parametros, nb_funcoes
# Pre-requisito: nb_silver deve ter sido executado com sucesso.
# Autor:
# Atualizado:

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

%run nb_parametros

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Celula 1: Carregar suporte
import json
_params_raw = mssparkutils.notebook.run("nb_parametros")
params = json.loads(_params_raw)

LAKEHOUSE_SRC = params["LAKEHOUSE_SILVER"]
SCHEMA_SRC    = params["SCHEMA_SILVER"]
LAKEHOUSE_DST = params["LAKEHOUSE_GOLD"]
SCHEMA_DST    = params["SCHEMA_GOLD"]
GOLD_TABLES   = params["GOLD_TABLES"]

log = get_logger("nb_gold")
log.info("Gold iniciado")


# ==============================================================================
# TABELA 1: fato_pokemon
# Visao consolidada de pokemon com tipo e informacoes de especie
# ==============================================================================

# Celula 2: Ler Silver
# ------------------------------------------------------------------------------
df_pokemon = ler_delta(LAKEHOUSE_SRC, SCHEMA_SRC, "pokemon")
df_species  = ler_delta(LAKEHOUSE_SRC, SCHEMA_SRC, "species")

# Celula 3: Enriquecer pokemon com dados de especie
# ------------------------------------------------------------------------------
# Join pelo nome do pokemon (chave natural entre as duas tabelas Silver)
df_fato_pokemon = (
    df_pokemon.alias("p")
    .join(
        df_species.alias("s"),
        on=F.col("p.name") == F.col("s.name"),
        how="left",
    )
    .select(
        F.col("p.id").alias("id_pokemon"),
        F.col("p.name").alias("nm_pokemon"),
        F.col("p.type_name").alias("nm_tipo_principal"),
        F.col("p.base_experience").alias("qt_base_experience"),
        F.col("p.height").alias("qt_altura"),
        F.col("p.weight").alias("qt_peso"),
        F.col("p.hp").alias("qt_hp"),
        F.col("p.attack").alias("qt_ataque"),
        F.col("p.defense").alias("qt_defesa"),
        F.col("s.is_legendary").alias("fl_lendario"),
        F.col("s.is_mythical").alias("fl_mitico"),
        F.col("s.base_happiness").alias("qt_base_happiness"),
        F.col("s.capture_rate").alias("qt_taxa_captura"),
        F.col("s.habitat_name").alias("nm_habitat"),
        F.current_date().alias("dt_carga"),
    )
    # Classificar por ranking de poder (attack + defense + hp)
    .withColumn(
        "qt_poder_total",
        F.col("qt_hp") + F.col("qt_ataque") + F.col("qt_defesa"),
    )
    .orderBy(F.col("qt_poder_total").desc())
)

# Celula 4: Escrever fato_pokemon
# ------------------------------------------------------------------------------
escrever_delta(
    df=df_fato_pokemon,
    lakehouse=LAKEHOUSE_DST,
    schema=SCHEMA_DST,
    tabela="pokemon",
    modo="overwrite",
)
log.info("Gold: fato_pokemon escrito.")


# ==============================================================================
# TABELA 2: fato_moves_power
# Ranking dos 50 moves com maior poder, com tipo associado
# ==============================================================================

# Celula 5: Ler Silver de moves
# ------------------------------------------------------------------------------
df_moves = ler_delta(LAKEHOUSE_SRC, SCHEMA_SRC, "moves")

# Celula 6: Filtrar e classificar
# ------------------------------------------------------------------------------
df_fato_moves = (
    df_moves
    .filter(F.col("power").isNotNull())
    .filter(F.col("power") > 0)
    .select(
        F.col("id").alias("id_move"),
        F.col("name").alias("nm_move"),
        F.col("type_name").alias("nm_tipo"),
        F.col("power").alias("qt_poder"),
        F.col("accuracy").alias("qt_precisao"),
        F.col("pp").alias("qt_pp"),
        F.col("priority").alias("qt_prioridade"),
        F.current_date().alias("dt_carga"),
    )
    .orderBy(F.col("qt_poder").desc())
    .limit(50)
)

# Adicionar ranking explícito
from pyspark.sql.window import Window
window_rank = Window.orderBy(F.col("qt_poder").desc())
df_fato_moves = df_fato_moves.withColumn("nr_ranking", F.rank().over(window_rank))

# Celula 7: Escrever fato_moves_power
# ------------------------------------------------------------------------------
escrever_delta(
    df=df_fato_moves,
    lakehouse=LAKEHOUSE_DST,
    schema=SCHEMA_DST,
    tabela="moves_power",
    modo="overwrite",
)
log.info("Gold: fato_moves_power escrito.")


# ==============================================================================
# RESULTADO
# ==============================================================================

# Celula 8: Sumario e preview
# ------------------------------------------------------------------------------
log.info("Gold finalizado com sucesso.")

print("\n=== Contagem Gold ===")
for nome_tabela in ["pokemon", "moves_power"]:
    try:
        n = ler_delta_gold(LAKEHOUSE_DST, SCHEMA_DST, nome_tabela).count()
        print(f"  {SCHEMA_DST}_{nome_tabela:25s}: {n:>6} linhas")
    except Exception as e:
        print(f"  {SCHEMA_DST}_{nome_tabela:25s}: ERRO — {e}")

print("\n=== Preview: fato_pokemon (top 5 por poder) ===")
colunas_preview = ["id_pokemon", "nm_pokemon", "nm_tipo_principal",
                    "qt_poder_total", "fl_lendario", "nm_habitat"]
display(
    ler_delta_gold(LAKEHOUSE_DST, SCHEMA_DST, "pokemon")
    .select(*colunas_preview)
    .limit(5)
)

print("\n=== Preview: fato_moves_power (top 5 moves) ===")
display(
    ler_delta_gold(LAKEHOUSE_DST, SCHEMA_DST, "moves_power")
    .select("nr_ranking", "nm_move", "nm_tipo", "qt_poder", "qt_precisao")
    .limit(5)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
