# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook — nb_parametros
# Propósito: centralizar todos os parâmetros usados pelos notebooks Bronze, Silver e Gold.
#            Executado via mwaitForNotebook pelos outros notebooks.
# Autor: nbb
# Atualizado: 2026-06

# ── Dependencias ──────────────────────────────────────────────────────────────
# Este notebook nao instala bibliotecas. Apenas declara variaveis.

# ==============================================================================
# PARAMETROS GERAIS
# ==============================================================================

# Celula 1: Ambiente e caminhos base
# ------------------------------------------------------------------------------
AMBIENTE = "dev"  # dev | hml | prd

LAKEHOUSE_BRONZE = "lk_bronze"
LAKEHOUSE_SILVER = "lk_silver"
LAKEHOUSE_GOLD   = "lk_gold"

# Prefixo de schema dentro de cada lakehouse
SCHEMA_BRONZE = "raw"
SCHEMA_SILVER = "trusted"
SCHEMA_GOLD   = "fato"

# Formato de particao padrao
PARTITION_COL = "dt_carga"


# ==============================================================================
# CONFIGURACAO DA API
# ==============================================================================

# Celula 2: Parametros da PokeAPI
# ------------------------------------------------------------------------------
API_BASE_URL = "https://pokeapi.co/api/v2"

# Limite de registros por endpoint (paginacao)
API_LIMIT = 100

# Timeout de requisicao em segundos
API_TIMEOUT = 30

# Numero de retentativas em caso de falha
API_RETRIES = 3


# ==============================================================================
# TABELAS — CONFIG POR CAMADA
# ==============================================================================

# Celula 3: Mapa de tabelas Bronze
# Cada entrada define: endpoint relativo, nome da tabela destino e chave primaria.
# ------------------------------------------------------------------------------
BRONZE_TABLES = [
    {
        "endpoint": "pokemon",          # /api/v2/pokemon?limit=100&offset=0
        "tabela":   "pokemon",
        "chave_pk": "id",
        "modo_escrita": "overwrite",    # overwrite | append
    },
    {
        "endpoint": "type",
        "tabela":   "types",
        "chave_pk": "id",
        "modo_escrita": "overwrite",
    },
    {
        "endpoint": "ability",
        "tabela":   "abilities",
        "chave_pk": "id",
        "modo_escrita": "overwrite",
    },
    {
        "endpoint": "move",
        "tabela":   "moves",
        "chave_pk": "id",
        "modo_escrita": "overwrite",
    },
    {
        "endpoint": "pokemon-species",
        "tabela":   "species",
        "chave_pk": "id",
        "modo_escrita": "overwrite",
    },
]


# Celula 4: Mapa de tabelas Silver
# Define transformacoes esperadas por tabela: cast de colunas e regras simples.
# ------------------------------------------------------------------------------
SILVER_TABLES = [
    {
        "tabela_origem": "pokemon",
        "tabela_destino": "pokemon",
        "colunas": [
            ("id",              "int"),
            ("name",            "string"),
            ("base_experience", "int"),
            ("height",          "int"),
            ("weight",          "int"),
            ("is_default",      "boolean"),
            ("order",           "int"),
            ("dt_carga",        "date"),
            ("type_name",       "string"),
            ("hp",              "int"),
            ("attack",          "int"),
            ("defense",         "int"),
        ],
        "filtros": [],              # lista de expressoes SQL WHERE, ex: "id IS NOT NULL"
    },
    {
        "tabela_origem": "types",
        "tabela_destino": "types",
        "colunas": [
            ("id",       "int"),
            ("name",     "string"),
            ("dt_carga", "date"),
        ],
        "filtros": [],
    },
    {
        "tabela_origem": "abilities",
        "tabela_destino": "abilities",
        "colunas": [
            ("id",              "int"),
            ("name",            "string"),
            ("is_main_series",  "boolean"),
            ("generation_name", "string"),
            ("dt_carga",        "date"),
        ],
        "filtros": [],
    },
    {
        "tabela_origem": "moves",
        "tabela_destino": "moves",
        "colunas": [
            ("id",          "int"),
            ("name",        "string"),
            ("accuracy",    "int"),
            ("power",       "int"),
            ("pp",          "int"),
            ("priority",    "int"),
            ("type_name",   "string"),
            ("dt_carga",    "date"),
        ],
        "filtros": [],
    },
    {
        "tabela_origem": "species",
        "tabela_destino": "species",
        "colunas": [
            ("id",                "int"),
            ("name",              "string"),
            ("is_legendary",      "boolean"),
            ("is_mythical",       "boolean"),
            ("base_happiness",    "int"),
            ("capture_rate",      "int"),
            ("gender_rate",       "int"),
            ("habitat_name",      "string"),
            ("dt_carga",          "date"),
        ],
        "filtros": [],
    },
]


# Celula 5: Mapa de tabelas Gold
# Define queries / logica analitica de cada tabela Gold.
# ------------------------------------------------------------------------------
GOLD_TABLES = [
    {
        "tabela_destino": "fato_pokemon",
        "descricao": "Visao consolidada de pokemon com tipo e especie",
        "tabelas_origem": ["pokemon", "species", "types"],
        "modo_escrita": "overwrite",
    },
    {
        "tabela_destino": "fato_moves_power",
        "descricao": "Ranking de moves por poder (top 50)",
        "tabelas_origem": ["moves"],
        "modo_escrita": "overwrite",
    },
]


# Celula 6: Exportar variaveis para outros notebooks
# ------------------------------------------------------------------------------
# Quando este notebook e chamado via mwaitForNotebook, as variaveis Python
# ficam disponiveis no escopo do notebook pai.
# Para passagem explicita, use mssparkutils.notebook.exit() com JSON.

import json

parametros_exportados = {
    "AMBIENTE": AMBIENTE,
    "LAKEHOUSE_BRONZE": LAKEHOUSE_BRONZE,
    "LAKEHOUSE_SILVER": LAKEHOUSE_SILVER,
    "LAKEHOUSE_GOLD":   LAKEHOUSE_GOLD,
    "SCHEMA_BRONZE":    SCHEMA_BRONZE,
    "SCHEMA_SILVER":    SCHEMA_SILVER,
    "SCHEMA_GOLD":      SCHEMA_GOLD,
    "API_BASE_URL":     API_BASE_URL,
    "API_LIMIT":        API_LIMIT,
    "API_TIMEOUT":      API_TIMEOUT,
    "API_RETRIES":      API_RETRIES,
    "PARTITION_COL":    PARTITION_COL,
    "BRONZE_TABLES":    BRONZE_TABLES,
    "SILVER_TABLES":    SILVER_TABLES,
    "GOLD_TABLES":      GOLD_TABLES,
}

mssparkutils.notebook.exit(json.dumps(parametros_exportados))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
