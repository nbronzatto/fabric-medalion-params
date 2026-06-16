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

# Fabric Notebook — nb_funcoes
# Propósito: biblioteca de funcoes reutilizadas pelos notebooks Bronze, Silver e Gold.
#            Executado via mwaitForNotebook antes de qualquer logica de negocio.
# Autor: nbb
# Atualizado: 2026-06

# ── IMPORTANTE ────────────────────────────────────────────────────────────────
# Este notebook APENAS define funcoes. Nao executa nada por si so.
# Chame-o com: mssparkutils.notebook.run("nb_funcoes")
# ==============================================================================


# ==============================================================================
# IMPORTS
# ==============================================================================

# Celula 1: Imports padrao
# ------------------------------------------------------------------------------
import requests
import time
import json
import logging
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

spark: SparkSession = spark  # referencia ao SparkSession do Fabric


# ==============================================================================
# LOGGING
# ==============================================================================

# Celula 2: Configurar logger padrao
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_logger(nome: str) -> logging.Logger:
    """Retorna um logger com o nome do notebook chamador."""
    return logging.getLogger(nome)


# ==============================================================================
# HTTP / API
# ==============================================================================

# Celula 3: Requisicao com retentativas
# ------------------------------------------------------------------------------
def http_get(url: str, params: dict = None, timeout: int = 30, retries: int = 3) -> dict:
    """
    GET simples com retentativa exponencial.

    Parametros
    ----------
    url     : URL completa do endpoint
    params  : query string como dicionario
    timeout : segundos ate timeout
    retries : numero de tentativas

    Retorno
    -------
    dict com o JSON da resposta
    """
    log = get_logger("http_get")
    for tentativa in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            log.warning(f"Tentativa {tentativa}/{retries} falhou para {url}: {e}")
            if tentativa == retries:
                raise
            time.sleep(2 ** tentativa)


# Celula 4: Paginacao de listagem
# ------------------------------------------------------------------------------
def paginar_endpoint(base_url: str, endpoint: str, limit: int = 100, timeout: int = 30) -> list[dict]:
    """
    Percorre todas as paginas de um endpoint REST com ?limit= e ?offset=.

    Retorna lista de dicionarios com os detalhes completos de cada recurso,
    buscando cada URL individual apos a listagem paginada.
    """
    log = get_logger("paginar_endpoint")
    url_lista = f"{base_url}/{endpoint}"
    offset = 0
    resultados = []

    while True:
        pagina = http_get(url_lista, params={"limit": limit, "offset": offset}, timeout=timeout)
        itens = pagina.get("results", [])
        log.info(f"{endpoint}: buscando offset={offset}, encontrados={len(itens)}")

        for item in itens:
            detalhe = http_get(item["url"], timeout=timeout)
            resultados.append(detalhe)

        if pagina.get("next") is None:
            break
        offset += limit

    log.info(f"{endpoint}: total de registros = {len(resultados)}")
    return resultados


# ==============================================================================
# DELTA LAKE — LEITURA
# ==============================================================================

# Celula 5: Ler tabela Delta
# ------------------------------------------------------------------------------
def ler_delta(lakehouse: str, schema: str, tabela: str) -> DataFrame:
    lh = notebookutils.lakehouse.get(lakehouse)
    caminho = f"{lh['properties']['abfsPath']}/Tables/{schema}_{tabela}"
    return spark.read.format("delta").load(caminho)

def ler_delta_gold(lakehouse, schema, tabela):
    lh = notebookutils.lakehouse.get(lakehouse)
    caminho_base = lh["properties"]["abfsPath"]
    caminho_tabela = f"{caminho_base}/Tables/{schema}_{tabela}"
    return spark.read.format("delta").load(caminho_tabela)
# ==============================================================================
# DELTA LAKE — ESCRITA
# ==============================================================================

# Celula 6: Escrever / sobrescrever tabela Delta
# ------------------------------------------------------------------------------
def escrever_delta(df, lakehouse, schema, tabela, modo="overwrite", particionar_por=None):
    log = get_logger("escrever_delta")
    
    # notebookutils.lakehouse.get() retorna dict — acessar como chave
    lh = notebookutils.lakehouse.get(lakehouse)
    caminho_base = lh["properties"]["abfsPath"]
    caminho_tabela = f"{caminho_base}/Tables/{schema}_{tabela}"

    writer = df.write.format("delta").mode(modo).option("overwriteSchema", "true")
    if particionar_por:
        writer = writer.partitionBy(*particionar_por)
    writer.save(caminho_tabela)
    
    log.info(f"Escrita concluida: {caminho_tabela} ({modo})")
# ------------------------------------------------------------------------------
def merge_delta(
    df_novo: DataFrame,
    lakehouse: str,
    schema: str,
    tabela: str,
    chave_pk: str,
) -> None:
    """
    Realiza MERGE (INSERT + UPDATE) em uma tabela Delta existente.

    Uso tipico: silver -> gold com atualizacoes incrementais.
    """
    log = get_logger("merge_delta")
    nome_completo = f"{lakehouse}.{schema}_{tabela}"

    delta_tbl = DeltaTable.forName(spark, nome_completo)

    (
        delta_tbl.alias("existente")
        .merge(
            df_novo.alias("novo"),
            f"existente.{chave_pk} = novo.{chave_pk}",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info(f"Merge concluido em {nome_completo}")


# ==============================================================================
# SCHEMA / QUALIDADE
# ==============================================================================

# Celula 8: Adicionar coluna de carga
# ------------------------------------------------------------------------------
def adicionar_dt_carga(df: DataFrame) -> DataFrame:
    """Adiciona coluna dt_carga com a data atual (date)."""
    return df.withColumn("dt_carga", F.current_date())


# Celula 9: Cast de colunas conforme mapa Silver
# ------------------------------------------------------------------------------
def aplicar_schema_silver(df: DataFrame, colunas: list[tuple]) -> DataFrame:
    """
    Seleciona e faz cast das colunas definidas no mapa SILVER_TABLES.

    Parametros
    ----------
    df      : DataFrame de origem (Bronze)
    colunas : lista de (nome_coluna, tipo_spark) ex: [("id", "int"), ...]
    """
    selecoes = []
    for nome, tipo in colunas:
        if nome in df.columns:
            selecoes.append(F.col(nome).cast(tipo).alias(nome))
        else:
            # coluna ausente: preenche com null para nao quebrar o schema
            selecoes.append(F.lit(None).cast(tipo).alias(nome))
    return df.select(*selecoes)


# Celula 10: Contar nulos por coluna (diagnostico)
# ------------------------------------------------------------------------------
def relatorio_nulos(df: DataFrame, nome_tabela: str = "") -> None:
    """Imprime contagem de nulos por coluna. Util em Silver e Gold."""
    log = get_logger("relatorio_nulos")
    total = df.count()
    for col in df.columns:
        nulos = df.filter(F.col(col).isNull()).count()
        pct = round(100 * nulos / total, 1) if total > 0 else 0
        if nulos > 0:
            log.warning(f"[{nome_tabela}] {col}: {nulos} nulos ({pct}%)")


# ==============================================================================
# UTILITARIOS GERAIS
# ==============================================================================

# Celula 11: Carregar parametros exportados pelo nb_parametros
# ------------------------------------------------------------------------------
def carregar_parametros(nome_notebook_params: str = "nb_parametros") -> dict:
    """
    Executa nb_parametros e retorna o dicionario de configuracao.

    Uso
    ---
    params = carregar_parametros()
    BRONZE_TABLES = params["BRONZE_TABLES"]
    """
    resultado = mssparkutils.notebook.run(nome_notebook_params)
    return json.loads(resultado)


# Celula 12: Helper para converter JSON aninhado em DataFrame flat
# ------------------------------------------------------------------------------
def json_para_dataframe(registros: list[dict]) -> DataFrame:
    import json as _json
    rdd = spark.sparkContext.parallelize([_json.dumps(r) for r in registros])
    return spark.read.json(rdd)  # infere schema olhando TODOS os registros antes de criar


# Celula 13: Extrair campo de lista de structs (ex: types[0].type.name)
# ------------------------------------------------------------------------------
def extrair_primeiro_de_lista(df: DataFrame, coluna_lista: str, campo_interno: str, alias: str) -> DataFrame:
    """
    Extrai o primeiro elemento de uma coluna ArrayType<StructType>.

    Exemplo: extrair_primeiro_de_lista(df, "types", "type.name", "type_name")
    Equivale a: df.withColumn("type_name", col("types")[0]["type"]["name"])
    """
    partes = campo_interno.split(".")
    expr = F.col(coluna_lista)[0]
    for parte in partes:
        expr = expr[parte]
    return df.withColumn(alias, expr)


print("nb_funcoes: todas as funcoes carregadas com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
