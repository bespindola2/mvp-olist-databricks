# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: ingestão dos CSVs
# MAGIC Lê os 9 arquivos CSV do volume `/Volumes/workspace/raw/olist/` e grava cada um como tabela Delta no schema `bronze`.
# MAGIC
# MAGIC **Decisões:**
# MAGIC - Todas as colunas são lidas como **texto** (sem `inferSchema`), preservando o dado exatamente como veio. A tipagem acontece na Silver.
# MAGIC - `multiLine` e `escape` habilitados porque o arquivo de avaliações tem comentários com quebras de linha e aspas.
# MAGIC - Único ajuste: remoção do caractere invisível BOM (`\ufeff`) que pode aparecer no nome da primeira coluna.
# MAGIC - Metadados de controle: `_ingestion_ts` (momento da carga) e `_source_file` (arquivo de origem).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
VOLUME_PATH = f"/Volumes/{CATALOG}/raw/olist"
spark.sql(f"USE CATALOG {CATALOG}")

ARQUIVOS = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificação: todos os arquivos estão no volume?

# COMMAND ----------

arquivos_no_volume = {f.name for f in dbutils.fs.ls(VOLUME_PATH)}
faltando = set(ARQUIVOS.values()) - arquivos_no_volume
assert not faltando, f"Arquivos faltando no volume: {faltando}"
print(f"OK: {len(ARQUIVOS)} arquivos encontrados em {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão

# COMMAND ----------

def ler_csv(caminho):
    return (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(caminho)
        .select("*", F.col("_metadata.file_path").alias("_source_file"))
    )

resumo = []
for tabela, arquivo in ARQUIVOS.items():
    df = ler_csv(f"{VOLUME_PATH}/{arquivo}")
    df = df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])
    df = df.withColumn("_ingestion_ts", F.current_timestamp())

    (df.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"bronze.{tabela}"))

    spark.sql(f"COMMENT ON TABLE bronze.{tabela} IS 'Bronze: cópia fiel do arquivo {arquivo} (Olist/Kaggle), todas as colunas como texto'")
    resumo.append((tabela, arquivo, spark.table(f"bronze.{tabela}").count()))

display(spark.createDataFrame(resumo, "tabela string, arquivo string, linhas long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidência: tabelas persistidas no schema bronze

# COMMAND ----------

display(spark.sql("SHOW TABLES IN bronze"))
