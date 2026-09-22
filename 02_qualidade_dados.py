# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Qualidade de Dados (sobre a Bronze)
# MAGIC Avalia cada atributo dos dados brutos antes de qualquer tratamento, nas dimensões pedidas no MVP:
# MAGIC **completude, consistência, unicidade, acurácia e outliers**.
# MAGIC
# MAGIC Saídas persistidas como evidência:
# MAGIC - `bronze.dq_perfil_colunas`: nulos/vazios e cardinalidade de **cada coluna de cada tabela**
# MAGIC - `bronze.dq_verificacoes`: verificações específicas, com a quantidade de ocorrências e o tratamento aplicado na Silver

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
spark.sql(f"USE CATALOG {CATALOG}")

TABELAS = ["customers", "geolocation", "order_items", "order_payments", "order_reviews",
           "orders", "products", "sellers", "product_category_name_translation"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Perfil por coluna (completude e cardinalidade)

# COMMAND ----------

def perfil(tabela):
    df = spark.table(f"bronze.{tabela}").drop("_ingestion_ts", "_source_file")
    total = df.count()
    exprs = []
    for c in df.columns:
        vazio = F.col(c).isNull() | (F.trim(F.col(c)) == "")
        exprs.append(F.sum(F.when(vazio, 1).otherwise(0)).alias(f"{c}__nulos"))
        exprs.append(F.approx_count_distinct(F.col(c)).alias(f"{c}__distintos"))
    r = df.agg(*exprs).collect()[0]
    return [
        (tabela, c, total, int(r[f"{c}__nulos"] or 0),
         round(100.0 * (r[f"{c}__nulos"] or 0) / total, 2) if total else 0.0,
         int(r[f"{c}__distintos"] or 0))
        for c in df.columns
    ]

linhas = [l for t in TABELAS for l in perfil(t)]
df_perfil = spark.createDataFrame(
    linhas, "tabela string, coluna string, total_linhas long, nulos_ou_vazios long, pct_nulos double, distintos_aprox long")
df_perfil.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.dq_perfil_colunas")
display(df_perfil.orderBy(F.desc("pct_nulos")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificações específicas

# COMMAND ----------

def outlier_sql(tabela, coluna):
    v = f"try_cast({coluna} AS DOUBLE)"
    return f"""
        WITH q AS (SELECT percentile_approx({v}, 0.25) AS q1, percentile_approx({v}, 0.75) AS q3 FROM bronze.{tabela})
        SELECT COUNT(*) FROM bronze.{tabela}, q WHERE {v} > q.q3 + 1.5 * (q.q3 - q.q1)"""

VERIFICACOES = [
    # (dimensão, tabela, verificação, SQL que retorna a contagem, tratamento na Silver)
    ("Unicidade", "orders", "order_id duplicado",
     "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM bronze.orders", "dropDuplicates por order_id"),
    ("Unicidade", "customers", "customer_id duplicado",
     "SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM bronze.customers", "dropDuplicates por customer_id"),
    ("Unicidade", "products", "product_id duplicado",
     "SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM bronze.products", "dropDuplicates por product_id"),
    ("Unicidade", "sellers", "seller_id duplicado",
     "SELECT COUNT(*) - COUNT(DISTINCT seller_id) FROM bronze.sellers", "dropDuplicates por seller_id"),
    ("Unicidade", "order_items", "(order_id, order_item_id) duplicado",
     "SELECT COUNT(*) - COUNT(DISTINCT order_id, order_item_id) FROM bronze.order_items", "dropDuplicates pela chave composta"),
    ("Unicidade", "order_reviews", "pedidos com mais de uma avaliação",
     "SELECT COUNT(*) FROM (SELECT order_id FROM bronze.order_reviews GROUP BY order_id HAVING COUNT(*) > 1)",
     "mantida apenas a avaliação mais recente de cada pedido"),
    ("Unicidade", "geolocation", "linhas totalmente duplicadas",
     "SELECT COUNT(*) - COUNT(DISTINCT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state) FROM bronze.geolocation",
     "tabela não utilizada nas análises (mantida só na Bronze)"),
    ("Completude", "orders", "pedido entregue sem data de entrega",
     "SELECT COUNT(*) FROM bronze.orders WHERE order_status = 'delivered' AND order_delivered_customer_date IS NULL",
     "prazo e atraso ficam nulos; excluídos das métricas de entrega"),
    ("Completude", "products", "produto sem categoria",
     "SELECT COUNT(*) FROM bronze.products WHERE product_category_name IS NULL",
     "categoria preenchida como sem_categoria"),
    ("Completude", "order_reviews", "avaliação sem comentário em texto",
     "SELECT COUNT(*) FROM bronze.order_reviews WHERE review_comment_message IS NULL",
     "sem tratamento: texto não é usado nas análises"),
    ("Consistência", "orders", "data de compra não conversível para timestamp",
     "SELECT COUNT(*) FROM bronze.orders WHERE order_purchase_timestamp IS NOT NULL AND try_cast(order_purchase_timestamp AS TIMESTAMP) IS NULL",
     "cast para timestamp com try_cast"),
    ("Consistência", "orders", "data de entrega anterior à data de compra",
     "SELECT COUNT(*) FROM bronze.orders WHERE try_cast(order_delivered_customer_date AS TIMESTAMP) < try_cast(order_purchase_timestamp AS TIMESTAMP)",
     "data de entrega anulada"),
    ("Consistência", "customers", "prefixo de CEP com tamanho diferente de 5",
     "SELECT COUNT(*) FROM bronze.customers WHERE length(trim(customer_zip_code_prefix)) <> 5",
     "lpad com zeros à esquerda"),
    ("Consistência", "products", "categorias sem tradução para inglês",
     """SELECT COUNT(DISTINCT p.product_category_name) FROM bronze.products p
        LEFT JOIN bronze.product_category_name_translation t ON p.product_category_name = t.product_category_name
        WHERE p.product_category_name IS NOT NULL AND t.product_category_name IS NULL""",
     "categoria_en recebe o nome em português"),
    ("Acurácia", "order_items", "preço menor ou igual a zero",
     "SELECT COUNT(*) FROM bronze.order_items WHERE try_cast(price AS DOUBLE) <= 0", "nenhum (verificação de sanidade)"),
    ("Acurácia", "order_items", "frete negativo",
     "SELECT COUNT(*) FROM bronze.order_items WHERE try_cast(freight_value AS DOUBLE) < 0", "nenhum (verificação de sanidade)"),
    ("Acurácia", "order_reviews", "nota fora do domínio 1 a 5 ou inválida",
     "SELECT COUNT(*) FROM bronze.order_reviews WHERE coalesce(try_cast(review_score AS INT), 0) NOT BETWEEN 1 AND 5",
     "registros descartados"),
    ("Acurácia", "order_payments", "pagamento com 0 parcelas",
     "SELECT COUNT(*) FROM bronze.order_payments WHERE try_cast(payment_installments AS INT) = 0", "ajustado para 1 parcela"),
    ("Acurácia", "order_payments", "forma de pagamento not_defined",
     "SELECT COUNT(*) FROM bronze.order_payments WHERE payment_type = 'not_defined'", "mantido; filtrado nas análises"),
    ("Acurácia", "products", "peso do produto igual a zero",
     "SELECT COUNT(*) FROM bronze.products WHERE try_cast(product_weight_g AS DOUBLE) = 0", "peso anulado"),
    ("Acurácia", "geolocation", "coordenadas fora do território brasileiro",
     """SELECT COUNT(*) FROM bronze.geolocation WHERE NOT (
          try_cast(geolocation_lat AS DOUBLE) BETWEEN -34 AND 6 AND try_cast(geolocation_lng AS DOUBLE) BETWEEN -74 AND -34)""",
     "tabela não utilizada nas análises"),
    ("Outliers", "order_items", "preço acima de Q3 + 1,5 x IQR", outlier_sql("order_items", "price"),
     "mantidos: são vendas reais; análises usam somas e medianas"),
    ("Outliers", "order_items", "frete acima de Q3 + 1,5 x IQR", outlier_sql("order_items", "freight_value"),
     "mantidos: são fretes reais; frete analisado como % do valor"),
]

resultado = []
for dim, tab, verif, sql, trat in VERIFICACOES:
    qtd = spark.sql(sql).collect()[0][0]
    resultado.append((dim, tab, verif, int(qtd or 0), trat))

df_dq = spark.createDataFrame(resultado, "dimensao string, tabela string, verificacao string, ocorrencias long, tratamento_silver string")
df_dq.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.dq_verificacoes")
display(df_dq)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Distribuição dos campos numéricos (domínio e outliers)

# COMMAND ----------

display(spark.sql("""
    SELECT 'price' AS campo, min(try_cast(price AS DOUBLE)) AS minimo, percentile_approx(try_cast(price AS DOUBLE), 0.5) AS mediana,
           round(avg(try_cast(price AS DOUBLE)), 2) AS media, max(try_cast(price AS DOUBLE)) AS maximo FROM bronze.order_items
    UNION ALL
    SELECT 'freight_value', min(try_cast(freight_value AS DOUBLE)), percentile_approx(try_cast(freight_value AS DOUBLE), 0.5),
           round(avg(try_cast(freight_value AS DOUBLE)), 2), max(try_cast(freight_value AS DOUBLE)) FROM bronze.order_items
    UNION ALL
    SELECT 'payment_value', min(try_cast(payment_value AS DOUBLE)), percentile_approx(try_cast(payment_value AS DOUBLE), 0.5),
           round(avg(try_cast(payment_value AS DOUBLE)), 2), max(try_cast(payment_value AS DOUBLE)) FROM bronze.order_payments
    UNION ALL
    SELECT 'review_score', min(try_cast(review_score AS DOUBLE)), percentile_approx(try_cast(review_score AS DOUBLE), 0.5),
           round(avg(try_cast(review_score AS DOUBLE)), 2), max(try_cast(review_score AS DOUBLE)) FROM bronze.order_reviews
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Domínio dos campos categóricos

# COMMAND ----------

display(spark.sql("SELECT order_status, COUNT(*) AS qtd FROM bronze.orders GROUP BY 1 ORDER BY 2 DESC"))

# COMMAND ----------

display(spark.sql("SELECT payment_type, COUNT(*) AS qtd FROM bronze.order_payments GROUP BY 1 ORDER BY 2 DESC"))

# COMMAND ----------

display(spark.sql("SELECT customer_state, COUNT(*) AS qtd FROM bronze.customers GROUP BY 1 ORDER BY 2 DESC"))
