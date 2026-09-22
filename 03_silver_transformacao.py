# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver: limpeza, tipagem e padronização
# MAGIC Aplica os tratamentos definidos a partir do notebook `02_qualidade_dados` e grava as tabelas no schema `silver`.
# MAGIC
# MAGIC | Tabela Silver | Origem (Bronze) | Principais transformações |
# MAGIC |---|---|---|
# MAGIC | `customers` | customers | trim, CEP com 5 dígitos (lpad), cidade em minúsculas, UF em maiúsculas, deduplicação |
# MAGIC | `sellers` | sellers | mesmas regras de customers |
# MAGIC | `orders` | orders | datas convertidas para timestamp, status padronizado, entrega anterior à compra anulada |
# MAGIC | `order_items` | order_items | preço e frete em DECIMAL(12,2), datas em timestamp, deduplicação pela chave composta |
# MAGIC | `order_payments` | order_payments | tipos numéricos, 0 parcelas ajustado para 1 |
# MAGIC | `order_reviews` | order_reviews | nota inteira validada (1 a 5), **uma avaliação por pedido** (a mais recente) |
# MAGIC | `products` | products + product_category_name_translation | **JOIN** pela categoria para trazer o nome em inglês, correção dos nomes `lenght`, categoria nula vira `sem_categoria`, peso 0 anulado |
# MAGIC
# MAGIC A tabela `geolocation` não segue para a Silver porque não é necessária para as perguntas do MVP.

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG = "workspace"
spark.sql(f"USE CATALOG {CATALOG}")

def limpo(c):
    """trim e string vazia vira nulo"""
    return F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c)))

def texto(c):
    return F.lower(limpo(c))

def ts(c):
    return F.expr(f"try_cast({c} AS TIMESTAMP)")

def dec(c):
    return F.expr(f"try_cast({c} AS DECIMAL(12,2))")

def inteiro(c):
    # passa por DOUBLE para aceitar valores como '40.0'
    return F.expr(f"try_cast(try_cast({c} AS DOUBLE) AS INT)")

def salvar(df, tabela, comentario):
    (df.withColumn("_silver_ts", F.current_timestamp())
       .write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"silver.{tabela}"))
    spark.sql(f"COMMENT ON TABLE silver.{tabela} IS '{comentario}'")
    print(f"silver.{tabela}: {spark.table(f'silver.{tabela}').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clientes e vendedores

# COMMAND ----------

customers = (
    spark.table("bronze.customers")
    .select(
        limpo("customer_id").alias("customer_id"),
        limpo("customer_unique_id").alias("customer_unique_id"),
        F.lpad(limpo("customer_zip_code_prefix"), 5, "0").alias("customer_zip_code_prefix"),
        texto("customer_city").alias("customer_city"),
        F.upper(limpo("customer_state")).alias("customer_state"),
    )
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
)
salvar(customers, "customers", "Silver: clientes padronizados. Origem bronze.customers")

sellers = (
    spark.table("bronze.sellers")
    .select(
        limpo("seller_id").alias("seller_id"),
        F.lpad(limpo("seller_zip_code_prefix"), 5, "0").alias("seller_zip_code_prefix"),
        texto("seller_city").alias("seller_city"),
        F.upper(limpo("seller_state")).alias("seller_state"),
    )
    .filter(F.col("seller_id").isNotNull())
    .dropDuplicates(["seller_id"])
)
salvar(sellers, "sellers", "Silver: vendedores padronizados. Origem bronze.sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pedidos

# COMMAND ----------

orders = (
    spark.table("bronze.orders")
    .select(
        limpo("order_id").alias("order_id"),
        limpo("customer_id").alias("customer_id"),
        texto("order_status").alias("order_status"),
        ts("order_purchase_timestamp").alias("order_purchase_timestamp"),
        ts("order_approved_at").alias("order_approved_at"),
        ts("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
        ts("order_delivered_customer_date").alias("order_delivered_customer_date"),
        ts("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
    )
    .filter(F.col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
    # consistência: entrega registrada antes da compra é considerada inválida
    .withColumn(
        "order_delivered_customer_date",
        F.when(F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"), F.lit(None))
         .otherwise(F.col("order_delivered_customer_date")),
    )
)
salvar(orders, "orders", "Silver: pedidos com datas tipadas. Origem bronze.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Itens, pagamentos e avaliações

# COMMAND ----------

order_items = (
    spark.table("bronze.order_items")
    .select(
        limpo("order_id").alias("order_id"),
        inteiro("order_item_id").alias("order_item_id"),
        limpo("product_id").alias("product_id"),
        limpo("seller_id").alias("seller_id"),
        ts("shipping_limit_date").alias("shipping_limit_date"),
        dec("price").alias("price"),
        dec("freight_value").alias("freight_value"),
    )
    .dropDuplicates(["order_id", "order_item_id"])
)
salvar(order_items, "order_items", "Silver: itens de pedido com valores tipados. Origem bronze.order_items")

order_payments = (
    spark.table("bronze.order_payments")
    .select(
        limpo("order_id").alias("order_id"),
        inteiro("payment_sequential").alias("payment_sequential"),
        texto("payment_type").alias("payment_type"),
        inteiro("payment_installments").alias("payment_installments"),
        dec("payment_value").alias("payment_value"),
    )
    .withColumn("payment_installments",
                F.when(F.col("payment_installments") < 1, F.lit(1)).otherwise(F.col("payment_installments")))
    .dropDuplicates(["order_id", "payment_sequential"])
)
salvar(order_payments, "order_payments", "Silver: pagamentos tipados, parcelas minimas = 1. Origem bronze.order_payments")

janela = Window.partitionBy("order_id").orderBy(
    F.col("review_answer_timestamp").desc_nulls_last(), F.col("review_creation_date").desc_nulls_last())

order_reviews = (
    spark.table("bronze.order_reviews")
    .select(
        limpo("review_id").alias("review_id"),
        limpo("order_id").alias("order_id"),
        inteiro("review_score").alias("review_score"),
        ts("review_creation_date").alias("review_creation_date"),
        ts("review_answer_timestamp").alias("review_answer_timestamp"),
    )
    .filter(F.col("review_score").between(1, 5) & F.col("order_id").isNotNull())
    .withColumn("_rn", F.row_number().over(janela))
    .filter("_rn = 1")
    .drop("_rn")
)
salvar(order_reviews, "order_reviews", "Silver: uma avaliacao por pedido (a mais recente), nota validada 1 a 5. Origem bronze.order_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Produtos (JOIN com a tabela de tradução de categorias)

# COMMAND ----------

traducao = (
    spark.table("bronze.product_category_name_translation")
    .select(texto("product_category_name").alias("product_category_name"),
            texto("product_category_name_english").alias("product_category_name_english"))
    .dropDuplicates(["product_category_name"])
)

products = (
    spark.table("bronze.products")
    .select(
        limpo("product_id").alias("product_id"),
        F.coalesce(texto("product_category_name"), F.lit("sem_categoria")).alias("product_category_name"),
        inteiro("product_name_lenght").alias("product_name_length"),
        inteiro("product_description_lenght").alias("product_description_length"),
        inteiro("product_photos_qty").alias("product_photos_qty"),
        inteiro("product_weight_g").alias("product_weight_g"),
        inteiro("product_length_cm").alias("product_length_cm"),
        inteiro("product_height_cm").alias("product_height_cm"),
        inteiro("product_width_cm").alias("product_width_cm"),
    )
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
    .join(traducao, "product_category_name", "left")
    .withColumn("product_category_name_english",
                F.coalesce("product_category_name_english", "product_category_name"))
    .withColumn("product_weight_g",
                F.when(F.col("product_weight_g") > 0, F.col("product_weight_g")))
)
salvar(products, "products", "Silver: produtos com categoria traduzida (join com product_category_name_translation). Origem bronze.products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validações pós-tratamento
# MAGIC Se alguma asserção falhar, o notebook para aqui, impedindo que dados inconsistentes cheguem à Gold.

# COMMAND ----------

def checar_unico(tabela, chaves):
    df = spark.table(f"silver.{tabela}")
    dup = df.count() - df.dropDuplicates(chaves).count()
    assert dup == 0, f"{tabela}: {dup} duplicatas em {chaves}"
    return (tabela, "unicidade " + ",".join(chaves), 0)

def checar_orfaos(filha, pai, chave):
    qtd = spark.table(f"silver.{filha}").join(spark.table(f"silver.{pai}"), chave, "left_anti").count()
    return (filha, f"registros sem correspondente em {pai} ({chave})", qtd)

validacoes = [
    checar_unico("customers", ["customer_id"]),
    checar_unico("sellers", ["seller_id"]),
    checar_unico("orders", ["order_id"]),
    checar_unico("order_items", ["order_id", "order_item_id"]),
    checar_unico("order_payments", ["order_id", "payment_sequential"]),
    checar_unico("order_reviews", ["order_id"]),
    checar_unico("products", ["product_id"]),
    checar_orfaos("order_items", "orders", "order_id"),
    checar_orfaos("order_items", "products", "product_id"),
    checar_orfaos("order_items", "sellers", "seller_id"),
    checar_orfaos("orders", "customers", "customer_id"),
    checar_orfaos("order_payments", "orders", "order_id"),
]
display(spark.createDataFrame(validacoes, "tabela string, validacao string, ocorrencias long"))

# COMMAND ----------

display(spark.sql("SHOW TABLES IN silver"))
