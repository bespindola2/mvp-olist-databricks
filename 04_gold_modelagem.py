# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold: modelo dimensional e catálogo de dados
# MAGIC Esquema estrela com **duas tabelas fato** que compartilham dimensões (constelação de fatos):
# MAGIC
# MAGIC ```
# MAGIC               dim_tempo
# MAGIC                   |
# MAGIC  dim_cliente -- fato_itens_pedido -- dim_produto
# MAGIC       |           |
# MAGIC       |       dim_vendedor
# MAGIC       |
# MAGIC  fato_pagamentos -- dim_tempo
# MAGIC ```
# MAGIC
# MAGIC - `fato_itens_pedido` (grão: item de pedido): receita, frete, prazos, atraso e nota da avaliação
# MAGIC - `fato_pagamentos` (grão: pagamento de um pedido): forma, parcelas e valor
# MAGIC
# MAGIC Ao final, o **catálogo de dados** é gravado no Unity Catalog (comentários de tabela e de coluna, com descrição, domínio e linhagem).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
spark.sql(f"USE CATALOG {CATALOG}")

def salvar(df, tabela):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"gold.{tabela}")
    print(f"gold.{tabela}: {spark.table(f'gold.{tabela}').count()} linhas")

REGIOES = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste",
    "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}
mapa_regiao = F.create_map(*[F.lit(x) for par in REGIOES.items() for x in par])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimensões

# COMMAND ----------

dim_cliente = spark.table("silver.customers").select(
    F.col("customer_id").alias("id_cliente"),
    F.col("customer_unique_id").alias("id_cliente_unico"),
    F.col("customer_zip_code_prefix").alias("cep_prefixo"),
    F.col("customer_city").alias("cidade"),
    F.col("customer_state").alias("uf"),
    mapa_regiao[F.col("customer_state")].alias("regiao"),
)
salvar(dim_cliente, "dim_cliente")

dim_vendedor = spark.table("silver.sellers").select(
    F.col("seller_id").alias("id_vendedor"),
    F.col("seller_zip_code_prefix").alias("cep_prefixo"),
    F.col("seller_city").alias("cidade"),
    F.col("seller_state").alias("uf"),
    mapa_regiao[F.col("seller_state")].alias("regiao"),
)
salvar(dim_vendedor, "dim_vendedor")

dim_produto = spark.table("silver.products").select(
    F.col("product_id").alias("id_produto"),
    F.col("product_category_name").alias("categoria"),
    F.col("product_category_name_english").alias("categoria_en"),
    F.col("product_photos_qty").alias("qtd_fotos"),
    F.col("product_weight_g").alias("peso_g"),
    F.col("product_length_cm").alias("comprimento_cm"),
    F.col("product_height_cm").alias("altura_cm"),
    F.col("product_width_cm").alias("largura_cm"),
)
salvar(dim_produto, "dim_produto")

lim = spark.table("silver.orders").agg(
    F.min(F.to_date("order_purchase_timestamp")).alias("ini"),
    F.max(F.to_date("order_purchase_timestamp")).alias("fim"),
).collect()[0]

dim_tempo = (
    spark.sql(f"SELECT explode(sequence(DATE'{lim.ini}', DATE'{lim.fim}', INTERVAL 1 DAY)) AS data")
    .select(
        F.date_format("data", "yyyyMMdd").cast("int").alias("sk_data"),
        "data",
        F.year("data").alias("ano"),
        F.quarter("data").alias("trimestre"),
        F.month("data").alias("mes"),
        F.date_format("data", "yyyy-MM").alias("ano_mes"),
        F.dayofweek("data").alias("dia_semana_num"),
        F.dayofweek("data").isin(1, 7).alias("fim_de_semana"),
    )
)
salvar(dim_tempo, "dim_tempo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fatos
# MAGIC `fato_itens_pedido` é o resultado do **JOIN** entre `order_items`, `orders` (datas, status e cliente) e `order_reviews` (nota).
# MAGIC Prazo e atraso são calculados aqui, em dias corridos.

# COMMAND ----------

orders = spark.table("silver.orders")
reviews = spark.table("silver.order_reviews").select("order_id", "review_score")

fato_itens = (
    spark.table("silver.order_items")
    .join(orders, "order_id", "inner")
    .join(reviews, "order_id", "left")
    .select(
        F.col("order_id").alias("id_pedido"),
        F.col("order_item_id").alias("num_item"),
        F.col("customer_id").alias("id_cliente"),
        F.col("product_id").alias("id_produto"),
        F.col("seller_id").alias("id_vendedor"),
        F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int").alias("sk_data_compra"),
        F.col("order_status").alias("status_pedido"),
        F.col("order_purchase_timestamp").alias("dt_compra"),
        F.col("order_delivered_customer_date").alias("dt_entrega"),
        F.col("order_estimated_delivery_date").alias("dt_entrega_estimada"),
        F.datediff(F.to_date("order_delivered_customer_date"), F.to_date("order_purchase_timestamp")).alias("prazo_entrega_dias"),
        F.datediff(F.to_date("order_delivered_customer_date"), F.to_date("order_estimated_delivery_date")).alias("atraso_dias"),
        F.col("price").alias("valor_produto"),
        F.col("freight_value").alias("valor_frete"),
        (F.col("price") + F.col("freight_value")).alias("valor_total_item"),
        F.col("review_score").alias("nota_avaliacao"),
    )
    .withColumn("flag_atraso", F.when(F.col("atraso_dias").isNotNull(), F.col("atraso_dias") > 0))
)
salvar(fato_itens, "fato_itens_pedido")

fato_pag = (
    spark.table("silver.order_payments")
    .join(orders.select("order_id", "customer_id", "order_purchase_timestamp"), "order_id", "inner")
    .select(
        F.col("order_id").alias("id_pedido"),
        F.col("payment_sequential").alias("seq_pagamento"),
        F.col("customer_id").alias("id_cliente"),
        F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int").alias("sk_data_compra"),
        F.col("payment_type").alias("forma_pagamento"),
        F.col("payment_installments").alias("qtd_parcelas"),
        F.col("payment_value").alias("valor_pagamento"),
    )
)
salvar(fato_pag, "fato_pagamentos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catálogo de dados (Unity Catalog)
# MAGIC Cada tabela e cada coluna recebe um comentário com **descrição, tipo/domínio e linhagem** (de onde veio e qual transformação sofreu).

# COMMAND ----------

LIN_C = "Linhagem: bronze.customers > silver.customers"
LIN_V = "Linhagem: bronze.sellers > silver.sellers"
LIN_P = "Linhagem: bronze.products + bronze.product_category_name_translation > silver.products"
LIN_F = "Linhagem: silver.order_items JOIN silver.orders JOIN silver.order_reviews"
LIN_PG = "Linhagem: silver.order_payments JOIN silver.orders"

CATALOGO = {
    "dim_cliente": {
        "_tabela": "Dimensão de clientes. Grão: um registro por id_cliente (cliente no contexto do pedido). Fonte: Olist (Kaggle), arquivo olist_customers_dataset.csv.",
        "id_cliente": f"Chave primária. Identificador do cliente no pedido (hash de 32 caracteres). {LIN_C}.customer_id",
        "id_cliente_unico": f"Identificador da pessoa; um mesmo cliente pode ter vários id_cliente. Hash de 32 caracteres. {LIN_C}.customer_unique_id",
        "cep_prefixo": f"5 primeiros dígitos do CEP. Texto de 5 dígitos, zeros à esquerda completados na Silver. {LIN_C}.customer_zip_code_prefix",
        "cidade": f"Cidade do cliente em minúsculas. {LIN_C}.customer_city",
        "uf": f"Sigla da UF do cliente. Domínio: as 27 UFs brasileiras. {LIN_C}.customer_state",
        "regiao": "Região geográfica. Domínio: Norte, Nordeste, Centro-Oeste, Sudeste, Sul. Derivada na Gold a partir da UF.",
    },
    "dim_vendedor": {
        "_tabela": "Dimensão de vendedores do marketplace. Grão: um registro por vendedor. Fonte: olist_sellers_dataset.csv.",
        "id_vendedor": f"Chave primária. Identificador do vendedor (hash de 32 caracteres). {LIN_V}.seller_id",
        "cep_prefixo": f"5 primeiros dígitos do CEP do vendedor. {LIN_V}.seller_zip_code_prefix",
        "cidade": f"Cidade do vendedor em minúsculas. {LIN_V}.seller_city",
        "uf": f"Sigla da UF do vendedor. Domínio: UFs brasileiras. {LIN_V}.seller_state",
        "regiao": "Região geográfica do vendedor. Domínio: Norte, Nordeste, Centro-Oeste, Sudeste, Sul. Derivada na Gold.",
    },
    "dim_produto": {
        "_tabela": "Dimensão de produtos. Grão: um registro por produto. Fonte: olist_products_dataset.csv enriquecido com product_category_name_translation.csv.",
        "id_produto": f"Chave primária. Identificador do produto (hash de 32 caracteres). {LIN_P}.product_id",
        "categoria": f"Categoria do produto em português (ex.: beleza_saude). Nulos substituídos por sem_categoria. {LIN_P}.product_category_name",
        "categoria_en": f"Categoria em inglês obtida por JOIN com a tabela de tradução; quando não há tradução, repete a categoria em português. {LIN_P}.product_category_name_english",
        "qtd_fotos": f"Quantidade de fotos no anúncio. Inteiro maior ou igual a 1; nulo quando não informado. {LIN_P}.product_photos_qty",
        "peso_g": f"Peso em gramas. Inteiro maior que 0; zero foi convertido em nulo na Silver. {LIN_P}.product_weight_g",
        "comprimento_cm": f"Comprimento da embalagem em cm. Inteiro positivo. {LIN_P}.product_length_cm",
        "altura_cm": f"Altura da embalagem em cm. Inteiro positivo. {LIN_P}.product_height_cm",
        "largura_cm": f"Largura da embalagem em cm. Inteiro positivo. {LIN_P}.product_width_cm",
    },
    "dim_tempo": {
        "_tabela": "Dimensão calendário. Grão: um dia. Gerada na Gold cobrindo do primeiro ao último dia de compra em silver.orders.",
        "sk_data": "Chave primária no formato AAAAMMDD (inteiro).",
        "data": "Data do calendário.",
        "ano": "Ano. Domínio: 2016 a 2018.",
        "trimestre": "Trimestre do ano. Domínio: 1 a 4.",
        "mes": "Mês do ano. Domínio: 1 a 12.",
        "ano_mes": "Ano e mês no formato AAAA-MM, usado em séries mensais.",
        "dia_semana_num": "Dia da semana. Domínio: 1 (domingo) a 7 (sábado).",
        "fim_de_semana": "Verdadeiro para sábado e domingo.",
    },
    "fato_itens_pedido": {
        "_tabela": "Fato de vendas. Grão: um item de um pedido. Contém valores, prazos de entrega e a nota da avaliação do pedido.",
        "id_pedido": f"Identificador do pedido (hash de 32 caracteres). Parte da chave primária. {LIN_F}",
        "num_item": "Número sequencial do item dentro do pedido. Inteiro a partir de 1. Parte da chave primária. Origem: order_items.order_item_id",
        "id_cliente": "Chave estrangeira para dim_cliente. Origem: orders.customer_id",
        "id_produto": "Chave estrangeira para dim_produto. Origem: order_items.product_id",
        "id_vendedor": "Chave estrangeira para dim_vendedor. Origem: order_items.seller_id",
        "sk_data_compra": "Chave estrangeira para dim_tempo (AAAAMMDD da data de compra). Derivada de orders.order_purchase_timestamp",
        "status_pedido": "Status do pedido. Domínio: delivered, shipped, canceled, unavailable, invoiced, processing, created, approved. Origem: orders.order_status",
        "dt_compra": "Data e hora da compra. Origem: orders.order_purchase_timestamp",
        "dt_entrega": "Data e hora da entrega ao cliente; nula se não entregue ou se inconsistente. Origem: orders.order_delivered_customer_date",
        "dt_entrega_estimada": "Data de entrega prometida ao cliente. Origem: orders.order_estimated_delivery_date",
        "prazo_entrega_dias": "Dias corridos entre compra e entrega. Inteiro maior ou igual a 0; nulo se não entregue. Calculado na Gold.",
        "atraso_dias": "Dias entre a entrega real e a estimada. Negativo = entregue antes do prazo, positivo = atraso. Calculado na Gold.",
        "flag_atraso": "Verdadeiro se atraso_dias maior que 0; nulo se o pedido não foi entregue. Calculado na Gold.",
        "valor_produto": "Preço do item em R$. Decimal maior que 0. Origem: order_items.price",
        "valor_frete": "Frete do item em R$. Decimal maior ou igual a 0. Origem: order_items.freight_value",
        "valor_total_item": "valor_produto + valor_frete, em R$. Calculado na Gold.",
        "nota_avaliacao": "Nota da avaliação do pedido. Domínio: 1 a 5; nula quando não há avaliação. Origem: order_reviews.review_score (avaliação mais recente do pedido).",
    },
    "fato_pagamentos": {
        "_tabela": "Fato de pagamentos. Grão: um pagamento de um pedido (um pedido pode ter vários, ex.: cartão + voucher).",
        "id_pedido": f"Identificador do pedido. Parte da chave primária. {LIN_PG}",
        "seq_pagamento": "Sequencial do pagamento dentro do pedido. Inteiro a partir de 1. Parte da chave primária. Origem: order_payments.payment_sequential",
        "id_cliente": "Chave estrangeira para dim_cliente. Origem: orders.customer_id",
        "sk_data_compra": "Chave estrangeira para dim_tempo (AAAAMMDD da data de compra). Derivada de orders.order_purchase_timestamp",
        "forma_pagamento": "Meio de pagamento. Domínio: credit_card, boleto, voucher, debit_card, not_defined. Origem: order_payments.payment_type",
        "qtd_parcelas": "Número de parcelas. Inteiro maior ou igual a 1 (valores 0 ajustados na Silver). Origem: order_payments.payment_installments",
        "valor_pagamento": "Valor pago em R$. Decimal maior ou igual a 0. Origem: order_payments.payment_value",
    },
}

def sql_str(s):
    return s.replace("'", "\\'")

for tabela, doc in CATALOGO.items():
    spark.sql(f"COMMENT ON TABLE gold.{tabela} IS '{sql_str(doc['_tabela'])}'")
    for coluna, texto in doc.items():
        if coluna != "_tabela":
            spark.sql(f"ALTER TABLE gold.{tabela} ALTER COLUMN {coluna} COMMENT '{sql_str(texto)}'")
print("Catálogo aplicado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidência: catálogo consultável via information_schema

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name AS tabela, column_name AS coluna, data_type AS tipo, comment AS descricao
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = 'gold'
    ORDER BY table_name, ordinal_position
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Domínio observado dos campos numéricos da fato (mínimo e máximo reais)

# COMMAND ----------

display(spark.table("gold.fato_itens_pedido")
        .select("prazo_entrega_dias", "atraso_dias", "valor_produto", "valor_frete", "nota_avaliacao")
        .summary("count", "min", "50%", "max"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validação: nenhuma linha perdida entre Silver e Gold

# COMMAND ----------

n_silver = spark.table("silver.order_items").count()
n_gold = spark.table("gold.fato_itens_pedido").count()
print(f"silver.order_items = {n_silver} | gold.fato_itens_pedido = {n_gold}")
assert n_silver == n_gold, "Itens perdidos no JOIN com orders"
