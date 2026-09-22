-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 - Análises: respondendo às perguntas de negócio
-- MAGIC **Problema:** entender o que impulsiona a receita e a satisfação dos clientes em um marketplace brasileiro de e-commerce.
-- MAGIC
-- MAGIC Dica para os screenshots: em cada resultado, clique em **+ > Visualization** para gerar um gráfico.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 1: Quais categorias de produto geram mais receita, e como essa receita evoluiu mês a mês?
-- MAGIC Receita = soma do valor dos produtos, sem frete, excluindo pedidos cancelados ou indisponíveis.

-- COMMAND ----------

SELECT
  p.categoria,
  ROUND(SUM(f.valor_produto), 2)                                         AS receita,
  COUNT(DISTINCT f.id_pedido)                                            AS pedidos,
  ROUND(100 * SUM(f.valor_produto) / SUM(SUM(f.valor_produto)) OVER (), 2) AS pct_receita
FROM gold.fato_itens_pedido f
JOIN gold.dim_produto p ON f.id_produto = p.id_produto
WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
GROUP BY p.categoria
ORDER BY receita DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Evolução mensal das 5 maiores categorias. O período vai de jan/2017 a ago/2018 porque 2016 e set/2018 têm poucos pedidos na base.

-- COMMAND ----------

WITH top5 AS (
  SELECT p.categoria
  FROM gold.fato_itens_pedido f
  JOIN gold.dim_produto p ON f.id_produto = p.id_produto
  WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
  GROUP BY p.categoria
  ORDER BY SUM(f.valor_produto) DESC
  LIMIT 5
)
SELECT
  t.ano_mes,
  p.categoria,
  ROUND(SUM(f.valor_produto), 2) AS receita
FROM gold.fato_itens_pedido f
JOIN gold.dim_produto p ON f.id_produto = p.id_produto
JOIN gold.dim_tempo t   ON f.sk_data_compra = t.sk_data
WHERE f.status_pedido NOT IN ('canceled', 'unavailable')
  AND p.categoria IN (SELECT categoria FROM top5)
  AND t.data BETWEEN DATE'2017-01-01' AND DATE'2018-08-31'
GROUP BY t.ano_mes, p.categoria
ORDER BY t.ano_mes, receita DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Discussão (P1):** ✏️ preencher após rodar. Quais categorias concentram a receita? A concentração é alta? Há sazonalidade (ex.: nov/2017, Black Friday)?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 2: Atrasos na entrega reduzem a nota de avaliação do cliente?
-- MAGIC Análise no nível de pedido (e não de item), apenas pedidos entregues e avaliados.

-- COMMAND ----------

WITH pedidos AS (
  SELECT DISTINCT id_pedido, atraso_dias, nota_avaliacao
  FROM gold.fato_itens_pedido
  WHERE status_pedido = 'delivered' AND dt_entrega IS NOT NULL AND nota_avaliacao IS NOT NULL
)
SELECT
  CASE
    WHEN atraso_dias <= 0  THEN '1. No prazo ou antes'
    WHEN atraso_dias <= 3  THEN '2. Atraso de 1 a 3 dias'
    WHEN atraso_dias <= 7  THEN '3. Atraso de 4 a 7 dias'
    WHEN atraso_dias <= 14 THEN '4. Atraso de 8 a 14 dias'
    ELSE                        '5. Atraso de 15+ dias'
  END                                                          AS faixa_atraso,
  COUNT(*)                                                     AS pedidos,
  ROUND(AVG(nota_avaliacao), 2)                                AS nota_media,
  ROUND(100 * AVG(CASE WHEN nota_avaliacao <= 2 THEN 1 ELSE 0 END), 1) AS pct_notas_1_ou_2
FROM pedidos
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

WITH pedidos AS (
  SELECT DISTINCT id_pedido, atraso_dias, nota_avaliacao
  FROM gold.fato_itens_pedido
  WHERE status_pedido = 'delivered' AND dt_entrega IS NOT NULL AND nota_avaliacao IS NOT NULL
)
SELECT
  ROUND(corr(atraso_dias, nota_avaliacao), 3)       AS correlacao_atraso_x_nota,
  ROUND(100 * AVG(CASE WHEN atraso_dias > 0 THEN 1 ELSE 0 END), 1) AS pct_pedidos_atrasados
FROM pedidos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Discussão (P2):** ✏️ preencher após rodar. Quanto a nota média cai entre "no prazo" e "atraso 15+"? A correlação é negativa? O que isso sugere para a operação logística?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 3: Quais estados têm maior prazo médio de entrega e maior peso do frete sobre o valor do pedido?

-- COMMAND ----------

WITH pedidos AS (
  SELECT
    f.id_pedido, c.uf, c.regiao,
    MAX(f.prazo_entrega_dias)                         AS prazo,
    MAX(CASE WHEN f.flag_atraso THEN 1 ELSE 0 END)    AS atrasado,
    SUM(f.valor_produto)                              AS valor_produto,
    SUM(f.valor_frete)                                AS valor_frete
  FROM gold.fato_itens_pedido f
  JOIN gold.dim_cliente c ON f.id_cliente = c.id_cliente
  WHERE f.status_pedido = 'delivered' AND f.dt_entrega IS NOT NULL
  GROUP BY f.id_pedido, c.uf, c.regiao
)
SELECT
  uf,
  regiao,
  COUNT(*)                                          AS pedidos,
  ROUND(AVG(prazo), 1)                              AS prazo_medio_dias,
  percentile_approx(prazo, 0.5)                     AS prazo_mediano_dias,
  ROUND(100 * AVG(atrasado), 1)                     AS pct_atrasados,
  ROUND(100 * SUM(valor_frete) / SUM(valor_produto), 1) AS frete_pct_do_valor
FROM pedidos
GROUP BY uf, regiao
ORDER BY prazo_medio_dias DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Discussão (P3):** ✏️ preencher após rodar. Norte e Nordeste pagam proporcionalmente mais frete e esperam mais? Compare com SP.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 4: Como se distribuem as formas de pagamento e o número de parcelas por faixa de valor do pedido?

-- COMMAND ----------

SELECT
  forma_pagamento,
  COUNT(DISTINCT id_pedido)                                                 AS pedidos,
  ROUND(SUM(valor_pagamento), 2)                                            AS valor_total,
  ROUND(100 * SUM(valor_pagamento) / SUM(SUM(valor_pagamento)) OVER (), 1)  AS pct_do_valor
FROM gold.fato_pagamentos
WHERE forma_pagamento <> 'not_defined'
GROUP BY forma_pagamento
ORDER BY valor_total DESC;

-- COMMAND ----------

WITH pedidos AS (
  SELECT
    id_pedido,
    SUM(valor_pagamento)                                                  AS valor_pedido,
    MAX(CASE WHEN forma_pagamento = 'credit_card' THEN qtd_parcelas END) AS parcelas_cartao
  FROM gold.fato_pagamentos
  GROUP BY id_pedido
)
SELECT
  CASE
    WHEN valor_pedido < 50  THEN '1. Até R$ 50'
    WHEN valor_pedido < 100 THEN '2. R$ 50 a 100'
    WHEN valor_pedido < 200 THEN '3. R$ 100 a 200'
    WHEN valor_pedido < 500 THEN '4. R$ 200 a 500'
    ELSE                         '5. Acima de R$ 500'
  END                                                              AS faixa_valor,
  COUNT(*)                                                         AS pedidos_com_cartao,
  ROUND(AVG(parcelas_cartao), 1)                                   AS parcelas_medias,
  ROUND(100 * AVG(CASE WHEN parcelas_cartao > 1 THEN 1 ELSE 0 END), 1) AS pct_parcelados
FROM pedidos
WHERE parcelas_cartao IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Discussão (P4):** ✏️ preencher após rodar. Qual a dependência do cartão de crédito? O parcelamento cresce com o valor do pedido?
-- MAGIC
-- MAGIC ## Discussão geral
-- MAGIC ✏️ Conectar as quatro respostas ao problema original: onde está a receita, o que derruba a satisfação e quais alavancas (logística regional, meios de pagamento) o negócio tem.
