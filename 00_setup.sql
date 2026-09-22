-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 - Setup do ambiente
-- MAGIC Cria a estrutura da Arquitetura Medalhão no catálogo `workspace` (padrão do Databricks Free Edition):
-- MAGIC
-- MAGIC | Schema | Papel |
-- MAGIC |---|---|
-- MAGIC | `raw` | Volume com os arquivos CSV originais (landing) |
-- MAGIC | `bronze` | Dados como vieram da fonte + metadados de ingestão |
-- MAGIC | `silver` | Dados limpos, tipados e deduplicados |
-- MAGIC | `gold` | Modelo dimensional (esquema estrela) pronto para análise |
-- MAGIC
-- MAGIC **Rode este notebook antes de fazer o upload dos CSVs**, pois ele cria o volume de destino.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.raw COMMENT 'Landing: arquivos brutos do dataset Olist (Kaggle)';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.raw.olist COMMENT 'CSVs originais do Brazilian E-Commerce Public Dataset by Olist';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.bronze COMMENT 'Camada Bronze: dados brutos persistidos em Delta, sem transformação';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.silver COMMENT 'Camada Silver: dados limpos, tipados e deduplicados';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.gold COMMENT 'Camada Gold: modelo dimensional (fatos e dimensões) para análise';

-- COMMAND ----------

SHOW SCHEMAS IN workspace;
