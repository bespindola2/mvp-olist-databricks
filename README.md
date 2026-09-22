# MVP – Pipeline de Dados na Nuvem: E-commerce Olist no Databricks

> MVP da disciplina de Engenharia de Dados (MBA PUC-Rio). Pipeline de ponta a ponta no **Databricks Free Edition** seguindo a **Arquitetura Medalhão** (Bronze → Silver → Gold).
> ✏️ Itens marcados com este símbolo devem ser preenchidos após executar os notebooks.

## Estrutura do repositório

```
(raiz do repositório)
00_setup.sql                 -> cria schemas (raw, bronze, silver, gold) e o volume
01_bronze_ingestao.py        -> CSV do volume  -> tabelas Delta na bronze
02_qualidade_dados.py        -> perfil de qualidade de cada atributo (sobre a bronze)
03_silver_transformacao.py   -> limpeza, tipagem, deduplicação, JOIN de categorias
04_gold_modelagem.py         -> modelo estrela + catálogo de dados no Unity Catalog
05_analises.sql              -> respostas às perguntas de negócio
docs/img/                      -> screenshots de evidência
```

---

## 1. Contexto de Negócio e Perguntas (Etapa 2 e 4.1)

### Problema
Entender o que impulsiona a **receita** e a **satisfação dos clientes** em um marketplace brasileiro de e-commerce, com foco em categorias de produto, logística de entrega e comportamento de pagamento.

### Perguntas de negócio
1. Quais categorias de produto geram mais receita, e como essa receita evoluiu mês a mês?
2. Atrasos na entrega reduzem a nota de avaliação do cliente?
3. Quais estados têm maior prazo médio de entrega e maior peso do frete sobre o valor do pedido?
4. Como se distribuem as formas de pagamento e o número de parcelas por faixa de valor do pedido?

### Fonte dos dados
**Brazilian E-Commerce Public Dataset by Olist**, disponível no Kaggle: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Dados reais e anonimizados de cerca de 100 mil pedidos feitos entre 2016 e 2018 em diversos marketplaces brasileiros por meio da Olist. São 9 arquivos CSV relacionados entre si por chaves:

| Arquivo | Conteúdo | Chaves |
|---|---|---|
| olist_orders_dataset.csv | pedidos, status e datas | order_id, customer_id |
| olist_order_items_dataset.csv | itens de cada pedido, preço e frete | order_id + order_item_id, product_id, seller_id |
| olist_order_payments_dataset.csv | pagamentos de cada pedido | order_id + payment_sequential |
| olist_order_reviews_dataset.csv | avaliações dos clientes | review_id, order_id |
| olist_customers_dataset.csv | clientes e localização | customer_id |
| olist_sellers_dataset.csv | vendedores e localização | seller_id |
| olist_products_dataset.csv | atributos dos produtos | product_id |
| product_category_name_translation.csv | tradução das categorias | product_category_name |
| olist_geolocation_dataset.csv | coordenadas por prefixo de CEP | geolocation_zip_code_prefix |

### Licença
✏️ Confirmar na página do Kaggle. O dataset é publicado sob **CC BY-NC-SA 4.0**: permite uso, adaptação e compartilhamento para fins **não comerciais**, com atribuição à Olist e compartilhamento sob a mesma licença. O uso acadêmico neste MVP está dentro dessas condições.

---

## 2. Carga dos Dados (Etapa 4.2)

1. Download manual dos 9 CSVs no Kaggle.
2. Criação do volume `workspace.raw.olist` no Unity Catalog (notebook `00_setup.sql`).
3. Upload dos arquivos pela interface do Catalog Explorer para `/Volumes/workspace/raw/olist/`.
4. Ingestão para tabelas Delta no schema `bronze` pelo notebook [`01_bronze_ingestao.py`](01_bronze_ingestao.py): todas as colunas lidas como texto, sem transformação, com os metadados `_ingestion_ts` e `_source_file`.

✏️ Screenshots: arquivos no volume e tabelas no schema bronze.

---

## 3. Modelagem e Catálogo de Dados (Etapa 4.3)

Modelo dimensional em **esquema estrela com duas tabelas fato** que compartilham dimensões (constelação de fatos), na camada Gold:

```
                 dim_tempo
                     |
 dim_cliente --- fato_itens_pedido --- dim_produto
      |              |
      |          dim_vendedor
      |
 fato_pagamentos --- dim_tempo
```

- **fato_itens_pedido**: grão = item de pedido. Responde às perguntas 1, 2 e 3.
- **fato_pagamentos**: grão = pagamento de um pedido. Responde à pergunta 4. Fica separada porque pagamentos são registrados por pedido, e não por item; juntá-la à outra fato duplicaria valores.

O catálogo foi gravado no **Unity Catalog** como comentários de tabela e de coluna (notebook [`04_gold_modelagem.py`](04_gold_modelagem.py)) e pode ser consultado em `workspace.information_schema.columns`. Transcrição:

### dim_cliente
Um registro por id_cliente. Linhagem: bronze.customers → silver.customers.

| Coluna | Tipo | Descrição e domínio |
|---|---|---|
| id_cliente | string | PK. Cliente no contexto do pedido (hash 32 caracteres) |
| id_cliente_unico | string | Pessoa; pode ter vários id_cliente |
| cep_prefixo | string | 5 primeiros dígitos do CEP, completados com zeros à esquerda |
| cidade | string | Cidade em minúsculas |
| uf | string | Sigla da UF (27 valores) |
| regiao | string | Norte, Nordeste, Centro-Oeste, Sudeste, Sul (derivada da UF) |

### dim_vendedor
Um registro por vendedor. Linhagem: bronze.sellers → silver.sellers.

| Coluna | Tipo | Descrição e domínio |
|---|---|---|
| id_vendedor | string | PK (hash 32 caracteres) |
| cep_prefixo | string | 5 primeiros dígitos do CEP |
| cidade | string | Cidade em minúsculas |
| uf | string | Sigla da UF |
| regiao | string | Região derivada da UF |

### dim_produto
Um registro por produto. Linhagem: bronze.products + bronze.product_category_name_translation → silver.products (JOIN pela categoria).

| Coluna | Tipo | Descrição e domínio |
|---|---|---|
| id_produto | string | PK (hash 32 caracteres) |
| categoria | string | Categoria em português; nulos viram `sem_categoria` |
| categoria_en | string | Categoria em inglês (JOIN com a tradução) |
| qtd_fotos | int | Fotos no anúncio, ≥ 1 |
| peso_g | int | Peso em gramas, > 0 (zero convertido em nulo) |
| comprimento_cm, altura_cm, largura_cm | int | Dimensões da embalagem, > 0 |

### dim_tempo
Um registro por dia, gerado do primeiro ao último dia de compra.

| Coluna | Tipo | Descrição e domínio |
|---|---|---|
| sk_data | int | PK, formato AAAAMMDD |
| data | date | Data |
| ano | int | 2016 a 2018 |
| trimestre | int | 1 a 4 |
| mes | int | 1 a 12 |
| ano_mes | string | AAAA-MM |
| dia_semana_num | int | 1 (domingo) a 7 (sábado) |
| fim_de_semana | boolean | Sábado ou domingo |

### fato_itens_pedido
Grão: item de pedido. Linhagem: silver.order_items JOIN silver.orders JOIN silver.order_reviews.

| Coluna | Tipo | Descrição e domínio |
|---|---|---|
| id_pedido, num_item | string, int | PK composta |
| id_cliente | string | FK → dim_cliente |
| id_produto | string | FK → dim_produto |
| id_vendedor | string | FK → dim_vendedor |
| sk_data_compra | int | FK → dim_tempo |
| status_pedido | string | delivered, shipped, canceled, unavailable, invoiced, processing, created, approved |
| dt_compra | timestamp | Data e hora da compra |
| dt_entrega | timestamp | Entrega ao cliente; nula se não entregue |
| dt_entrega_estimada | timestamp | Data prometida |
| prazo_entrega_dias | int | Compra → entrega, ≥ 0 |
| atraso_dias | int | Entrega real − estimada (negativo = adiantado) |
| flag_atraso | boolean | atraso_dias > 0 |
| valor_produto | decimal(12,2) | Preço do item em R$, > 0 |
| valor_frete | decimal(12,2) | Frete em R$, ≥ 0 |
| valor_total_item | decimal | Produto + frete |
| nota_avaliacao | int | 1 a 5; nula sem avaliação |

### fato_pagamentos
Grão: pagamento de um pedido. Linhagem: silver.order_payments JOIN silver.orders.

| Coluna | Tipo | Descrição e domínio |
|---|---|---|
| id_pedido, seq_pagamento | string, int | PK composta |
| id_cliente | string | FK → dim_cliente |
| sk_data_compra | int | FK → dim_tempo |
| forma_pagamento | string | credit_card, boleto, voucher, debit_card, not_defined |
| qtd_parcelas | int | ≥ 1 |
| valor_pagamento | decimal(12,2) | Valor em R$, ≥ 0 |

✏️ Screenshots: Catalog Explorer com os schemas, uma tabela com os comentários das colunas e a aba Lineage de `fato_itens_pedido`.

---

## 4. Pipeline de Dados (Etapa 4.4)

O pipeline foi ramificado em **um notebook por etapa**, executados em sequência:

| Notebook | Entrada | Saída | O que faz |
|---|---|---|---|
| [00_setup](00_setup.sql) | — | schemas e volume | Prepara a estrutura medalhão |
| [01_bronze_ingestao](01_bronze_ingestao.py) | CSVs no volume | 9 tabelas `bronze.*` | Ingestão fiel + metadados |
| [02_qualidade_dados](02_qualidade_dados.py) | `bronze.*` | `bronze.dq_*` | Diagnóstico de qualidade |
| [03_silver_transformacao](03_silver_transformacao.py) | `bronze.*` | 7 tabelas `silver.*` | Limpeza, tipagem, deduplicação, JOIN de tradução |
| [04_gold_modelagem](04_gold_modelagem.py) | `silver.*` | 6 tabelas `gold.*` | Modelo estrela + catálogo |
| [05_analises](05_analises.sql) | `gold.*` | resultados | Respostas às perguntas |

Principais transformações documentadas:
- **JOIN produtos × tradução** pelo campo `product_category_name` para enriquecer cada produto com a categoria em inglês.
- **JOIN itens × pedidos × avaliações** por `order_id` para montar a fato com valores, datas e nota em uma única tabela.
- **Avaliações**: mantida apenas a mais recente por pedido (janela `row_number`), para não duplicar itens no JOIN.
- **Cálculos na Gold**: prazo de entrega, dias de atraso, flag de atraso e região a partir da UF.
- Validações com `assert` na Silver (unicidade de chaves) e na Gold (nenhum item perdido no JOIN).

✏️ Screenshots: `SHOW TABLES` de cada schema e a execução bem-sucedida dos notebooks.

---

## 5. Qualidade de Dados (Etapa 4.5)

Diagnóstico sobre a Bronze, antes de qualquer tratamento, persistido em `bronze.dq_perfil_colunas` (nulos e cardinalidade de cada coluna) e `bronze.dq_verificacoes` (verificações específicas).

✏️ Preencher a coluna "Ocorrências" com o resultado de `bronze.dq_verificacoes`:

| Dimensão | Verificação | Ocorrências | Tratamento na Silver |
|---|---|---|---|
| Unicidade | Pedidos com mais de uma avaliação | ✏️ | Mantida a avaliação mais recente |
| Completude | Pedido entregue sem data de entrega | ✏️ | Excluído das métricas de prazo |
| Completude | Produto sem categoria | ✏️ | `sem_categoria` |
| Consistência | Entrega anterior à compra | ✏️ | Data de entrega anulada |
| Consistência | CEP com tamanho ≠ 5 | ✏️ | lpad com zeros |
| Consistência | Categoria sem tradução | ✏️ | Mantém nome em português |
| Acurácia | Pagamento com 0 parcelas | ✏️ | Ajustado para 1 |
| Acurácia | Forma de pagamento not_defined | ✏️ | Filtrado nas análises |
| Acurácia | Peso do produto = 0 | ✏️ | Anulado |
| Outliers | Preço acima de Q3 + 1,5×IQR | ✏️ | Mantidos (vendas reais) |

✏️ Screenshots das duas tabelas de qualidade.

---

## 6. Análise de Dados (Etapa 4.5)

✏️ Para cada pergunta: screenshot do resultado (tabela ou gráfico) e 1 a 2 parágrafos de discussão.

### Pergunta 1 – Receita por categoria
### Pergunta 2 – Atraso × nota de avaliação
### Pergunta 3 – Prazo e frete por estado
### Pergunta 4 – Formas de pagamento e parcelamento
### Discussão geral

---

## 7. Autoavaliação

✏️ Pontos a cobrir:
- Quais perguntas foram respondidas integralmente e quais parcialmente, e por quê.
- Dificuldades encontradas (ex.: CSV com textos multilinha, avaliações duplicadas por pedido, pagamentos em grão diferente dos itens).
- Trabalhos futuros: incluir a geolocalização para calcular distância vendedor → cliente; cruzar com dados do IBGE (população e renda por UF); agendar o pipeline com Databricks Jobs; análise de sentimento dos comentários das avaliações.
