# MVP – Pipeline de Dados na Nuvem: E-commerce Olist no Databricks

> MVP da disciplina de Engenharia de Dados (MBA PUC-Rio). Pipeline de ponta a ponta no **Databricks Free Edition** seguindo a **Arquitetura Medalhão** (Bronze → Silver → Gold).
> 

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
   Conforme informado na página do dataset no Kaggle, ele é publicado sob **CC BY-NC-SA 4.0**: permite uso, adaptação e compartilhamento para fins **não comerciais**, com atribuição à Olist e compartilhamento sob a mesma licença. O uso acadêmico neste MVP está dentro dessas condições.

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

Resultado de `bronze.dq_verificacoes`:

| Dimensão | Verificação | Ocorrências | Tratamento na Silver |
|---|---|---|---|
| Unicidade | Pedidos com mais de uma avaliação | 547 | Mantida a avaliação mais recente |
| Completude | Pedido entregue sem data de entrega | 8 | Excluído das métricas de prazo |
| Completude | Produto sem categoria | 610 | `sem_categoria` |
| Consistência | Entrega anterior à compra | 0 | Data de entrega anulada |
| Consistência | CEP com tamanho ≠ 5 | 0 | lpad com zeros |
| Consistência | Categoria sem tradução | 2 | Mantém nome em português |
| Acurácia | Pagamento com 0 parcelas | 2 | Ajustado para 1 |
| Acurácia | Forma de pagamento not_defined | 3 | Filtrado nas análises |
| Acurácia | Peso do produto = 0 | 4 | Anulado |
| Outliers | Preço acima de Q3 + 1,5×IQR | 8427 | Mantidos (vendas reais) |

✏️ Screenshots das duas tabelas de qualidade.

---

## 6. Análise de Dados (Etapa 4.5)

✏️ Para cada pergunta: screenshot do resultado (tabela ou gráfico) e 1 a 2 parágrafos de discussão.

### Pergunta 1 – Receita por categoria

A receita é relativamente pulverizada: nenhuma categoria passa de 10% do total, mas as 10 maiores somam cerca de 62% dela. Beleza e saúde lidera (R$ 1,26 milhão; 9,3%), seguida de relógios e presentes (R$ 1,20 milhão; 8,9%) e cama, mesa e banho (R$ 1,04 milhão; 7,7%). O volume de pedidos conta uma história diferente da receita: cama, mesa e banho tem o maior número de pedidos (9.399), mas com tíquete médio de cerca de R$ 110, enquanto relógios e presentes chega ao segundo lugar em receita com apenas 5.604 pedidos, a um tíquete de cerca de R$ 214. Na evolução mensal, as cinco maiores categorias cresceram entre 5 e 15 vezes de jan/2017 a ago/2018, um reflexo da expansão da própria plataforma. Beleza e saúde consolidou a liderança no fim do período (R$ 120 mil em ago/2018, contra R$ 12,6 mil em jan/2017), enquanto informática e acessórios, que chegou a liderar em mar/2017, terminou em quinto lugar.

### Pergunta 2 – Atraso × nota de avaliação

Sim, o atraso é um dos fatores que mais derrubam a satisfação. Pedidos entregues no prazo ou antes têm nota média de 4,29, e só 9,3% deles recebem nota 1 ou 2. Com apenas 1 a 3 dias de atraso, a média já cai um ponto inteiro (3,29) e as notas ruins sobem para 32,1%. A partir de uma semana de atraso, a avaliação praticamente colapsa: média entre 1,67 e 1,72 e cerca de 80% de notas 1 ou 2. A correlação linear de −0,27 parece modesta, mas é atenuada por dois fatores: 93,3% dos pedidos chegam no prazo, e a relação não é linear, pois a nota despenca nos primeiros dias de atraso e estabiliza no patamar mínimo depois disso. O dado mais relevante para o negócio é que os 6,7% de pedidos atrasados concentram boa parte das avaliações negativas.

### Pergunta 3 – Prazo e frete por estado

Há uma desigualdade logística clara entre as regiões. São Paulo, com 40.494 pedidos (cerca de 42% dos pedidos entregues), tem o menor prazo médio (8,7 dias; mediana de 7) e uma das menores taxas de atraso (4,5%). No outro extremo, estados do Norte como Roraima (29,3 dias), Amapá (27,2) e Amazonas (26,4) esperam mais de três vezes esse prazo, e os nove primeiros do ranking são todos do Norte ou do Nordeste. Prazo longo, porém, não significa necessariamente atraso: Amazonas (2,8%) e Amapá (3,0%) têm taxas de atraso baixas, o que indica que a data prometida já considera a distância.

As maiores taxas de atraso estão em Alagoas (21,4%), Maranhão (17,4%) e Sergipe (15,2%), e chama atenção o Rio de Janeiro (12,1%), o segundo maior mercado, com um índice quase três vezes maior que o de SP e MG. O custo do frete segue o mesmo padrão geográfico: em São Paulo ele equivale a 13,9% do valor dos produtos, o menor índice do país, enquanto no Norte e no Nordeste fica tipicamente entre 20% e 28%, com os maiores pesos em Roraima (28,1%), Maranhão (26,3%) e Amazonas (24,5%). Nas regiões Sul, Sudeste e Centro-Oeste, os estados ficam numa faixa intermediária, entre 16% e 19%. Ou seja, o cliente do Norte e do Nordeste não só espera mais pela entrega, como paga proporcionalmente até o dobro de frete que o paulista.

### Pergunta 4 – Formas de pagamento e parcelamento

O cartão de crédito domina: responde por 78,3% do valor transacionado e aparece em 76.505 pedidos. O boleto vem a seguir, com 17,9%, e voucher (2,4%) e débito (1,4%) têm participação marginal. O parcelamento cresce de forma consistente com o valor do pedido. Até R$ 50, a média é de 1,7 parcela e 38,8% das compras são parceladas; acima de R$ 500, esses números sobem para 7,1 parcelas e 91,8%. Na faixa de R$ 100 a 200, que concentra o maior número de pedidos com cartão (25.112), três em cada quatro compras já são parceladas. Isso mostra que o parcelamento é um viabilizador importante das compras de maior valor no e-commerce brasileiro.

### Discussão geral

As quatro análises ajudam a responder ao problema original: o que sustenta a receita e o que determina a satisfação dos clientes. Do lado da receita, o marketplace tem uma base diversificada, sem dependência de uma única categoria, mas com liderança crescente de beleza e saúde e categorias de tíquete alto, como relógios e presentes, contribuindo de forma desproporcional ao seu volume de pedidos. Essa receita é fortemente apoiada no cartão de crédito parcelado, especialmente nas faixas de maior valor. Em outras palavras, o crédito é um componente estrutural da conversão, e não apenas uma opção de pagamento.

Do lado da satisfação, o principal achado é que o cumprimento do prazo prometido pesa mais do que a rapidez da entrega. Estados do Norte recebem em quase 30 dias e ainda assim têm poucos atrasos. Já poucos dias de atraso bastam para derrubar a nota média em um ponto, e uma semana leva a maioria dos clientes a avaliar com nota 1 ou 2. Como a logística é desigual entre regiões, o risco de insatisfação também é: ele se concentra em estados como Alagoas, Maranhão, Sergipe e Rio de Janeiro, onde a taxa de atraso é de duas a cinco vezes a de São Paulo. Soma-se a isso o custo: nessas regiões o frete pesa até o dobro do observado em São Paulo em relação ao valor da compra, o que encarece o acesso ao marketplace justamente onde a experiência de entrega já é mais longa.

Para o negócio, isso aponta para duas alavancas práticas: prometer datas realistas por região, protegendo a avaliação mesmo quando a entrega é longa, e atacar os gargalos específicos dos estados com maior taxa de atraso, com atenção especial ao Rio de Janeiro pelo seu volume. Vale registrar as limitações da análise: as relações encontradas são associações, e não prova de causalidade, e os dados cobrem apenas 2016 a 2018, período de forte expansão da plataforma, o que pode não refletir o comportamento atual.
---

## 7. Autoavaliação

O objetivo traçado no início do trabalho foi atingido: as quatro perguntas de negócio foram respondidas integralmente com os dados disponíveis, e o pipeline foi construído de ponta a ponta no Databricks Free Edition, da ingestão dos arquivos brutos à camada Gold modelada em esquema estrela, com o catálogo de dados documentado no Unity Catalog. As respostas conversaram entre si e permitiram uma leitura coerente do problema original, conectando receita, meios de pagamento, logística regional e satisfação do cliente.

A principal dificuldade surgiu na etapa inicial, na definição das perguntas. Formular questões que fossem, ao mesmo tempo, relevantes para o negócio e respondíveis com a base escolhida exigiu mais reflexão do que eu imaginava, e ficou claro na prática por que o documento do MVP insiste em "começar pelo porquê". Outro desafio foi a navegação no Databricks: embora eu tenha contato com a plataforma no meu trabalho, atuo como Data Product Owner, acompanhando a evolução das demandas que os engenheiros de dados executam, e não operando diretamente o ambiente. Localizar recursos como volumes, Git folders, compute e o Catalog Explorer demandou uma curva de adaptação.

Utilizei auxílio de inteligência artificial para a construção dos códigos em PySpark e SQL, e para entender os tratamentos que seriam necessários. Minha experiência prévia era concentrada em Power BI, com conhecimento de DAX e noções básicas de SQL, que vim aprimorando ao longo das aulas e exercícios práticos do curso. A arquitetura medalhão foi um grande facilitador nesse processo: ela estabelece uma linha de raciocínio coerente e estruturada do início ao fim, guiando o passo a passo macro que um pipeline deve seguir (preservar o dado bruto, depois limpar e padronizar, e só então modelar para o consumo). Isso tornou mais fácil entender o papel de cada etapa e de cada transformação. Como resultado, o trabalho também ampliou minha visão sobre o dia a dia dos engenheiros de dados com quem atuo profissionalmente.

Como trabalhos futuros para enriquecer o projeto, identifico:

- conectar a camada Gold ao Power BI para construir um dashboard de acompanhamento, unindo o pipeline à minha experiência atual com visualização;
- utilizar a tabela de geolocalização, hoje mantida apenas na Bronze, para calcular a distância entre vendedor e cliente e avaliar seu efeito sobre prazo e frete;
- cruzar os dados com informações do IBGE, como população e renda por UF, para normalizar os indicadores regionais;
- agendar o pipeline com Databricks Jobs, tornando a execução automática;
- aplicar análise de sentimento aos comentários das avaliações, para entender o que motiva as notas baixas além do atraso.
