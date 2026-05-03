# 🏅 Camada Gold

A camada gold é a camada final do pipeline de dados. Responsável por **agregar, consolidar e disponibilizar** os dados prontos para consumo analítico por Analistas de BI e liderança das áreas de Operações, Comercial e Atendimento.

---

## Decisões de design

### Granularidade temporal
As tabelas gold são agregadas em **mês/ano** (`yyyy-MM`), pois análises de negócio raramente ocorrem no nível de dia. Isso reduz o volume de dados e melhora a performance das consultas no BI.

### Separação por dimensão
Cada tabela gold foi construída ao redor de uma dimensão principal (produto, cliente, entrega, atendimento), evitando tabelas genéricas demais que dificultam a manutenção e a legibilidade.

### Manutenção do `status_order`
O campo `status_order` é mantido em todas as golds para que pedidos **cancelados não mascarem os resultados**. Filtrar cancelados antes da agregação inflaria métricas como receita e ticket médio, distorcendo a análise real do negócio.

### High-level API SQL
Todas as tabelas gold foram escritas em **Spark SQL** (`%sql`), que passa automaticamente pelo **Catalyst Optimizer** do Databricks — pipeline de otimização que aplica predicate pushdown, eliminação de colunas desnecessárias, escolha do melhor plano de join e geração de código vetorizado via Tungsten. O resultado é equivalente ao PySpark DataFrame API em performance, com maior legibilidade para revisão e documentação.

---

## Melhorias futuras

| Técnica | Quando usar | Benefício |
|---|---|---|
| `BROADCAST` hint | Joins com tabelas de dimensão pequenas | Evita shuffle, reduz tempo de join |
| `CACHE TABLE` | Dimensões reutilizadas em múltiplas queries | Evita releitura do disco a cada execução |
| `CLUSTER BY` (Liquid Clustering) | Após definição dos padrões de consulta do BI | Melhora performance de filtros nas colunas mais usadas |

```sql
-- BROADCAST
SELECT /*+ BROADCAST(v) */
  p.order_id, v.canal_id
FROM workspace.silver.tb_pedidos_cabecalho p
LEFT JOIN workspace.silver.dim_vendedores v ON p.seller_id = v.seller_id

-- CACHE
CACHE TABLE workspace.silver.dim_vendedores;
CACHE TABLE workspace.silver.dim_canais;

-- LIQUID CLUSTERING (exemplo futuro)
CREATE OR REPLACE TABLE workspace.gold.gold_comercial_produto
CLUSTER BY (order_date, category)
AS SELECT ...
```

---

## Tabelas geradas

| Tabela | Fontes principais | Pergunta respondida |
|---|---|---|
| `gold.gold_comercial_produto` | `tb_pedidos_cabecalho` + `tb_pedidos_itens` + `dim_produto` | Quais categorias e produtos vendem mais? |
| `gold.gold_comercial_clientes` | `tb_pedidos_cabecalho` + `dim_clientes` + `dim_vendedores` | Como cada cliente se comporta ao longo do tempo? |
| `gold.gold_entrega_pedido` | `tb_entrega` + `tb_pedidos_cabecalho` | Onde estão os gargalos logísticos e de prazo? |
| `gold.gold_atendimentos_pedido` | `tb_atendimentos` + `tb_pedidos_cabecalho` | Quais são os principais motivos de ocorrência e criticidade? |

---

## Detalhamento por tabela

### `gold.gold_comercial_produto`
- **Fontes:** `silver.tb_pedidos_cabecalho` + `silver.tb_pedidos_itens` + `silver.dim_produto`
- **Granularidade:** `order_date` (mês/ano) + `status_order` + `category` + `name`
- **Objetivo:** visão comercial segmentada por categoria e produto — permite identificar os itens de maior e menor desempenho e acompanhar a evolução temporal de cada produto
- **Métricas:**

| Métrica | Descrição |
|---|---|
| `receita_bruta` | Soma do `gross_amount_num` |
| `receita_liquida` | Soma do `net_amount` |
| `desconto_total` | Soma do `discount_amount` |
| `perc_desconto` | Percentual de desconto sobre receita bruta |
| `ticket_medio` | Média do `gross_amount_num` |
| `total_itens` | Soma das quantidades vendidas |

---

### `gold.gold_comercial_clientes`
- **Fontes:** `silver.tb_pedidos_cabecalho` + `silver.dim_clientes` + `silver.dim_vendedores`
- **Granularidade:** `ano_mes` + `status_order` + `customer_id` + `canal_id` + `regional_code`
- **Objetivo:** visão do comportamento de compra por cliente ao longo do tempo, cruzando perfil cadastral (segmento, porte, cidade, estado) com performance comercial e dimensões de canal e região
- **Observação:** inclui `canal_id` e `regional_code` via `dim_vendedores` — permite segmentar clientes pelo canal pelo qual foram atendidos
- **Métricas:**

| Métrica | Descrição |
|---|---|
| `total_pedidos` | Quantidade de pedidos distintos |
| `receita_bruta` | Soma do `gross_amount_num` |
| `receita_liquida` | Soma do `net_amount` |
| `desconto_total` | Soma do `discount_amount` |
| `perc_desconto` | Percentual de desconto sobre receita bruta |
| `ticket_medio` | Média do `gross_amount_num` |

---

### `gold.gold_entrega_pedido`
- **Fontes:** `silver.tb_entrega` + `silver.tb_pedidos_cabecalho`
- **Granularidade:** `ano_mes` + `status_order` + `carrier_name` + `carrier_mode` + `delivery_status` + `state` + `city`
- **Objetivo:** visão operacional de entregas — permite identificar gargalos por transportadora, modal, região e status de entrega, além de medir cumprimento de prazo
- **Normalização do `delivery_status`:**

| Valor original | Valor normalizado |
|---|---|
| `delivered` | `Entregue` |
| `in_transit` | `Em Trânsito` |
| `atrasado` | `Atrasado` |
| `cancelled` | `Cancelado` |
| outros | `NULL` |

- **Métricas:**

| Métrica | Descrição |
|---|---|
| `total_entregas` | Contagem de entregas distintas (`order_ref`) |
| `custo_total_frete` | Soma do `cost` |
| `custo_medio_frete` | Média do `cost` |
| `media_dias_ate_expedicao` | Média de dias entre `order_date` e `shipped_at` |
| `media_dias_transito` | Média de dias entre `shipped_at` e `delivered_at` |
| `media_dias_ciclo_total` | Média de dias entre `order_date` e `delivered_at` |
| `media_dias_atraso` | Média de dias entre `delivered_at` e `promised_date` |
| `total_atrasados` | Entregas com `delivered_at > promised_date` |
| `perc_atrasados` | Percentual de entregas atrasadas sobre o total |
| `receita_bruta` | Soma do `gross_amount_num` do pedido vinculado |
| `receita_liquida` | Soma do `net_amount` do pedido vinculado |

---

### `gold.gold_atendimentos_pedido`
- **Fontes:** `silver.tb_atendimentos` + `silver.tb_pedidos_cabecalho`
- **Granularidade:** `ano_mes` + `event_type` + `severity` + `status_atendimento` + `seller_id` + `status_order`
- **Objetivo:** visão de atendimento e ocorrências — permite identificar os principais motivos de contato, criticidade dos tickets, tempo de resposta e impacto financeiro dos pedidos afetados
- **Normalizações aplicadas:**

| Campo | Valor original | Valor normalizado |
|---|---|---|
| `event_type` | `Delay` | `Atraso` |
| `event_type` | `Refund` | `Reembolso` |
| `event_type` | `Troca` | `Troca` |
| `event_type` | `Complaint` | `Reclamação` |
| `event_type` | `Cancel_request` | `Cancelamento` |
| `status` | `Open` | `Aberto` |
| `status` | `Closed` | `Encerrado` |

- **Métricas:**

| Métrica | Descrição |
|---|---|
| `total_tickets` | Contagem de tickets distintos |
| `tickets_high` | Tickets com severity `High` |
| `tickets_medium` | Tickets com severity `Medium` |
| `tickets_low` | Tickets com severity `Low` |
| `tickets_abertos` | Tickets com status `Open` |
| `tickets_encerrados` | Tickets com status `Closed` |
| `perc_abertos` | Percentual de tickets ainda abertos |
| `tickets_criticos` | Tickets abertos há mais de 7 dias |
| `media_dias_pedido_ate_ticket` | Média de dias entre `order_date` e abertura do ticket |
| `receita_bruta` | Soma do `gross_amount_num` dos pedidos afetados |
| `receita_liquida` | Soma do `net_amount` dos pedidos afetados |
| `desconto_total` | Soma do `discount_amount` dos pedidos afetados |
| `total_pedidos_afetados` | Quantidade de pedidos distintos com ocorrência |

---

## Padrão de gravação

```sql
CREATE OR REPLACE TABLE workspace.gold.<tabela> AS
SELECT ...
```

O `CREATE OR REPLACE` recria a tabela a cada execução com os dados mais recentes das camadas silver, garantindo consistência sem necessidade de lógica incremental.

---

## Camadas anteriores

As tabelas gold dependem das camadas silver estarem atualizadas antes da execução.
Ver: `README_bronze.md` e `README_silver.md`
