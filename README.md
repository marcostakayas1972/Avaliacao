# Case Técnico — Engenheiro de Dados

## Visão geral

Este projeto estrutura uma solução completa de engenharia de dados para uma empresa de serviços com operação nacional. Os dados do negócio estavam distribuídos em diferentes fontes brutas — arquivos CSV, JSON, NDJSON, Excel e TXT — sem padronização, com problemas de qualidade e sem uma base consolidada para consumo analítico.

A solução transforma essas fontes em uma base analítica organizada, confiável e pronta para consumo por Analistas de BI, utilizando uma arquitetura em três camadas: **Bronze → Silver → Gold**.

---

## Arquitetura da solução

```
sources/
├── atendimento_ocorrencias.ndjson
├── logistica_entregas.json
├── cadastro_produtos_api_dump.json
├── crm_clientes_export.xlsx
├── comercial_canais.xlsx
├── erp_pedidos_itens_2025.csv
├── erp_pedidos_cabecalho_2025.csv
├── vendedores.csv
└── legado_regioes_pipe.txt
        │
        ▼
┌─────────────┐
│   BRONZE    │  Ingestão bruta — sem transformações
└─────────────┘
        │
        ▼
┌─────────────┐
│   SILVER    │  Limpeza, padronização e qualidade
└─────────────┘
        │
        ▼
┌─────────────┐
│    GOLD     │  Agregações analíticas prontas para BI
└─────────────┘
```

---

## Estrutura do repositório

```
/
├── +
│   ├── [01] - Carga_Bronze.py        # Ingestão das fontes brutas
│   ├── [02] - Carga_Silver.py        # Transformações e qualidade
│   ├── [03] - Limpeza_Silver.py      # Validações e decisões sobre nulos
│   └── [04] - Carga_Gold.sql         # Tabelas analíticas agregadas
├── +
│   ├── README_bronze.md
│   ├── README_silver.md
│   └── README_gold.md
└── README.md               # Este arquivo
```

---

## Principais decisões técnicas

### Camada Bronze
- Dados ingeridos **sem transformação** — preserva o estado original para rastreabilidade
- Arquivos Excel lidos via `pandas + openpyxl` e convertidos para Spark DataFrame
- JSONs com estrutura aninhada achatados já na ingestão (`carrier.name`, `timestamps.shipped_at` etc.)
- Todas as tabelas gravadas em formato **Delta** com `mode=overwrite`

### Camada Silver
- Função `parse_date()` centraliza o tratamento de datas em múltiplos formatos (`dd/MM/yyyy`, `yyyy-MM-dd`, ISO etc.) usando `try_to_date` para tolerância a erros
- Normalização de UF via `CASE WHEN` com `isin()` — aceita sigla, nome por extenso e variações com/sem acento
- Padronização textual: `UPPER` para códigos e identificadores, `INITCAP` para nomes e descrições, `TRIM` em todos os campos string
- Valores nulos **mantidos** — a ausência de informação é um dado válido e removê-la distorceria as análises
- Registros inválidos removidos com justificativa documentada (`dim_regioes`, `dim_canais`)

### Camada Gold
- Agregação em **mês/ano** (`yyyy-MM`) — análises de negócio raramente ocorrem no nível de dia
- `status_order` mantido em todas as tabelas para que pedidos cancelados **não mascarem** receita e ticket médio
- Tabelas separadas por dimensão principal para facilitar filtros no BI sem joins desnecessários

---

## Principais problemas encontrados nos dados

| Problema | Tabela | Tratamento |
|---|---|---|
| Datas em múltiplos formatos | Várias | `parse_date()` com `try_to_date` em cascata |
| `regional_code` por extenso (`sul`) | `dim_vendedores` | Mapeamento `sul → S` |
| UF por extenso (`Rio De Janeiro`) | `dim_clientes`, `tb_entrega` | CASE WHEN com todos os estados |
| `canal_id` em minúsculo (`ch07`) | `dim_vendedores` | `UPPER()` |
| Duplicatas de `seller_id` | `dim_vendedores` | Identificadas e documentadas |
| `gross_amount` com vírgula decimal e `N/A` | `tb_pedidos_cabecalho` | `regexp_replace + cast` |
| `cost` com valor `unknown` | `tb_entrega` | `WHEN lower(cost) = 'unknown' THEN NULL` |
| `delivery_status` inconsistente | `tb_entrega` | Normalizado na gold via CASE WHEN |
| `event_type` em inglês | `tb_atendimentos` | Traduzido na gold |
| Campos nulos generalizados | Várias | Mantidos — decisão documentada |
| Registro `CH06` sem vendedores | `dim_canais` | Removido da silver |
| Registros inválidos em regiões | `dim_regioes` | Filtro `NOT IN ('sul', 'XX')` |

---

## Tabelas gold e perguntas respondidas

| Tabela | Pergunta respondida |
|---|---|
| `gold_comercial_produto` | Quais categorias e produtos vendem mais? |
| `gold_comercial_clientes` | Como cada cliente se comporta ao longo do tempo? Como o negócio performou por canal e região?|
| `gold_entrega_pedido` | Onde estão os gargalos logísticos e de prazo? |
| `gold_atendimentos_pedido` | Quais são os principais motivos de ocorrência e criticidade? |

Todas as tabelas permitem segmentação por **período e status**, e estão prontas para consumo direto no BI sem necessidade de tratamentos adicionais.

---

## Limitações conhecidas

- `customer_code` em `tb_atendimentos` está quase inteiramente nulo — impossibilita análise de atendimento por cliente
- Vendedores inativos ou sem canal/região representam ~50% das vendas — mantidos para não distorcer o volume real
- Obviamente na versão free do databricks não dá pra implementar grandes melhorias como em um cluster pago
---

## Próximos passos recomendados

- Implementar **Liquid Clustering** nas tabelas gold após análise dos padrões de consulta do BI
- Analise profunda dos JOINS em busca de otimização em processamento distribuido, as vezes usar o **Photon** não resolve
- Aplicar **BROADCAST** nas dimensões pequenas para otimizar performance dos joins
- Analisar a entrada de dados em busca de reduzir o tempo, como por exemplo **spark.sql.files.maxPartitionBytes** ser aumentado dependendo do volume de entrada futura.
- Investigar a origem do `customer_code` nulo nos atendimentos com a área de negócio
- Evoluir o pipeline para **ingestão incremental** (Structured Streaming ou DLT) em vez de overwrite total
- Implementar **testes de qualidade automatizados** (ex: Great Expectations ou dbt tests) nas camadas silver e gold
- Criar um notebook de **monitoramento de dados** com alertas para anomalias (volume, nulos, duplicatas)
- **SCD2** pode ser implementado nas dimensões, mas prefiro modelar em **Data Vault** eliminando essa necessidade
- Construi a camada silver com a **High-level API PySpark**, porque quem não conhece processamento distribuido acha que a **High-level API SQL** é lenta, pelo contrário SQL é mais rápida que o PySpark, porque ambas passam pelo **Catalyst Optimizer** para chegar no **Lower-Level**. Só que o PySpark ainda precisa ser convertido para SQL. PySpark é para ingestão, na transformação depende muito do conhecimento de processamento distribuido do engenheiro para decidir qual **High-level API** usar.
- Pode ser necessário criar uma gold como fato geral com todas as dimensões ligadas à ela, para uma visão geral e ampla, mas a quantidade de linhas sobe bastante, nessa modelagem evitei ao máximo chegar perto do grão. 
---

## Ambiente

- **Plataforma:** Databricks Community Edition
- **Linguagem:** Python / PySpark / Spark SQL
- **Formato de armazenamento:** Delta Lake
- **Versionamento:** GitHub

## Modelo Entidade-Relacionamento

```
+------------------+     +--------------------+     +------------------------+
| dim_canais       |     | dim_vendedores     |     | tb_pedidos_cabecalho   |
|------------------|     |--------------------|     |------------------------|
| PK id_canal      |<----| FK canal_id        |<----| FK seller_id           |
| nome_canal       |     | PK seller_id       |     | PK order_id            |
| tipo_canal       |     | seller_name        |     | FK customer_code       |
| ativo            |     | FK regional_code   |     | status_order           |
| observacao       |     | hire_date          |     | order_date             |
+------------------+     | status             |     | promised_date          |
                         +--------------------+     | gross_amount_num       |
+------------------+          ^                     | discount_amount        |
| dim_regioes      |          |                     | net_amount             |       +--------------------+
|------------------|          |   +-----------------| payment_details        | <---- | tb_atendimentos    |
| PK regional_code |----------+   |                 | last_update            |       |--------------------|
| regional_name    |              |                 +------------------------+       | PK ticket_id       |
| state            |              |                       |           |              | FK order_id        |
| manager_name     |              |                       | 1         | 1            | FK customer_code   |
| active_flag      |              |                       |           |              | event_type         |
+------------------+              |                       v           v              | severity           |
                                  |                       N           N              | status             |
+------------------+              |       +------------------+   +----------------+  | created_at         |
| dim_clientes     |              |       | tb_pedidos_itens |   | tb_entrega     |  | metadata           |
|------------------|              |       | -----------------|   |----------------|  +--------------------+
| PK customer_id   |<-------------+       | FK order_id      |   | PK delivery_id |
| nome_cliente     |  1           N       | PK item_seq      |   | FK order_ref   |
| segmento         |                      | FK product_code  |   | carrier_name   |
| porte            |                      | code             |   | carrier_mode   |
| cidade           |                      | quantity         |   | delivery_status|
| estado           |                      | unit_price       |   | shipped_at     |
| status_cliente   |                      | total_item       |   | delivered_at   |
| data_cadastro    |                      | item_status      |   | state          |
| email            |                      +------------------+   | city           |
| updated_at       |                              ^              | cost           |
+------------------+                              |              +----------------+
                                                  |
+------------------+                              |
| dim_produto      |                              |
|------------------|                              |
| PK product_id    |------------------------------+
| name             |  1                        N
| category         |
| subcategory      |
| status           |
| list_price       |
| currency         |
| family           |
| tags             |
| updated_at       |
+------------------+
```
