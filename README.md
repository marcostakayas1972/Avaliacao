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
├── notebooks/
│   ├── 01_bronze.py        # Ingestão das fontes brutas
│   ├── 02_silver.py        # Transformações e qualidade
│   ├── 03_gold.sql         # Tabelas analíticas agregadas
│   └── 04_qualidade.py     # Validações e decisões sobre nulos
├── docs/
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
| `gold_comercial_canal_regiao` | Como o negócio performou por canal e região? |
| `gold_comercial_produto` | Quais categorias e produtos vendem mais? |
| `gold_comercial_clientes` | Como cada cliente se comporta ao longo do tempo? |
| `gold_entrega_pedido` | Onde estão os gargalos logísticos e de prazo? |
| `gold_atendimentos_pedido` | Quais são os principais motivos de ocorrência e criticidade? |

Todas as tabelas permitem segmentação por **período, região, canal, categoria e status**, e estão prontas para consumo direto no BI sem necessidade de tratamentos adicionais.

---

## Limitações conhecidas

- `customer_code` em `tb_atendimentos` está quase inteiramente nulo — impossibilita análise de atendimento por cliente
- Vendedores inativos ou sem canal/região representam ~50% das vendas — mantidos para não distorcer o volume real
- `Liquid Clustering` e hints de `BROADCAST`/`CACHE` não aplicados — identificados como melhorias futuras após definição dos padrões de consulta

---

## Próximos passos recomendados

- Implementar **Liquid Clustering** nas tabelas gold após análise dos padrões de consulta do BI
- Aplicar **BROADCAST** nas dimensões pequenas para otimizar performance dos joins
- Investigar a origem do `customer_code` nulo nos atendimentos com a área de negócio
- Evoluir o pipeline para **ingestão incremental** (Structured Streaming ou Delta CDF) em vez de overwrite total
- Implementar **testes de qualidade automatizados** (ex: Great Expectations ou dbt tests) nas camadas silver e gold
- Criar um notebook de **monitoramento de dados** com alertas para anomalias (volume, nulos, duplicatas)

---

## Ambiente

- **Plataforma:** Databricks Community Edition
- **Linguagem:** Python / PySpark / Spark SQL
- **Formato de armazenamento:** Delta Lake
- **Versionamento:** GitHub
