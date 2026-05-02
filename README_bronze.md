# 🥉 Camada Bronze

A camada bronze é a primeira camada do pipeline de dados. Responsável por **ingerir os dados brutos** das fontes originais e gravá-los como tabelas Delta no catálogo `bronze`, sem nenhuma transformação de conteúdo.

---

## Fontes e tabelas geradas

| Tabela | Arquivo de origem | Formato | Separador |
|---|---|---|---|
| `bronze.tb_atendimentos` | `atendimento_ocorrencias.ndjson` | NDJSON | — |
| `bronze.tb_entrega` | `logistica_entregas.json` | JSON (multiline) | — |
| `bronze.dim_clientes` | `crm_clientes_export.xlsx` | Excel | — |
| `bronze.dim_canais` | `comercial_canais.xlsx` | Excel | — |
| `bronze.dim_produto` | `cadastro_produtos_api_dump.json` | JSON (multiline) | — |
| `bronze.tb_pedidos_itens` | `erp_pedidos_itens_2025.csv` | CSV | `,` |
| `bronze.tb_pedidos_cabecalho` | `erp_pedidos_cabecalho_2025.csv` | CSV | `;` |
| `bronze.dim_vendedores` | `vendedores.csv` | CSV | `;` |
| `bronze.dim_regioes` | `legado_regioes_pipe.txt` | TXT | `\|` |

---

## Detalhamento por tabela

### `bronze.tb_atendimentos`
- **Origem:** `atendimento_ocorrencias.ndjson`
- **Formato:** NDJSON — um objeto JSON por linha
- **Leitura:** `spark.read.json()` sem `multiline`
- **Observações:** campos como `severity`, `status` e `event_type` chegam com capitalização inconsistente; tratados na silver

---

### `bronze.tb_entrega`
- **Origem:** `logistica_entregas.json`
- **Formato:** JSON array multilinha
- **Leitura:** `spark.read.json()` com `multiline=true`
- **Estrutura aninhada achatada na ingestão:**

| Campo original | Campo gerado |
|---|---|
| `carrier.name` | `carrier_name` |
| `carrier.mode` | `carrier_mode` |
| `timestamps.shipped_at` | `shipped_at` |
| `timestamps.delivered_at` | `delivered_at` |
| `destination.state` | `state` |
| `destination.city` | `city` |

- **Observações:** datas chegam em múltiplos formatos (`dd/MM/yyyy HH:mm` e ISO); tratadas na silver

---

### `bronze.dim_clientes`
- **Origem:** `crm_clientes_export.xlsx`
- **Formato:** Excel
- **Leitura:** `pandas.read_excel()` + `spark.createDataFrame()`
- **Dependência:** requer `openpyxl` instalado no cluster (`%pip install openpyxl`)
- **Observações:** campos de estado chegam com valores por extenso, sigla e abreviações; normalizados na silver

---

### `bronze.dim_canais`
- **Origem:** `comercial_canais.xlsx`
- **Formato:** Excel
- **Leitura:** `pandas.read_excel()` + `spark.createDataFrame()`
- **Dependência:** requer `openpyxl` instalado no cluster

---

### `bronze.dim_produto`
- **Origem:** `cadastro_produtos_api_dump.json`
- **Formato:** JSON array multilinha
- **Leitura:** `spark.read.json()` com `multiline=true`
- **Estrutura aninhada achatada na ingestão:**

| Campo original | Campo gerado |
|---|---|
| `product.product_id` | `product_id` |
| `product.name` | `name` |
| `product.category` | `category` |
| `product.subcategory` | `subcategory` |
| `product.status` | `status` |
| `pricing.list_price` | `list_price` |
| `pricing.currency` | `currency` |
| `attributes.family` | `family` |
| `attributes.tags` | `tags` |

---

### `bronze.tb_pedidos_itens`
- **Origem:** `erp_pedidos_itens_2025.csv`
- **Formato:** CSV com separador `,`
- **Leitura:** `spark.read.csv()` com `inferSchema=true` e `header=true`
- **Observações:** `unit_price` pode conter vírgula como separador decimal; tratado na silver

---

### `bronze.tb_pedidos_cabecalho`
- **Origem:** `erp_pedidos_cabecalho_2025.csv`
- **Formato:** CSV com separador `;`
- **Leitura:** `spark.read.csv()` com `inferSchema=true` e `header=true`
- **Observações:** `gross_amount` pode conter valores `N/A` e vírgula decimal; datas em múltiplos formatos; tratados na silver

---

### `bronze.dim_vendedores`
- **Origem:** `vendedores.csv`
- **Formato:** CSV com separador `;`
- **Leitura:** `spark.read.csv()` com `inferSchema=false` — tudo lido como string
- **Observações:** contém duplicatas de `seller_id`, `canal_id` com capitalização mista (`ch07` vs `CH01`), `regional_code` por extenso (`sul`) e datas em dois formatos; todos tratados na silver

---

### `bronze.dim_regioes`
- **Origem:** `legado_regioes_pipe.txt`
- **Formato:** TXT com separador `|`
- **Leitura:** `spark.read.csv()` com `sep=|` e `inferSchema=false`
- **Observações:** contém registros inválidos (`regional_code = 'sul'`, `'XX'`) filtrados na silver

---

## Configuração

```python
SOURCE_PATH = "file:/Workspace/Users/marcos.takayas@atlanteam.com.br/Avaliacao/sources"
```

Todos os arquivos devem estar presentes neste diretório antes da execução do notebook.

### Dependência de biblioteca

```python
%pip install openpyxl
```

Necessário para leitura dos arquivos `.xlsx`. Deve ser executado em célula separada antes das demais.

---

## Padrão de gravação

Todas as tabelas são gravadas no formato **Delta** com `mode=overwrite`:

```python
df.write.format('delta').mode('overwrite').saveAsTable('bronze.<tabela>')
```

Isso garante que cada reexecução do notebook recria as tabelas com os dados mais recentes da fonte.

---

## Próxima camada

Após a execução deste notebook, os dados estão disponíveis para transformação na **camada Silver**.  
Ver: `README_silver.md`

