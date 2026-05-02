#🥈 Decisões de design — Camada Silver

A camada silver é a segunda camada do pipeline de dados. Responsável por **limpar, padronizar e enriquecer** os dados vindos da bronze, aplicando regras de qualidade e normalizações antes de disponibilizar para a camada gold.

---

## Tabelas geradas

| Tabela | Origem bronze | Principais tratamentos |
|---|---|---|
| `silver.tb_atendimentos` | `bronze.tb_atendimentos` | Parse de data, initcap nos textuais |
| `silver.tb_entrega` | `bronze.tb_entrega` | Normalização de UF, parse de data, cast de custo |
| `silver.dim_clientes` | `bronze.dim_clientes` | Normalização de UF, initcap, parse de data |
| `silver.dim_canais` | `bronze.dim_canais` | Upper em id, initcap nos demais, filtro de registros inválidos |
| `silver.tb_pedidos_itens` | `bronze.tb_pedidos_itens` | Upper em ids, fix de decimal, initcap em status |
| `silver.tb_pedidos_cabecalho` | `bronze.tb_pedidos_cabecalho` | Initcap, parse de data, fix de decimal em gross_amount |
| `silver.dim_produto` | `bronze.dim_produto` | Initcap, fix de decimal em list_price, cast de timestamp |
| `silver.dim_vendedores` | `bronze.dim_vendedores` | Upper em canal_id, normalização de regional_code, parse de data |
| `silver.dim_regioes` | `bronze.dim_regioes` | Filtro de registros inválidos |

---

## Função utilitária: `parse_date`

Usada em todas as tabelas que possuem campos de data. Tenta múltiplos formatos em ordem e retorna o primeiro que não for nulo:

```python
def parse_date(col_name):
    return F.coalesce(
        F.try_to_date(F.col(col_name), "yyyy/MM/dd HH:mm"),
        F.try_to_date(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss"),
        F.try_to_date(F.col(col_name), "yyyy-MM-dd HH:mm:ss"),
        F.try_to_date(F.col(col_name), "yyyy-MM-dd"),
        F.try_to_date(F.col(col_name), "yyyy/MM/dd"),
        F.try_to_date(F.col(col_name), "dd/MM/yyyy"),
        F.try_to_date(F.col(col_name), "dd/MM/yyyy HH:mm")
    )
```

> Requer **Spark 3.5+** — `try_to_date` retorna `NULL` em vez de lançar erro quando o formato não bate.

---

## Normalização de UF

Aplicada em `tb_entrega` (campo `state`) e `dim_clientes` (campo `estado`). Aceita sigla, nome por extenso e variações com/sem acento, normalizando para a sigla oficial de 2 letras:

```
"rio de janeiro" → "RJ"
"São Paulo"      → "SP"
"sul"            → NULL  (valor inválido)
```

Valores não reconhecidos retornam `NULL`.

---

## Detalhamento por tabela

### `silver.tb_atendimentos`
- **Campos tratados:**

| Campo | Tratamento |
|---|---|
| `created_at` | `parse_date()` — múltiplos formatos |
| `event_type` | `initcap` |
| `order_id` | `initcap` |
| `severity` | `initcap` |
| `status` | `initcap` |
| `ticket_id` | `initcap` |
| `customer_code`, `metadata` | mantidos como estão |

---

### `silver.tb_entrega`
- **Campos tratados:**

| Campo | Tratamento |
|---|---|
| `delivery_id` | `initcap` |
| `order_ref` | `initcap` |
| `carrier_name` | `initcap` |
| `carrier_mode` | `initcap` |
| `delivery_status` | `initcap` |
| `shipped_at` | `parse_date()` |
| `delivered_at` | `parse_date()` |
| `state` | normalização de UF via CASE WHEN |
| `city` | `initcap` |
| `cost` | valores `unknown` / nulos → `NULL`; vírgula → ponto; cast `DECIMAL(10,2)` |

---

### `silver.dim_clientes`
- **Campos tratados:**

| Campo | Tratamento |
|---|---|
| `customer_id` | `initcap` |
| `nome_cliente` | `trim` + `initcap` |
| `segmento` | `initcap` (preserva `NULL`) |
| `porte` | `initcap` (preserva `NULL`) |
| `cidade` | `trim` + `initcap` |
| `estado` | normalização de UF via CASE WHEN |
| `status_cliente` | `trim` + `initcap` (preserva `NULL`) |
| `data_cadastro` | `parse_date()` |
| `updated_at` | `to_timestamp` formato `yyyy-MM-dd HH:mm:ss` |
| `email` | mantido como está |

---

### `silver.dim_canais`
- **Campos tratados:**

| Campo | Tratamento |
|---|---|
| `id_canal` | `upper` |
| `nome_canal` | `initcap` |
| `tipo_canal` | `initcap` |
| `ativo` | `initcap` |
| `observacao` | `initcap` |

- **Filtro aplicado:** remove registros onde `id_canal = 'CH05'` e `observacao is not null` (registros inconsistentes identificados na análise da bronze)

---

### `silver.tb_pedidos_itens`
- **Campos tratados:**

| Campo | Tratamento |
|---|---|
| `order_id` | `upper` |
| `product_code` | `upper` |
| `unit_price` | vírgula → ponto; cast `double` |
| `item_status` | `lower` + `initcap` (preserva `NULL`) |
| `item_seq`, `quantity`, `total_item` | mantidos como estão |

---

### `silver.tb_pedidos_cabecalho`
- **Campos tratados:**

| Campo | Tratamento |
|---|---|
| `order_id` | `initcap` |
| `customer_code` | `initcap` |
| `seller_id` | `initcap` |
| `status_order` | `initcap` |
| `order_date` | `parse_date()` |
| `promised_date` | `parse_date()` |
| `gross_amount` | vírgula → ponto; cast `double` → `gross_amount_num` |
| `discount_amount`, `net_amount`, `payment_details`, `last_update` | mantidos como estão |

---

### `silver.dim_produto`
- **Campos tratados:**

| Campo | Tratamento |
|---|---|
| `product_id` | `initcap` |
| `name` | `initcap` |
| `category` | `initcap` |
| `subcategory` | `initcap` |
| `status` | `initcap` (preserva `NULL`) |
| `family` | `initcap` (preserva `NULL`) |
| `list_price` | vírgula → ponto; cast `double` |
| `updated_at` | `to_timestamp` |
| `currency`, `tags` | mantidos como estão |

---

### `silver.dim_vendedores`
- **Campos tratados:**

| Campo | Tratamento |
|---|---|
| `seller_id` | `initcap` |
| `seller_name` | `initcap` |
| `canal_id` | `upper` (preserva `NULL`) |
| `regional_code` | `"sul"` → `"S"`; demais mantidos |
| `hire_date` | `parse_date()` |
| `status` | `initcap` (preserva `NULL`) |

- **Observação:** duplicatas de `seller_id` presentes na bronze não são deduplicadas explicitamente neste notebook — recomenda-se adicionar `dropDuplicates(["seller_id"])` se necessário

---

### `silver.dim_regioes`
- **Sem transformações de conteúdo** — apenas filtros de registros inválidos:

| Filtro | Motivo |
|---|---|
| `regional_code not in ('sul', 'XX')` | códigos inválidos ou sem gestor |
| `state <> 'sao paulo'` | registro duplicado com grafia incorreta |

---

## Padrão de gravação

```python
df_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.<tabela>")
```

O `overwriteSchema=true` é necessário quando há mudança de tipo em relação à tabela Delta já existente (ex: `list_price` de `string` para `double`).

---

## Próxima camada

Após a execução deste notebook, os dados estão disponíveis para agregação e análise na **camada Gold**.  
Ver: `README_gold.md`
