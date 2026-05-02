%md
# 🏅 Decisões de design — Camada Gold

### Granularidade temporal
As tabelas gold são agregadas em **mês/ano** (`yyyy-MM`), pois análises de negócio raramente ocorrem no nível de dia. Isso reduz o volume de dados e melhora a performance das consultas no BI.

---

### Separação por dimensão
Cada tabela gold foi construída ao redor de uma dimensão principal (canal, região, categoria, entrega, atendimento), evitando tabelas genéricas demais que dificultam a manutenção e a legibilidade.

---

### Manutenção do `status_order`
O campo `status_order` é mantido em todas as golds para que pedidos **cancelados não mascarem os resultados**. Filtrar cancelados antes da agregação inflaria métricas como receita e ticket médio, distorcendo a análise real do negócio.

---

### Melhorias futuras

| Técnica | Quando usar | Benefício |
|---|---|---|
| `BROADCAST` hint | Joins com tabelas de dimensão pequenas | Evita shuffle, reduz tempo de join |
| `CACHE` | Dimensões reutilizadas em múltiplas queries | Evita releitura do disco a cada execução |

```sql

-- exemplo de BROADCAST
SELECT /*+ BROADCAST(v) */ ...
FROM tb_pedidos_cabecalho p
LEFT JOIN dim_vendedores v ON p.seller_id = v.seller_id

-- exemplo de CACHE
CACHE TABLE workspace.silver.dim_vendedores;
```

- **Liquid Clustering** (`CLUSTER BY`) nas tabelas Delta — melhora performance de consultas filtrando por colunas específicas. Como os padrões de consulta ainda não estão definidos, será implementado após análise de uso real:

```sql
CREATE OR REPLACE TABLE workspace.gold.gold_comercial_geral
CLUSTER BY (ano_mes, canal_id, regional_code)
AS SELECT ...
```