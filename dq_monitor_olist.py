#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitoramento de Qualidade de Dados (DQ) para o DW do Olist
- Conecta ao PostgreSQL via SQLAlchemy
- Calcula métricas para 5+ dimensões: COMPLETENESS, UNIQUENESS, VALIDITY, CONSISTENCY, TIMELINESS
- Gera gráficos (matplotlib) e CSVs em ./monitoring/

Requisitos:
    pip install SQLAlchemy psycopg2-binary python-dotenv pandas matplotlib
Config .env (na raiz do projeto ou mesmo diretório):
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=olist
    DB_USER=postgres
    DB_PASSWORD=postgres123
"""
import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# --- Configuração e conexões ---
load_dotenv()
OUTPUT_PLOTS = Path("monitoring/plots")
OUTPUT_CSV   = Path("monitoring/csv")
for d in [OUTPUT_PLOTS, OUTPUT_CSV]:
    d.mkdir(parents=True, exist_ok=True)

url = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("DB_USER","postgres"),
    password=os.getenv("DB_PASSWORD",""),
    host=os.getenv("DB_HOST","localhost"),
    port=int(os.getenv("DB_PORT","5432")),
    database=os.getenv("DB_NAME","olist"),
)
engine = create_engine(url, pool_pre_ping=True)

def save_bar(df, x, y, title, fname):
    import matplotlib.pyplot as plt
    plt.figure(figsize=(10,5))
    plt.bar(df[x], df[y])
    plt.title(title)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(OUTPUT_PLOTS / fname, dpi=140)
    plt.close()

def save_hist(series, bins, title, fname):
    import matplotlib.pyplot as plt
    plt.figure(figsize=(8,5))
    plt.hist(series.dropna(), bins=bins)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(OUTPUT_PLOTS / fname, dpi=140)
    plt.close()

# ------------------ 1) COMPLETENESS ------------------
def completeness():
    sql = """
    WITH
      f AS (
        SELECT 'fact_order_item' AS entity, 'price' AS col,
               100.0*SUM(CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*) AS pct
        FROM olist_dw.fact_order_item
        UNION ALL
        SELECT 'fact_order_item','freight_value',
               100.0*SUM(CASE WHEN freight_value IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*)
        FROM olist_dw.fact_order_item
        UNION ALL
        SELECT 'fact_order_item','purchase_date_id',
               100.0*SUM(CASE WHEN purchase_date_id IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*)
        FROM olist_dw.fact_order_item
        UNION ALL
        SELECT 'dim_customer','city',
               100.0*SUM(CASE WHEN city IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*)
        FROM olist_dw.dim_customer
        UNION ALL
        SELECT 'dim_customer','state',
               100.0*SUM(CASE WHEN state IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*)
        FROM olist_dw.dim_customer
        UNION ALL
        SELECT 'dim_product','category_name',
               100.0*SUM(CASE WHEN category_name IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*)
        FROM olist_dw.dim_product
      )
    SELECT entity || '.' || col AS feature, ROUND(pct,2) AS completeness_pct
    FROM f
    ORDER BY feature;
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)
    df.to_csv(OUTPUT_CSV / "completeness.csv", index=False)
    save_bar(df, "feature", "completeness_pct", "Completeness (%) por coluna crítica", "01_completeness.png")
    return df

# ------------------ 2) UNIQUENESS ------------------
def uniqueness():
    sql = """
    SELECT 'raw_orders.order_id' AS entity, COUNT(*) - COUNT(DISTINCT order_id) AS duplicates
    FROM olist_stage.raw_orders
    UNION ALL
    SELECT 'raw_customers.customer_id', COUNT(*) - COUNT(DISTINCT customer_id)
    FROM olist_stage.raw_customers
    UNION ALL
    SELECT 'raw_sellers.seller_id', COUNT(*) - COUNT(DISTINCT seller_id)
    FROM olist_stage.raw_sellers
    UNION ALL
    SELECT 'raw_products.product_id', COUNT(*) - COUNT(DISTINCT product_id)
    FROM olist_stage.raw_products
    UNION ALL
    SELECT 'raw_order_items(order_id,order_item_id)',
           COUNT(*) - COUNT(DISTINCT (order_id, order_item_id))
    FROM olist_stage.raw_order_items
    UNION ALL
    SELECT 'raw_payments(order_id,payment_sequential)',
           COUNT(*) - COUNT(DISTINCT (order_id, payment_sequential))
    FROM olist_stage.raw_payments;
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)
    df.to_csv(OUTPUT_CSV / "uniqueness.csv", index=False)
    save_bar(df, "entity", "duplicates", "Duplicatas por chave natural (staging)", "02_uniqueness.png")
    return df

# ------------------ 3) VALIDITY ------------------
def validity():
    valid_states = ("'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'")
    sql = f"""
    WITH v AS (
      SELECT 'price>0' AS rule, 100.0*AVG(CASE WHEN price>0 THEN 1 ELSE 0 END) AS pass_pct
      FROM olist_stage.raw_order_items
      UNION ALL
      SELECT 'freight>=0', 100.0*AVG(CASE WHEN freight_value>=0 THEN 1 ELSE 0 END)
      FROM olist_stage.raw_order_items
      UNION ALL
      SELECT 'seller_state in UF', 100.0*AVG(CASE WHEN seller_state IN ({valid_states}) THEN 1 ELSE 0 END)
      FROM olist_stage.raw_sellers
      UNION ALL
      SELECT 'customer_state in UF', 100.0*AVG(CASE WHEN customer_state IN ({valid_states}) THEN 1 ELSE 0 END)
      FROM olist_stage.raw_customers
      UNION ALL
      SELECT 'dims>0 (produto)', 100.0*AVG(CASE WHEN COALESCE(product_weight_g,1)>0
                                                AND COALESCE(product_length_cm,1)>0
                                                AND COALESCE(product_height_cm,1)>0
                                                AND COALESCE(product_width_cm,1)>0
                                           THEN 1 ELSE 0 END)
      FROM olist_stage.raw_products
    )
    SELECT rule, ROUND(pass_pct,2) AS pass_pct FROM v ORDER BY rule;
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)
    df.to_csv(OUTPUT_CSV / "validity.csv", index=False)
    save_bar(df, "rule", "pass_pct", "Validity: % linhas que passam as regras", "03_validity.png")
    return df

# ------------------ 4) CONSISTENCY ------------------
def consistency():
    sql = """
    WITH
    rules AS (
      -- se status entregue, data de entrega não nula e >= compra
      SELECT 'delivered_has_date' AS rule,
             100.0*AVG(CASE WHEN order_status = 'delivered'
                               THEN CASE WHEN order_delivered_customer_date IS NOT NULL THEN 1 ELSE 0 END
                               ELSE 1 END) AS pass_pct
      FROM olist_stage.raw_orders
      UNION ALL
      SELECT 'delivered_after_purchase',
             100.0*AVG(CASE WHEN order_status = 'delivered'
                               THEN CASE WHEN order_delivered_customer_date >= order_purchase_timestamp THEN 1 ELSE 0 END
                               ELSE 1 END)
      FROM olist_stage.raw_orders
      UNION ALL
      SELECT 'shipping_limit_after_purchase',
             100.0*AVG(CASE WHEN shipping_limit_date >= o.order_purchase_timestamp THEN 1 ELSE 0 END)
      FROM olist_stage.raw_order_items i
      JOIN olist_stage.raw_orders o ON o.order_id = i.order_id
      UNION ALL
      -- pagamentos ~= soma itens (tolerância 1.00)
      SELECT 'sum(payments)≈sum(items)',
             100.0*AVG(CASE WHEN abs(payments.total - items.total) <= 1.00 THEN 1 ELSE 0 END)
      FROM (
        SELECT order_id, SUM(payment_value) AS total
        FROM olist_stage.raw_payments GROUP BY order_id
      ) payments
      JOIN (
        SELECT order_id, SUM(price + freight_value) AS total
        FROM olist_stage.raw_order_items GROUP BY order_id
      ) items USING(order_id)
    )
    SELECT rule, ROUND(pass_pct,2) AS pass_pct FROM rules ORDER BY rule;
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn)
    df.to_csv(OUTPUT_CSV / "consistency.csv", index=False)
    save_bar(df, "rule", "pass_pct", "Consistency: % linhas que passam as regras", "04_consistency.png")
    return df

# ------------------ 5) TIMELINESS ------------------
def timeliness():
    # Histograma do lead time e taxa de entrega no prazo
    sql_lead = """
      SELECT (DATE(order_delivered_customer_date) - DATE(order_purchase_timestamp))::INT AS lead_time_days
      FROM olist_stage.raw_orders
      WHERE order_delivered_customer_date IS NOT NULL AND order_purchase_timestamp IS NOT NULL
    """
    sql_ontime = """
      SELECT 100.0*AVG(CASE WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 1 ELSE 0 END) AS ontime_pct
      FROM olist_stage.raw_orders
      WHERE order_delivered_customer_date IS NOT NULL AND order_estimated_delivery_date IS NOT NULL
    """
    with engine.begin() as conn:
        lead_df = pd.read_sql(text(sql_lead), conn)
        ontime_df = pd.read_sql(text(sql_ontime), conn)
    lead_df.to_csv(OUTPUT_CSV / "timeliness_leadtime.csv", index=False)
    ontime_df.to_csv(OUTPUT_CSV / "timeliness_ontime.csv", index=False)

    save_hist(lead_df["lead_time_days"], bins=30, title="Distribuição do Lead Time (dias)", fname="05_timeliness_lead_hist.png")

    # gráfico de barra simples para taxa on-time
    df = ontime_df.copy()
    df["metric"] = ["on_time_delivery_rate"]
    df["value"] = df["ontime_pct"].round(2)
    df = df[["metric","value"]]
    save_bar(df, "metric", "value", "Timeliness: % de entregas no prazo", "06_timeliness_on_time.png")
    return lead_df, ontime_df

def main():
    print("[DQ] Iniciando monitoramento...")
    completeness()
    print("[DQ] Completeness OK")
    uniqueness()
    print("[DQ] Uniqueness OK")
    validity()
    print("[DQ] Validity OK")
    consistency()
    print("[DQ] Consistency OK")
    timeliness()
    print("[DQ] Timeliness OK")
    print("[DQ] Relatórios salvos na pasta monitoring/.")

if __name__ == "__main__":
    main()
