# Bibliotecas
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# Configurações
load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "data/raw"))
DDL_PATH = Path("ddl_olist.sql")   # opcional: DDL completo aqui

url = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", ""),
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5433")),
    database=os.getenv("DB_NAME", "olist"),
)
engine = create_engine(url, pool_pre_ping=True)

FILES = {
  "olist_stage.raw_customers": "olist_customers_dataset.csv",
  "olist_stage.raw_orders": "olist_orders_dataset.csv",
  "olist_stage.raw_order_items": "olist_order_items_dataset.csv",
  "olist_stage.raw_products": "olist_products_dataset.csv",
  "olist_stage.raw_sellers": "olist_sellers_dataset.csv",
  "olist_stage.raw_payments": "olist_order_payments_dataset.csv",
  "olist_stage.raw_reviews": "olist_order_reviews_dataset.csv",
  "olist_stage.raw_category_translation": "product_category_name_translation.csv",
}

def exec_sql(sql: str):
    with engine.begin() as conn:
        conn.execute(text(sql))

def copy_from_csv(table: str, csv_path: Path):
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV não encontrado: {csv_path}")
    # COPY via conexão "raw" do driver psycopg2 (rápido e confiável)
    with engine.raw_connection() as raw_conn:
        with raw_conn.cursor() as cur, open(csv_path, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER true)",
                f
            )
        raw_conn.commit()

# ETL: STAGING
def load_staging():
    # (opcional) aplicar DDL
    if DDL_PATH.exists():
        ddl = DDL_PATH.read_text(encoding="utf-8")
        exec_sql(ddl)

    # Limpar staging
    with engine.begin() as conn:
        for tbl in FILES.keys():
            conn.execute(text(f"TRUNCATE {tbl}"))

    # COPY cada CSV
    for table, fname in FILES.items():
        copy_from_csv(table, DATA_DIR / fname)
    print("[STAGING] COPY concluído.")

# ETL: DW (transform + load)
def upsert_dim_date():
    exec_sql("""
    WITH d AS (
      SELECT DISTINCT DATE(order_purchase_timestamp) AS d FROM olist_stage.raw_orders WHERE order_purchase_timestamp IS NOT NULL
      UNION SELECT DISTINCT DATE(order_approved_at) FROM olist_stage.raw_orders WHERE order_approved_at IS NOT NULL
      UNION SELECT DISTINCT DATE(order_delivered_customer_date) FROM olist_stage.raw_orders WHERE order_delivered_customer_date IS NOT NULL
      UNION SELECT DISTINCT DATE(order_estimated_delivery_date) FROM olist_stage.raw_orders WHERE order_estimated_delivery_date IS NOT NULL
      UNION SELECT DISTINCT DATE(shipping_limit_date) FROM olist_stage.raw_order_items WHERE shipping_limit_date IS NOT NULL
      UNION SELECT DISTINCT DATE(review_creation_date) FROM olist_stage.raw_reviews WHERE review_creation_date IS NOT NULL
      UNION SELECT DISTINCT DATE(review_answer_timestamp) FROM olist_stage.raw_reviews WHERE review_answer_timestamp IS NOT NULL
    )
    INSERT INTO olist_dw.dim_date (date_id, year, quarter, month, day, week, dow)
    SELECT d,
           EXTRACT(YEAR FROM d)::SMALLINT,
           ((EXTRACT(MONTH FROM d)::INT - 1)/3 + 1)::SMALLINT,
           EXTRACT(MONTH FROM d)::SMALLINT,
           EXTRACT(DAY FROM d)::SMALLINT,
           EXTRACT(WEEK FROM d)::SMALLINT,
           EXTRACT(DOW FROM d)::SMALLINT
    FROM d
    ON CONFLICT (date_id) DO NOTHING;
    """)

def upsert_dim_customer():
    exec_sql("""
    INSERT INTO olist_dw.dim_customer (customer_id, customer_unique_id, zip_prefix, city, state)
    SELECT DISTINCT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
    FROM olist_stage.raw_customers
    ON CONFLICT (customer_id) DO UPDATE
      SET customer_unique_id = COALESCE(EXCLUDED.customer_unique_id, olist_dw.dim_customer.customer_unique_id),
          zip_prefix = COALESCE(EXCLUDED.zip_prefix, olist_dw.dim_customer.zip_prefix),
          city = COALESCE(EXCLUDED.city, olist_dw.dim_customer.city),
          state = COALESCE(EXCLUDED.state, olist_dw.dim_customer.state);
    """)

def upsert_dim_seller():
    exec_sql("""
    INSERT INTO olist_dw.dim_seller (seller_id, zip_prefix, city, state)
    SELECT DISTINCT seller_id, seller_zip_code_prefix, seller_city, seller_state
    FROM olist_stage.raw_sellers
    ON CONFLICT (seller_id) DO UPDATE
      SET zip_prefix = COALESCE(EXCLUDED.zip_prefix, olist_dw.dim_seller.zip_prefix),
          city = COALESCE(EXCLUDED.city, olist_dw.dim_seller.city),
          state = COALESCE(EXCLUDED.state, olist_dw.dim_seller.state);
    """)

def upsert_dim_product():
    exec_sql("""
    INSERT INTO olist_dw.dim_product (product_id, category_name, category_name_en,
                                      weight_g, length_cm, height_cm, width_cm)
    SELECT DISTINCT p.product_id,
           p.product_category_name,
           t.product_category_name_english,
           p.product_weight_g, p.product_length_cm, p.product_height_cm, p.product_width_cm
    FROM olist_stage.raw_products p
    LEFT JOIN olist_stage.raw_category_translation t
      ON t.product_category_name = p.product_category_name
    ON CONFLICT (product_id) DO UPDATE
      SET category_name = COALESCE(EXCLUDED.category_name, olist_dw.dim_product.category_name),
          category_name_en = COALESCE(EXCLUDED.category_name_en, olist_dw.dim_product.category_name_en),
          weight_g = COALESCE(EXCLUDED.weight_g, olist_dw.dim_product.weight_g),
          length_cm = COALESCE(EXCLUDED.length_cm, olist_dw.dim_product.length_cm),
          height_cm = COALESCE(EXCLUDED.height_cm, olist_dw.dim_product.height_cm),
          width_cm = COALESCE(EXCLUDED.width_cm, olist_dw.dim_product.width_cm);
    """)

def insert_fact_items():
    exec_sql("""
    INSERT INTO olist_dw.fact_order_item
      (order_id, order_item_id, customer_sk, seller_sk, product_sk,
       purchase_date_id, approved_date_id, shipping_limit_date_id,
       delivered_customer_date_id, estimated_delivery_date_id,
       order_status, price, freight_value)
    SELECT
      i.order_id,
      i.order_item_id,
      dc.customer_sk,
      ds.seller_sk,
      dp.product_sk,
      DATE(o.order_purchase_timestamp) AS purchase_date_id,
      DATE(o.order_approved_at) AS approved_date_id,
      DATE(i.shipping_limit_date) AS shipping_limit_date_id,
      DATE(o.order_delivered_customer_date) AS delivered_customer_date_id,
      DATE(o.order_estimated_delivery_date) AS estimated_delivery_date_id,
      o.order_status,
      i.price,
      i.freight_value
    FROM olist_stage.raw_order_items i
    JOIN olist_stage.raw_orders o         ON o.order_id = i.order_id
    JOIN olist_stage.raw_customers c      ON c.customer_id = o.customer_id
    JOIN olist_stage.raw_sellers s        ON s.seller_id = i.seller_id
    JOIN olist_stage.raw_products p       ON p.product_id = i.product_id
    JOIN olist_dw.dim_customer dc         ON dc.customer_id = c.customer_id
    JOIN olist_dw.dim_seller ds           ON ds.seller_id   = s.seller_id
    JOIN olist_dw.dim_product dp          ON dp.product_id  = p.product_id
    ON CONFLICT (order_id, order_item_id)
    DO UPDATE SET
      price = EXCLUDED.price,
      freight_value = EXCLUDED.freight_value,
      order_status = EXCLUDED.order_status,
      delivered_customer_date_id = EXCLUDED.delivered_customer_date_id;
    """)

def insert_fact_payments():
    exec_sql("""
    INSERT INTO olist_dw.fact_payment
      (order_id, payment_sequential, payment_type, payment_installments, payment_value, purchase_date_id)
    SELECT
      p.order_id, p.payment_sequential, p.payment_type, p.payment_installments, p.payment_value,
      DATE(o.order_purchase_timestamp) AS purchase_date_id
    FROM olist_stage.raw_payments p
    JOIN olist_stage.raw_orders o ON o.order_id = p.order_id
    ON CONFLICT (order_id, payment_sequential) DO UPDATE
      SET payment_type = EXCLUDED.payment_type,
          payment_installments = EXCLUDED.payment_installments,
          payment_value = EXCLUDED.payment_value;
    """)

def run():
    load_staging()
    upsert_dim_date()
    upsert_dim_customer()
    upsert_dim_seller()
    upsert_dim_product()
    insert_fact_items()
    insert_fact_payments()
    print("[OK] Pipeline concluído.")

if __name__ == "__main__":
    run()