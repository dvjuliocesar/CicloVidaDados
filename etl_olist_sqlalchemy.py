# Bibliotecas
import os
from pathlib import Path
from contextlib import closing
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ----------------- CONFIG -----------------
load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "data/raw"))
DDL_PATH = Path("ddl_olist.sql")   # DDL opcional (SEM CREATE DATABASE!)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "olist")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "")

# URL de administração (conectando no DB 'postgres') e do banco alvo (DB_NAME)
url_admin  = URL.create("postgresql+psycopg2", username=DB_USER, password=DB_PASS,
                        host=DB_HOST, port=DB_PORT, database="postgres")
url_target = URL.create("postgresql+psycopg2", username=DB_USER, password=DB_PASS,
                        host=DB_HOST, port=DB_PORT, database=DB_NAME)

# Engine principal (somente será usada depois de garantir que o DB existe)
engine = create_engine(url_target, pool_pre_ping=True)

# Arquivos esperados (nome do arquivo -> tabela de staging)
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

# ------------- Helpers --------------------
def ensure_database():
    """
    Cria o banco DB_NAME se não existir.
    Executa em AUTOCOMMIT e conectado ao DB 'postgres' para evitar transações.
    """
    admin_engine = create_engine(url_admin, isolation_level="AUTOCOMMIT", pool_pre_ping=True)
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": DB_NAME}).scalar()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))
    finally:
        admin_engine.dispose()

def exec_sql(sql: str):
    """
    Executa SQL arbitrário (SEM CREATE DATABASE) no banco alvo.
    Aceita múltiplas sentenças separadas por ';'.
    """
    if not sql or not sql.strip():
        return
    with engine.begin() as conn:
        conn.execute(text(sql))

def copy_from_csv(table: str, csv_path: Path):
    """
    Faz COPY FROM STDIN (server-side) do CSV para a tabela 'table'.
    Usa closing() pois raw_connection() não é context manager no SQLAlchemy 2.x.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV não encontrado: {csv_path}")

    with closing(engine.raw_connection()) as raw_conn, \
         closing(raw_conn.cursor()) as cur, \
         open(csv_path, "r", encoding="utf-8") as f:
        try:
            cur.copy_expert(
                f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER true)",
                f
            )
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise

# ------------- ETL: STAGING ---------------
def load_staging():
    # 1) Garante que o DB existe
    ensure_database()

    # 2) (Opcional) aplicar DDL (SEM CREATE DATABASE)
    if DDL_PATH.exists():
        ddl = DDL_PATH.read_text(encoding="utf-8")

        # Segurança: remove linhas com CREATE DATABASE, caso alguém esqueça no arquivo
        safe_lines = []
        for ln in ddl.splitlines():
            if ln.strip().upper().startswith("CREATE DATABASE"):
                continue
            safe_lines.append(ln)
        ddl = "\n".join(safe_lines)

        exec_sql(ddl)

    # 3) TRUNCATE staging (idempotente)
    with engine.begin() as conn:
        for tbl in FILES.keys():
            conn.execute(text(f"TRUNCATE {tbl}"))

    # 4) COPY de cada CSV para o staging
    for table, fname in FILES.items():
        path = DATA_DIR / fname
        copy_from_csv(table, path)

    print("[STAGING] COPY concluído.")

# ------------- ETL: DW (transform + load) -------------
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

def transform_load_dw():
    upsert_dim_date()
    upsert_dim_customer()
    upsert_dim_seller()
    upsert_dim_product()
    insert_fact_items()
    insert_fact_payments()
    print("[DW] Dimensões e fatos populados.")

def run():
    load_staging()
    transform_load_dw()
    print("[OK] Pipeline concluído.")

if __name__ == "__main__":
    run()