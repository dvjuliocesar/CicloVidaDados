-- SCHEMAS
CREATE SCHEMA IF NOT EXISTS olist_stage;
CREATE SCHEMA IF NOT EXISTS olist_dw;

-- ====== STAGING: estruturas iguais aos CSVs ======
-- customers
CREATE TABLE IF NOT EXISTS olist_stage.raw_customers (
  customer_id TEXT,
  customer_unique_id TEXT,
  customer_zip_code_prefix INT,
  customer_city TEXT,
  customer_state TEXT
);

-- orders
CREATE TABLE IF NOT EXISTS olist_stage.raw_orders (
  order_id TEXT,
  customer_id TEXT,
  order_status TEXT,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP
);

-- order_items
CREATE TABLE IF NOT EXISTS olist_stage.raw_order_items (
  order_id TEXT,
  order_item_id INT,
  product_id TEXT,
  seller_id TEXT,
  shipping_limit_date TIMESTAMP,
  price NUMERIC(12,2),
  freight_value NUMERIC(12,2)
);

-- products
CREATE TABLE IF NOT EXISTS olist_stage.raw_products (
  product_id TEXT,
  product_category_name TEXT,
  product_name_lenght INT,
  product_description_lenght INT,
  product_photos_qty INT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT
);

-- sellers
CREATE TABLE IF NOT EXISTS olist_stage.raw_sellers (
  seller_id TEXT,
  seller_zip_code_prefix INT,
  seller_city TEXT,
  seller_state TEXT
);

-- payments
CREATE TABLE IF NOT EXISTS olist_stage.raw_payments (
  order_id TEXT,
  payment_sequential INT,
  payment_type TEXT,
  payment_installments INT,
  payment_value NUMERIC(12,2)
);

-- reviews
CREATE TABLE IF NOT EXISTS olist_stage.raw_reviews (
  review_id TEXT,
  order_id TEXT,
  review_score INT,
  review_comment_title TEXT,
  review_comment_message TEXT,
  review_creation_date TIMESTAMP,
  review_answer_timestamp TIMESTAMP
);

-- category translation (PT->EN)
CREATE TABLE IF NOT EXISTS olist_stage.raw_category_translation (
  product_category_name TEXT,
  product_category_name_english TEXT
);


-- ====== DW ======
-- Dimensões
CREATE TABLE IF NOT EXISTS olist_dw.dim_date (
  date_id DATE PRIMARY KEY,
  year SMALLINT NOT NULL,
  quarter SMALLINT NOT NULL,
  month SMALLINT NOT NULL,
  day SMALLINT NOT NULL,
  week SMALLINT NOT NULL,
  dow SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS olist_dw.dim_customer (
  customer_sk SERIAL PRIMARY KEY,
  customer_id TEXT UNIQUE NOT NULL,
  customer_unique_id TEXT,
  zip_prefix INT,
  city TEXT,
  state TEXT
);

CREATE TABLE IF NOT EXISTS olist_dw.dim_seller (
  seller_sk SERIAL PRIMARY KEY,
  seller_id TEXT UNIQUE NOT NULL,
  zip_prefix INT,
  city TEXT,
  state TEXT
);

CREATE TABLE IF NOT EXISTS olist_dw.dim_product (
  product_sk SERIAL PRIMARY KEY,
  product_id TEXT UNIQUE NOT NULL,
  category_name TEXT,
  category_name_en TEXT,
  weight_g INT,
  length_cm INT,
  height_cm INT,
  width_cm INT
);

-- FATO (grão: item do pedido)
CREATE TABLE IF NOT EXISTS olist_dw.fact_order_item (
  fact_id BIGSERIAL PRIMARY KEY,
  order_id TEXT NOT NULL,
  order_item_id INT NOT NULL,
  customer_sk INT NOT NULL REFERENCES olist_dw.dim_customer(customer_sk),
  seller_sk INT NOT NULL REFERENCES olist_dw.dim_seller(seller_sk),
  product_sk INT NOT NULL REFERENCES olist_dw.dim_product(product_sk),
  purchase_date_id DATE REFERENCES olist_dw.dim_date(date_id),
  approved_date_id DATE REFERENCES olist_dw.dim_date(date_id),
  shipping_limit_date_id DATE REFERENCES olist_dw.dim_date(date_id),
  delivered_customer_date_id DATE REFERENCES olist_dw.dim_date(date_id),
  estimated_delivery_date_id DATE REFERENCES olist_dw.dim_date(date_id),
  order_status TEXT,
  price NUMERIC(12,2),
  freight_value NUMERIC(12,2),
  CONSTRAINT uq_item UNIQUE (order_id, order_item_id)
);

-- Pagamentos (opcional: grão = ordem x parcela)
CREATE TABLE IF NOT EXISTS olist_dw.fact_payment (
  payment_id BIGSERIAL PRIMARY KEY,
  order_id TEXT NOT NULL,
  payment_sequential INT NOT NULL,
  payment_type TEXT,
  payment_installments INT,
  payment_value NUMERIC(12,2),
  purchase_date_id DATE REFERENCES olist_dw.dim_date(date_id),
  CONSTRAINT uq_pay UNIQUE (order_id, payment_sequential)
);

-- ====== Índices ======
CREATE INDEX IF NOT EXISTS ix_fact_item_purchase ON olist_dw.fact_order_item(purchase_date_id);
CREATE INDEX IF NOT EXISTS ix_fact_item_seller   ON olist_dw.fact_order_item(seller_sk);
CREATE INDEX IF NOT EXISTS ix_fact_item_product  ON olist_dw.fact_order_item(product_sk);

-- ====== Consultas ======
-- 1) Ticket médio por mês
SELECT d.year, d.month, ROUND(SUM(price + freight_value)/COUNT(DISTINCT order_id), 2) AS ticket_medio
FROM olist_dw.fact_order_item f
JOIN olist_dw.dim_date d ON d.date_id = f.purchase_date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 2) Top 10 categorias por receita (price+freight)
SELECT p.category_name_en, SUM(f.price + f.freight_value) AS receita
FROM olist_dw.fact_order_item f
JOIN olist_dw.dim_product p ON p.product_sk = f.product_sk
GROUP BY p.category_name_en
ORDER BY receita DESC
LIMIT 10;

-- 3) SLA de entrega (em dias)
SELECT AVG(f.delivered_customer_date_id - f.purchase_date_id) AS media_dias
FROM olist_dw.fact_order_item f
WHERE f.delivered_customer_date_id IS NOT NULL;

-- 4) Curva ABC por seller
SELECT s.seller_id,
       SUM(f.price + f.freight_value) AS receita
FROM olist_dw.fact_order_item f
JOIN olist_dw.dim_seller s ON s.seller_sk = f.seller_sk
GROUP BY s.seller_id
ORDER BY receita DESC;

