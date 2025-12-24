CREATE SCHEMA IF NOT EXISTS warehouse;

-- ============================
-- DIM CUSTOMER (SCD TYPE 2)
-- ============================
CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20),
    full_name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    age_group VARCHAR,
    customer_segment VARCHAR,
    registration_date DATE,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- ============================
-- DIM PRODUCT (SCD TYPE 2)
-- ============================
CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20),
    product_name VARCHAR,
    category VARCHAR,
    sub_category VARCHAR,
    brand VARCHAR,
    price_range VARCHAR,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- ============================
-- DIM DATE
-- ============================
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    month_name VARCHAR,
    day_name VARCHAR,
    week_of_year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- ============================
-- DIM PAYMENT METHOD
-- ============================
CREATE TABLE IF NOT EXISTS warehouse.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method_name VARCHAR(50) UNIQUE,
    payment_type VARCHAR(20)
);

-- ============================
-- FACT SALES
-- ============================
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES warehouse.dim_date(date_key),
    customer_key INTEGER REFERENCES warehouse.dim_customers(customer_key),
    product_key INTEGER REFERENCES warehouse.dim_products(product_key),
    payment_method_key INTEGER REFERENCES warehouse.dim_payment_method(payment_method_key),
    transaction_id VARCHAR(20),
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    discount_amount NUMERIC(10,2),
    line_total NUMERIC(10,2),
    profit NUMERIC(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================
-- AGGREGATE TABLES
-- ============================
CREATE TABLE IF NOT EXISTS warehouse.agg_daily_sales (
    date_key INTEGER PRIMARY KEY,
    total_transactions INTEGER,
    total_revenue NUMERIC,
    total_profit NUMERIC,
    unique_customers INTEGER
);

CREATE TABLE IF NOT EXISTS warehouse.agg_product_performance (
    product_key INTEGER PRIMARY KEY,
    total_quantity_sold INTEGER,
    total_revenue NUMERIC,
    total_profit NUMERIC,
    avg_discount_percentage NUMERIC
);

CREATE TABLE IF NOT EXISTS warehouse.agg_customer_metrics (
    customer_key INTEGER PRIMARY KEY,
    total_transactions INTEGER,
    total_spent NUMERIC,
    avg_order_value NUMERIC,
    last_purchase_date DATE
);
