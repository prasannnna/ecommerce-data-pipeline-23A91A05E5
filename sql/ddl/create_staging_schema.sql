CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE staging.customers (
    customer_id VARCHAR(20),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(30),
    registration_date DATE,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    age_group VARCHAR(10),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.products (
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    price DECIMAL(12,2),
    cost DECIMAL(12,2),
    brand VARCHAR(100),
    stock_quantity INT,
    supplier_id VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.transactions (
    transaction_id VARCHAR(20),
    customer_id VARCHAR(20),
    transaction_date DATE,
    transaction_time TIME,
    payment_method VARCHAR(50),
    shipping_address TEXT,
    total_amount DECIMAL(12,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging.transaction_items (
    item_id VARCHAR(20),
    transaction_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INT,
    unit_price DECIMAL(12,2),
    discount_percentage INTEGER,
    line_total DECIMAL(12,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
