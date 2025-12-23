CREATE SCHEMA IF NOT EXISTS production;

CREATE TABLE production.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(30),
    registration_date DATE,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    age_group VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE production.products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    price DECIMAL(12,2) CHECK (price >= 0),
    cost DECIMAL(12,2),
    brand VARCHAR(100),
    stock_quantity INT CHECK (stock_quantity >= 0),
    supplier_id VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE production.transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES production.customers(customer_id),
    transaction_date DATE,
    transaction_time TIME,
    payment_method VARCHAR(50),
    shipping_address TEXT,
    total_amount DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE production.transaction_items (
    item_id VARCHAR(20) PRIMARY KEY,
    transaction_id VARCHAR(20) REFERENCES production.transactions(transaction_id),
    product_id VARCHAR(20) REFERENCES production.products(product_id),
    quantity INT CHECK (quantity > 0),
    unit_price DECIMAL(12,2),
    discount_percentage INTEGER CHECK (discount_percentage BETWEEN 0 AND 100),
    line_total DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_transactions_customer ON production.transactions(customer_id);
CREATE INDEX idx_transactions_date ON production.transactions(transaction_date);
