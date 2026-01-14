/*
Database initialization script.
Creates sample tables in the 'core' schema for demonstration.
*/

-- Create core schema
CREATE SCHEMA IF NOT EXISTS core;

-- Sample tables for demonstration
-- These represent a simple e-commerce database

CREATE TABLE IF NOT EXISTS core.users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS core.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS core.orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES core.users(user_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2),
    status VARCHAR(50) DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS core.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES core.orders(order_id),
    product_id INTEGER NOT NULL REFERENCES core.products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    subtotal DECIMAL(10, 2)
);

-- Create indexes
CREATE INDEX idx_orders_user_id ON core.orders(user_id);
CREATE INDEX idx_orders_order_date ON core.orders(order_date);
CREATE INDEX idx_order_items_order_id ON core.order_items(order_id);
CREATE INDEX idx_order_items_product_id ON core.order_items(product_id);

-- Insert sample data
INSERT INTO core.users (username, email, created_at) VALUES
    ('john_doe', 'john@example.com', NOW() - INTERVAL '6 months'),
    ('jane_smith', 'jane@example.com', NOW() - INTERVAL '4 months'),
    ('bob_wilson', 'bob@example.com', NOW() - INTERVAL '2 months')
ON CONFLICT DO NOTHING;

INSERT INTO core.products (product_name, category, price, stock_quantity) VALUES
    ('Laptop Pro', 'Electronics', 1299.99, 15),
    ('Wireless Mouse', 'Accessories', 29.99, 50),
    ('USB-C Cable', 'Accessories', 19.99, 100),
    ('Monitor 27"', 'Electronics', 399.99, 20)
ON CONFLICT DO NOTHING;

INSERT INTO core.orders (user_id, order_date, total_amount, status) VALUES
    (1, NOW() - INTERVAL '30 days', 1329.98, 'completed'),
    (2, NOW() - INTERVAL '15 days', 429.98, 'completed'),
    (1, NOW() - INTERVAL '7 days', 29.99, 'shipped')
ON CONFLICT DO NOTHING;

INSERT INTO core.order_items (order_id, product_id, quantity, unit_price, subtotal) VALUES
    (1, 1, 1, 1299.99, 1299.99),
    (1, 2, 1, 29.99, 29.99),
    (2, 4, 1, 399.99, 399.99),
    (2, 2, 1, 29.99, 29.99),
    (3, 2, 1, 29.99, 29.99)
ON CONFLICT DO NOTHING;

-- Grant permissions (adjust as needed for your security model)
-- For production, create separate roles with restricted permissions
GRANT USAGE ON SCHEMA core TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO PUBLIC;
