-- ============================================================================
-- Database: tamson_ecommerce
-- Mô tả: Schema quản lý bán hàng xa xỉ Tam Sơn
-- ============================================================================

-- Core Tables: Cấu hình hệ thống
-- ============================================================================

CREATE TABLE core_website (
    website_id SMALLSERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    default_group_id SMALLINT,
    is_default SMALLINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE core_store (
    store_id SMALLSERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    website_id SMALLINT NOT NULL REFERENCES core_website(website_id),
    group_id SMALLINT,
    name VARCHAR(255) NOT NULL,
    sort_order SMALLINT DEFAULT 0,
    is_active SMALLINT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- EAV Structure: Nhóm thuộc tính
-- ============================================================================

CREATE TABLE eav_attribute_set (
    attribute_set_id SMALLSERIAL PRIMARY KEY,
    entity_type_id SMALLINT DEFAULT 1,
    attribute_set_name VARCHAR(255) NOT NULL,
    sort_order SMALLINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Catalog: Danh mục và sản phẩm
-- ============================================================================

CREATE TABLE catalog_category_entity (
    entity_id SERIAL PRIMARY KEY,
    parent_id INT REFERENCES catalog_category_entity(entity_id),
    name VARCHAR(255) NOT NULL,
    path VARCHAR(255),
    level INT DEFAULT 0,
    position INT DEFAULT 0,
    children_count INT DEFAULT 0,
    is_active SMALLINT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE catalog_product_entity (
    entity_id SERIAL PRIMARY KEY,
    sku VARCHAR(64) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    brand VARCHAR(255),
    type_id VARCHAR(50) DEFAULT 'simple',
    attribute_set_id SMALLINT REFERENCES eav_attribute_set(attribute_set_id),
    category_id INT REFERENCES catalog_category_entity(entity_id),
    cost_price DECIMAL(19, 4),
    list_price DECIMAL(19, 4),
    country_of_origin VARCHAR(255),
    is_seasonal SMALLINT DEFAULT 0,
    season_tag VARCHAR(100),
    is_active SMALLINT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- EAV Tables for Product Attributes
-- ============================================================================

CREATE TABLE catalog_product_entity_varchar (
    value_id SERIAL PRIMARY KEY,
    entity_id INT NOT NULL REFERENCES catalog_product_entity(entity_id),
    attribute_id SMALLINT,
    store_id SMALLINT DEFAULT 0,
    value VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE catalog_product_entity_decimal (
    value_id SERIAL PRIMARY KEY,
    entity_id INT NOT NULL REFERENCES catalog_product_entity(entity_id),
    attribute_id SMALLINT,
    store_id SMALLINT DEFAULT 0,
    value DECIMAL(19, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Category-Product Relations
-- ============================================================================

CREATE TABLE catalog_category_product (
    category_id INT NOT NULL REFERENCES catalog_category_entity(entity_id),
    product_id INT NOT NULL REFERENCES catalog_product_entity(entity_id),
    position INT DEFAULT 0,
    PRIMARY KEY (category_id, product_id)
);

CREATE TABLE catalog_product_website (
    product_id INT NOT NULL REFERENCES catalog_product_entity(entity_id),
    website_id SMALLINT NOT NULL REFERENCES core_website(website_id),
    PRIMARY KEY (product_id, website_id)
);

CREATE TABLE cataloginventory_stock (
    stock_id SMALLSERIAL PRIMARY KEY,
    stock_name VARCHAR(255) NOT NULL UNIQUE
);

-- Inventory Management
-- ============================================================================

CREATE TABLE cataloginventory_stock_item (
    item_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES catalog_product_entity(entity_id),
    stock_id SMALLINT DEFAULT 1,
    qty DECIMAL(12, 4) DEFAULT 0,
    is_in_stock SMALLINT DEFAULT 0,
    manage_stock SMALLINT DEFAULT 1,
    max_sale_qty DECIMAL(12, 4) DEFAULT 0,
    backorders SMALLINT DEFAULT 0,
    notify_stock_qty DECIMAL(12, 4) 4) DEFAULT 0,
    backorders SMALLINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, stock_id)
);

CREATE TABLE cataloginventory_stock_status (
    product_id INT NOT NULL REFERENCES catalog_product_entity(entity_id),
    website_id SMALLINT NOT NULL REFERENCES core_website(website_id),
    stock_id SMALLINT DEFAULT 1,
    qty DECIMAL(12, 4) DEFAULT 0,
    stock_status SMALLINT DEFAULT 0,
    PRIMARY KEY (product_id, website_id, stock_id)
);

-- Customer Management
-- ============================================================================

CREATE TABLE customer_entity (
    entity_id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(255) NOT NULL,
    gender VARCHAR(50),
    birth_year INT,
    age_group VARCHAR(50),
    occupation VARCHAR(255),
    province VARCHAR(255),
    nationality VARCHAR(255),
    website_id SMALLINT REFERENCES core_website(website_id),
    store_id SMALLINT REFERENCES core_store(store_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updalty_tier VARCHAR(50) DEFAULT 'Silver',
    group_id SMALLINT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active SMALLINT DEFAULT 1
);

CREATE TABLE customer_address_entity (
    entity_id SERIAL PRIMARY KEY,
    parent_id INT NOT NULL REFERENCES customer_entity(entity_id),
    address_type VARCHAR(50),
    firstname VARCHAR(255),
    lastname VARCHAR(255),
    street VARCHAR(500),
    postcode VARCHAR(20),
    is_active SMALLINT DEFAULT 1,
    city VARCHAR(255),
    country_id VARCHAR(2),
    telephone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ausername VARCHAR(255) UNIQUE,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    gender VARCHAR(50),
    birth_date DATE,
    hire_date DATE,
    position VARCHAR(255),
    department VARCHAR(255),
    store_id SMALLINT REFERENCES core_store(store_id),
    manager_id INT REFERENCES admin_user(user_id),
    has_training SMALLINT DEFAULT 0,
    training_level VARCHAR(255),
    total_training_days INT DEFAULT 0,
    training_score DECIMAL(5, 2),
    last_training_date DATE,
    is_active SMALLINT DEFAULT 1,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modifiedng SMALLINT DEFAULT 0,
    training_level VARCHAR(255),
    total_training_days INT DEFAULT 0,
    training_score DECIMAL(5, 2),
    is_active SMALLINT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sales: Quote (Giỏ hàng)
-- ============================================================================

CREATE TABLE sales_flat_quote (
    entity_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customer_entity(entity_id),
    store_id SMALLINT NOT NULL REFERENCES core_store(store_id),
    is_active SMALLINT DEFAULT 1,
    items_count INT DEFAULT 0,
    items_qty DECIMAL(12, 4) DEFAULT 0,
    grand_total DECIMAL(19, 4) DEFAULT 0,
    coupon_code VARCHAR(255),
    customer_email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
name VARCHAR(255),
    
CREATE TABLE sales_flat_quote_item (
    item_id SERIAL PRIMARY KEY,
    quote_id INT NOT NULL REFERENCES sales_flat_quote(entity_id),
    product_id INT NOT NULL REFERENCES catalog_product_entity(entity_id),
    sku VARCHAR(64),
    qty DECIMAL(12, 4) DEFAULT 1,
    price DECIMAL(19, 4),
    row_total DECIMAL(19, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sales: Order (Đơn hàng)
-- ============================================================================

CREATE TABLE sales_flat_order (
    entity_id SERIAL PRIMARY KEY,
    increment_id VARCHAR(50) NOT NULL UNIQUE,
    quote_id INT REFERENCES sales_flat_quote(entity_id),
    customer_id INT REFERENCES customer_entity(entity_id),
    store_id SMALLINT NOT NULL REFERENCES core_store(store_id),
    employee_id INT REFERENCES admin_user(user_id),
    status VARCHAR(50) DEFAULT 'pending',
    brand VARCHAR(255),
    category VARCHAR(255),
    grand_total DECIMAL(19, 4) DEFAULT 0,
    total_qty_ordered DECIMAL(12, 4) DEFAULT 0,
    subtotal DECIMAL(19, 4) DEFAULT 0,
    tax_amount DECIMAL(19, 4) DEFAULT 0,
    discount_amount DECIMAL(19, 4) DEFAULT 0,
    promo_code VARCHAR(255),
    payment_method VARCHAR(255),
    cost DECIMAL(19, 4) DEFAULT 0,
    gross_profit DECIMAL(19, 4) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales_flat_order_item (
    item_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES sales_flat_order(entity_id),
    product_id INT REFERENCES catalog_product_entity(entity_id),
    sku VARCHAR(64),
    name VARCHAR(255),
    brand VARCHAR(255),
    category VARCHAR(255),
    qty_ordered DECIMAL(12, 4) DEFAULT 1,
    qty_invoiced DECIMAL(12, 4) DEFAULT 0,
    qty_shipped DECIMAL(12, 4) DEFAULT 0,
    price DECIMAL(19, 4),
    cost DECIMAL(19, 4),
    discount_amount DECIMAL(19, 4) DEFAULT 0,
    row_total DECIMAL(19, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales_flat_order_address (
    entity_id SERIAL PRIMARY KEY,
    parent_id INT NOT NULL REFERENCES sales_flat_order(entity_id),
    address_type VARCHAR(50),
    firstname VARCHAR(255),
    lastname VARCHAR(255),
    street VARCHAR(500),
    city VARCHAR(255),
    country_id VARCHAR(2),
    telephone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales_flat_order_payment (
    entity_id SERIAL PRIMARY KEY,
    parent_id INT NOT NULL REFERENCES sales_flat_order(entity_id),
    method VARCHAR(255),
    amount_ordered DECIMAL(19, 4) DEFAULT 0,
    amount_paid DECIMAL(19, 4) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI & Targets
-- ============================================================================

CREATE TABLE sales_target (
    target_id SERIAL PRIMARY KEY,
    period VARCHAR(10),
    year INT,
    month INT,
    store_id INT REFERENCES core_store(store_id),
    employee_id INT REFERENCES admin_user(user_id),
    brand VARCHAR(255),
    target_revenue DECIMAL(19, 4),
    target_orders DECIMAL(12, 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for Performance
-- ============================================================================

CREATE INDEX idx_product_sku ON catalog_product_entity(sku);
CREATE INDEX idx_product_brand ON catalog_product_entity(brand);
CREATE INDEX idx_product_category ON catalog_product_entity(category_id);
CREATE INDEX idx_order_customer ON sales_flat_order(customer_id);
CREATE INDEX idx_order_store ON sales_flat_order(store_id);
CREATE INDEX idx_order_created ON sales_flat_order(created_at);
CREATE INDEX idx_order_item_order ON sales_flat_order_item(order_id);
CREATE INDEX idx_customer_email ON customer_entity(email);
CREATE INDEX idx_stock_product ON cataloginventory_stock_item(product_id);
CREATE INDEX idx_quote_customer ON sales_flat_quote(customer_id);
CREATE INDEX idx_category_parent ON catalog_category_entity(parent_id);
