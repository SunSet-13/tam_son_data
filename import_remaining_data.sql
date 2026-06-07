-- Import CSV data vào database tamson_ecommerce
-- Thực hiện sau khi disable constraints

CREATE TABLE IF NOT EXISTS cataloginventory_stock (
	stock_id SMALLSERIAL PRIMARY KEY,
	stock_name VARCHAR(255) NOT NULL UNIQUE
);

\copy catalog_category_entity FROM '/tamson_sales_data/catalog_category_entity.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy catalog_product_entity FROM '/tamson_sales_data/catalog_product_entity.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy catalog_product_entity_varchar FROM '/tamson_sales_data/catalog_product_entity_varchar.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy catalog_product_entity_decimal FROM '/tamson_sales_data/catalog_product_entity_decimal.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy catalog_category_product FROM '/tamson_sales_data/catalog_category_product.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy catalog_product_website FROM '/tamson_sales_data/catalog_product_website.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\\copy cataloginventory_stock FROM '/tamson_sales_data/cataloginventory_stock.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy cataloginventory_stock_item FROM '/tamson_sales_data/cataloginventory_stock_item.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy cataloginventory_stock_status FROM '/tamson_sales_data/cataloginventory_stock_status.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy sales_flat_quote_item FROM '/tamson_sales_data/sales_flat_quote_item.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy sales_flat_order_item FROM '/tamson_sales_data/sales_flat_order_item.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy sales_flat_order_address FROM '/tamson_sales_data/sales_flat_order_address.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy sales_flat_order_payment FROM '/tamson_sales_data/sales_flat_order_payment.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
\copy sales_target FROM '/tamson_sales_data/sales_target.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
