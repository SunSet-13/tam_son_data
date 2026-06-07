-- Disable all foreign key constraints
ALTER TABLE IF EXISTS catalog_category_entity DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS catalog_product_entity DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS catalog_product_entity_varchar DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS catalog_product_entity_decimal DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS catalog_category_product DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS catalog_product_website DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS cataloginventory_stock DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS cataloginventory_stock_item DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS cataloginventory_stock_status DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS customer_address_entity DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS admin_user DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_quote DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_quote_item DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_order DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_order_item DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_order_address DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_order_payment DISABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_target DISABLE TRIGGER ALL;

-- Import CSV data
\COPY catalog_category_entity FROM '/tamson_sales_data/catalog_category_entity.csv'  WITH (FORMAT csv, HEADER true)
\COPY catalog_product_entity FROM '/tamson_sales_data/catalog_product_entity.csv'  WITH (FORMAT csv, HEADER true)
\COPY catalog_product_entity_varchar FROM '/tamson_sales_data/catalog_product_entity_varchar.csv'  WITH (FORMAT csv, HEADER true)
\COPY catalog_product_entity_decimal FROM '/tamson_sales_data/catalog_product_entity_decimal.csv'  WITH (FORMAT csv, HEADER true)
\COPY catalog_category_product FROM '/tamson_sales_data/catalog_category_product.csv'  WITH (FORMAT csv, HEADER true)
\COPY catalog_product_website FROM '/tamson_sales_data/catalog_product_website.csv'  WITH (FORMAT csv, HEADER true)
\\COPY cataloginventory_stock FROM '/tamson_sales_data/cataloginventory_stock.csv'  WITH (FORMAT csv, HEADER true)
\COPY cataloginventory_stock_item FROM '/tamson_sales_data/cataloginventory_stock_item.csv'  WITH (FORMAT csv, HEADER true)
\COPY cataloginventory_stock_status FROM '/tamson_sales_data/cataloginventory_stock_status.csv'  WITH (FORMAT csv, HEADER true)
\COPY sales_flat_quote_item FROM '/tamson_sales_data/sales_flat_quote_item.csv'  WITH (FORMAT csv, HEADER true)
\COPY sales_flat_order_item FROM '/tamson_sales_data/sales_flat_order_item.csv'  WITH (FORMAT csv, HEADER true)
\COPY sales_flat_order_address FROM '/tamson_sales_data/sales_flat_order_address.csv'  WITH (FORMAT csv, HEADER true)
\COPY sales_flat_order_payment FROM '/tamson_sales_data/sales_flat_order_payment.csv'  WITH (FORMAT csv, HEADER true)
\COPY sales_target FROM '/tamson_sales_data/sales_target.csv'  WITH (FORMAT csv, HEADER true)

-- Re-enable all triggers
ALTER TABLE IF EXISTS catalog_category_entity ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS catalog_product_entity ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS catalog_product_entity_varchar ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS catalog_product_entity_decimal ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS catalog_category_product ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS catalog_product_website ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS cataloginventory_stock ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS cataloginventory_stock_item ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS cataloginventory_stock_status ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS customer_address_entity ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS admin_user ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_quote ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_quote_item ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_order ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_order_item ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_order_address ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_flat_order_payment ENABLE TRIGGER ALL;
ALTER TABLE IF EXISTS sales_target ENABLE TRIGGER ALL;
