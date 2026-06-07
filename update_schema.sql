-- Update schema to drop NOT NULL constraints and add defaults
ALTER TABLE IF EXISTS catalog_category_entity 
  ALTER COLUMN is_active DROP NOT NULL,
  ALTER COLUMN is_active SET DEFAULT 1,
  ALTER COLUMN created_at SET DEFAULT NOW(),
  ALTER COLUMN updated_at SET DEFAULT NOW();

ALTER TABLE IF EXISTS catalog_product_entity 
  ALTER COLUMN created_at SET DEFAULT NOW(),
  ALTER COLUMN updated_at SET DEFAULT NOW();

ALTER TABLE IF EXISTS catalog_product_entity_varchar 
  ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE IF EXISTS catalog_product_entity_decimal 
  ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE IF EXISTS cataloginventory_stock_item 
  ALTER COLUMN created_at SET DEFAULT NOW(),
  ALTER COLUMN updated_at SET DEFAULT NOW();

ALTER TABLE IF EXISTS sales_flat_quote_item 
  ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE IF EXISTS sales_flat_order_address 
  ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE IF EXISTS sales_flat_order_payment 
  ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE IF EXISTS sales_target 
  ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE IF EXISTS admin_user 
  ALTER COLUMN created SET DEFAULT NOW(),
  ALTER COLUMN modified SET DEFAULT NOW();
