# separate each individual product order to one row
CREATE TABLE products AS
SELECT
unnest(string_to_array(order_products, ', ')) AS items
FROM chesterfield;

# split each items into two columns (product & price)
ALTER TABLE products RENAME TO products_old;
CREATE TABLE products AS
SELECT RTRIM(SUBSTRING(items FROM '^[^0-9,.]+'), ' - ')as product,
SUBSTRING (items FROM  '[0-9,.]+' ) as price FROM products_old;
DROP TABLE products_old;

# delete duplicated items
ALTER TABLE products RENAME TO products_old;
CREATE TABLE products AS
SELECT DISTINCT product, price FROM products ORDER BY product, price;
DROP TABLE products_old;

# create id for distinct product
ALTER TABLE products ADD COLUMN id INTEGER;
CREATE SEQUENCE product_id_seq OWNED BY products.id;
ALTER TABLE products ALTER COLUMN id SET DEFAULT nextval('product_id_seq');
UPDATE products SET id = nextval('product_id_seq');

#changes need to be done for orders table
#data type for price needs to be changed