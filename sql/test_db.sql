SELECT * FROM orders LIMIT 100;
SELECT * FROM customers;
SELECT * FROM products;
SELECT * FROM regions;
SELECT * FROM return_stat LIMIT 100;

SELECT 
	orders.*,
	customers.customer_name,
	customers.country,
	products.product_name,
	products.category,
	regions.manager,
	return_stat.returned
FROM orders
JOIN customers ON orders.customer_id = customers.customer_id
JOIN products ON products.product_id = orders.product_id
JOIN regions ON regions.region = customers.region
JOIN return_stat ON return_stat.order_id = orders.order_id
LIMIT 100;