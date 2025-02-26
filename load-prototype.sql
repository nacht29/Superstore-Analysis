CREATE DATABASE IF NOT EXISTS superstore;
USE superstore;

CREATE TABLE IF NOT EXISTS regions (
	region_name VARCHAR(10) PRIMARY KEY,
	manager VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS products (
	product_id VARCHAR(20) PRIMARY KEY,
	product_name VARCHAR(150),
	category VARCHAR(20),
	sub_category VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS customers (
	customer_id VARCHAR(20) PRIMARY KEY,
	customer_name VARCHAR(50),
	segment VARCHAR(20),
	country VARCHAR(50),
	city VARCHAR(50),
	province VARCHAR(50),
	post_code VARCHAR(15),
	region_name VARCHAR(10),
	FOREIGN KEY (region_name) REFERENCES regions(region_name)
);

CREATE TABLE IF NOT EXISTS orders (
	order_id VARCHAR(20),
	order_date DATE,
	ship_date DATE,
	ship_mode VARCHAR(20),
	customer_id VARCHAR(20),
	product_id VARCHAR(50),
	sales DECIMAL(10,2),
	quantity INT,
	discount DECIMAL(10,2),
	profit DECIMAL(10,2),
	PRIMARY KEY (order_id, product_id),
	FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
	FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE IF NOT EXISTS returns (
	order_id VARCHAR(20) PRIMARY KEY,
	returned VARCHAR(1)
);