USE test;

INSERT INTO products (name, description, price, stock)
VALUES
('Laptop', 'Powerful laptop', 1200.00, 10),
('Mouse', 'Wireless mouse', 25.00, 50);

INSERT INTO users (username, password, email)
VALUES
('admin', SHA2('admin123', 256), 'admin@example.com');

INSERT INTO orders (user_id, product_id, quantity, total_price, status)
VALUES
(1, 1, 1, 1200.00, 'pending');

