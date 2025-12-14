#!/bin/bash

# ================================
# TiDB CDC TEST SCRIPT
# ================================
# This script inserts, updates, deletes and batch-loads data
# to verify TiCDC → Kafka → Node Consumer → Elasticsearch → Prometheus
# ================================

NET="tidb-cdc_tidb-net"
MYSQL="docker run --rm --network $NET mysql:8"

echo "🔵 Running CDC Data Tests..."
echo ""

run_sql() {
    $MYSQL mysql -h tidb-server -P 4000 -u root test -e "$1"
}

print_section() {
    echo ""
    echo "============================"
    echo ">>> $1"
    echo "============================"
}

# ------------------------------
print_section "Creating sample products"
# ------------------------------
run_sql "
INSERT INTO products (name, description, price, stock) VALUES
 ('Laptop Stand','Aluminum stand',150,30),
 ('Mouse','Wireless mouse',45,50),
 ('Keyboard','Mechanical keyboard',120,40)
;
"

# ------------------------------
print_section "Updating product prices"
# ------------------------------
run_sql "
UPDATE products SET price = price * 1.10 WHERE stock > 20;
"

# ------------------------------
print_section "Creating sample users"
# ------------------------------
run_sql "
INSERT INTO users (username, password, email) VALUES
 ('user1', SHA2('pass1',256), 'u1@example.com'),
 ('user2', SHA2('pass2',256), 'u2@example.com');
"

# ------------------------------
print_section "Creating sample orders"
# ------------------------------
run_sql "
INSERT INTO orders (user_id, product_id, quantity, total_price, status) VALUES
 (1,1,1,150,'pending'),
 (2,2,2,90,'pending');
"

# ------------------------------
print_section "Updating orders → completed"
# ------------------------------
run_sql "
UPDATE orders SET status='completed' WHERE status='pending';
"

# ------------------------------
print_section "Deleting out-of-stock products"
# ------------------------------
run_sql "
DELETE FROM products WHERE stock < 10;
"

# ------------------------------
print_section "Batch Inserts of 20 products"
# ------------------------------
for i in $(seq 1 20); do
  PRICE=$(( (RANDOM % 200) + 20 ))
  STOCK=$(( (RANDOM % 50) + 1 ))
  run_sql "INSERT INTO products (name, price, stock) VALUES ('BatchProduct$i', $PRICE, $STOCK);"
done

echo ""
echo "✅ DONE! All operations executed."
echo "Now check:"
echo " - Elasticsearch:  curl -X GET localhost:9200/tidb-cdc-events/_search?pretty"
echo " - Prometheus:     http://localhost:9090/metrics"
echo " - Grafana:        http://localhost:3001"

