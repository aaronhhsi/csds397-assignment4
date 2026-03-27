#!/bin/bash
set -e

DB_NAME="company_db"
DBT_USER="dbt_user"
DBT_PASSWORD="dbt_pass"

DB_HOST="localhost"
DB_PORT="5432"

echo "======================================"
echo "SETTING UP POSTGRES + DBT JAFFLE SHOP"
echo "======================================"

# -------------------------
# Fix broken Yarn repo (Codespaces)
# -------------------------
sudo rm -f /etc/apt/sources.list.d/yarn.list
sudo rm -f /etc/apt/trusted.gpg.d/yarn.gpg

# -------------------------
# Install PostgreSQL
# -------------------------
if ! command -v psql >/dev/null 2>&1; then
  sudo apt-get update -y
  sudo apt-get install -y postgresql postgresql-contrib
fi

# Start PostgreSQL (NO sudo password prompt)
sudo su -c "service postgresql start" > /dev/null 2>&1
sleep 2

# -------------------------
# Recreate database
# -------------------------
sudo su -c "sudo -u postgres dropdb $DB_NAME 2>/dev/null || true"
sudo su -c "sudo -u postgres createdb $DB_NAME"

# -------------------------
# Create dbt role + permissions (NO sudo password prompt)
# -------------------------
sudo su -c "sudo -u postgres psql -q <<'EOF' > /dev/null 2>&1
DROP ROLE IF EXISTS dbt_user;
CREATE ROLE dbt_user LOGIN PASSWORD 'dbt_pass';
GRANT ALL PRIVILEGES ON DATABASE company_db TO dbt_user;
ALTER DATABASE company_db OWNER TO dbt_user;
EOF"

# -------------------------
# Python + dbt
# -------------------------
python3 -m pip install --upgrade pip
python3 -m pip install dbt-postgres

# -------------------------
# Clone jaffle-shop
# -------------------------
if [ ! -d "jaffle-shop" ]; then
  git clone https://github.com/dbt-labs/jaffle-shop.git
fi

cd jaffle-shop

# -------------------------
# dbt profile setup
# -------------------------
mkdir -p ~/.dbt

cat > ~/.dbt/profiles.yml <<EOF
default:
  target: dev
  outputs:
    dev:
      type: postgres
      host: $DB_HOST
      user: $DBT_USER
      password: $DBT_PASSWORD
      port: $DB_PORT
      dbname: $DB_NAME
      schema: jaffle_shop
      threads: 4
EOF

# -------------------------
# Run dbt
# -------------------------
dbt debug
dbt deps
dbt seed --full-refresh --vars '{"load_source_data": true}'
dbt build

# -------------------------
# Validate (top 10 FOOD orders by order_total with customer name)
# -------------------------
sudo su -c "sudo -u postgres psql -d $DB_NAME -c \"
SELECT
  o.order_id,
  c.customer_name,
  o.ordered_at,
  o.order_total
FROM jaffle_shop.orders o
JOIN jaffle_shop.customers c
  ON o.customer_id = c.customer_id
WHERE o.is_food_order = true
ORDER BY o.order_total DESC
LIMIT 10;
\""




# -------------------------
# dbt Docs
# -------------------------
dbt docs generate
dbt docs serve --port 8082
