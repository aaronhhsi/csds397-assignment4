#!/bin/bash
set -e

DB_NAME="company_db"
DBT_USER="dbt_user"
DBT_PASSWORD="dbt_pass"

DB_HOST="localhost"
DB_PORT="5432"

echo "======================================"
echo "SETTING UP POSTGRES + DBT"
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
# Create dbt role + permissions
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
# Set up DBT project
# -------------------------
PROJECT_NAME="aaron_dbt_data_processing"

if [ ! -d "$PROJECT_NAME" ]; then
  dbt init $PROJECT_NAME --skip-profile-setup
fi

cd $PROJECT_NAME

# -------------------------
# Remove jaffle_shop example models
# -------------------------
rm -f models/example/my_first_dbt_model.sql
rm -f models/example/my_second_dbt_model.sql
rm -f models/example/schema.yml
rmdir models/example 2>/dev/null || true

# -------------------------
# dbt profile setup
# -------------------------
mkdir -p ~/.dbt

cat > ~/.dbt/profiles.yml <<EOF
aaron_dbt_data_processing:
  target: dev
  outputs:
    dev:
      type: postgres
      host: $DB_HOST
      user: $DBT_USER
      password: $DBT_PASSWORD
      port: $DB_PORT
      dbname: $DB_NAME
      schema: dbt
      threads: 4
EOF

# -------------------------
# Copy CSV into seeds/
# -------------------------
mkdir -p seeds
cp ../employee_data_clean.csv seeds/employee_data_clean.csv

# -------------------------
# Write dbt_project.yml cleanly (avoids duplicate keys)
# -------------------------
cat > dbt_project.yml <<'EOF'
name: 'aaron_dbt_data_processing'
version: '1.0.0'
config-version: 2

profile: 'aaron_dbt_data_processing'

model-paths: ["models"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  aaron_dbt_data_processing:
    staging:
      +schema: staging
      +materialized: view
    marts:
      +schema: gold
      +materialized: table
EOF

# -------------------------
# Create project folder structure
# -------------------------
mkdir -p models/staging
mkdir -p models/marts

# -------------------------
# Staging model — ref() the seed directly, no sources.yml needed
# -------------------------
cat > models/staging/stg_employees.sql <<'EOF'
SELECT
    "Employee Id"           AS employee_id,
    "Name"                  AS name,
    "Age"                   AS age,
    "Department"            AS department,
    "Date of Joining"       AS date_of_joining,
    "Years of Experience"   AS years_of_experience,
    "Country"               AS country,
    "Salary"                AS salary,
    "Performance Rating"    AS performance_rating,
    "Total Sales"           AS total_sales,
    "Support Rating"        AS support_rating
FROM {{ ref('employee_data_clean') }}
EOF

# -------------------------
# Mart models
# -------------------------
cat > models/marts/salary_to_department_analysis.sql <<'EOF'
SELECT
    department,
    COUNT(*)                                AS employee_count,
    ROUND(AVG(salary)::numeric, 2)          AS avg_salary,
    ROUND(MIN(salary)::numeric, 2)          AS min_salary,
    ROUND(MAX(salary)::numeric, 2)          AS max_salary,
    ROUND(STDDEV(salary)::numeric, 2)       AS salary_stddev
FROM {{ ref('stg_employees') }}
GROUP BY department
ORDER BY avg_salary DESC
EOF

cat > models/marts/salary_to_tenure_analysis.sql <<'EOF'
WITH tenure_classified AS (
    SELECT
        salary,
        years_of_experience,
        CASE
            WHEN years_of_experience BETWEEN 0  AND 2  THEN 1
            WHEN years_of_experience BETWEEN 3  AND 5  THEN 2
            WHEN years_of_experience BETWEEN 6  AND 8  THEN 3
            WHEN years_of_experience BETWEEN 9  AND 11 THEN 4
            ELSE 5
        END AS tenure_bucket,
        CASE
            WHEN years_of_experience BETWEEN 0  AND 2  THEN '0-2 yrs (Entry)'
            WHEN years_of_experience BETWEEN 3  AND 5  THEN '3-5 yrs (Mid)'
            WHEN years_of_experience BETWEEN 6  AND 8  THEN '6-8 yrs (Senior)'
            WHEN years_of_experience BETWEEN 9  AND 11 THEN '9-11 yrs (Expert)'
            ELSE '12+ yrs (Veteran)'
        END AS tenure_range
    FROM {{ ref('stg_employees') }}
)
SELECT
    tenure_bucket,
    tenure_range,
    COUNT(*)                                AS employee_count,
    ROUND(AVG(salary)::numeric, 2)          AS avg_salary,
    ROUND(MIN(salary)::numeric, 2)          AS min_salary,
    ROUND(MAX(salary)::numeric, 2)          AS max_salary,
    ROUND(STDDEV(salary)::numeric, 2)       AS salary_stddev
FROM tenure_classified
GROUP BY tenure_bucket, tenure_range
ORDER BY tenure_bucket
EOF

cat > models/marts/performance_by_salary_analysis.sql <<'EOF'
SELECT
    performance_rating,
    COUNT(*)                                AS employee_count,
    ROUND(AVG(salary)::numeric, 2)          AS avg_salary,
    ROUND(MIN(salary)::numeric, 2)          AS min_salary,
    ROUND(MAX(salary)::numeric, 2)          AS max_salary,
    ROUND(STDDEV(salary)::numeric, 2)       AS salary_stddev
FROM {{ ref('stg_employees') }}
GROUP BY performance_rating
ORDER BY performance_rating
EOF

cat > models/marts/salary_by_country_analysis.sql <<'EOF'
SELECT
    country,
    COUNT(*)                                    AS employee_count,
    ROUND(AVG(salary)::numeric, 2)              AS avg_salary,
    ROUND(MIN(salary)::numeric, 2)              AS min_salary,
    ROUND(MAX(salary)::numeric, 2)              AS max_salary,
    ROUND(AVG(performance_rating)::numeric, 2)  AS avg_performance_rating
FROM {{ ref('stg_employees') }}
GROUP BY country
ORDER BY avg_salary DESC
EOF

cat > models/marts/department_performance_analysis.sql <<'EOF'
SELECT
    department,
    performance_rating,
    COUNT(*)                                        AS employee_count,
    ROUND(AVG(salary)::numeric, 2)                  AS avg_salary,
    ROUND(AVG(years_of_experience)::numeric, 2)     AS avg_years_experience
FROM {{ ref('stg_employees') }}
GROUP BY department, performance_rating
ORDER BY department, performance_rating
EOF

cat > models/marts/sales_productivity_analysis.sql <<'EOF'
WITH sales_classified AS (
    SELECT
        salary,
        performance_rating,
        total_sales,
        CASE
            WHEN total_sales = 0                        THEN 1
            WHEN total_sales BETWEEN 1 AND 50000        THEN 2
            WHEN total_sales BETWEEN 50001 AND 100000   THEN 3
            ELSE 4
        END AS sales_tier,
        CASE
            WHEN total_sales = 0                        THEN 'Non-Sales Role'
            WHEN total_sales BETWEEN 1 AND 50000        THEN '$1-$50k'
            WHEN total_sales BETWEEN 50001 AND 100000   THEN '$50k-$100k'
            ELSE '$100k+'
        END AS sales_range
    FROM {{ ref('stg_employees') }}
)
SELECT
    sales_tier,
    sales_range,
    COUNT(*)                                        AS employee_count,
    ROUND(AVG(salary)::numeric, 2)                  AS avg_salary,
    ROUND(AVG(performance_rating)::numeric, 2)      AS avg_performance_rating,
    ROUND(AVG(total_sales)::numeric, 2)             AS avg_total_sales
FROM sales_classified
GROUP BY sales_tier, sales_range
ORDER BY sales_tier
EOF

cat > models/marts/age_band_salary_analysis.sql <<'EOF'
WITH age_classified AS (
    SELECT
        salary,
        years_of_experience,
        performance_rating,
        CASE
            WHEN age < 30              THEN 1
            WHEN age BETWEEN 30 AND 39 THEN 2
            WHEN age BETWEEN 40 AND 49 THEN 3
            ELSE 4
        END AS age_band,
        CASE
            WHEN age < 30              THEN 'Under 30'
            WHEN age BETWEEN 30 AND 39 THEN '30-39'
            WHEN age BETWEEN 40 AND 49 THEN '40-49'
            ELSE '50+'
        END AS age_range
    FROM {{ ref('stg_employees') }}
)
SELECT
    age_band,
    age_range,
    COUNT(*)                                        AS employee_count,
    ROUND(AVG(salary)::numeric, 2)                  AS avg_salary,
    ROUND(AVG(years_of_experience)::numeric, 2)     AS avg_years_experience,
    ROUND(AVG(performance_rating)::numeric, 2)      AS avg_performance_rating
FROM age_classified
GROUP BY age_band, age_range
ORDER BY age_band
EOF

# -------------------------
# Run dbt
# -------------------------
dbt debug
dbt seed --full-refresh
dbt run

# -------------------------
# Print all mart outputs
# -------------------------
echo ""
echo "======================================"
echo "1. SALARY BY DEPARTMENT"
echo "======================================"
sudo su -c "sudo -u postgres psql -d $DB_NAME -c 'SELECT * FROM dbt_gold.salary_to_department_analysis;'"

echo ""
echo "======================================"
echo "2. SALARY BY TENURE"
echo "======================================"
sudo su -c "sudo -u postgres psql -d $DB_NAME -c 'SELECT * FROM dbt_gold.salary_to_tenure_analysis;'"

echo ""
echo "======================================"
echo "3. PERFORMANCE BY SALARY"
echo "======================================"
sudo su -c "sudo -u postgres psql -d $DB_NAME -c 'SELECT * FROM dbt_gold.performance_by_salary_analysis;'"

echo ""
echo "======================================"
echo "4. SALARY BY COUNTRY"
echo "======================================"
sudo su -c "sudo -u postgres psql -d $DB_NAME -c 'SELECT * FROM dbt_gold.salary_by_country_analysis;'"

echo ""
echo "======================================"
echo "5. DEPARTMENT PERFORMANCE ANALYSIS"
echo "======================================"
sudo su -c "sudo -u postgres psql -d $DB_NAME -c 'SELECT * FROM dbt_gold.department_performance_analysis;'"

echo ""
echo "======================================"
echo "6. SALES PRODUCTIVITY ANALYSIS"
echo "======================================"
sudo su -c "sudo -u postgres psql -d $DB_NAME -c 'SELECT * FROM dbt_gold.sales_productivity_analysis;'"

echo ""
echo "======================================"
echo "7. AGE BAND SALARY ANALYSIS"
echo "======================================"
sudo su -c "sudo -u postgres psql -d $DB_NAME -c 'SELECT * FROM dbt_gold.age_band_salary_analysis;'"

# -------------------------
# dbt Docs
# -------------------------
dbt docs generate
dbt docs serve --port 8082