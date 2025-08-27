# Data Warehouse Analytics (Advanced SQL)

A hands-on mini data warehouse built with **SQL Server** to transform transactional data into business insights.  
It includes a **star schema** (fact + dimensions), **advanced SQL** (CTEs, window functions, views), and **reporting queries** for customer and product analytics.

---

## ✨ Highlights

- Star schema with `fact_sales`, `dim_customers`, `dim_products`
- Time-series analysis: monthly/yearly trends, running totals, moving averages
- Customer segmentation: **VIP / Regular / New**
- Product performance bands: **High / Mid / Low performers**
- Reusable reporting views:
  - `gold.report_customers`
  - `gold.report_products`

---

## 🧱 Data Model

**Database:** `DataWarehouseAnalytics`  
**Schema:** `gold`

**Tables**
- `gold.fact_sales` — order grain (order_number, product_key, customer_key, dates, quantity, price, sales_amount)
- `gold.dim_customers` — customer attributes (name, demographics, create/birth dates, etc.)
- `gold.dim_products` — product attributes (name, category, subcategory, cost, product_line)

> Tip: keep an ER/Star schema diagram in `/docs/schema.png` and reference it here:
>
> ![Star Schema](docs/schema.png)

---

## 🚀 Getting Started

### 1) Prereqs
- SQL Server (local or container)
- Ability to run `BULK INSERT` (mount or copy CSVs to the SQL container/host)
- CSVs:
  - `gold.dim_customers.csv`
  - `gold.dim_products.csv`
  - `gold.fact_sales.csv`

### 2) Create DB, Schema, and Load Data
> ⚠️ The setup script **drops and recreates** the `DataWarehouseAnalytics` database. Back up first if needed.

```sql
-- Create database & schema (DANGEROUS: drops if exists)
-- USE master;
-- ... CREATE DATABASE DataWarehouseAnalytics;
-- ... CREATE SCHEMA gold;

-- Create tables
CREATE TABLE gold.dim_customers (...);
CREATE TABLE gold.dim_products (...);
CREATE TABLE gold.fact_sales (...);

-- Load data (adjust file paths for your environment)
BULK INSERT gold.dim_customers FROM '/var/opt/mssql/import/csv-files/gold.dim_customers.csv' WITH (FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
BULK INSERT gold.dim_products  FROM '/var/opt/mssql/import/csv-files/gold.dim_products.csv'  WITH (FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
BULK INSERT gold.fact_sales    FROM '/var/opt/mssql/import/csv-files/gold.fact_sales.csv'    WITH (FIRSTROW=2, FIELDTERMINATOR=',', TABLOCK);
