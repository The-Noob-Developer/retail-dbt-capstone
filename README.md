# Retail Data Engineering Capstone — dbt + Snowflake

An end-to-end ETL pipeline built using the Medallion Architecture (Bronze → Silver → Gold)
on Snowflake and dbt, with raw data sourced from Azure Data Lake Storage (ADLS).

---

## Project Overview

This capstone project simulates a real-world retail analytics platform. Raw JSON data files covering customers, products, orders, stores, employees, suppliers, and marketing campaigns are ingested from Azure Data Lake Storage, progressively transformed through three data layers, and finally served as dimensional models ready for business intelligence and reporting.

---

## Architecture

```
Azure Data Lake Storage (ADLS)
           |
           v
  Snowflake External Tables
           |
           v
     BRONZE LAYER
     (Raw, unprocessed data)
           |
           v
     SILVER LAYER
     (Cleaned and transformed data)
           |
           v
     GOLD LAYER
     (Star Schema: Dimensions and Facts)
           |
           v
  Reporting Views / BI Dashboards
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Snowflake | Cloud data warehouse |
| dbt (Snowflake adapter) | Data transformation and pipeline orchestration |
| Azure Data Lake Storage | Raw JSON file storage |
| SQL | Transformations |
| GitHub | Version control |

---

## Project Structure

```
retail_capstone/
|
├── models/
|   ├── bronze/
|   |   ├── br_customer.sql
|   |   ├── br_product.sql
|   |   ├── br_orders.sql
|   |   ├── br_store.sql
|   |   ├── br_employee.sql
|   |   ├── br_supplier.sql
|   |   └── br_campaign.sql
|   |
|   ├── silver/
|   |   ├── stg_customer.sql
|   |   ├── stg_product.sql
|   |   ├── stg_orders.sql
|   |   ├── stg_store.sql
|   |   ├── stg_employee.sql
|   |   ├── stg_supplier.sql
|   |   └── stg_campaign.sql
|   |
|   └── gold/
|       ├── dimensions/
|       |   ├── dim_customer.sql
|       |   ├── dim_product.sql
|       |   ├── dim_store.sql
|       |   ├── dim_supplier.sql
|       |   ├── dim_employee.sql
|       |   ├── dim_date.sql
|       |   └── dim_marketing_campaign.sql
|       |
|       └── facts/
|           ├── fact_sales.sql
|           ├── fact_inventory.sql
|           └── fact_marketing_performance.sql
|
├── dbt_project.yml
├── profiles.yml
└── README.md
```

---

## Source Data

Raw data is stored in ADLS as JSON files:

| Dataset | Description |
|---------|-------------|
| customers.json | Customer demographics and contact information |
| products.json | Product catalog with pricing and stock levels |
| orders.json | Transactional sales records |
| stores.json | Store locations and metadata |
| employees.json | Employee details and performance data |
| suppliers.json | Supplier contact information and payment terms |
| marketing_campaign.json | Campaign spend, targets, and revenue data |

---

## Bronze Layer — Raw Data

**Purpose:** Ingest raw data from Snowflake external tables into the warehouse without transformation.

- Data copied directly from external tables backed by ADLS
- Incremental loading enabled
- SCD Type 2 tracking enabled
- Duplicate rows removed

**Output tables:** `bronze.customer`, `bronze.product`, `bronze.orders`, `bronze.store`, `bronze.employee`, `bronze.supplier`, `bronze.campaign`

---

## Silver Layer — Cleaned and Transformed Data

**Purpose:** Apply business logic, clean data, and derive enriched columns.

### Generic Cleaning (applied to all tables)
- Trim whitespace and standardize text casing
- Handle NULL values and validate email and phone formats
- Normalize date formats and currency values
- Remove special characters

### Table-Specific Transformations

| Table | Key Transformations |
|-------|-------------------|
| Customer | Full name, age calculation, customer segment (Young / Middle-Aged / Senior) |
| Product | Profit margin, low stock flag, product hierarchy, full description |
| Orders | Profit, profit margin, time-of-day derivation, week / month / quarter / year columns, shipping efficiency |
| Employee | Full name, tenure calculation, standardized role names, performance metrics |
| Store | Size category (Small / Medium / Large), store age, revenue and performance metrics |
| Marketing Campaign | Campaign duration, ROI calculation, audience segmentation, budget normalization |

---

## Gold Layer — Dimensional Model (Star Schema)

**Purpose:** Serve clean, analytics-ready tables for business intelligence tools and reporting dashboards.

### Dimension Tables

| Table | Key Columns |
|-------|-------------|
| dim_customer | CustomerKey, FullName, Segment, Email, SCD Type 2 tracking |
| dim_product | ProductKey, Category, Brand, UnitPrice, CostPrice |
| dim_date | DateKey, Year, Quarter, Month, Week, Season, HolidayFlag |
| dim_store | StoreKey, Region, SizeCategory, StoreType, OpeningDate |
| dim_supplier | SupplierKey, SupplierName, PaymentTerms, SupplierType |
| dim_employee | EmployeeKey, Role, Tenure, PerformanceMetrics |
| dim_marketing_campaign | CampaignKey, Budget, Duration, ROI, StartDate, EndDate |

### Fact Tables

| Table | Description |
|-------|-------------|
| fact_sales | Order-level sales transactions including profit, discount, and shipping cost |
| fact_inventory | Stock levels, inventory valuation, turnover ratios, and supplier contribution |
| fact_marketing_performance | Campaign influence on sales, new customers acquired, and ROI |

---

## Reporting Views

Analytical views are built on the Gold layer to support dashboard consumption across the following domains:

- **Sales Performance** — Total sales by category and region, top-selling products
- **Inventory Analysis** — Stock turnover, slow-moving products, inventory valuation
- **Customer Analytics** — Customer lifetime value, repeat purchase rate, segmentation
- **Employee Performance** — Sales contribution by employee, tenure versus performance analysis
- **Marketing Performance** — ROI per campaign, sales impact, customer engagement metrics

---

## Key Concepts Demonstrated

- Medallion Architecture (Bronze, Silver, Gold)
- Star Schema and Dimensional Modeling
- Slowly Changing Dimensions (SCD Type 2)
- Incremental dbt models
- Window functions for surrogate key generation and rankings
- Data quality validation and transformation at scale
- ETL pipeline orchestration using dbt

---

## Author

**Harsh Raj Gupta**  
Data Engineer Intern  
Accordion
