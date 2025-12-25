# E-Commerce Data Pipeline & Analytics Platform
![CI](https://github.com/prasannnna/ecommerce-data-pipeline-23A91A05E5/actions/workflows/ci.yml/badge.svg)
>  CI / Testing Note  
> GitHub Actions runs automated tests using an ephemeral PostgreSQL service.  
> Database readiness is ensured using health checks and retries.  
> Local pipeline execution is stable and fully reproducible.


**Student Name:** Siva Sai Prasanna Rameswari Bojja  
**Roll Number:** 23A91A05E5  
**Submission Date:** 25 Dec 2025  

---

##  Project Overview

This project implements a **complete end-to-end Data Engineering pipeline** for an E-Commerce Analytics Platform.  
It covers data generation, ingestion, validation, transformation, warehousing, analytics, BI dashboards, automation, monitoring, and testing following **industry-grade best practices**.

The pipeline processes **30,000+ records**, maintains **100% referential integrity**, and delivers actionable insights via **Power BI dashboards**.

---

##  Project Architecture

### Data Flow

Raw CSV Data

↓

Staging Schema (staging)

↓

Production Schema (production)

↓

Warehouse Schema (warehouse - Star Schema)

↓

Analytics (SQL + Python)

↓

Power BI Dashboard


---

##  Technology Stack

| Layer | Technology |
|-----|-----------|
| Data Generation | Python, Faker |
| Data Ingestion | Python, Pandas |
| Data Validation | Python, SQL |
| Database | PostgreSQL |
| ORM / Connectivity | SQLAlchemy, psycopg2 |
| Data Warehouse | PostgreSQL (Star Schema) |
| Analytics | SQL, Pandas |
| BI Tool | Power BI Desktop |
| Orchestration | Python Scheduler |
| Monitoring | Python, SQL |
| Containerization | Docker, Docker Compose |
| Testing | Pytest, pytest-cov |
| Version Control | Git, GitHub |

---

##  Project Structure
```
ecommerce-data-pipeline-23A91A05E5/
│
├── data/
│ ├── raw/
│ ├── staging/
│ └── processed/
│
├── scripts/
│ ├── data_generation/
│ ├── ingestion/
│ ├── transformation/
│ ├── quality_checks/
│ ├── monitoring/
│ ├── pipeline_orchestrator.py
│ ├── scheduler.py
│ └── cleanup_old_data.py
│
├── sql/
│ ├── ddl/
│ ├── dml/
│ └── queries/
│
├── dashboards/
│ ├── powerbi/
│ └── screenshots/
│
├── tests/
│
├── docs/
│ ├── architecture.md
│ └── dashboard_guide.md
│
├── docker/
│
├── requirements.txt
├── pytest.ini
├── README.md
├── .gitignore
└── .env.example
```

---

##  Setup Instructions
> Note  
> Generated CSV files are excluded from version control using `.gitignore`.  
> All data can be regenerated deterministically by running the pipeline scripts.


### Prerequisites
- Python 3.10+
- PostgreSQL 12+
- Docker & Docker Compose
- Power BI Desktop
- Git

### Installation

```bash
git clone https://github.com/prasannnna/ecommerce-data-pipeline-23A91A05E5.git
cd ecommerce-data-pipeline-23A91A05E5
pip install -r requirements.txt
```
### Database Setup (Docker)

```bash
docker compose -f docker/docker-compose.yml up -d
```
## Running the Pipeline
### Full Pipeline
``` bash
python scripts/pipeline_orchestrator.py
```
### Individual Steps

``` bash
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/quality_checks/validate_data.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py
```
## Running Tests

```bash
pytest tests/ -v
```


### Coverage enabled using pytest-cov.

## Dashboard Access

- Power BI File: dashboards/powerbi/ecommerce_analytics.pbix

- Exported PDF: dashboards/powerbi/dashboard_export.pdf

- Screenshots: dashboards/screenshots/

## Database Schemas
### Staging Schema

staging.customers

staging.products

staging.transactions

staging.transaction_items

### Production Schema

production.customers

production.products

production.transactions

production.transaction_items

### Warehouse Schema

warehouse.dim_customers

warehouse.dim_products

warehouse.dim_date

warehouse.dim_payment_method

warehouse.fact_sales

warehouse.agg_daily_sales

warehouse.agg_product_performance

warehouse.agg_customer_metrics

## Data Quality & Validation

The pipeline enforces multiple data quality checks, including:
- Null and type validation on critical columns
- Referential integrity checks between customers, products, and transactions
- Duplicate detection for primary keys
- Schema-level validation before promotion to production

Invalid records are logged and excluded from downstream processing.


## Key Insights from Analytics

- Electronics is the highest revenue generating category

- Revenue shows a steady month-on-month growth

- VIP customers contribute a disproportionate share of revenue

- Weekends show higher transaction volume

- UPI and Credit Card dominate payment methods

## Project Statistics

- Total records processed: **30,000+**
- Database schemas: **3**
- Fact tables: **1**
- Dimension tables: **4**
- Aggregate tables: **3**
- Dashboard visuals: **15+**



## Challenges and Solutions

| Challenge                         | Solution                                                                  |
| --------------------------------- | ------------------------------------------------------------------------- |
| Maintaining referential integrity | Implemented controlled ID generation and validation across all schemas    |
| Query performance                 | Added indexing and created aggregate tables in the warehouse layer        |
| Idempotent pipeline runs          | Used table truncation with safe reload and incremental execution logic    |
| Dashboard performance             | Connected BI dashboards to pre-aggregated warehouse tables                |
| Test database connection issues   | Switched to environment-based configuration using `.env` and config files |


## Future Enhancements

- Kafka-based real-time streaming

- Cloud deployment (AWS / GCP)

- ML-based demand forecasting

- Real-time alerting via Slack / Email

# Contact

Name: Siva Sai Prasanna Rameswari Bojja

Roll No: 23A91A05E5

Email: 23a91a05e5@aec.edu.in


---



