
# E-Commerce Data Pipeline Architecture

## Overview
This document describes the architecture and design decisions of the E-Commerce Analytics Platform.

---

## System Components

### 1. Data Generation Layer
- Generates synthetic e-commerce data using Python Faker
- Outputs CSV files for customers, products, transactions, and items

---

### 2. Data Ingestion Layer
- Loads raw CSVs into staging schema
- Uses batch ingestion
- Minimal constraints for speed

---

### 3. Data Storage Layer

#### Staging Schema
- Raw data as-is
- No business rules
- Temporary storage

#### Production Schema
- Fully normalized (3NF)
- Constraints enforced
- Cleaned and validated data

#### Warehouse Schema
- Star schema
- Optimized for analytics
- SCD Type 2 for dimensions

---

### 4. Data Processing Layer
- Data quality checks
- Business rule enforcement
- Profit calculations
- Aggregated metrics

---

### 5. Data Serving Layer
- Analytical SQL queries
- Python-based exports
- Pre-aggregated tables

---

### 6. Visualization Layer
- Power BI dashboards
- Interactive slicers
- Drill-down enabled

---

### 7. Orchestration Layer
- Pipeline orchestrator
- Scheduler
- Monitoring & alerting

---

## Data Models

### Star Schema
- 4 Dimensions
- 1 Fact table
- 3 Aggregate tables

---

## Deployment Architecture

- Dockerized PostgreSQL
- Pipeline services via Python
- Persistent volumes for data

