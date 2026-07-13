# Energy-Consumption-Databricks

# ⚡ Energy Consumption Forecasting Pipeline

## 📌 Project Overview

This project implements an **Enterprise End-to-End Energy Consumption Forecasting Pipeline** using **Microsoft Azure** and **Databricks** following the **ELT (Extract, Load, Transform)** approach and **Medallion Architecture (Bronze → Silver → Gold)**.

The solution ingests household energy consumption datasets from CSV files stored in **Azure Data Lake Storage Gen2**, processes them using **Azure Databricks, PySpark, Delta Lake, and dbt**, builds analytical Dimension and Fact tables, generates business-ready Aggregation models, and visualizes insights using **Databricks SQL Dashboards**.

The project demonstrates production-ready Data Engineering concepts including:

- Azure Data Lake Storage Gen2
- Azure Databricks
- Delta Lake
- Unity Catalog
- Databricks Workflows
- dbt Transformations
- Watermarking
- Schema Evolution
- Audit Logging
- Error Handling
- Data Validation
- Delta Optimization
- Databricks SQL Warehouse
- Databricks SQL Dashboards
- Apache Airflow
- Slack Notifications
- Git & GitHub

---

# ⚡ Business Problem

Energy utility companies collect millions of smart meter readings every day.

Raw datasets are inconsistent and cannot be directly used for reporting or forecasting.

The objective is to build a scalable cloud-based Data Engineering solution that:

- Automates data ingestion
- Cleans and validates raw data
- Builds analytical datasets
- Generates business dashboards
- Supports energy consumption forecasting
- Enables better energy resource planning

---

# 🏗 Solution Architecture

```
Household Power Consumption Dataset (Kaggle)
                │
                ▼
Azure Data Lake Storage Gen2
                │
                ▼
Azure Databricks
                │
                ▼
Bronze Layer
                │
                ▼
Silver Layer
                │
                ▼
Gold Layer (dbt)
                │
                ▼
Databricks SQL Warehouse
                │
                ▼
Databricks SQL Dashboards
                │
                ▼
Export Gold Tables to ADLS Gen2
```

---

# 📐 High-Level Architecture

```
CSV Files
     │
     ▼
Azure Data Lake Storage Gen2
     │
     ▼
Azure Databricks
     │
     ▼
Bronze
     │
     ▼
Silver
     │
     ▼
Gold
     │
     ▼
Databricks SQL Warehouse
     │
     ▼
Databricks SQL Dashboards
```

---

# 🏛 Medallion Architecture

```
Raw Data
    │
    ▼
Bronze Layer
    │
    ▼
Silver Layer
    │
    ▼
Gold Layer
```

---

# ⭐ Star Schema

### Fact Table

FACT_ENERGY_USAGE

### Dimension Tables

- DIM_DEVICE
- DIM_GRID
- DIM_HOUSEHOLD
- DIM_TARIFF
- DIM_WEATHER

---

# 🛠 Technology Stack

| Layer | Technology |
|--------|------------|
| Cloud | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Azure Databricks |
| Programming | PySpark |
| SQL | Spark SQL |
| Data Warehouse | Delta Lake |
| Governance | Unity Catalog |
| Transformation | dbt |
| Workflow | Databricks Workflows |
| Orchestration | Apache Airflow |
| Dashboard | Databricks SQL Dashboard |
| Notifications | Slack |
| Version Control | Git & GitHub |

---

# 📂 Source Dataset

The project uses five source datasets.

- Device Metrics
- Energy Usage
- Grid Load
- Tariff
- Weather

---

# 📁 ADLS Folder Structure

```
ADLS

raw/

bronze/

silver/

gold/

golddata/

logs/

archive/
```

---

# 🔷 Batch Processing Flow

```
CSV Files

↓

Azure Data Lake Storage Gen2

↓

Azure Databricks

↓

Bronze Layer

↓

Silver Layer

↓

Gold Layer

↓

Databricks SQL Warehouse

↓

Databricks SQL Dashboard

↓

Export Gold Tables to ADLS
```

---

# 🥉 Bronze Layer

### Catalog

Bronze_Catalog

### Schema

Bronze_SCH

### Tables

- Bronze_Device_Metrics
- Bronze_Energy_Usage
- Bronze_Grid_Load
- Bronze_Tariff
- Bronze_Weather

### Features

- Raw Data Storage
- Watermarking
- Schema Evolution
- Audit Logging
- Error Handling
- Delta Tables

---

# 🥈 Silver Layer

### Catalog

Silver_Catalog

### Schema

Silver_SCH

### Tables

- Silver_Device_Metrics
- Silver_Energy_Usage
- Silver_Grid_Load
- Silver_Tariff
- Silver_Weather

### Transformations

- Remove Duplicates
- Data Validation
- Null Handling
- Standardization
- Type Casting
- Business Rules
- Merge
- Delta Optimization
- Time Travel

---

# 🥇 Gold Layer

### Catalog

Gold_Catalog

### Schema

Gold_SCH

## Dimension Tables

- DIM_DEVICE
- DIM_GRID
- DIM_HOUSEHOLD
- DIM_TARIFF
- DIM_WEATHER

## Fact Table

- FACT_ENERGY_USAGE

## Aggregate Tables

- DAILY_ENERGY_SUMMARY
- MONTHLY_ENERGY_SUMMARY
- YEARLY_ENERGY_SUMMARY

## Dashboard Tables

- EXECUTIVE_DASHBOARD
- BILLING_ANALYTICS
- DEVICE_MONITORING
- WEATHER_IMPACT

---

# 📊 Business KPIs

- Total Energy Consumption
- Average Energy Consumption
- Peak Demand
- Region-wise Consumption
- City-wise Consumption
- Device Efficiency
- Device Runtime
- Weather Impact
- Monthly Billing
- Tariff Comparison
- Household Consumption
- Energy Cost Analysis

---

# 📈 Data Quality

The project performs

- Duplicate Validation
- Null Validation
- Schema Validation
- Data Type Validation
- Record Count Validation
- Business Rule Validation

---

# ⚙ Delta Lake Features

- ACID Transactions
- Schema Evolution
- Time Travel
- Merge
- Vacuum
- Optimize
- Delta History

---

# 📊 Dashboards

Developed using:

- Databricks SQL Warehouse
- Databricks SQL Dashboards

### Reports

- Energy Consumption Dashboard
- Executive Dashboard
- Billing Analytics Dashboard
- Weather Impact Dashboard
- Device Monitoring Dashboard

---

# 🔒 Security & Governance

- Unity Catalog
- Role-Based Access Control (RBAC)
- Secret Management
- Audit Logging
- Data Lineage

---

# 🔄 Workflow Automation

Pipeline orchestration using:

- Databricks Workflows
- Apache Airflow

Pipeline Flow

Bronze

↓

Silver

↓

Gold

↓

Export to ADLS

↓

Dashboard Refresh

↓

Slack Notification

---

# 🧪 Testing

- Unit Testing
- Data Validation
- Schema Validation
- Row Count Validation
- Business Rule Validation

---

# 🚀 Future Enhancements

- Real-Time Energy Streaming using Azure Event Hub
- Predictive Energy Forecasting using Machine Learning
- Azure Synapse Integration
- CI/CD using Azure DevOps
- Data Quality Monitoring using Great Expectations

---

# 👩‍💻 Author

**Bhagya Sree**

## GitHub Repository

https://github.com/Deepikajagarla/Energy-Consumption-Databricks
