# databricks-medallion-etl-pipeline
# Monthly Returns & Shipments ETL Pipeline

## Overview

This project implements an end-to-end Monthly Returns & Shipments ETL Pipeline using Medallion Architecture (Bronze → Silver → Gold) in PySpark with Databricks.

The pipeline processes ecommerce monthly returns and shipment datasets stored in Azure Data Lake Storage (ADLS), applies cleaning and standardization logic, and generates business-ready analytical datasets for reporting and Business Intelligence (BI) purposes.

The project is designed to support scalable and incremental monthly data processing using Databricks Autoloader, Delta Lake, and Databricks Workflows.

---

# Dataset Information

The project processes monthly ecommerce datasets stored as CSV files in Azure Data Lake Storage (ADLS).

The datasets are ingested incrementally using Databricks Autoloader and processed through the Bronze → Silver → Gold layers.

---
# Project Objectives

The main objectives of this project are:

- Build a scalable monthly ETL pipeline
- Implement Medallion Architecture
- Process ecommerce returns and shipments data
- Generate BI-ready analytical datasets
- Support incremental monthly ingestion
- Automate execution using orchestration and scheduling
- Enable reliable cloud-based data processing using Azure Databricks

---

# Medallion Architecture

This project follows the Medallion Architecture approach:

<p align="center">
  <img src="screenshot results/medallion architecture.png" width="900"/>
</p>

## Bronze Layer

The Bronze Layer is responsible for ingesting raw monthly data from Azure Data Lake Storage into Delta tables.

### Features

- Raw monthly data ingestion
- Schema preservation
- Incremental monthly processing
- Append-only ingestion
- Databricks Autoloader integration
- Reliable cloud storage ingestion

### Purpose

The Bronze layer acts as the raw data storage layer and preserves source data exactly as received from upstream systems.

---

## Silver Layer

The Silver Layer performs data cleaning, standardization, and preparation for downstream analytics.

### Features

- Data cleaning
- Deduplication
- Data type standardization
- Metadata enrichment
- Data quality improvements
- Standardized business datasets

### Purpose

The Silver layer transforms raw data into trusted and standardized datasets suitable for business processing and analytics.

---

## Gold Layer

The Gold Layer contains business-enriched and analytics-ready datasets.

### Features

- Business KPI generation
- Policy compliance metrics
- Carrier categorization
- Analytical feature engineering
- Reporting-ready datasets
- BI-ready curated tables

### Purpose

The Gold layer is optimized for:
- Business Intelligence
- Dashboarding
- Reporting
- Analytical consumption

This layer provides curated and business-ready data for downstream users and BI tools.

---

# Azure Data Lake Storage (ADLS)

The project uses Azure Data Lake Storage (ADLS) as the primary cloud storage layer for storing monthly source datasets.

### ADLS Features Used

- Scalable cloud storage
- Secure data storage
- Distributed file handling
- Integration with Databricks
- Incremental file ingestion support

---

# Connecting ADLS to Azure Databricks

Azure Databricks is connected to ADLS for reading and processing monthly datasets.

### Features

- Secure cloud integration
- Centralized storage access
- Scalable distributed processing
- Cloud-native ETL architecture
- Seamless ingestion using Autoloader

This integration enables efficient processing of large-scale ecommerce datasets in a cloud environment.

---

# Databricks Autoloader

Databricks Autoloader is used for incremental monthly ingestion.

### Features

- Automatic file detection
- Incremental ingestion
- Schema evolution support
- Scalable ingestion framework
- Efficient cloud file processing

### Benefits

- Avoids duplicate processing
- Processes only newly arrived files
- Reduces operational overhead
- Supports scalable ETL pipelines

---

# Delta Lake

Delta Lake is used as the storage format across Bronze, Silver, and Gold layers.

### Features

- ACID transactions
- Reliable data storage
- Scalable table management
- Optimized query performance
- Incremental processing support

---

# Orchestration and Scheduling

Databricks Workflows is used for orchestration and scheduling of the ETL pipeline.

The workflow automates execution of the Bronze → Silver → Gold pipeline in the correct order.

---

# Databricks Workflow Features

### Workflow Orchestration

- Sequential task execution
- Dependency management
- Layer-wise execution control
- Automated pipeline management

### Scheduling

- Monthly scheduled execution
- Automated recurring runs
- Time-based job triggering

### Monitoring

- Workflow execution tracking
- Job monitoring
- Failure handling
- Retry support


# Incremental Processing

The project supports incremental monthly processing.

### Features

- Processes only new monthly files
- Prevents duplicate ingestion
- Append-based ingestion strategy
- Checkpoint-based tracking

### Benefits

- Improved efficiency
- Reduced processing time
- Scalable monthly ingestion
- Reliable data pipeline execution

---

# Project Architecture

<p align="center">
  <img src="architecture/project_architecture.png" width="900"/>
</p>

---

# Technology Stack

| Technology | Purpose |
|------------|---------|
| Databricks | Data Engineering Platform |
| PySpark | Distributed Data Processing |
| Azure Data Lake Storage | Cloud Storage |
| Delta Lake | Storage Layer |
| Databricks Autoloader | Incremental Ingestion |
| Databricks Workflows | Orchestration & Scheduling |
| Azure Cloud | Cloud Infrastructure |

---

# Key Features

- Medallion Architecture
- Incremental ETL Processing
- Cloud-Based Data Engineering
- Azure Databricks Integration
- ADLS Integration
- Delta Lake Storage
- Databricks Autoloader
- Workflow Orchestration
- Monthly Scheduling
- BI-Ready Gold Layer


# Future Improvements

Potential future enhancements include:

- Data quality validation
- Real-time ingestion
- Power BI dashboard integration
- CI/CD pipeline integration
- Monitoring and alerting framework
- Unit testing
- Parameterized workflows

---

