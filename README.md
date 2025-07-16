# Ecommerce Data Pipeline

This project demonstrates an ecommerce data pipeline that automates the extraction, transformation, and loading (ETL) of data from the OLTP system of an ecommerce platform. The pipeline centralizes sales and payment data, enabling efficient processing, regulatory compliance, and analytics to support business decision-making and reporting.

## Table of Contents
- [Ecommerce Data Pipeline](#ecommerce-data-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Architecture](#architecture)
  - [Project Structure](#project-structure)
  - [Data Source](#data-source)
  - [Quickstart](#quickstart)
    - [Clone the repository](#clone-the-repository)
    - [Setup virtual environment](#setup-virtual-environment)
    - [Start services](#start-services)
  - [Tech Stack](#tech-stack)
    - [Apache Airflow](#apache-airflow)
    - [Amazon S3](#amazon-s3)
    - [DuckDB](#duckdb)
    - [Snowflake](#snowflake)
    - [dbt \& Great Expectations](#dbt--great-expectations)
    - [PowerBI](#powerbi)
  - [Business Insights](#business-insights)
    - [Ecommerce Overview](#ecommerce-overview)
    - [Recommendations](#recommendations)

## Architecture

<!-- Add a diagram if available -->
<p align="center">
    <img src="assets/diagrams/architecture.svg" alt="architecture" style="border-radius: 10px;">
    </br>
  Project Architecture
</p>

## Project Structure
```shell
.
├── airflow/                /* Airflow folder, contains DAGs and scripts */
├── assets/                 /* Assets folder, contains diagrams, dashboards, etc. */
├── dbt_ecommerce/            /* dbt project folder, contains dbt models */
├── docker/                 /* Docker services configuration folder */
│   ├── airflow/               /* Airflow orchestrator configurations */
│   ├── spark-app              /* Spark container for transformation logic */
│   ├── spark-master           /* Spark container for distributing workloads */
│   ├── spark-worker           /* Spark container for code execution */
├── .gitignore
├── .python-version
├── uv.lock
├── README.md
├── snowflake-setup.md      /* Instructions to setup Snowflake beforehand */
├── pyproject.toml          /* Project dependencies, run uv sync in virtual environment */
├── docker-compose.yaml     /* Docker Compose file to define services */
```

## Data Source

The ecommerce datasets include sales and payments information. These are typically exported from the OLTP system of an ecommerce platform and ingested into the pipeline for processing and analytics.

<p align="center">
    <img src="assets/diagrams/diagram.png" alt="source-relational-model" style="border-radius: 10px;">
    </br>
  Source Relational Model
</p>

## Quickstart

> **Prerequisites:**
> - **Git** for version control.
> - **uv** or **Conda/Mamba** for virtual environment management.
> - **Docker** for containerization.

### Clone the repository
```shell
git clone https://github.com/PhoenixPhungNguyen/ecommerce-data-pipeline.git
```

### Setup virtual environment
Navigate to your cloned directory:
```shell
uv venv --python 3.11
source .venv/Scripts/activate
uv sync
```

### Start services

> - Create a `.env` file at the root level for environment variables (see `.env.example`).
> - Ensure any required data files are placed in the appropriate folders as described in the documentation.

To start all services:
```shell
make up
```
Or, without Makefile:
```shell
docker compose up -d --build
```

## Tech Stack

### Apache Airflow
Orchestrates ETL workflows for ecommerce data processing.
<p align="center">
    <img src="assets/diagrams/airflow-dag.png" alt="airflow-dag" style="border-radius: 10px;">
    </br>
  Airflow overview
</p>

### Amazon S3
Data Lake
<p align="center">
    <img src="assets/diagrams/amazon-s3.png" alt="amazon-s3" style="border-radius: 10px;">
    </br>
  Amazon S3 overview
</p>

### DuckDB
Used for lightweight data transformation and analytics at the data lake layer.

### Snowflake
Data Warehouse
<p align="center">
    <img src="assets/diagrams/snowflake.png" alt="snowflake" style="border-radius: 10px;">
    </br>
  Snowflake overview
</p>

### dbt & Great Expectations
dbt transforms data into analytics-ready models; Great Expectations validates data quality.
<p align="center">
    <img src="assets/diagrams/dbt-dag.png" alt="dbt-dag" style="border-radius: 10px;">
    </br>
  dbt overview
</p>

### PowerBI
For dashboarding and visualization of ecommerce analytics.
<p align="center">
    <img src="assets/diagrams/powerbi.png" alt="powerbi" style="border-radius: 10px;">
    </br>
  PowerBI overview
</p>

## Business Insights

Once the pipeline is operational, you can generate insights such as:

### Ecommerce Overview
- Total ecommerce expenses by month, department, or location.
- Trends in salary, overtime, and deductions.

### Recommendations
- Optimize ecommerce schedules to improve cash flow.
- Target retention strategies for key employee segments.
- Automate compliance checks to reduce audit risks.
