# Story 4 – E-commerce Data Pipeline

## 1. Project Overview

One of my hands-on projects during my Data Engineering course was an E-commerce Data Pipeline, which focused on processing sales and payment data for analytics and reporting.

The project demonstrated an end-to-end data engineering workflow, covering data ingestion, cloud storage, data transformation, data modelling, data quality, orchestration and reporting.

### Technology Stack

- **Data Ingestion & Orchestration:** Apache Airflow
- **Cloud Storage:** Amazon S3
- **Data Processing:** DuckDB, Apache Spark
- **Data Warehouse:** Snowflake
- **Data Transformation:** dbt
- **Data Quality:** dbt Tests, Great Expectations
- **Data Modelling:** Star Schema, SCD Type 2
- **Visualisation:** Power BI
- **Notifications:** Slack
- **Environment:** Docker, Docker Compose

## 2. Story – Interview Answer

One of my hands-on projects during my Data Engineering course was an E-commerce Data Pipeline, which focused on processing sales and payment data for analytics and reporting.

The project started with eight CSV files containing sales and payment data stored in a shared Google Drive folder.

I used Apache Airflow to automate downloading these files and storing them in Amazon S3, in the raw folder.

Next, I used DuckDB for lightweight transformations, including data type casting, renaming columns, trimming whitespace, standardising text formats and removing duplicate records. The processed data was saved as Parquet files in the processed folder in S3.

After that, I used Apache Spark for larger-scale transformations, including joins and filters, before writing the output into the landing schema in Snowflake.

For the Snowflake integration, I configured Storage Integration and External Stages to securely access the Amazon S3 data. I also worked with AWS IAM Role trust relationships and Snowflake SQL commands to inspect staged files and load data.

For data modelling, I used dbt to transform the landing data through staging and intermediate models into a final Star Schema, consisting of five dimension tables and three fact tables.

I also worked with Slowly Changing Dimension Type 2 to track historical changes in selected dimension tables, and used dbt tests and Great Expectations to validate data quality.

Finally, I connected the analytical data model to Power BI to build dashboards for sales, revenue, customer behaviour and delivery performance. I also configured Slack notifications for Airflow workflow successes and failures.

One interesting insight from the analysis was that average delivery time dropped from 55 days to 8 days in the dataset.

This project gave me hands-on experience across the data engineering lifecycle, from data ingestion and storage to transformation, data modelling, validation, orchestration and reporting.

## 3. Architecture Overview

### End-to-End Data Flow

```text
Google Drive
    |
    | 8 CSV Files
    v
Apache Airflow
    |
    | Download and automate ingestion
    v
Amazon S3 - Raw
    |
    v
DuckDB
    |
    | Lightweight transformations
    | - Data type casting
    | - Rename columns
    | - Trim whitespace
    | - Standardise formats
    | - Remove duplicates
    v
Amazon S3 - Processed
    |
    | Parquet Files
    v
Apache Spark
    |
    | Larger-scale transformations
    | - Joins
    | - Filters
    v
Snowflake - Landing
    |
    v
dbt
    |
    +--> Staging
    |
    +--> Intermediate
    |
    +--> Marts
    |
    | Star Schema
    v
Snowflake - Analytics
    |
    +--> Dimension Tables
    |
    +--> Fact Tables
    |
    v
Power BI Dashboards

Additional:
- Great Expectations: Data Quality
- dbt Tests: Data Validation
- Airflow + Slack: Workflow Notifications
- Docker Compose: Local Environment
```

### Data Modelling

The final analytical model used a Star Schema with five dimension tables and three fact tables.

**Dimension Tables**

| Table | Purpose |
|---|---|
| dim_customers | Customer information |
| dim_products | Product information |
| dim_product_category_name_translation | Product category translations |
| dim_sellers | Seller information |
| dim_dates | Date dimension |

**Fact Tables**

| Table | Purpose |
|---|---|
| fact_order_items | Order item transactions |
| fact_order_payments | Payment transactions |
| fact_order_reviews | Customer reviews |

### Final Table Row Counts

| Table | Approximate Rows |
|---|---:|
| dim_customers | 99.4K |
| dim_products | 33K |
| dim_product_category_name_translation | 71 |
| dim_sellers | 3.1K |
| dim_dates | 1.5K |
| fact_order_items | 112.7K |
| fact_order_payments | 103.9K |
| fact_order_reviews | 99.2K |

## 4. Key Technical Concepts

### 4.1 Apache Airflow

**Purpose:** Orchestrate and automate the data pipeline.

In this project, Airflow downloaded CSV files from Google Drive and uploaded them to Amazon S3.

Key concepts:
- DAG: Defines the workflow.
- Task: An individual unit of work.
- Operator: Defines what a task executes.
- Dependencies: Define task execution order.
- Retry: Allows failed tasks to run again.
- Scheduling: Controls when a DAG runs.

### 4.2 Amazon S3

**Purpose:** Store raw and processed data.

The project used separate folders:

- `raw/`: Original CSV files.
- `processed/`: Cleaned Parquet files.

Benefits:
- Scalable object storage.
- Separation of raw and processed data.
- Supports different data formats.
- Integrates with Snowflake through Storage Integration and External Stages.

### 4.3 DuckDB

**Purpose:** Perform lightweight data transformations.

Transformations included:
- Data type casting.
- Column renaming.
- Trimming whitespace.
- Standardising text formats.
- Removing duplicates using `SELECT DISTINCT`.
- Writing transformed data as Parquet files.

DuckDB was used before Spark to handle lightweight transformations efficiently.

### 4.4 Apache Spark

**Purpose:** Perform larger-scale data processing.

In this project, Spark handled:
- Larger-scale transformations.
- Joins.
- Filters.
- Preparing data for the Snowflake landing schema.

### 4.5 Snowflake Storage Integration and External Stage

**Purpose:** Allow Snowflake to securely access data stored in Amazon S3.

The process:

1. Create a Storage Integration in Snowflake.
2. Run `DESC INTEGRATION` to retrieve the AWS IAM user ARN and External ID.
3. Configure the AWS IAM Role trust relationship.
4. Create an External Stage pointing to the S3 location.
5. Run `LIST` to inspect staged files.
6. Use `COPY INTO` to load data into Snowflake tables.

Important concepts:
- Storage Integration: Secure authentication between Snowflake and AWS.
- AWS IAM Role: Controls access to the S3 bucket.
- External ID: Helps establish the trusted relationship.
- External Stage: References files stored outside Snowflake.
- COPY INTO: Loads staged files into Snowflake tables.

### 4.6 dbt

**Purpose:** Transform raw warehouse data into analytics-ready datasets.

The project used the following structure:

| Layer | Purpose |
|---|---|
| Landing | Raw data loaded into Snowflake |
| Staging | Standardise names, types and basic business rules |
| Intermediate | Handle joins and enrichment |
| Marts | Build curated dimensions and fact tables |

Important dbt concepts:

- Models: SQL transformations.
- Sources: Define upstream source tables.
- Seeds: Load static CSV data.
- Snapshots: Track historical changes.
- Tests: Validate data quality.
- Macros: Reusable SQL logic using Jinja.
- Documentation: Generate model documentation and lineage.

### 4.7 Slowly Changing Dimension Type 2 (SCD Type 2)

**Purpose:** Preserve historical changes in dimension records.

The project used dbt snapshots for:
- Customers.
- Products.
- Sellers.

SCD Type 2 creates historical versions of records rather than simply overwriting existing values.

Important concepts:
- `dbt_valid_from`
- `dbt_valid_to`
- Historical records
- Current and previous versions

### 4.8 Data Quality

The project used two approaches:

**dbt Tests**
- Unique.
- Not null.
- Relationships.
- Other model-level validation.

**Great Expectations**
- Schema validation.
- Null value checks.
- Uniqueness checks.
- Referential integrity checks.

The purpose was to detect data quality issues before relying on the final analytical datasets.

### 4.9 Power BI

**Purpose:** Present the analytical data through dashboards.

The dashboards covered:
- Sales and revenue KPIs.
- Monthly delivery time trends.
- Revenue by product category.
- Customer behaviour.

The project also involved DAX calculations, distinct counts and slicers.

### 4.10 Airflow and Slack Notifications

Slack notifications were configured to report:
- Airflow DAG success.
- Airflow DAG failure.

This provided visibility into pipeline execution.

## 5. Technical Interview Questions

### Q1. Can you explain your E-commerce Data Pipeline architecture from source to reporting?

**Suggested Answer:**

The project started with eight CSV files stored in a shared Google Drive folder.

I used Apache Airflow to download the files and store them in Amazon S3.

Then, I used DuckDB for lightweight transformations, such as data type casting, column renaming, trimming whitespace, standardising text formats and removing duplicates.

The processed data was stored in Parquet format in S3.

After that, Apache Spark handled larger-scale transformations, including joins and filters, before writing the data into Snowflake.

I then used dbt to transform the data through staging and intermediate models into a Star Schema with five dimension tables and three fact tables.

I also implemented SCD Type 2 snapshots and data quality checks using dbt tests and Great Expectations.

Finally, Power BI consumed the analytical datasets to build dashboards for sales, revenue, customer behaviour and delivery performance.

**Key Points:**
- Google Drive → Airflow → S3.
- DuckDB → Lightweight transformations.
- Spark → Larger-scale transformations.
- Snowflake → Data warehouse.
- dbt → Data modelling.
- Power BI → Reporting.

**Story Reference:** Story 4 – E-commerce Data Pipeline.

### Q2. Why did you use both DuckDB and Apache Spark?

**Suggested Answer:**

I used DuckDB for lightweight transformations, such as data type casting, standardising text formats, trimming whitespace and removing duplicates.

I used Apache Spark for larger-scale transformations, particularly joins and filters.

The idea was to use the appropriate processing engine for each stage instead of using a distributed processing framework for every simple transformation.

**Key Points:**
- DuckDB: Lightweight analytical processing.
- Spark: Distributed data processing.
- Choose the engine based on workload and scale.

### Q3. How did you load data from Amazon S3 into Snowflake?

**Suggested Answer:**

I configured a Snowflake Storage Integration to establish secure access to Amazon S3.

First, I created the Storage Integration and used `DESC INTEGRATION` to retrieve the AWS IAM user ARN and External ID.

Then, I configured the AWS IAM Role trust relationship using those details.

After that, I created an External Stage pointing to the S3 bucket location.

I used `LIST` to inspect the staged files and `COPY INTO` to load the data into Snowflake tables.

**Key Points:**
- CREATE STORAGE INTEGRATION.
- DESC INTEGRATION.
- AWS IAM Role Trust Relationship.
- CREATE STAGE.
- LIST @stage.
- COPY INTO.

### Q4. What is the difference between a Storage Integration and an External Stage?

**Suggested Answer:**

A Storage Integration handles secure authentication between Snowflake and the external cloud storage platform.

An External Stage defines the location of the files that Snowflake needs to access.

In my project, the Storage Integration established the trusted connection with AWS, while the External Stage referenced the S3 bucket location containing the data files.

**Key Points:**
- Storage Integration = Secure access configuration.
- External Stage = External file location.
- Both work together to access S3 data.

### Q5. What is the difference between a fact table and a dimension table?

**Suggested Answer:**

A fact table stores measurable business events, such as order items, payments and reviews.

A dimension table stores descriptive information used to analyse those events, such as customers, products, sellers and dates.

In my project, I designed a Star Schema with five dimension tables and three fact tables.

**Key Points:**
- Fact = Business events and measurements.
- Dimension = Descriptive attributes.
- Star Schema = Facts connected to dimensions.

### Q6. What is SCD Type 2, and why did you use it?

**Suggested Answer:**

SCD Type 2 is a data warehousing technique used to preserve historical changes in dimension records.

Instead of overwriting an existing record when an attribute changes, we create a new version of that record and maintain the historical information.

In my project, I used dbt snapshots to track historical changes in customers, products and sellers.

This allows analytical queries to understand how dimension attributes changed over time.

**Key Points:**
- Preserve historical records.
- Create new versions when data changes.
- Use validity timestamps.
- Implemented with dbt snapshots.

### Q7. How did you handle data quality?

**Suggested Answer:**

I used both dbt tests and Great Expectations.

With dbt, I applied tests such as uniqueness, not-null and relationships to validate the models.

I also used Great Expectations for checks such as schema validation, null values, uniqueness and referential integrity.

The goal was to identify data quality problems before the data was used for reporting.

**Key Points:**
- Schema validation.
- Not-null checks.
- Uniqueness.
- Referential integrity.
- dbt tests and Great Expectations.

### Q8. What challenges did you face during the project?

**Suggested Answer:**

One of the challenges was transforming flat CSV files into a structured analytical model.

I needed to understand the business relationships between orders, order items, payments, customers and products.

Another challenge was defining the correct grain of the fact tables, especially distinguishing between an order-level record and an order-item-level record.

I also needed to decide which date fields should be used for delivery performance analysis, because the dataset contained different dates such as purchase, approval and delivery dates.

I addressed these challenges by defining the fact table grain first, separating complex transformations into intermediate dbt models and validating the relationships using data quality tests.

**Key Points:**
- Flat CSV to analytical model.
- Fact table grain.
- Date field selection.
- Complex joins.
- dbt intermediate models.
- Data quality validation.

### Q9. What is the difference between a full refresh and an incremental load?

**Suggested Answer:**

A full refresh processes the entire dataset again, while an incremental load processes only new or changed records.

Incremental processing can reduce unnecessary data processing and improve efficiency when working with large datasets.

In a data engineering pipeline, the choice depends on the source system, data volume and business requirements.

**Key Points:**
- Full refresh = Process all records.
- Incremental = Process new or changed records.
- Reduce unnecessary processing.
- Consider data volume and update patterns.

### Q10. What business insights did you get from the project?

**Suggested Answer:**

One interesting insight from the analysis was that average delivery time dropped from approximately 55 days to 8 days in the dataset.

I also analysed revenue by product category and customer purchasing behaviour.

For example, the average number of orders per customer was approximately 1.14.

These insights helped demonstrate how the analytical data model could support business reporting and decision-making.

**Key Points:**
- Delivery time trend: 55 days to 8 days in the dataset.
- Average orders per customer: approximately 1.14.
- Revenue by product category.
- Power BI dashboards.

## 6. Commands to Review

### 6.1 Snowflake Storage Integration

```sql
CREATE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = '<AWS_IAM_ROLE_ARN>'
STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>/<path>/');
```

Inspect the integration:

```sql
DESC INTEGRATION s3_int;
```

Important returned values:

```text
STORAGE_AWS_IAM_USER_ARN
STORAGE_AWS_EXTERNAL_ID
```

### 6.2 Create an External Stage

```sql
CREATE STAGE my_s3_stage
URL = 's3://<bucket>/<path>/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
```

List files:

```sql
LIST @my_s3_stage;
```

### 6.3 Load Data into Snowflake

```sql
COPY INTO my_table
FROM @my_s3_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
```

### 6.4 dbt Commands

Check the project configuration:

```bash
dbt debug
```

Run models:

```bash
dbt run
```

Run data quality tests:

```bash
dbt test
```

Run snapshots:

```bash
dbt snapshot
```

Load seed files:

```bash
dbt seed
```

Generate documentation:

```bash
dbt docs generate
```

### 6.5 SQL Concepts to Practise

```sql
-- SELECT
SELECT *
FROM customers;

-- JOIN
SELECT
    o.order_id,
    c.customer_id
FROM orders o
JOIN customers c
    ON o.customer_id = c.customer_id;

-- GROUP BY
SELECT
    customer_id,
    COUNT(*) AS total_orders
FROM orders
GROUP BY customer_id;

-- DISTINCT
SELECT DISTINCT customer_id
FROM orders;
```

Additional concepts:
- INNER JOIN, LEFT JOIN.
- GROUP BY and HAVING.
- CTEs.
- Subqueries.
- Window Functions.
- CASE WHEN.
- Deduplication.
- Incremental processing.

## 7. Important Interview Reminders

- Describe this as a hands-on end-to-end project completed during a Data Engineering course.
- Do not describe it as an enterprise production deployment.
- Explain the role of each technology rather than only listing tools.
- Be able to explain why DuckDB and Spark were used at different stages.
- Remember the Snowflake sequence: Storage Integration → AWS IAM Role → External Stage → LIST → COPY INTO.
- Understand the difference between fact tables and dimension tables.
- Be prepared to explain SCD Type 2 and dbt snapshots.
- Explain that the 55-day to 8-day delivery-time result is an observation from the dataset, not an improvement caused by the pipeline.
- Do not share AWS credentials, Snowflake passwords, API keys or Slack tokens.

## 8. Quick Revision Checklist

### Project Architecture
- [ ] Explain the full pipeline from Google Drive to Power BI.
- [ ] Explain the role of Airflow.
- [ ] Explain the difference between DuckDB and Spark.
- [ ] Explain raw and processed folders in S3.
- [ ] Explain the Snowflake landing layer.

### Snowflake and AWS
- [ ] Explain Storage Integration.
- [ ] Explain `DESC INTEGRATION`.
- [ ] Explain AWS IAM Role Trust Relationship.
- [ ] Explain External Stage.
- [ ] Remember `LIST` and `COPY INTO`.

### dbt and Data Modelling
- [ ] Explain staging, intermediate and marts.
- [ ] Explain Star Schema.
- [ ] Explain fact and dimension tables.
- [ ] Explain SCD Type 2.
- [ ] Explain dbt snapshots.
- [ ] Remember `dbt run`, `dbt test` and `dbt snapshot`.

### Data Quality and Reporting
- [ ] Explain dbt tests.
- [ ] Explain Great Expectations.
- [ ] Explain Power BI dashboards.
- [ ] Explain the delivery-time insight.
- [ ] Explain Airflow and Slack notifications.

---

**Main Interview Message:**

This project demonstrates my hands-on understanding of the end-to-end Data Engineering lifecycle, including ingestion, cloud storage, data processing, Snowflake integration, data modelling, data quality, orchestration and analytics.
