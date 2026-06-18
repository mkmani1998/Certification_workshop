# Databricks Certified Data Engineer Associate Complete Study Guide

**May 2026 Exam Blueprint**

---

## Section 1: Databricks Data Intelligence Platform (6%)

### 1.1 Architecture & Core Components

The Databricks Data Intelligence Platform is built on a three-tier architecture that ensures scalability, security, and performance:

```
+-----------------------------------------------------------------+
|                       CONTROL PLANE                             |
|    (Databricks Web UI, Job Orchestration, Cluster Manager,      |
|             Notebook Management, SaaS Security)                 |
+-----------------------------------------------------------------+
                                |
        Secure Cluster Connectivity (No inbound open ports)
                                |
+-----------------------------------------------------------------+
|                       DATA PLANE                                |
|    (Compute Clusters, Warehouses, Workers processing data)      |
|    * Classic: Hosted in customer VPC                            |
|    * Serverless: Managed securely in Databricks cloud account   |
+-----------------------------------------------------------------+
                                |
+-----------------------------------------------------------------+
|                   STORAGE & GOVERNANCE                          |
|    * Cloud Object Storage (S3 / ADLS / GCS)                     |
|    * Unity Catalog (Central Metadata, Lineage, Auditing)        |
+-----------------------------------------------------------------+
```

---

## Section 2: Data Ingestion and Integration

### 2.1 The Storage Layer: Delta Lake Mechanics

Delta Lake is an open-source storage layer built on top of standard cloud object storage. It is structured around the following core design elements:

#### Delta Lake Features:
- **ACID Transactions**: Ensures data consistency
- **Scalable Metadata**: Handles billions of files
- **Time Travel**: Query historical versions
- **Schema Enforcement**: Maintains data quality

### 2.2 Ingestion Methods

#### 1. COPY INTO (SQL)

For low-to-moderate volume file ingestion from cloud object storage:

```sql
COPY INTO prod.sales.bronze_orders
FROM 's3://my-bucket/landing/orders/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('force' = 'false'); -- force=false ensures idempotency
```

#### 2. Auto Loader (cloudFiles)

A highly optimized engine for streaming or incrementally ingesting files from cloud object storage.

**Directory Listing vs. File Notification:**
- **Directory Listing (Default)**: Periodic scanning of cloud storage
- **File Notification**: Event-driven using cloud storage notifications (more efficient for high-volume scenarios)

```python
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/checkpoints/orders_schema")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("s3://my-bucket/raw/orders/"))

# The data is written to a target table governed by Unity Catalog
(df.writeStream
   .format("delta")
   .option("checkpointLocation", "dbfs:/checkpoints/orders_data")
   .trigger(availableNow=True)  # Runs incrementally as a cost-effective batch
   .toTable("prod.sales.orders_raw"))
```

#### 3. Lakeflow Connect

Lakeflow Connect provides native, agentless connectors to replicate transactional databases and enterprise SaaS applications directly into Unity Catalog.

**Mechanism**: It connects to source systems, extracts data incrementally, and writes to Unity Catalog tables.

```python
jdbc_url = f"jdbc:postgresql://db-host:5432/crm"
connection_properties = {
    "user": dbutils.secrets.get("vault", "db_username"),
    "password": dbutils.secrets.get("vault", "db_password"),
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=jdbc_url, table="customers", properties=connection_properties)
df.write.mode("append").saveAsTable("prod.sales.bronze_customers")
```

### 2.3 Ingestion Selection Framework

```
                    What is your data source?
                                  |
         +------------------------+------------------------+
         |                                                 |
Cloud Object Storage Files                         Database / SaaS (SQL/NoSQL)
         |                                                 |
   File Volume?                                            |
         |                                                 |
         +-------------+                                   |
         |             |                                   |
    Low/Moderate      Very High (Millions)           Continuous Sync?
         |             |                                   |
     COPY INTO     Auto Loader (File Notification)         +---------+---------+
                                                           |                   |
                                                          Yes                  No
                                                           |                   |
                                                    Lakeflow Connect       JDBC Batch
```

---

## Section 3: Data Transformation and Modeling (22%)

### 3.1 Medallion Architecture Transformation Patterns

The Medallion design pattern organizes data into layers of increasing structure, cleanliness, and business value:

```
        │
        ▼
+─────────────────────────────────+
|         BRONZE LAYER            |  <-- Append-Only Raw Landing
|  (Raw Ingestion, No Cleaning)   |      Preserves full historical record
+─────────────────────────────────+
        │
        ▼
+─────────────────────────────────+
|         SILVER LAYER            |  <-- Standardized & Cleaned
|  (Cleaned, Deduplicated,        |      Validated schemas, NULL enforcement
|   Enforced Constraints)         |
+─────────────────────────────────+
        │
        ▼
+─────────────────────────────────+
|          GOLD LAYER             |  <-- Project-Specific Analytics
|  (Aggregated Business Views,    |      Business logic, materialized metrics
|   Materialized Views, Metrics)  |
+─────────────────────────────────+
```

#### Bronze to Silver Transformations:

- Filter out NULL keys on critical primary columns
- Standardize data formats (e.g., timestamp layouts, currency units)
- Deduplicate records based on unique business transaction identifiers
- Enforce NOT NULL constraints on business keys

### 3.2 Join Operations

```python
df_orders = spark.table("prod.sales.orders")
df_customers = spark.table("prod.sales.customers")
```

#### Inner Join
Retains only rows where join keys match on both sides:

```python
df_inner = df_orders.join(df_customers, "customer_id", "inner")
```

#### Left Join
Retains all records from the left DataFrame, filling in values from the right side with NULL if there are no matching keys:

```python
df_left = df_orders.join(df_customers, "customer_id", "left")
```

#### Broadcast Join (Map-Side Join)
Replicates a small DataFrame to all worker nodes. This allows workers to perform the join locally, avoiding a network shuffle of the larger dataset:

```python
from pyspark.sql.functions import broadcast

df_broadcast = df_orders.join(broadcast(df_customers), "customer_id", "inner")
```

#### Cross Join
Computes the Cartesian product of both relations. Highly inefficient and should be avoided unless specifically needed:

```python
df_cross = df_orders.crossJoin(df_customers)
```

#### Union All
Appends the row contents of two structurally identical datasets vertically. Runs quickly because it does not deduplicate records:

```python
df_union_all = df_orders_jan.unionByName(df_orders_feb)
```

#### Union (Distinct)
Appends rows vertically and removes duplicate records, which requires a network shuffle:

```python
df_union_distinct = df_orders_jan.union(df_orders_feb)
```

### 3.3 Column and Schema Manipulation Operations

#### Manipulating Schema Structure:

```python
from pyspark.sql.functions import col, split, explode

# Adding, renaming, casting, and dropping columns
df_transformed = (df_orders
                  .withColumn("tax_amount", col("subtotal") * 0.08)
                  .withColumnRenamed("registration_date", "signup_date")
                  .withColumn("order_id", col("order_id").cast("string"))
                  .drop("temporary_session_id"))
```

#### String Splitting:

Splits a string (e.g., "Jane Doe") into an array ["Jane", "Doe"]:

```python
df_split = df_transformed.withColumn("name_array", split(col("full_name"), " "))
```

#### Array Exploding:

Transforms each element of an array into its own separate row:

```python
df_exploded = df_transformed.withColumn("single_item", explode(col("item_cart_array")))
```

#### Filtering and Subsets:

```python
df_filtered = df_transformed.filter((col("country") == "US") & (col("subtotal") > 50))
```

### 3.4 Aggregation & Deduplication Operations

#### Deduplication

Removes duplicate records based on a subset of unique business keys:

```python
df_unique = df_orders.dropDuplicates(["order_id", "transaction_timestamp"])
```

#### Aggregations

```python
from pyspark.sql.functions import count, approx_count_distinct, mean

df_summary = (df_orders.groupBy("store_region")
              .agg(count("order_id").alias("total_orders"),
                   approx_count_distinct("customer_id", rsd=0.05).alias("estimated_unique_customers"),
                   mean("subtotal").alias("average_order_value")))
```

### 3.5 Spark Performance & Resource Optimization

#### Key Configuration Parameters:

- **spark.sql.shuffle.partitions** (Default: 200): Controls the number of target partitions created when data is shuffled during wide transformations (joins, aggregations, unions)

#### Data Constraints in SQL:

```sql
ALTER TABLE prod.sales.silver_customers ADD CONSTRAINT valid_age_check CHECK (age_years > 0);

-- Enforces that key columns cannot contain null values
ALTER TABLE prod.sales.silver_customers CHANGE COLUMN customer_id SET NOT NULL;
```

---

## Section 4: Working with Lakeflow Jobs (16%)

### 4.1 Orchestration & Workflow DAGs

Lakeflow Jobs allow data engineers to build, coordinate, and monitor data pipelines using Directed Acyclic Graphs (DAGs).

```
                        [ Task 1: Ingest Bronze ]
                                   │
                     ┌─────────────┴─────────────┐
                     ▼                           ▼
         [ Task 2: Build Silver ]     [ Task 3: Load CRM DB ]
                     └─────────────┬─────────────┘
                                   ▼
                       [ Task 4: Refresh Gold ]
```

#### Task Types

Tasks inside a DAG can run various resources, including:
- Interactive Notebooks
- Delta Live Tables (DLT) declarative pipelines
- SQL Queries or complete Dashboards
- Custom Python scripts or wheels

### 4.2 Databricks Asset Bundles (DAB)

Configuration-as-code for deploying Databricks workflows and resources:

```yaml
name: billing-pipeline

targets:
  dev:
    variables:
      catalog_name: dev_catalog
      compute_size: S
    workspace:
      host: https://dev-workspace.cloud.databricks.com

  prod:
    variables:
      catalog_name: prod_catalog
      compute_size: L
    workspace:
      host: https://prod-workspace.cloud.databricks.com
    resources:
      jobs:
        billing_job:
          compute:
            - label: default
              node_type_id: i3.xlarge
              num_workers: 8
```

### 4.3 Databricks CLI Lifecycle Management

Use the Databricks CLI within CI/CD automation pipelines (such as GitHub Actions or Jenkins) to validate and deploy configuration bundles:

**Validate the configuration:**
```bash
databricks bundle validate -t prod
```

**Deploy assets to the specified target workspace:**
```bash
databricks bundle deploy -t prod
```

**Trigger a workflow run remotely:**
```bash
databricks bundle run billing_job -t prod
```

---

## Section 5: Working with Delta Live Tables (DLT) (12%)

Delta Live Tables provide a declarative framework for building reliable data pipelines with automatic data quality monitoring and lineage tracking.

---

## Section 6: Troubleshooting, Monitoring, and Optimization (10%)

### 6.1 Performance Tracking using Run History

**Baselines**: Compare current task execution times against historical averages to detect performance degradation.

#### Spark UI Diagnostics

```
┌──────────────────────────────────────────────────────┐
│                 SPARK UI DIAGNOSTICS                 │
├──────────────────────────────────────────────────────┤
│                                                      │
│  Data Skew:                                          │
│  Task Duration: [30s][29s][31s].................[45m]│
│  Shuffle Read:  [12MB][11MB]....................[4.8GB]
│                                                      │
│  Disk Spilling:                                      │
│  Spill (Memory): 12.4 GB    Spill (Disk): 4.1 GB    │
│                                                      │
└──────────────────────────────────────────────────────┘
```

#### Detecting Data Skew

**Symptom**: Most tasks complete in seconds, but a single task takes several minutes. The Spark UI task summary shows the maximum shuffle read size is significantly larger than the median.

**Resolution**: Repartition on a more evenly distributed column or apply salting techniques.

---

## Section 7: Data Governance with Unity Catalog

### 7.1 Managed vs. External Tables

#### Managed Table
```sql
CREATE TABLE prod.sales.managed_orders (
  order_id INT,
  amount DOUBLE
);
```

#### External Table
```sql
CREATE TABLE prod.sales.external_orders (
  order_id INT,
  amount DOUBLE
)
LOCATION 's3://my-bucket/external/sales/orders/';
```

### 7.2 Access Control Policies (SQL DCL Syntax)

Secure resources in Unity Catalog by applying permission privileges (GRANT, REVOKE, DENY) to users, groups, or service principals:

```sql
-- Granting read access on a table
GRANT SELECT ON TABLE prod.sales.silver_customers TO `analyst_group`;

-- Granting catalog metadata viewing rights
GRANT USE CATALOG prod TO `analyst_group`;
GRANT USE SCHEMA prod.sales TO `analyst_group`;

-- Revoking insert permissions
REVOKE MODIFY ON TABLE prod.sales.silver_customers FROM `temporary_contractors`;

-- Explicitly denying all privileges on a sensitive schema
DENY ALL PRIVILEGES ON SCHEMA prod.hr_records TO `external_vendors`;
```

### 7.3 Fine-Grained Security: Row and Column Controls

#### Column-Level Masking

Obfuscates or masks sensitive values within a column based on the identity or group of the user executing the query:

```sql
CREATE OR REPLACE FUNCTION prod.sales.ssn_mask(ssn STRING)
  RETURN CASE WHEN is_member('hr_admins') THEN ssn 
              ELSE 'XXX-XX-' || RIGHT(ssn, 4) 
         END;

-- Apply the masking function to a column
ALTER TABLE prod.sales.employees ALTER COLUMN social_security_number SET MASK prod.sales.ssn_mask;
```

#### Row-Level Security

Filters out data rows dynamically based on user group membership at query execution time:

```sql
-- Create a row-level filter function
CREATE OR REPLACE FUNCTION prod.sales.region_filter(region STRING)
  RETURN is_member('global_managers') OR is_member(region || '_analysts');

-- Apply the row-level filter to a table
ALTER TABLE prod.sales.regional_orders SET ROW FILTER prod.sales.region_filter ON (store_region);
```

### 7.4 Attribute-Based Access Control (ABAC)

Definition: ABAC allows administrators to secure data based on descriptive tags applied to schemas, tables, or columns, rather than managing thousands of separate access policies.

---

**End of Study Guide**
