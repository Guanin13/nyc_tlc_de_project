# NYC Taxi Data Engineering Pipeline

![CI](https://img.shields.io/badge/CI-GitHub%20Actions-blue)
![Python](https://img.shields.io/badge/Python-3.11-yellow)
![dbt](https://img.shields.io/badge/dbt-1.0-orange)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Athena%20%7C%20Glue-black)
![Iceberg](https://img.shields.io/badge/Table%20Format-Apache%20Iceberg-blue)

---

## Overview

This project builds a **production-style data engineering pipeline** that ingests NYC Taxi trip data and transforms it into analytics-ready datasets using a **modern data lake architecture (Bronze → Silver → Gold)**.

It demonstrates how real-world data platforms are designed using:

- **Airflow** for orchestration  
- **dbt** for transformation and testing  
- **Apache Iceberg** for ACID data lakes  
- **AWS S3 + Athena** for scalable storage and querying  

---

## Project Goals

- Build an **end-to-end pipeline** from ingestion to analytics  
- Apply **data engineering best practices** (partitioning, incremental loads, testing)  
- Ensure **data quality and reliability** through CI/CD  
- Simulate a **real production data stack**  

---

## Architecture
API → Airflow → S3 (Bronze)
↓
dbt + Iceberg
↓
S3 (Silver → Gold)
↓
Athena


### Key Design Decisions

- **S3 as data lake** → scalable & cost-efficient  
- **Iceberg format** → ACID + time travel + schema evolution  
- **dbt incremental models** → efficient processing  
- **Partitioning (year/month)** → faster queries  

---

## Tech Stack

| Layer            | Tools Used |
|------------------|-----------|
| Orchestration    | Apache Airflow |
| Transformation   | dbt |
| Storage          | AWS S3 |
| Table Format     | Apache Iceberg |
| Query Engine     | AWS Athena |
| Language         | Python |
| CI/CD            | GitHub Actions (pytest, ruff, dbt test) |
| Containerisation | Docker |

---

## Project Structure
nyc_tlc_de_project/
│
├── .github/
│ └── workflows/ # CI/CD pipelines
│ ├── ci.yml
│ └── dbt-ci.yml
│
├── dags/ # Airflow DAGs (orchestration)
│ └── extract_dag.py
│
├── src/ # Core pipeline logic
│ ├── init.py
│ └── extract.py
│
├── dbt_project/ # dbt transformation layer
│ ├── models/
│ │ └── silver/
│ │ ├── silver_yellow_trip.sql
│ │ ├── schema.yml
│ │ └── sources.yml
│ └── logs/ # dbt logs (ignored)
│
├── tests/ # dbt data tests (SQL-based)
│ ├── test_dropoff_after_pickup.sql
│ ├── test_non_negative_fields.sql
│ └── test_total_amount_reconciles.sql
│
├── pytests/ # Python unit tests
│ ├── init.py
│ └── test_extract_dag.py
│
├── docker-compose.yaml # Local Airflow setup
├── requirements.txt # Python dependencies
├── dbt_project.yml # dbt configuration
├── .gitignore
└── README.md
---

## Data Pipeline Flow

### Bronze Layer — Raw Ingestion

- Data fetched from NYC Taxi source (Parquet)
- Stored in S3 as partitioned files
s3://bucket/nyc_taxi/raw/year=YYYY/month=MM/


✔ Append-only  
✔ Immutable raw data  
✔ Partitioned for scalability  

---

### 🥈 Silver Layer — Cleaned Data (dbt + Iceberg)

- Standardised schema  
- Incremental processing  
- Deduplicated using ingestion timestamp  

#### Transformations:

- Type casting (timestamps, numeric fields)  
- Filtering invalid records  
- Handling null values  
- Removing duplicates (latest batch logic)  

✔ Stored as **Iceberg tables**  
✔ Optimised for analytics  

---

### Gold Layer — Business Metrics *(In Progress)*

Planned outputs:

- Trips per day/month  
- Revenue by vendor  
- Average trip distance  
- Demand trends  

---

## Data Quality & Testing

### dbt Tests

- Dropoff > Pickup  
- Non-negative values  
- Total amount consistency  

### Python Tests

- Mocked API calls  
- S3 upload validation  
- URL generation logic  

---

## CI/CD Pipeline

Automated with **GitHub Actions**

### Runs on every push:

- `pytest` → unit tests  
- `dbt test` → data validation  
- `ruff` → linting  

### Why this matters:

Ensures:
- No broken pipelines  
- Reliable transformations  
- Production-ready code  

---

## Why Apache Iceberg?

Traditional data lakes lack reliability. Iceberg solves this:

- ACID transactions on S3  
- Time travel (snapshot rollback)  
- Schema evolution  
- Partition pruning  

Enables warehouse-like behavior on a data lake  

---

## Example Analytics Use Cases

- Peak taxi demand hours  
- Vendor performance comparison  
- Seasonal travel patterns  
- Revenue analysis  

---

## How to Run Locally

### 1. Start Airflow

```bash
docker-compose up --build
```

### 2. Open AirFlow UI

 ```
http://localhost:8080
```

### 3. Run Pipeline
* Enable DAG
* Trigger extraction

### 4. Run dbt

```bash
dbt run
dbt test
```
---

## Future Improvements
* Gold layer completion
* Dashboard integration (Power BI / Tableau)
* Deployment to AWS (EC2 / MWAA)
* Data observability tools

---

## Author
**Quang Son Nguyen**
Master of Data Science – Monash University
