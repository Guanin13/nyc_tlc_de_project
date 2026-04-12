# 🚕 NYC Taxi Data Engineering Pipeline (In Progress)

## 📌 Overview

This project demonstrates an end-to-end **data engineering pipeline** that ingests NYC Taxi data from a public API and processes it through a modern **data lake architecture (Bronze → Silver → Gold)**.

The pipeline leverages industry-standard tools including **Apache Airflow for orchestration**, **dbt for transformation**, and **Apache Iceberg on AWS S3** for scalable and versioned data storage. A **CI/CD pipeline** ensures code quality and reliability.

---

## 🏗️ Architecture

```
API → Airflow → S3 (Bronze)
        ↓
     dbt + Iceberg
        ↓
S3 (Silver → Gold)
        ↓
     Athena
```

---

## ⚙️ Tech Stack

* **Orchestration:** Apache Airflow
* **Transformation:** dbt
* **Table Format:** Apache Iceberg
* **Storage:** AWS S3
* **Query Engine:** AWS Athena
* **Programming:** Python
* **CI/CD:** GitHub Actions (pytest, ruff)
* **Containerisation:** Docker & Docker Compose

---

## 📂 Project Structure (To Be Continued)

```
nyc_taxi_data_pipeline/
│
├── dags/                      # Airflow DAGs (pipeline orchestration)
│   └── extract_dags.py
│
├── src/                       # Core Python logic
│   └── extract.py             # API extraction & URL builder
│
├── tests/                     # Unit tests
│   └── test_extract_dag.py
│
├── .github/workflows/         # CI/CD pipeline
│   └── ci.yml
│
├── docker-compose.yaml        # Local Airflow setup
├── requirements.txt           # Dependencies
└── README.md
```

---

## 🔄 Data Pipeline Flow

### 1. Extract → Bronze Layer (Finished)

* Data is pulled from the NYC Taxi API
* Airflow schedules and manages extraction
* Raw data is stored in **S3 as partitioned Parquet files**

```
s3://bucket/nyc_taxi/bronze/year=YYYY/month=MM/
```

---

### 2. Transform → Silver Layer (In Progress)

* dbt cleans and standardises raw data
* Data is converted into **Apache Iceberg tables**
* Partitioned for efficient querying

Key transformations:

* Data type casting
* Filtering invalid records
* Handling missing values

---

### 3. Business Layer → Gold (In Progress)

* Aggregated, analytics-ready tables
* Supports business insights such as:

  * Trips per day/month
  * Revenue by vendor
  * Average trip distance

---

## ❄️ Why Apache Iceberg?

* ACID transactions on data lakes
* Time travel (snapshot versioning)
* Schema evolution
* Efficient partition pruning

Iceberg enables warehouse-like reliability directly on S3.

---

## 🔁 Orchestration (Airflow)

Airflow DAG manages:

* API extraction
* Upload to S3 (Bronze)
* Triggering dbt transformations

Features:

* Retry logic
* Modular tasks
* Logging and monitoring

---

## 🧪 CI/CD Pipeline

Implemented with **GitHub Actions**

### Continuous Integration:

* Run **pytest** for unit testing
* Run **ruff** for linting
* Validate code on every push and pull request

### Future Improvements:

* Automated deployment to AWS (EC2 / MWAA)
* Data quality checks (e.g., Great Expectations)

---

## 🧱 Data Lake Design

| Layer  | Purpose                        | Format  |
| ------ | ------------------------------ | ------- |
| Bronze | Raw ingested data              | Parquet |
| Silver | Cleaned & structured data      | Iceberg |
| Gold   | Business-ready aggregated data | Iceberg |

---

## ▶️ How to Run Locally

### 1. Start Airflow

```bash
docker-compose up --build
```

### 2. Access Airflow UI

```
http://localhost:8080
```

### 3. Trigger Pipeline

* Enable DAG from Airflow UI
* Run extraction pipeline

### 4. Run dbt Models

```bash
dbt run
```

---

## 📊 Example Use Cases

* Analyze taxi demand trends
* Revenue and vendor performance
* Time-based trip patterns
* Demonstration of modern data lake architecture

---

## 🚀 Future Enhancements

* Data quality monitoring
* Dashboard integration (Power BI / Tableau)
* Automated deployment to AWS

---

## 👤 Author

Quang Son Nguyen
Master of Data Science – Monash University

---

## ⭐ Key Takeaway

This project showcases how to build a **modern, scalable data pipeline** using Airflow, dbt, Iceberg, and AWS — aligned with real-world data engineering practices.
