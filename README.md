\# Airflow Employee Data Pipeline



\## 📌 Project Overview

This project implements an \*\*end-to-end data engineering pipeline\*\* using \*\*Apache Airflow (2.8.0)\*\* orchestrated with \*\*Docker Compose\*\*.  

The pipeline ingests employee data from a CSV file, loads it into PostgreSQL, transforms the data, exports it to Parquet format, and demonstrates advanced Airflow concepts such as branching, notifications, and unit testing.



---



\## 🏗️ Architecture

\- \*\*Apache Airflow\*\* – Workflow orchestration

\- \*\*PostgreSQL\*\* – Metadata database + data storage

\- \*\*Docker \& Docker Compose\*\* – Containerized environment

\- \*\*Pandas \& PyArrow\*\* – Data processing and Parquet export

\- \*\*Pytest\*\* – Unit testing of DAG structure



---



\## 🧰 Technology Stack

\- Apache Airflow 2.8.0

\- PostgreSQL 14

\- Python 3.8

\- Pandas

\- PyArrow

\- Docker \& Docker Compose

\- Pytest



---



\## 📁 Project Structure

airflow-employee-pipeline/

├── docker-compose.yml

├── README.md

├── requirements.txt

├── dags/

│ ├── dag1\_csv\_to\_postgres.py

│ ├── dag2\_data\_transformation.py

│ ├── dag3\_postgres\_to\_parquet.py

│ ├── dag4\_conditional\_workflow.py

│ └── dag5\_notification\_workflow.py

├── tests/

│ ├── test\_dag1.py

│ ├── test\_dag2.py

│ └── test\_utils.py

├── data/

│ └── input.csv

├── output/

│ └── (generated parquet files)

├── logs/

└── plugins/



---



\## ⚙️ Prerequisites

\- Docker Desktop

\- Docker Compose

\- Git Bash (Windows) or terminal (Linux/Mac)



---



\## 🚀 Setup Instructions



\### 1️⃣ Clone Repository

```bash

git clone <repository-url>

cd airflow-employee-pipeline

\*\*Start Airflow:

docker compose up -d

Access Airflow UI:

URL: http://localhost:8080



Username: admin

Password: admin



📊DAG Descriptions

🔹 DAG 1: CSV to PostgreSQL Ingestion

DAG ID: csv\_to\_postgres\_ingestion



Schedule: Daily



Creates table → truncates → loads CSV



Fully idempotent



🔹 DAG 2: Data Transformation

DAG ID: data\_transformation\_pipeline



Schedule: Daily



Creates derived columns:



full\_info



age\_group



salary\_category



year\_joined



🔹 DAG 3: PostgreSQL to Parquet Export

DAG ID: postgres\_to\_parquet\_export



Schedule: Weekly



Exports transformed data to Parquet using PyArrow



🔹 DAG 4: Conditional Workflow

DAG ID: conditional\_workflow\_pipeline



Uses BranchPythonOperator



Executes different paths based on day of week



🔹 DAG 5: Notification Workflow

DAG ID: notification\_workflow



Demonstrates:



Success callback



Failure callback



Cleanup task (always runs)

🧪 Unit Testing

✔ What is tested

DAGs load without errors



Correct task count



Proper task dependencies



Schedule intervals



Unique DAG IDs



▶ Run Tests

MSYS\_NO\_PATHCONV=1 docker exec -it airflow python -m pytest /opt/airflow/tests -v

All tests pass successfully using pytest without running Airflow.



📥 Sample Input Data (CSV)

id,name,age,city,salary,join\_date

1,John Doe,28,New York,55000.50,2022-03-15

2,Jane Smith,35,San Francisco,75000.00,2021-07-22

3,Mike Johnson,42,Chicago,68000.75,2020-11-10

...



👤 Author:

Name:G.Chaitanya Sruthi       Roll num: 23MH1A4420

