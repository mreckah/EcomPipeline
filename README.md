# Batch Data Engineering Pipeline – Attijariwafa Bank Internship Project

## Project Overview

This project presents a **batch data engineering pipeline** developed during my internship at **Attijariwafa Bank**. It automates the collection, processing, and visualization of branch data to support **predictive analytics** and **anomaly detection** for sales and operations.

Key objectives:

* Automate data ingestion and batch processing
* Predict branch income, sales, and profit
* Detect anomalies in operational data
* Provide dashboards for decision support

---

## Project Architecture

The pipeline is modular and scalable, covering data ingestion, storage, processing, and visualization.

<img width="1062" height="452" alt="image" src="https://github.com/user-attachments/assets/50b9c81c-9365-491b-8fd2-8f69392d85e5" />

* Data is collected from multiple sources and stored in HDFS
* Apache Spark processes and aggregates the data
* Processed data is stored in PostgreSQL
* Grafana dashboards display KPIs and predictions
* Apache Airflow orchestrates all batch jobs

---

## Tools & Technologies

* **Apache NiFi** – Data ingestion
* **HDFS** – Distributed storage
* **Apache Spark** – Data processing & ML analytics
* **PostgreSQL** – Structured storage
* **Grafana** – Dashboards & visualization
* **Apache Airflow** – Workflow orchestration
* **Docker & Docker Compose** – Containerized deployment

---

## Pipeline Workflow

1. **Ingestion** – Collect branch CSVs and external data
2. **Storage** – Store raw, cleaned, and aggregated data in HDFS
3. **Processing** – Clean, transform, aggregate, and run ML models with Spark
4. **Database Storage** – Load curated datasets into PostgreSQL
5. **Visualization** – Display dashboards and anomaly alerts in Grafana
6. **Orchestration & Monitoring** – Airflow schedules tasks and ELK Stack monitors logs

![InternPipeline (5)](https://github.com/user-attachments/assets/e6d583ca-29bb-45f7-a415-df3ee3265b78)

---

## Results

* Automated ingestion and processing of large datasets
* Predicted branch income, sales, and profit
* Detected anomalies in operational data
* Dashboards provide actionable insights for management

**Key Figures to Include** from report:

<img width="1016" height="236" alt="image" src="https://github.com/user-attachments/assets/e8ac19f8-0f32-46fc-8483-40e302d5788f" />

<img width="945" height="550" alt="image" src="https://github.com/user-attachments/assets/822d34f1-1717-4585-8571-68394f16ed52" />

<img width="945" height="550" alt="image" src="https://github.com/user-attachments/assets/4f7f787c-a882-43e0-aaa5-0f7e7971ab41" />

---

## Future Work

* Add **real-time streaming** analytics using Kafka or Flink
* Include more **predictive models** for branch insights
* Extend analytics to multiple departments
* Enhance **security and data governance**

---

## References

1. Apache NiFi – [https://nifi.apache.org](https://nifi.apache.org)
2. Apache Spark – [https://spark.apache.org](https://spark.apache.org)
3. Apache Airflow – [https://airflow.apache.org](https://airflow.apache.org)
4. HDFS – [https://hadoop.apache.org](https://hadoop.apache.org)
5. Grafana – [https://grafana.com](https://grafana.com)

---
