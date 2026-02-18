# 🚀 Azure Databricks – End-to-End Data Engineering Project (NYC Taxi Data)

<p>
<img src="https://img.shields.io/badge/Microsoft%20Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white" height="35"/>
<img src="https://img.shields.io/badge/Azure%20Data%20Factory-FF9900?style=for-the-badge" height="35"/>
<img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" height="35"/>
<img src="https://img.shields.io/badge/Delta%20Lake-00B3A4?style=for-the-badge" height="35"/>
<img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" height="35"/>
<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" height="35"/>
</p>

---

## 📌 Project Overview

This project demonstrates a **complete Azure Data Engineering pipeline** using:

- Azure Data Factory  
- Azure Databricks  
- PySpark  
- Delta Lake  
- Azure Data Lake Storage Gen2  

The pipeline processes **NYC Taxi data** using the **Bronze–Silver–Gold (Medallion) architecture** and implements real-world best practices such as dynamic ingestion, secure authentication, and incremental data processing.

---

## 🏗 Architecture – Medallion Design

### 🥉 Bronze Layer (Raw Zone)
- Data dynamically pulled from an API using **Azure Data Factory**
- Parameterized pipelines ingest monthly data automatically
- Stored in **Parquet format**
- Saved in Azure Data Lake Gen2
- No transformations applied

---

### 🥈 Silver Layer (Processed Zone)
- Data read from Bronze using **Azure Databricks**
- Transformations applied using **PySpark**
- Data cleaning and enrichment performed
- Written back to Data Lake in refined format

---

### 🥇 Gold Layer (Curated Zone)
- Curated datasets stored in **Delta format**
- Enables:
  - ACID transactions
  - Table versioning
  - Time travel
- Optimized for analytics and querying

---

## 🔄 Project Phases

### Phase 1 – Data Ingestion (Azure Data Factory)

- Created **dynamic pipelines** using parameters and loops
- Ingested NYC Taxi data directly from API
- Eliminated manual uploads
- Stored raw data in Bronze layer (Parquet format)
- Implemented secure authentication using:
  - Azure AD Service Principal
  - Linked Services

---

### Phase 2 – Data Transformation (Azure Databricks + PySpark)

- Accessed Azure Data Lake securely from Databricks
- Applied PySpark transformations:
  - Column selection
  - Data cleaning
  - Enrichment logic
- Pushed refined data to Silver layer

---

### Phase 3 – Delta Lake Implementation

- Converted processed data to **Delta tables**
- Explored:
  - Delta Log
  - Data versioning
  - Time travel capabilities
- Stored curated data in Gold layer

---

## ⚙ Key Features

✅ Medallion architecture (Bronze / Silver / Gold)  
✅ Dynamic ADF pipeline using parameters  
✅ Secure Service Principal authentication  
✅ Parquet storage in ADLS Gen2  
✅ PySpark transformations in Databricks  
✅ Delta Lake ACID compliance  
✅ Incremental and automated data processing  

---

## 🧰 Tech Stack

- Azure Data Factory
- Azure Databricks
- PySpark
- Delta Lake
- Azure Data Lake Gen2
- Parquet
- SQL
- Python

---

## 📂 Repository Structure

```text
databricks_notebooks/
 ├── 1_Autoloader.ipynb
 ├── 2_silver.ipynb
 ├── 3_lookup.ipynb
 ├── 4_silver.ipynb
 ├── 5_LookUpNotebook.ipynb
 ├── 6_GetDayNumber.ipynb

factory/
dataset/
pipeline/
linkedService/
README.md
```
