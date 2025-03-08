# Super-Store-Analysis

## Introduction

### This project has a few goals:

- Perform Extract, Transform and Load (ETL) on raw datasets using **Python** and **Google Bigquery**.
- Create an ingestion script that runs on **Apache Airflow** to warehouse data in intervals.

---

## Input data

### Raw data
The input data is in Excel file format (.xls, .xlsx etc). The file contains 3 sheets, hence 3 tables: ```Orders```, ```People``` and ```Returns```.

- ```Orders``` : contains sales data entries. It will serve as the basis of the fact table being used.
- ```People```: contains region names and the name of regional managers.
- ```Returns```: contains the return status for each unique product for each unique order.

### **ERD diagram**

This is how the data will be transformed and warehoused.

<img src="https://github.com/user-attachments/assets/99a8505b-cc9a-4d7d-96db-6410d922f0ee" alt="Image" width="800" height="450">

---

### **Data pipleline DAG diagram**


<img src="https://github.com/user-attachments/assets/2693e71b-aefc-47f1-aadb-892cea5721d0" alt="Image" width="800" height="450">
---

## Files

### Expermimenting with the ETL process

- **Jupyter Notebook:** [Superstore-Analysis.ipynb](https://github.com/nacht29/Super-Store-Analysis/blob/main/Superstore-Analysis.ipynb)

### Ingestion script (data pipeline)

- **Jupyter Notebook:** [ingestion.ipynb](https://github.com/nacht29/Super-Store-Analysis/blob/main/ingestion.ipynb)
- **Python script (Apache Airflow DAG):** [superstore_pipeline.py](https://github.com/nacht29/Superstore-Analysis/blob/main/superstore_pipeline.py)
