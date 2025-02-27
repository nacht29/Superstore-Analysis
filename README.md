# Super-Store-Analysis

#### **What I did**:
- extract data from the .xls file, clean for null values and duplicates
- check the data type for each column
- reformat the header to snake case (abc_def)
- draft an ERD to break down the large ```Orders``` table.
- break the data into smaller dataframes
- create a star shcema in MySQL to store the data
- loaded the data into MySQL and tested the database with some queries

#### **ERD diagram**

<img src="https://github.com/user-attachments/assets/bc9bce47-408f-4c4f-9676-d2fdf79db532" alt="Image" width="800" height="300">

#### **Some questions**
1. is testing in MySQL redundant as I could've just moved the data into Bigquery?
2. is there anything I could have done better when transforming the data?
3. is there a more efficient way to facilitate the ETL process?