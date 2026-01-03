
# FlexiMart ETL Pipeline 

## Overview

FlexiMart is faced with the challenge of consolidating and cleaning customer, product, and sales data stored in three separate CSV files. These files contain various data quality issues, such as missing values, inconsistent formats, and duplicate records. The objective of this is to design and implement a robust ETL (Extract, Transform, Load) pipeline that addresses these issues and loads the cleaned data into a relational database (MySQL).

The ETL pipeline performs the following:

- **Extraction:** Reads raw data from the provided CSV files (`customers_raw.csv`, `products_raw.csv`, `sales_raw.csv`).
- **Transformation:** Cleans the data by removing duplicates, handling missing values with appropriate strategies, standardizing phone numbers and category names, and converting dates to a consistent format. Surrogate keys are generated to ensure unique identification of records.
- **Loading:** Inserts the cleaned and validated data into a normalized database structure, ensuring referential integrity and compliance with best practices for relational databases.
- **Data Quality Reporting:** Generates a comprehensive report (`data_quality_report.txt`) that summarizes the number of records processed, duplicates removed, missing values handled, and records successfully loaded for each file.

This process ensures that FlexiMart’s data is accurate, consistent, and ready for advanced analytics and business intelligence tasks. The ETL pipeline is implemented in Python, with clear comments, logging, and error handling to facilitate maintainability and transparency.

For further details on the database schema and normalization, refer to the included documentation.

---

## Tasks

- **Extract:** Read all three CSV files containing customer, product, and sales data.
- **Transform:** 
    - Remove duplicate records.
    - Handle missing values (drop, fill, or use defaults as appropriate).
    - Standardize phone formats (e.g., `+91-9876543210`).
    - Standardize category names (e.g., "electronics", "Electronics", "ELECTRONICS" → "Electronics").
    - Convert date formats to `YYYY-MM-DD`.
    - Add surrogate keys (auto-incrementing IDs).
- **Load:** Insert the cleaned data into a MySQL/PostgreSQL database using Python.
- **Report:** Generate a data quality report summarizing the ETL process.

---

## Deliverables

- `etl_pipeline.py` – Complete ETL script with comments and logging.
- `data_quality_report.txt` – Generated report showing:
    - Number of records processed per file
    - Number of duplicates removed
    - Number of missing values handled
    - Number of records loaded successfully

---

## Setup Instructions

1. **Install Python 3.x**  
   Make sure Python 3.x is installed on your system. You can download it from [python.org](https://www.python.org/downloads/).

2. **Clone or Download the Project Files**  
   Place `etl_pipeline.py`, `requirements.txt`, and the three CSV files (`customers_raw.csv`, `products_raw.csv`, `sales_raw.csv`) in your working directory.  
   Ensure the CSV files are located in a folder named `data/` (or update the script paths accordingly).

3. **Configure Database Credentials**  
   Create a `.env` file in your project root with the following variables (update with your actual MySQL credentials):


4. **How to Run**

   1. Ensure you have Python 3.x installed.
   2. Place `etl_pipeline.py`, `requirements.txt`, and the three CSV files (`customers_raw.csv`, `products_raw.csv`, `sales_raw.csv`) in your working directory. Make sure the CSV files are in a folder named `data/` (or update the script paths accordingly).
   3. Create a `.env` file in your project root with your database credentials:
    ```
    DB_HOST=your_host
    DB_USER=your_username
    DB_PASS=your_password
    DB_NAME=fleximart
    ```
   4. Run the ETL pipeline using the following command:
    ```bash
    python etl_pipeline.py
    ```
   5. After execution, check for:
    - Cleaned data loaded into your database
    - A summary report generated as `data_quality_report.txt`

---


## Troubleshooting Tips

- **Database Connection Errors:**
  - Verify your `.env` file for correct credentials.
  - Ensure your database server is running and accessible.
  - Confirm the database `fleximart` exists.

- **Missing or Incorrect CSV Files:**
  - Make sure `customers_raw.csv`, `products_raw.csv`, and `sales_raw.csv` are present in the correct directory.
  - Check file paths in the script if you use a different folder structure.

- **Package Installation Issues:**
  - If dependencies fail to install, manually run `pip install -r requirements.txt`.
  - Ensure you have internet access and the correct Python environment activated.

- **Permission Issues:**
  - Run the script with appropriate permissions, especially if writing files or connecting to the database.
  - On Linux/macOS, you may need to use `sudo` for certain operations (use with caution).

- **Error Logs:**
  - Check `etl_pipeline.log` for detailed error messages and troubleshooting information.

- **Other Issues:**
  - Review script comments and logging output for guidance.
  - Ensure all required Python packages are installed and compatible with your Python version.