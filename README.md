# FlexiMart Data Architecture Project

**Student Name:** Haneesh Singhal <br>
**Student ID:** BA_25071841 <br>
**Email:** haneesh_singhal@yahoo.com <br>
**Date:** 04-Feb-2026 <br>

---

## Project Overview

The FlexiMart Data Architecture Project focuses on designing and implementing a complete data ecosystem for a retail business. It includes building an ETL pipeline to extract, transform, and load transactional data into a relational database, performing NoSQL operations for product catalog management using MongoDB, and creating a star schema-based data warehouse for advanced analytics. The project demonstrates integration of structured and semi-structured data, efficient query handling, and aggregation techniques. It provides a scalable architecture to support business insights and decision-making.

---

## Repository Structure
```
├── data/
│   ├── customers_raw.csv
│   ├── products_raw.csv
│   ├── sales_raw.csv
├── part1-database-etl/
│   ├── etl_pipeline.py
│   ├── schema_documentation.md
│   ├── business_queries.sql
│   ├── data_quality_report.txt
│   ├── README.md
│   └── requirements.txt
├── part2-nosql/
│   ├── nosql_analysis.md
│   ├── mongodb_operations.py
│   ├── README.md
│   └── products_catalog.json
├── part3-datawarehouse/
│   ├── star_schema_design.md
│   ├── warehouse_schema.sql
│   ├── warehouse_data.sql
│   ├── README.md
│   └── analytics_queries.sql
├── .gitignore
└── README.md
```
---

## Technologies Used

- Python 3.x, pandas, mysql-connector-python
- MySQL 8+ 
- MongoDB 6.0

---

## Setup Instructions

### Database Setup

```bash
# Create databases
mysql -u root -p -e "CREATE DATABASE fleximart;"
mysql -u root -p -e "CREATE DATABASE fleximart_dw;"

# Run Part 1 - ETL Pipeline
python part1-database-etl/etl_pipeline.py

# Run Part 1 - Business Queries
mysql -u root -p fleximart < part1-database-etl/business_queries.sql

# Run Part 3 - Data Warehouse
mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_schema.sql
mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_data.sql
mysql -u root -p fleximart_dw < part3-datawarehouse/analytics_queries.sql

### MongoDB Operations
python part2-nosql/mongodb_operations.py

```

---


## **Key Learnings**
- Developed an end-to-end data architecture integrating relational databases, NoSQL systems, and data warehouses.
- Implemented ETL pipelines using Python and SQL for efficient data transformation and loading into MySQL.
- Gained experience with MongoDB operations for semi-structured data, including CRUD and aggregation pipelines.
- Learned dimensional modeling principles and designed a star schema to optimize analytical queries.


---


## **Challenges Faced**

#### **1. Data Consistency During ETL**
- **Challenge:** Ensuring consistency between raw CSV files and MySQL database.  
- **Solution:** Implemented robust data validation and logging mechanisms to maintain data quality.

#### **2. MongoDB Connection Management**
- **Challenge:** Handling connection errors and resource cleanup.  
- **Solution:** Used error handling and context managers to manage database connections safely.

#### **3. Optimizing Star Schema Design**
- **Challenge:** Designing a schema that supports complex analytical queries efficiently.  
- **Solution:** Applied dimensional modeling principles and validated performance through query testing.
