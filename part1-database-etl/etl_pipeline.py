# 
# # FlexiMart ETL Pipeline
# 
# This Python Program demonstrates a robust ETL pipeline for FlexiMart's customer, product, and sales data.
# 
# **Features:**
# - Data cleaning, validation, and transformation
# - Saving cleaned data to CSV files (with validation and error handling)
# - Loading cleaned data into MySQL database tables (with validation and error handling)
# 


# 
# ## 1. Imports, Logger, and Configuration
# 
# This cell imports all necessary libraries, sets up logging, and configures global settings.
# 

import subprocess
import sys


# Function to install requirements from requirements.txt
def install_requirements():
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

# Install requirements
install_requirements()


import os
import logging
import pandas as pd
import numpy as np
import phonenumbers
import mysql.connector
import logging
from dotenv import load_dotenv
from datetime import datetime

# Suppress SettingWithCopyWarning
pd.options.mode.chained_assignment = None  # default='warn'

# -------------------- SETUP LOGGER --------------------
# Configure logging
logging.basicConfig(
    filename='etl_pipeline.log',  # Log file name
    level=logging.INFO,           # Log level: INFO records all major events
    format='%(asctime)s %(levelname)s:%(message)s'  # Log format with timestamp
)

# Create a logger object
logger = logging.getLogger()


# 
# 2. Utility Functions
# 
# Create helper functions for data cleaning, validation, and database operations.
# These functions will be used throughout the ETL pipeline.

# -------------------- HELPER FUNCTION --------------------

# Function to remove the first character from a string value
def remove_first_char(val):
    """
    Removes the first character from a string value.
    Used for surrogate key creation.
    Example: 'C001' -> '001', 'P003' -> '003'
    """
    try:
        if pd.isna(val):
            return np.nan
        return str(val)[1:]
    except Exception as e:
        logger.error(f"Error removing first character from '{val}': {e}")
        return np.nan

# Function to trim leading/trailing spaces from all string columns in a DataFrame
def trim_str_cols(df):
    """
    Trims leading/trailing spaces from all string columns in a DataFrame.
    """
    try:
        str_cols = df.select_dtypes(include=['object']).columns
        for col in str_cols:
            df[col] = df[col].str.strip()
        logger.info("Trimmed string columns in DataFrame.")
        return df
    except Exception as e:
        logger.error(f"Error trimming string columns: {e}")
        return df

# Function to standardize phone numbers
def standardize_phone(phone):
    """
    Standardizes phone numbers to +91-XXXXXXXXXX format.
    Removes spaces, dashes, leading zeros, and ensures country code.
    Returns np.nan if unable to process.
    """
    try:
        parsed = phonenumbers.parse(phone, "IN")
        if phonenumbers.is_valid_number(parsed):
            cc = parsed.country_code
            national = str(parsed.national_number)[-10:]  # Last 10 digits
            phonenumber = f'+{cc}-{national}'
            return phonenumber
        else:
            logger.warning(f"Invalid phone number: {phone}")
            return np.nan
    except Exception as e:
        logger.error(f"Error formatting phone number '{phone}': {e}")
        return np.nan

# Function to standardize product category names
def standardize_category(cat):
    """
    Standardizes product category names to title case and strips spaces.
    """
    try:
        if pd.isna(cat):
            return None
        cat = cat.strip().lower()
        if 'electronic' in cat:
            return 'Electronics'
        elif 'fashion' in cat:
            return 'Fashion'
        elif 'grocer' in cat:
            return 'Groceries'
        else:
            return cat.title()
    except Exception as e:
        logger.error(f"Error standardizing category '{cat}': {e}")
        return None

# Function to standardize date formats to YYYY-MM-DD
def standardize_date(date_str):
    """
    Converts various date formats to YYYY-MM-DD using pandas.
    """
    try:
        # Try parsing with known formats first
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y', '%d-%m-%Y', '%m/%d/%Y'):
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except:
                continue
        # Fallback to pandas parsing
        return pd.to_datetime(date_str, errors='coerce', dayfirst=False).strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f"Error parsing date '{date_str}': {e}")
        return np.nan

# Function to clean leading/trailing spaces from a string value
def clean_spaces(val):
    """
    Removes leading and trailing spaces from a string value.
    If the value is not a string, returns it unchanged.
    """
    try:
        if isinstance(val, str):
            return val.strip()
        else:
            return val
    except Exception as e:
        logger.error(f"Error cleaning spaces for '{val}': {e}")
        return val

# Function to fill missing product data (Price and stock_quantity) with median values
def fill_missing_product_data_with_median(df):
    """
    Fills missing values in 'price' and 'stock_quantity' columns with their median.
    """
    try:
        df['price'] = df['price'].fillna(df['price'].median())
        df['stock_quantity'] = df['stock_quantity'].fillna(df['stock_quantity'].median())
        logger.info("Filled missing product data with median values.")
        return df
    except Exception as e:
        logger.error(f"Error filling missing product data: {e}")
        return df

# Function to fill missing email addresses with a placeholder
def fill_missing_email(customers_df):
    """
    Fills missing email addresses in the customers DataFrame with a placeholder.
    """
    try:
        customers_df['email'] = customers_df.apply(
            lambda row: f"unknown_email_{row['customer_id']}" if pd.isnull(row['email']) or row['email'] == '' else row['email'],
            axis=1
        )
        logger.info("Filled missing emails with placeholders.")
        return customers_df
    except Exception as e:
        logger.error(f"Error filling missing emails: {e}")
        return customers_df

# Function to validate DataFrame structure and content
def validate_dataframe(df, expected_columns, df_name):
    """
    Validates that the DataFrame is not empty and contains all expected columns.
    Returns True if valid, False otherwise.
    """
    try:
        if df is None:
            logger.error(f"{df_name} is None.")
            print(f"{df_name} is None.")
            return False
        if df.empty:
            logger.error(f"{df_name} is empty.")
            print(f"{df_name} is empty.")
            return False
        missing_cols = [col for col in expected_columns if col not in df.columns]
        if missing_cols:
            logger.error(f"{df_name} is missing columns: {missing_cols}")
            print(f"{df_name} is missing columns: {missing_cols}")
            return False
        logger.info(f"{df_name} validated successfully.")
        return True
    except Exception as e:
        logger.error(f"Error validating DataFrame {df_name}: {e}")
        return False

# Function to clean CSV file if it exists
def clean_csv_if_exists(filepath):
    """
    If the CSV file exists, erase its contents (clean the file).
    """
    try:
        if os.path.exists(filepath):
            open(filepath, 'w').close()
            logger.info(f"Cleaned existing file: {filepath}")
    except Exception as e:
        logger.error(f"Error cleaning file {filepath}: {e}")

# Function to get database connection
def get_db_connection():
    """
    Establish and return a MySQL database connection using credentials from .env file.
    Includes error handling and logging.
    """
    try:
        # Load environment variables from .env file in the root directory
        load_dotenv()
        # Read DB credentials from environment variables
        host = os.getenv("DB_HOST")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        database = os.getenv("DB_NAME")
        logger.info(f"Database credentials read: host={host}, user={user}, database={database}")

        # Validate credentials
        if not all([host, user, password, database]):
            logger.error("Missing database credentials in .env file.")
            return None

        # Connect to MySQL
        conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
        logger.info("Database connection established successfully.")
        return conn

    except mysql.connector.Error as err:
        logger.error(f"Database connection error: {err}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during DB connection: {e}")
        return None

# Function to create all required tables
def create_all_tables(conn):
    """
    Creates all required tables if they do not exist.
    """
    # Define table creation queries
    table_queries = [
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT PRIMARY KEY AUTO_INCREMENT,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            phone VARCHAR(20),
            city VARCHAR(50),
            registration_date DATE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY AUTO_INCREMENT,
            product_name VARCHAR(100) NOT NULL,
            category VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            stock_quantity INT DEFAULT 0
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT PRIMARY KEY AUTO_INCREMENT,
            customer_id INT NOT NULL,
            order_date DATE NOT NULL,
            total_amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(20) DEFAULT 'Pending',
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INT PRIMARY KEY AUTO_INCREMENT,
            order_id INT NOT NULL,
            product_id INT NOT NULL,
            quantity INT NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL,
            subtotal DECIMAL(10,2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
        """
    ]
    # Execute table creation queries
    try:
        # Create a cursor object
        cursor = conn.cursor()
        # Execute each table creation query
        for query in table_queries:
            cursor.execute(query)
        conn.commit()
        logger.info("All tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
    finally:
        cursor.close()

# Function to generate data quality report
def generate_quality_report(df, file_name):
    """
    Generates a data quality report for the given DataFrame.
    - Number of records processed per file
    - Number of duplicates removed
    - Number of missing values handled
    - Number of records loaded successfully
    """
    # Total Number of records processed
    records_processed = len(df)
    logger.info(f"Records processed in {file_name}: {records_processed}")
    
    # Number of duplicates removed
    duplicates_removed = df.duplicated().sum()
    logger.info(f"Duplicates removed in {file_name}: {duplicates_removed}")
    
    # Number of missing values handled
    missing_values_handled = df.isnull().sum().sum()
    logger.info(f"Missing values handled in {file_name}: {missing_values_handled}")
    
    # Number of records dropped due to duplicates and missing values
    if file_name == 'customers_raw.csv' or file_name == 'products_raw.csv':
        cleaned_df = df.drop_duplicates()
    else:
        cleaned_df = df.drop_duplicates().dropna()
    logger.info(f"Number of records after cleaning in {file_name}: {len(cleaned_df)}")

    # Number of records loaded successfully
    records_loaded_successfully = len(cleaned_df)
    logger.info(f"Records loaded successfully from {file_name}: {records_loaded_successfully}")

    # Compile report
    return {
        "File": file_name,
        "Records Processed": records_processed,
        "Duplicates Removed": duplicates_removed,
        "Missing Values Handled": missing_values_handled,
        "Records Loaded Successfully": records_loaded_successfully
    }

# 
# ## 3. Extract Raw Data
# 
# Read the raw CSV files from the `data` folder.
# 

# -------------------- EXTRACT LOGIC --------------------
logger.info("------------------------ Date Extract Logic from CSV File -----------------")
def extract_raw_data_from_csv():
    """
    Reads raw CSV files and returns DataFrames for customers, products, and sales.
    """
    data_dir='../data'
    try:
        # Load customers_raw CSV files into DataFrames
        customers = pd.read_csv(os.path.join(data_dir, 'customers_raw.csv'))
        logger.info(f"Loaded customers_raw.csv file into DataFrame with shape {customers.shape}")
    except Exception as e:
        logger.error(f"Error loading customers_raw.csv: {e}")
        customers = pd.DataFrame()
        print(f"Error loading customers_raw.csv: {e}")

    try:
        # Load products_raw.csv into DataFrame
        products = pd.read_csv(os.path.join(data_dir, 'products_raw.csv'))
        logger.info(f"Loaded products_raw.csv file into DataFrame with shape {products.shape}")
    except Exception as e:
        logger.error(f"Error loading products_raw.csv: {e}")
        products = pd.DataFrame()
        print(f"Error loading products_raw.csv: {e}")

    try:
        # Load sales_raw.csv into DataFrame
        sales = pd.read_csv(os.path.join(data_dir, 'sales_raw.csv'))
        logger.info(f"Loaded sales_raw.csv file into DataFrame with shape {sales.shape}")
    except Exception as e:
        logger.error(f"Error loading sales_raw.csv: {e}")
        sales = pd.DataFrame()
        print(f"Error loading sales_raw.csv: {e}")

    return customers, products, sales

# ## 4. Transform or Clean Data
# 
# Clean all Customer raw, product raw, sales raw CSV file and split the Sales Raw clean dataset to Order and Order Items CSV file and load the data into MySQL database
# 
# -------------------- CLEAN CUSTOMERS --------------------
# Clean customers data: remove duplicates, handle missing emails, standardize phone/city/date
def clean_customers(customers_df):
    """
    Cleans the customers DataFrame:
    - Trims all string columns
    - Removes duplicate rows based on customer_id
    - Drops rows with missing customer_id
    - Standardizes phone numbers, city names, and registration dates
    - Returns the cleaned DataFrame.
    """
    try:             
        # Remove first character from customer_id for surrogate key
        customers_df['customer_id'] = customers_df['customer_id'].apply(remove_first_char)
        logger.info("Surrogate customer_id created in customers.csv.")
        
        # Trim string columns (assuming trim_str_cols trims all string columns in df)
        customers_df = trim_str_cols(customers_df)
        logger.info("Trimmed string columns in customers DataFrame.")
        
        # Remove duplicate customers based on customer_id
        customers_df = customers_df.drop_duplicates(subset=['customer_id'])
        logger.info(f"Customer duplicates removed. Remaining records: {len(customers_df)}")
        
        # Remove rows where 'email' is NaN or empty
        customers_df = fill_missing_email(customers_df)
        logger.info("Missing emails filled with placeholder values.")

        # Standardize phone numbers
        customers_df['phone'] = customers_df['phone'].apply(standardize_phone)
        logger.info("Phone numbers standardized.")

        # Standardize city names to Title Case
        customers_df['city'] = customers_df['city'].str.title()
        logger.info("City names standardized in Title Case.")

        # Standardize registration dates to YYYY-MM-DD format
        customers_df['registration_date'] = customers_df['registration_date'].apply(standardize_date)
        logger.info("Registration dates standardized in YYYY-MM-DD format.")

        # Trim string columns again after transformations
        customers_df = trim_str_cols(customers_df)
        logger.info("String columns trimmed of leading/trailing spaces.")

        logger.info(f"Cleaned customers data. Shape: {customers_df.shape}")

        logger.info("Customer data cleaning complete.") 
        # Reorder columns to match SQL table
        return customers_df[['customer_id', 'first_name', 'last_name', 'email', 'phone', 'city', 'registration_date']]

    except Exception as e:
        logger.error(f"Error cleaning customers data: {e}")
        print(f"Error cleaning customers data: {e}")

# -------------------- CLEAN PRODUCTS --------------------
# Clean products data: remove duplicates, handle missing prices/stock, standardize category/name
def clean_products(products_df):
    """
    Cleans the products DataFrame:
    - Trims all string columns
    - Removes duplicate rows based on product_id
    - Drops rows with missing product_id
    - Standardizes product names and categories
    - Returns the cleaned DataFrame.
    """
    try:  
        # Remove first character from product_id for surrogate key
        products_df['product_id'] = products_df['product_id'].apply(remove_first_char)
        logger.info("Surrogate product_id created in products.csv.")

        # Trim string columns (assuming trim_str_cols trims all string columns in df)
        products_df = trim_str_cols(products_df)
        
        # Remove duplicate products based on product_id
        products_df = products_df.drop_duplicates(subset=['product_id'])
        logger.info(f"Product duplicates removed. Remaining records: {len(products_df)}")

        # Fill missing values with median        
        logger.info("Missing stock quantities filled with median.")
        products_df = fill_missing_product_data_with_median(products_df)        
        logger.info("Missing prices filled from sales_raw.csv mapping.")

        # Standardize category names to Title Case
        products_df['category'] = products_df['category'].apply(standardize_category)
        logger.info("Category names standardized in Title Case.")

        # Standardize product names (trim spaces)
        products_df['product_name'] = products_df['product_name'].apply(clean_spaces)
        logger.info("Product names trimmed of leading/trailing spaces.")

        # Trim string columns again after transformations
        logger.info("String columns trimmed of leading/trailing spaces.")
        products_df = trim_str_cols(products_df)        

        logger.info(f"Cleaned products data. Shape: {products_df.shape}")

        logger.info("Product data cleaning complete.")

        # Reorder columns to match SQL table
        return products_df[['product_id', 'product_name', 'category', 'price', 'stock_quantity']]

    except Exception as e:
        logger.error(f"Error cleaning products data: {e}")
        print(f"Error cleaning products data: {e}")

# -------------------- CLEAN SALES --------------------
# Clean sales data: remove duplicates, handle missing IDs, standardize date
def clean_sales(sales_df):    
    """
    Cleans the sales DataFrame:
    - Trims all string columns
    - Removes duplicate rows based on transaction_id and product_id
    - Drops rows with missing customer_id or product_id
    - Standardizes transaction_date to YYYY-MM-DD format
    - Returns the cleaned DataFrame.
    """
    try:
        
        # Remove first character from customer_id, product_id, transaction_id for surrogate keys
        sales_df['customer_id'] = sales_df['customer_id'].apply(remove_first_char)
        sales_df['product_id'] = sales_df['product_id'].apply(remove_first_char)

        sales_df['transaction_id'] = sales_df['transaction_id'].apply(remove_first_char)
        logger.info("Surrogate transaction_id created in sales.csv.")

        # Trim string columns (assuming trim_str_cols trims all string columns in df)
        sales_df = trim_str_cols(sales_df)
        logger.info("Trimmed string columns in sales DataFrame.")

        # Remove duplicate products based on product_id
        sales_df = sales_df.drop_duplicates(subset=['transaction_id'])
        logger.info(f"Sales duplicates removed. Remaining records: {len(sales_df)}")

        # Drop rows with missing customer_id or product_id
        sales_df = sales_df.dropna(subset=['customer_id'])
        sales_df = sales_df.dropna(subset=['product_id'])
        logger.info(f"Dropped rows with missing customer_id or product_id. Remaining records: {len(sales_df)}")   
             
        # Standardize transaction_date to YYYY-MM-DD format
        sales_df['transaction_date'] = sales_df['transaction_date'].apply(standardize_date)
        logger.info(f"Cleaned sales data. Shape: {sales_df.shape}")
        logger.info("Sales data cleaning complete.")        
        return sales_df
    except Exception as e:
        logger.error(f"Error cleaning sales data: {e}")
        print(f"❌ Error cleaning sales data: {e}")
        return pd.DataFrame()


# -------------------- SPLIT ORDERS --------------------
# Split sales data into orders and order_items DataFrames
def split_sales_to_orders(sales_clean):
    """
    Splits the sales DataFrame into orders and order_items DataFrames
    with columns renamed to match the SQL table structure. 
    Creates the orders DataFrame from cleaned sales data, calculates total_amount,
    and saves it to CSV before loading into the database.
    """
    try:    
        # Copy sales_clean to avoid modifying original DataFrame
        logger.info("Starting split of sales into orders.")
        df = sales_clean.copy()
        logger.info("Copied sales_clean DataFrame.")

        # Create a unique order_id for each transaction
        df['order_id'] = df['transaction_id']
        logger.info("Generated surrogate order_id for each transaction.")

        # Calculate total_amount for each order (quantity * unit_price)
        df['total_amount'] = df['quantity'] * df['unit_price']
        logger.info("Calculated total_amount for each order.")

        # Orders DataFrame
        orders = df[['order_id', 'customer_id', 'transaction_date', 'total_amount', 'status']].drop_duplicates()
        logger.info(f"Created orders DataFrame with shape: {orders.shape}")

        # Rename columns to match SQL table structure
        orders = orders.rename(columns={
            'transaction_date': 'order_date'
        })
        logger.info("Renamed columns in orders DataFrame to match SQL structure.")

        # Clean existing CSV before saving Orders DataFrame:
        csv_path = '../data/orders.csv'
        clean_csv_if_exists(csv_path)

        # Save to CSV
        orders.to_csv(csv_path, index=False)
        logger.info("Saved orders DataFrame to {csv_path}")

        logger.info(f"Split sales into orders ({orders.shape})")
        return orders
        
    except Exception as e:
        logger.error(f"Error splitting sales data: {e}")
        return pd.DataFrame()


# -------------------- SPLIT ORDER ITEMS --------------------
def split_sales_to_order_items(sales_clean):
    
    """
    Splits the sales DataFrame into ororder_items DataFrames
    with columns renamed to match the SQL table structure. 
    Creates the order_items DataFrame from cleaned sales data, calculates total_amount,
    and saves it to CSV before loading into the database.
    """
    try:
        
        # Copy sales_clean to avoid modifying original DataFrame
        logger.info("Starting split of sales into order_items.")
        order_items = sales_clean.copy()

        # Create a unique order_item_id for each transaction (same as in orders)
        order_items['order_item_id'] = order_items.index + 1
        logger.info("Generated surrogate order_item_id for each transaction.")
        
        # Calculate total_amount for each order (quantity * unit_price)
        order_items['subtotal'] = order_items['quantity'] * order_items['unit_price']
        logger.info("Calculated subtotal for each order item.")

        # Order Items DataFrame
        order_items = order_items[['order_item_id', 'transaction_id', 'product_id', 'quantity', 'unit_price', 'subtotal']].copy()
        logger.info(f"Created order_items DataFrame with shape: {order_items.shape}")

        # Rename columns to match SQL table structure
        order_items = order_items.rename(columns={
            'transaction_id': 'order_id'
        })
        logger.info("Renamed columns in order_items DataFrame to match SQL structure.")

        # Before saving your DataFrame:
        csv_path = '../data/order_items.csv'
        clean_csv_if_exists(csv_path)

        # Save to CSV
        order_items.to_csv(csv_path, index=False)
        logger.info("Saved order_items DataFrame to {csv_path}")

        logger.info(f"Split sales into order_items ({order_items.shape})")
        return order_items
        
    except Exception as e:
        logger.error(f"Error splitting sales data: {e}")
        return pd.DataFrame()

# ## 5. Functions for loading the data into different MYSQL tables (Customers, Products, Orders and Order_items)
# 1. It validate the Dataframe
# 2. If table not exists then create the table
# 3. Insert the data into tables


# -------------------- LOAD LOGIC --------------------
# -------------------- LOADING CUSTOMERS DATA INTO DATABASE --------------------

# --- Database Loading Functions ---
def load_data_to_table(df, table_name, columns, insert_query, delete_queries=None):
    # if dataframe is None or empty, log error and return
    if df is None or df.empty:
        logger.error(f"{table_name} DataFrame is empty or None. Aborting load.")
        return
    
    # Establish database connection
    conn = get_db_connection()
    
    # If connection failed, log error and return
    if conn is None:
        logger.error("Failed to establish database connection.")
        return
    if table_name.lower() == "customers":
        create_all_tables(conn)
    try:
        # Create a cursor object using the connection
        cursor = conn.cursor()
        
        # If there are delete queries, execute them
        logger.info(f"Deleting existing data from {table_name} table before loading new data.")
        if delete_queries:
            for dq in delete_queries:
                cursor.execute(dq)
        
        # Prepare data for insertion
        data = df[columns].values.tolist()
        
        # Execute the insert query for all rows
        logger.info(f"Inserting data into {table_name} table.")
        cursor.executemany(insert_query, data)

        # Commit the transaction
        conn.commit()
        logger.info(f"{table_name} data loaded successfully.")
    except mysql.connector.Error as err:
        logger.error(f"Error loading {table_name}: {err}")
    except Exception as e:
        logger.error(f"Unexpected error loading {table_name}: {e}")
    finally:
        cursor.close()
        conn.close()

# Function to load customers data into the database
def load_data_to_customers_db(customers_df):    
    """
    Validates the DataFrame, creates the 'customers' table if it doesn't exist,
    """
    logger.info("Starting to load customers data into database.")
    
    # SQL Query to insert customer data
    insert_query = """
        INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, registration_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    # Delete order_items first, then orders, then customers
    delete_queries = [
        "DELETE FROM order_items",
        "DELETE FROM orders",
        "DELETE FROM customers"
    ]

    # Define the columns to be inserted
    columns = [
        'customer_id', 
        'first_name', 
        'last_name', 
        'email', 
        'phone', 
        'city', 
        'registration_date']

    # Loading data to table
    load_data_to_table(customers_df, "customers", columns, insert_query, delete_queries)
    

# -------------------- LOADING PRODUCTS DATA INTO DATABASE --------------------

# Function to load products data into the database
def load_data_to_products_db(products_df):    
    """
    Validates the DataFrame, creates the 'products' table if it doesn't exist,
    """ 

    logger.info("Starting to load products data into database.")

    # SQL Query to insert product data
    query_insert = """
        INSERT INTO products (product_id, product_name, category, price, stock_quantity)
        VALUES (%s, %s, %s, %s, %s)
    """
    # Delete order_items first, then products
    delete_queries = [
        "DELETE FROM order_items",
        "DELETE FROM products"
    ]

    # Define the columns to be inserted
    columns = [
        'product_id', 
        'product_name', 
        'category', 
        'price', 
        'stock_quantity']
    
    #loading data to table
    load_data_to_table(products_df, "products", columns, query_insert, delete_queries)
    

# -------------------- LOADING ORDERS DATA INTO DATABASE --------------------

# Function to load orders data into the database
def load_data_to_orders_db(orders_df):
    
    """
    Validate DataFrame, create 'orders' table if not exists, delete existing rows, and insert new data.
    """

    logger.info("Starting to load orders data into database.")
    
    # SQL Query to insert order data
    query_insert = """
        INSERT INTO orders (order_id, customer_id, order_date, total_amount, status)
        VALUES (%s, %s, %s, %s, %s)
    """

    # Delete order data from orders table
    delete_queries = [
        "DELETE FROM orders"
    ]
    
    # Define the columns to be inserted
    columns = [
        'order_id', 
        'customer_id', 
        'order_date', 
        'total_amount', 
        'status']
    
    #loading data to table
    load_data_to_table(orders_df, "orders", columns, query_insert, delete_queries)

# -------------------- LOADING ORDER_ITEMS DATA INTO DATABASE --------------------

# Function to load order items data into the database
def load_data_to_order_items_db(order_items_df):
    """
    Validate DataFrame, create 'order_items' table if not exists, delete existing rows, and insert new data.
    """
    logger.info("Starting to load order items data into database.")    
   
    # SQL Query to insert order item data
    query_insert = """
        INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, subtotal)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    # Delete order items data from order_items table
    delete_queries = [
        "DELETE FROM order_items"
    ]

    # Define the columns to be inserted
    columns = [
        'order_item_id', 
        'order_id', 
        'product_id', 
        'quantity', 
        'unit_price', 
        'subtotal']
    
    #loading data to table
    load_data_to_table(order_items_df, "order_items", columns, query_insert, delete_queries)


# 
# ## 6. Data Quality Report
# 
# Print a summary of the ETL process for data quality assurance.
# - Number of records processed per file
# - Number of duplicates removed
# - Number of missing values handled
# - Number of records loaded successfully
# 


# Generate report for each file

def write_data_quality_report(customers, products, sales_raw):
    """
    Generates and writes a data quality report for the ETL process.
    """
    report = []
    report.append(generate_quality_report(customers, "customers_raw.csv"))
    report.append(generate_quality_report(products, "products_raw.csv"))
    report.append(generate_quality_report(sales_raw, "sales_raw.csv"))

    with open("data_quality_report.txt", "w") as f:
        f.write("Data Quality Report (ETL Summary):\n\n")
        for r in report:
            f.write(f"File: {r['File']}\n")
            f.write(f"- Records Processed: {r['Records Processed']}\n")
            f.write(f"- Duplicates Removed: {r['Duplicates Removed']}\n")
            f.write(f"- Missing Values Handled: {r['Missing Values Handled']}\n")
            f.write(f"- Records Loaded Successfully: {r['Records Loaded Successfully']}\n\n")


# -------------------- MAIN ETL PIPELINE --------------------
def main():
    
    logger.info("------- ETL Pipeline Started ---------------")
    # 1. Extract Raw Data
    logger.info("---------------- Data Extraction started from CSV files ----------------")
    customers, products, sales = extract_raw_data_from_csv()
    logger.info("---------------- Data Extraction Ended from CSV files ------------------")

    # 2. Transform or Clean Data
    logger.info("---------------- Data Transformation started ---------------------------")
    customers_clean = clean_customers(customers)
    products_clean = clean_products(products)
    sales_clean = clean_sales(sales)
    logger.info("---------------- Data Transformation Ended -----------------------------")

    # 3. Split sales into orders and order_items
    logger.info("---------------- Data Splitting started for Order and Order Item ----------------")
    orders = split_sales_to_orders(sales_clean)
    order_items = split_sales_to_order_items(sales_clean)
    logger.info("---------------- Data Splitting Ended for Order and Order Item ----------------")
    
    # 4. Load Data into Database
    logger.info("---------------- Data Loading to Database started -----------------------")
    load_data_to_customers_db(customers_clean)
    load_data_to_products_db(products_clean)
    load_data_to_orders_db(orders)
    load_data_to_order_items_db(order_items)
    logger.info("---------------- Data Loading to Database Ended -------------------------")

    # 5. Generate Data Quality Report
    logger.info("---------------- Quality Report Generation started ----------------------")
    write_data_quality_report(customers, products, sales)
    logger.info("Data quality report generated at data_quality_report.txt.")
    logger.info("---------------- Quality Report Generation Ended ----------------------")

    logger.info("------- ETL Pipeline Ended ---------------")


# -------------------- ENTRY POINT --------------------
if __name__ == "__main__":
    main()
