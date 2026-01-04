
# MongoDB Operations for Product Catalog

This repository contains a self-executable Python script, **`mongodb_operations.py`**, which demonstrates essential MongoDB operations for a product catalog. The script is modular, robust, and includes error handling and logging for all major events.

---

## Features

- **Loads product data** from a JSON file into MongoDB
- **Queries products** by category and price
- **Aggregates reviews** to find products with high average ratings
- **Updates products** by adding new reviews
- **Performs complex aggregations** (e.g., average price by category)
- **Comprehensive logging** to `mongodb_operations.log`
- **Error handling** for all database operations

---

## Prerequisites

- Python 3.7+
- MongoDB instance (local or remote)
- `products_catalog.json` file (sample product data)
- `.env` file with MongoDB connection details

---

## Installation

- **Clone this repository** (or copy the script and data files into a directory).

- **Install dependencies:**
    ```bash
    pip install pymongo python-dotenv json5
    OR
    Run the Python Script, it will automatically install the dependencies from "requirements.txt" file.
    ```

- **Prepare your `.env` file** in the same directory:
   **Example:**
    ```    
    MONGODB_URI=mongodb://localhost:27017/fleximart
    MONGODB_DB=fleximart
    MONGODB_COLLECTION=products
    ```
    *(Adjust the URI, DB, and collection as needed for your setup.)*

- **Ensure `products_catalog.json` is present** in the same directory.

---

## Usage

### 1. Run the Script

To execute all operations (load data, query, aggregation, update, etc.):

```bash
python mongodb_operations.py
```
### 2. Example Output
--- Electronics under 50000 ---
{'name': 'Sony WH-1000XM5 Headphones', 'price': 29990.0, 'stock': 200}
{'name': 'OnePlus Nord CE 3', 'price': 26999.0, 'stock': 180}
...

--- Products with average rating >= 4.0 ---
{'product_id': 'ELEC001', 'name': 'Samsung Galaxy S21 Ultra', 'avg_rating': 4.67}
{'product_id': 'ELEC003', 'name': 'Sony WH-1000XM5 Headphones', 'avg_rating': 4.67}
...

--- Adding review to product ELEC001 ---
Review added.

--- Average price by category ---
{'category': 'Electronics', 'avg_price': 66197.0, 'product_count': 6}
{'category': 'Fashion', 'avg_price': 5948.5, 'product_count': 6}
...

All operations are logged to mongodb_operations.log for auditing and debugging.


### 3. Script Structure

* connect_to_mongodb()
Connects to MongoDB and returns the client, database, and collection objects.


* load_data(json_path, collection) (**Operation 1: Load Data**)
Import or Load the provided JSON file into collection 'products'.


* query_electronics_under_50000(collection) (**Operation 2: Basic Query**)
** Find all products in "Electronics" category with price less than 50000.
** Return only: name, price, stock


* products_with_high_avg_rating(collection, min_rating=4.0) (**Operation 3: Review Analysis**)
Finds products with an average review rating ≥ min_rating.


* Add a new review to product "ELEC001" (**Operation 4: Update Operation**)
Review: {user: "U999", rating: 4, comment: "Good value", date: ISODate()}.

* Calculate average price by category (**Operation 5: Complex Aggregation**)
Return: category, avg_price, product_count.
Sort by avg_price descending


### 4. Logging

All major events and errors are logged to mongodb_operations.log for debugging and auditing.


### 5. Customization

* You can modify the script to use your own product data or extend the MongoDB operations as needed. 
* Adjust the .env file to connect to a different MongoDB instance or database.