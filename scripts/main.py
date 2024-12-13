import os
import pandas as pd
import sqlite3

dataset_name = "aungpyaeap/supermarket-sales"
output_dir = "./supermarket_sales"

os.makedirs(output_dir, exist_ok=True)

os.system(f"kaggle datasets download -d {dataset_name} -p {output_dir} --unzip")

files = os.listdir(output_dir)
csv_file = [file for file in files if file.endswith('.csv')][0]

file_path = os.path.join(output_dir, csv_file)
df = pd.read_csv(file_path)

df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('%', 'percentage')

df_customer = df[['invoice_id', 'customer_type', 'gender', 'city']]
df_product = df[['invoice_id', 'product_line', 'unit_price', 'branch']]
df_sales = df[['invoice_id', 'tax_5percentage', 'date', 'time', 'payment', 'cogs', 'gross_margin_percentage', 'gross_income', 'rating']]

print("DataFrame Columns:", df.columns)

database_file = "supermarket_sales_data.db"

conn = sqlite3.connect(database_file)
cursor = conn.cursor()

create_customer_data_table_query = """
CREATE TABLE IF NOT EXISTS customer_data (
    invoice_id TEXT PRIMARY KEY,
    city TEXT,
    customer_type TEXT,
    gender TEXT
);
"""

create_product_data_table_query = """
CREATE TABLE IF NOT EXISTS product_data (
    invoice_id TEXT PRIMARY KEY,
    product_line TEXT,
    unit_price REAL,
    branch TEXT
);
"""

create_sales_data_table_query = """
CREATE TABLE IF NOT EXISTS sales_data (
    invoice_id TEXT PRIMARY KEY,
    tax_5percentage REAL,
    total REAL,
    date TEXT,
    time TEXT,
    payment TEXT,
    cogs REAL,
    gross_margin_percentage REAL,
    gross_income REAL,
    rating REAL
);
"""

cursor.execute(create_sales_data_table_query)
print("sales_data Table created successfully.")

cursor.execute(create_customer_data_table_query)
print("customer_data Table created successfully.")

cursor.execute(create_product_data_table_query)
print("product_data Table created successfully.")

df_customer.to_sql("customer_data", conn, if_exists="append", index=False)
print("Data ingested successfully into customer_data table.")

df_product.to_sql("product_data", conn, if_exists="append", index=False)
print("Data ingested successfully into product_data table.")

df_sales.to_sql("sales_data", conn, if_exists="append", index=False)
print("Data ingested successfully into sales_data table.")

# Close the database connection
conn.close()
print("Database connection closed.")
