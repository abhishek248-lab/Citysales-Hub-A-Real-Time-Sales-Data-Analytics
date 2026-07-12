# Databricks notebook source

from pyspark.sql.functions import col, max
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,TimestampType


from datetime import datetime
import random,time

table_name = "workspace.store.bhubaneswar_order"
table_name1 = "workspace.store.khordha_order"
products = "workspace.store.products_info"
customers = "workspace.store.customers_info"

#1--------------------------------------------------------------------------------------
delta_schema = StructType([
    StructField("Order_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("sale_date", TimestampType(), True)
])

def generate_random_sales(start_id):
    rows = []
    names = [
    "Alice", "Bob", "Charlie", "Diana", "Ethan",
    "Fiona", "George", "Hannah", "Ian", "Julia",
    "Kevin", "Luna", "Mason", "Nina", "Oscar",
    "Paula", "Quentin", "Riya", "Sam", "Tina"
    ]
    num_rows = random.randint(10, 12)
    for _ in range(num_rows):
        Order_id = start_id
        customer_name = random.choice(names)
        customer_id = random.randint(101, 150)
        product_id = random.randint(201, 250)
        quantity = random.randint(1, 5)
        amount = round(random.uniform(100.0, 500.0),2)
        sale_date = datetime.now()
        rows.append((Order_id, customer_name, customer_id, product_id, quantity,amount, sale_date))
        start_id += 1
    return rows

#2--------------------------------------------------------------------------------------  
def generate_random_sales1(start_id):
    rows = []
    names = [
    "Alice", "Bob", "Charlie", "Diana", "Ethan",
    "Fiona", "George", "Hannah", "Ian", "Julia",
    "Kevin", "Luna", "Mason", "Nina", "Oscar",
    "Paula", "Quentin", "Riya", "Sam", "Tina"
    ]
    num_rows = random.randint(10, 15)
    for _ in range(num_rows):
        Order_id = start_id+1
        customer_name = random.choice(names)
        customer_id = random.randint(101, 150)
        product_id = random.randint(201, 250)
        quantity = random.randint(1, 5)
        amount = round(random.uniform(100.0, 500.0),2)
        sale_date = datetime.now()
        rows.append((Order_id, customer_name, customer_id, product_id, quantity,amount, sale_date))
        start_id += 1
    return rows

#3______________________________________________________________

delta_schema3 = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sale_date", TimestampType(), True)
])
def generate_random_products():
    rows = []
    productname = [
    "laptop", "phone", "headphone", "monitor", "chair", None,
    "tablet", "smartwatch", "keyboard", "mouse", "printer",
    "desk", "lamp", "router", "camera", "speaker",
    "microwave", "refrigerator", "television", "air conditioner", "fan",
    "sofa", "bed", "bookshelf", "blender", "toaster"
     ]
    prodcat = [
    "electronics", "home essentials", "fashion", "accessories", "Other Categories"
     ]
    num_rows = random.randint(10,12)
    for _ in range(num_rows):
        product_id=random.randint(200,300)
        product_name = random.choice(productname)
        catagory=random.choice(prodcat)
        sale_date = datetime.now()
        rows.append((product_id, product_name, catagory, sale_date))
    return rows


#4-------------------------------------------------------------------------------------- 
delta_schema4 = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])

def generate_random_customers():
    rows = []
    names = [
    "Alice", "Bob", "Charlie", "Diana", "Ethan",
    "Fiona", "George", "Hannah", "Ian", "Julia",
    "Kevin", "Luna", "Mason", "Nina", "Oscar",
    "Paula", "Quentin", "Riya", "Sam", "Tina"
    ]
    regions = [
    "Cuttack","Khordha","Puri",
    "Ganjam","Sambalpur","Mayurbhanj",
    "Koraput","Kalahandi","Rayagada","Angul",
    "Bargarh","Jajpur","Bhadrak","Sundargarh"
]
    num_rows = random.randint(10, 15)
    for _ in range(num_rows):
        customer_id = random.randint(101, 106)
        customer_name = random.choice(names)
        region = random.choice(regions)
        last_updated = datetime.now()
        rows.append((customer_id, customer_name, region, last_updated))
    return rows



while True:
#1--------------------------------------------------------------------------------------
    if spark.catalog.tableExists(table_name):
        table_data = spark.read.table(table_name)
        curr_maxid = table_data.agg(max(col("Order_id"))).collect()[0][0]
        new_sales_data = generate_random_sales(curr_maxid)
        new_row = spark.createDataFrame(new_sales_data, schema=delta_schema)
        new_row.write.format("delta").mode("append").saveAsTable(table_name)
        display(new_row)
    else:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
            Order_id INT PRIMARY KEY,
            customer_name STRING,
            customer_id INT,
            product_id INT,
            quantity INT,
            amount DOUBLE,
            sale_date TIMESTAMP
        )
        USING DELTA
        """)
        spark.sql(f"""
        INSERT INTO {table_name} VALUES
             (1, 'Alice', 207, 1, 2, 0, '2025-08-01 00:00:00'),
             (2, 'Bob', 208, 2, 4, 0, '2025-08-01 00:00:00'),
             (3, NULL, 209, 3, 5, 390.00, '2025-08-01 00:00:00')
        """)
    
#2--------------------------------------------------------------------------------------    

    if spark.catalog.tableExists(table_name1):
        table_data = spark.read.table(table_name1)
        curr_maxid1 = table_data.agg(max(col("Order_id"))).collect()[0][0]
        start_id = curr_maxid1 or 0
        new_sales_data = generate_random_sales1(start_id)
        new_row = spark.createDataFrame(new_sales_data, schema=delta_schema)
        new_row.write.format("delta").mode("append").saveAsTable(table_name1)
        display(new_row)
    else:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name1} (
            Order_id INT PRIMARY KEY,
            customer_name STRING,
            customer_id INT,
            product_id INT,
            quantity INT,
            amount DOUBLE,
            sale_date TIMESTAMP
        )
        USING DELTA
        """)
        spark.sql(f"""
        INSERT INTO {table_name1} VALUES
            (1, 'Alice', 207, 1, 2, 0, '2025-08-01 00:00:00'),
            (2, 'Bob', 208, 2, 4, 0, '2025-08-01 00:00:00'),
            (3, NULL, 209, 3, 5, 390.00, '2025-08-01 00:00:00')

        """)

#3__________________________________________________________________________________________________________________________  

    if spark.catalog.tableExists(products):
        new_sales_data = generate_random_products()
        new_row = spark.createDataFrame(new_sales_data, schema=delta_schema3)
        new_row.write.format("delta").mode("append").saveAsTable(products)
        display(new_row)
    else:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {products} (
            product_id INT PRIMARY KEY,
            product_name STRING,
            category STRING,
            sale_date TIMESTAMP
        )
        USING DELTA
        """)
        spark.sql(f"""
        INSERT INTO {products} VALUES
           (201, 'Laptop', NULL, '2025-07-31 00:00:00'),
           (202, 'Phone', 'Electronics', '2025-07-31 00:00:00'),
           (203, 'Monitor', 'Electronics', '2025-07-31 00:00:00')
        """)

#4__________________________________________________________________________________________________________________________  

    if spark.catalog.tableExists(customers):
        products_data=generate_random_customers()
        new_row = spark.createDataFrame(products_data, schema=delta_schema4)
        new_row.write.format("delta").mode("append").saveAsTable(customers)
        display(new_row)
    else:
        # Create the Delta table
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {customers} (
            customer_id INT PRIMARY KEY,
            customer_name VARCHAR(100),
            region VARCHAR(50),
            last_updated TIMESTAMP
        )
        USING DELTA
        """)

        # Insert initial data
        spark.sql(f"""
        INSERT INTO {customers} VALUES
        (101, NULL, 'Cuttack', '2025-07-31 00:00:00'),
        (102, 'Bob', 'Khordha', '2025-07-31 00:00:00'),
        (103, 'Ram', NULL, '2025-07-31 00:00:00')

        """)
    
    time.sleep(20)

