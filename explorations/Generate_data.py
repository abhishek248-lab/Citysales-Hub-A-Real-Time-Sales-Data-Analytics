# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DecimalType
from datetime import datetime, timedelta
import random
import time

# Volume paths for CSV file storage
volume_base = "/Volumes/workspace/store"
bhubaneswar_volume = f"{volume_base}/bhubaneswar_order_raw"
khordha_volume = f"{volume_base}/khordha_order_raw"
products_volume = f"{volume_base}/products_info_raw"
customers_volume = f"{volume_base}/customer_info_raw"

# Helper function to generate unique filename
def get_unique_filename():
    return f"data_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.csv"

# Industry-standard schemas
order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_name", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("sale_date", TimestampType(), False)
])

# Dynamic data generation components
CATEGORIES = ["electronics", "fashion", "home appliances", "furniture"]
FIRST_NAMES = ["Rajesh", "Priya", "Amit", "Sneha", "Vikram", "Ananya", "Sanjay", "Pooja", "Rahul", "Kavita",
               "Arjun", "Divya", "Karthik", "Neha", "Aditya", "Ritu", "Suresh", "Meera", "Varun", "Lakshmi",
               "Rohan", "Anjali", "Nikhil", "Shreya", "Manoj", "Deepak", "Swati", "Manish", "Nisha", "Prakash"]
LAST_NAMES = ["Kumar", "Sharma", "Patel", "Singh", "Reddy", "Gupta", "Mehta", "Desai", "Verma", "Joshi",
              "Nair", "Iyer", "Rao", "Kapoor", "Malhotra", "Agarwal", "Pillai", "Srinivasan", "Chopra", "Menon",
              "Shah", "Bhat", "Jain", "Kulkarni", "Krishnan", "Mishra", "Pandey", "Das", "Sen", "Bose"]

def generate_bhubaneswar_order(start_id):
    """Generate orders for Bhubaneswar - IDs: 10001, 10002, 10003..."""
    data = []
    num_rows = random.randint(8, 15)
    
    for _ in range(num_rows):
        order_id = 10000 + start_id
        customer_name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        product_id = f"PRD{random.randint(10000, 99999)}"
        # Add realistic null values (10% chance)
        quantity = random.randint(1, 50) if random.random() > 0.1 else None
        amount = round(random.uniform(100.0, 30000.0), 2) if random.random() > 0.1 else None
        sale_date = datetime.now()
        
        data.append((order_id, customer_name, product_id, quantity, amount, sale_date))
        start_id += 1
    
    return data

def generate_khordha_order(start_id):
    """Generate orders for Khordha - IDs: 60001, 60002, 60003..."""
    data = []
    num_rows = random.randint(8, 15)
    
    for _ in range(num_rows):
        order_id = 60000 + start_id
        customer_name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        product_id = f"PRD{random.randint(10000, 99999)}"
        # Add realistic null values (10% chance)
        quantity = random.randint(1, 50) if random.random() > 0.1 else None
        amount = round(random.uniform(100.0, 30000.0), 2) if random.random() > 0.1 else None
        sale_date = datetime.now()
        
        data.append((order_id, customer_name, product_id, quantity, amount, sale_date))
        start_id += 1
    
    return data

product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sale_date", TimestampType(), False)
])

def generate_product_data():
    """Generate realistic product catalog entries"""
    data = []
    num_products = random.randint(8, 15)
    
    for _ in range(num_products):
        product_id = f"PRD{random.randint(10000, 99999)}"
        # Add realistic null values (10% chance)
        product_name = random.choice(["laptop", "phone", "tablet", "watch", "camera", "headphones", "tv", "speaker", 
                                       "jeans", "shirt", "shoes", "jacket", "dress", "handbag", "sunglasses",
                                       "refrigerator", "washing machine", "microwave", "vacuum", "air fryer", "blender",
                                       "sofa", "chair", "table", "bed", "desk", "bookshelf", "cabinet", "lamp"]) if random.random() > 0.1 else None
        category = random.choice(CATEGORIES) if random.random() > 0.1 else None
        sale_date = datetime.now()
        data.append((product_id, product_name, category, sale_date))
    
    return data

schema_customer = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("region", StringType(), True),
    StructField("purchase_date", TimestampType(), False)
])

# Odisha districts for regional distribution
ODISHA_REGIONS = [
    "Cuttack", "Khordha", "Puri", "Ganjam", "Sambalpur", 
    "Mayurbhanj", "Koraput", "Kalahandi", "Rayagada", "Angul",
    "Bargarh", "Jajpur", "Bhadrak", "Sundargarh", "Balasore", 
    "Dhenkanal", "Kendrapara", "Jagatsinghpur", "Nayagarh", "Boudh"
]

def generate_randomcustomer():
    """Generate realistic customer registrations"""
    data = []
    num_rows = random.randint(8, 15)
    
    for _ in range(num_rows):
        customer_id = f"CUST{random.randint(1000, 9999)}"
        customer_name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        # Add realistic null values (10% chance)
        region = random.choice(ODISHA_REGIONS) if random.random() > 0.1 else None
        days_ago = random.randint(0, 5)
        purchase_date = datetime.now() - timedelta(days=days_ago, hours=random.randint(0, 23))
        
        data.append((customer_id, customer_name, region, purchase_date))
    
    return data


# Initialize order counters
# Bhubaneswar: 10001, 10002... | Khordha: 60001, 60002...
bhubaneswar_order_id = 1
khordha_order_id = 1

batch_count = 0

while True:
    batch_count += 1
    
    # Bhubaneswar orders
    data_bhubaneswar_order = generate_bhubaneswar_order(bhubaneswar_order_id)
    df_bbsr = spark.createDataFrame(data=data_bhubaneswar_order, schema=order_schema)
    display(df_bbsr)
    df_bbsr.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{bhubaneswar_volume}/{get_unique_filename()}")
    bhubaneswar_order_id += len(data_bhubaneswar_order)

    # Khordha orders
    data_khordha_order = generate_khordha_order(khordha_order_id)
    df_khordha = spark.createDataFrame(data=data_khordha_order, schema=order_schema)
    display(df_khordha)
    df_khordha.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{khordha_volume}/{get_unique_filename()}")
    khordha_order_id += len(data_khordha_order)
    
    # Product data
    new_sales_data = generate_product_data()
    df_products = spark.createDataFrame(new_sales_data, schema=product_schema)
    display(df_products)
    df_products.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{products_volume}/{get_unique_filename()}")

    # Customer data
    customer_data = generate_randomcustomer()
    df_customers = spark.createDataFrame(customer_data, schema=schema_customer)
    display(df_customers)
    df_customers.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{customers_volume}/{get_unique_filename()}")
    
    time.sleep(20)