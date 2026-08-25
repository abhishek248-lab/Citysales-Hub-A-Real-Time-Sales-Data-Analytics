# Databricks notebook source
# DBTITLE 1,Generate sales data
from pyspark.sql.functions import col,lit,when
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,TimestampType,DateType
from constants import first_name, last_name, gender, city, street_name, locality,sales_channel,payment_method,products
from datetime import datetime,timedelta
import random,time


volume_base = "/Volumes/workspace/store"

customers_volume = f"{volume_base}/customer_info_raw"
product_volume = f"{volume_base}/product_info_raw"
bhubaneswar_volume = f"{volume_base}/bhubaneswar_order_raw"
khordha_volume = f"{volume_base}/khordha_order_raw"


num_row=random.randint(300,400)


#customer
schema_customer = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("date_of_birth", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("locality", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("country", StringType(), False),
    StructField("postal_code",IntegerType(), False),
    StructField("address", StringType(), False)
])


def generate_customer():
    data=[]
    for _ in range(num_row):
        Customer_id = f"CUST{random.randint(1000,2000)}"
        Customer_name = f"{random.choice(first_name)} {random.choice(last_name)}"
        Email = f"{Customer_name.replace(' ', '').lower()}{random.randint(100,9999)}@gmail.com" if random.random() > 0.1 else None
        Phone_number=f"+91{random.randint(8900000000,9899999999)}"
        Gender = random.choice(gender)
        dob = (
            datetime.now() - timedelta(days=random.randint(18 * 365, 65 * 365))
        ).date()
        Date_of_birth = (
            random.choice([
                dob.strftime("%Y-%m-%d"),
                dob.strftime("%Y/%m/%d"),
                dob.strftime("%d/%m/%Y")
            ])
            if random.random() > 0.1
            else None
        )
        Street_name=random.choice(street_name) if random.random() > 0.1 else None
        Locality=random.choice(locality)
        City=random.choice(city)
        State= "Odisha"
        Country= "India"
        Postal_code=random.randint(750000,759999)
        Address=f"{Street_name},{Locality},{City},{State},{Country},{Postal_code}"

        data.append((Customer_id, Customer_name, Email, Phone_number, Gender, Date_of_birth, Street_name, Locality, City, State, Country, Postal_code, Address))
    return data


#product
product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("price", DoubleType(), False),
    StructField("category", StringType(), True),
    StructField("expiry_date", StringType(), True),
])
def generate_products():
    data = []
    for _ in range(num_row):
        Product_id = f"PROD{random.randint(1000,1250)}"
        Product_name = random.choice(list(products.keys()))
        Price = round(random.uniform(10, 500), 2) if random.random() > 0.1 else - round(random.uniform(10, 500), 2)
        Category = random.choice(products[Product_name])
        exp = (
            datetime.now() + timedelta(days=random.randint(1, 365))
        ).date()

        Expiry_date = (
            random.choice([
                exp.strftime("%Y-%m-%d"),
                exp.strftime("%Y/%m/%d"),
                exp.strftime("%d/%m/%Y")
            ])
            if random.random() > 0.1
            else None
        )
        data.append((Product_id, Product_name, Price, Category, Expiry_date))
    return data


#order_bhubaneswar
order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("payment_method",StringType(),False),
    StructField("amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("sales_channel", StringType(), False)
])
  
def generate_bhubaneswar_orders(customer_df,product_df,start_id):
    data = []
    customer_ids = [row.customer_id for row in customer_df.select("customer_id").collect()]
    product_ids = [row.product_id for row in product_df.select("product_id").collect()]
    product_price_map = { row.product_id: row.price for row in product_df.select("product_id", "price").collect()}
    
    for _ in range(num_row//2):
        Order_id = start_id

        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        
        quantity = random.randint(1, 8)
        Payment_method = random.choice(payment_method)
        amount = product_price_map[product_id]

        order_date = datetime.now().date()
        Order_date = (
            random.choice([
                order_date.strftime("%Y-%m-%d"),
                order_date.strftime("%Y/%m/%d"),
                order_date.strftime("%d/%m/%Y")
            ])
            if random.random() > 0.1 
            else None
        )

        Sales_channel = random.choice(sales_channel)
        data.append((Order_id, customer_id, product_id, quantity, Payment_method, amount, Order_date, Sales_channel))

        start_id +=1
    return data

#khordha_order
def generate_khordha_orders(customer_df,product_df,start_id):
    data = []
    customer_ids = [row.customer_id for row in customer_df.select("customer_id").collect()]
    product_ids = [row.product_id for row in product_df.select("product_id").collect()]
    product_price_map = { row.product_id: row.price for row in product_df.select("product_id", "price").collect()}

    for _ in range(num_row//2):
        Order_id = start_id

        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        
        quantity = random.randint(1, 8)
        Payment_method = random.choice(payment_method)
        amount = product_price_map[product_id]
        order_date = datetime.now().date()
        Order_date = (
            random.choice([
                order_date.strftime("%Y-%m-%d"),
                order_date.strftime("%Y/%m/%d"),
                order_date.strftime("%d/%m/%Y")
            ])
            if random.random() > 0.1 
            else None
        )
        Sales_channel = random.choice(sales_channel)
        data.append((Order_id, customer_id, product_id, quantity, Payment_method, amount, Order_date, Sales_channel))

        start_id +=1
    return data



bhubaneswar_order_id=10000
khordha_order_id=60000


while True:
    
    customer_data = generate_customer()
    df_customers = spark.createDataFrame(data=customer_data, schema=schema_customer)
    display(df_customers)
    df_customers.coalesce(1).write.mode("append").option("header", "true").csv(f"{customers_volume}")

    product_data = generate_products()
    df_products = spark.createDataFrame(data=product_data, schema=product_schema)
    display(df_products)
    df_products.coalesce(1).write.mode("append").option("header", "true").csv(f"{product_volume}")

    data_bhubaneswar_order = generate_bhubaneswar_orders(df_customers, df_products, bhubaneswar_order_id)
    df_bbsr = spark.createDataFrame(data=data_bhubaneswar_order, schema=order_schema)
    display(df_bbsr)
    df_bbsr.coalesce(1).write.mode("append").option("header", "true").csv(f"{bhubaneswar_volume}")
    bhubaneswar_order_id += len(data_bhubaneswar_order)

    data_khordha_order = generate_khordha_orders(df_customers, df_products, khordha_order_id)
    df_khordha = spark.createDataFrame(data=data_khordha_order, schema=order_schema)
    display(df_khordha)
    df_khordha.coalesce(1).write.mode("append").option("header", "true").csv(f"{khordha_volume}")
    khordha_order_id += len(data_khordha_order)
    
    

    time.sleep(20)