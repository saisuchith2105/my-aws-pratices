import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, year
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("CustomerAnalysis")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.YOUR KEY", "your_access_key_id")\
    .config("spark.hadoop.fs.s3a.Your_secret.key", "your_secret_key")\
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.metastore.metrics.enabled", "false") \
    .config("spark.hadoop.io.native.lib.available", "false")\
    .config("spark.executor.memory", "4g")\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()


orders_df = spark.read.csv("s3a://my-aws-assessment/ordersdata.csv",header = True, inferSchema = True)
customers_df = spark.read.csv("s3a://my-aws-assessment/CustomerData.csv",header = True, inferSchema = True)
employees_df = spark.read.csv("s3a://my-aws-assessment/employeedata.csv",header = True, inferSchema = True)

orders_df.show(5)
customers_df.show(5)
employees_df.show(5)


#filter
high_value_orders_df = orders_df.filter(orders_df['amount'] >1000)

#add a discount
high_value_order_df = high_value_orders_df.withcolunm("discounted_price",high_value_orders_df['amount'])

#group by product
sales_by_category_df = high_value_order_df.grouby("priduct_category").sum("amount")

#results
sales_by_category_df.show()

#join the order dat with customer data
customer_orders_df = high_value_order_df.join(customers_df,high_value_order_df['customer_id'] == customers_df['customer_id'],"inner")

customer_orders_df.show()
#yrs of exp dataframe
employees_df =employee_df.withColumn("yers_of_experience",datediff(current_date(),employee_df['joining_date']) / 365)
#results
employees_df.show()

#output files

sales_by_category_df.write.parquet("s3://aws-output-bkt/category-1.parquet")


customer_orders_df.write.parquet("s3://aws-output-bkt/customer-1.parquet")