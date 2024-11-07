import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, unix_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType


spark = SparkSession.builder.appName("CustomerAnalysis")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.access.key", "acees_key")\
    .config("spark.hadoop.fs.s3a.secret.key", "secret_key")\
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.metastore.metrics.enabled", "false") \
    .config("spark.hadoop.io.native.lib.available", "false")\
    .config("spark.executor.memory", "4g")\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()

os.environ['AWS_ACCESS_KEY_ID'] = 'your_access_key' 
os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret_key' 
os.environ['AWS_DEFAULT_REGION'] = 'region'

session = boto3.Session(aws_access_key_id='your_access_key', aws_secret_access_key='your_secret_key', region_name='region')  
dynamodb = session.resource('dynamodb') 
table = dynamodb.Table('Customers')

# Load customer data from DynamoDB
response = table.scan()
cust_data = response['Items']

# Convert the DynamoDB items to DataFrame
cust_df = spark.createDataFrame(cust_data)

# Join DataFrames on customer_id
joined_df = sales_df.join(cust_df, on="customer_id", how="inner")

# Define window partitioned by customer_id and ordered by purchase_date
window_spec = Window.partitionBy("customer_id").orderBy("purchase_date")

# Calculate time difference between each transaction (in seconds)
interval_df = joined_df.withColumn(
    "previous_purchase_date", lag("purchase_date").over(window_spec)
).withColumn(
    "purchase_interval",
    (unix_timestamp("purchase_date") - unix_timestamp("previous_purchase_date"))
)

# Calculate average transaction interval per customer (in seconds)
avg_interval_df = interval_df.groupBy("customer_id").agg(
    avg("purchase_interval").alias("avg_purchase_interval_seconds")
)

# Convert seconds to days for easier interpretation
avg_interval_df = avg_interval_df.withColumn(
    "avg_purchase_interval_days", col("avg_purchase_interval_seconds") / 86400
)

# Identify high-engagement customers (e.g., customers with average interval < 30 days)
high_engagement_df = avg_interval_df.filter(col("avg_purchase_interval_days") < 30)
high_engagement_df.show()

print("Customer purchase interval analysis completed.")
spark.stop()