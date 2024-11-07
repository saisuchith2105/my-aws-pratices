import boto3
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, year, month, lit, count
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("CustomerAnalysis")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.access.key","access_key")\
    .config("spark.hadoop.fs.s3a.secret.key", "secret-key")\
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.metastore.metrics.enabled", "false") \
    .config("spark.hadoop.io.native.lib.available", "false")\
    .config("spark.executor.memory", "4g")\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()
os.environ['AWS_ACCESS_KEY_ID'] = 'acess_key' 
os.environ['AWS_SECRET_ACCESS_KEY'] = 'esecret_key' 
os.environ['AWS_DEFAULT_REGION'] = 'region'

trans_input = "s3a://my-aws-assessment/transaction.csv"
trans_df = spark.read.csv(trans_input, header=True, inferSchema=True)

# Create a session with your credentials 
session = boto3.Session(aws_access_key_id='acess_key', aws_secret_access_key='secret_key', region_name='region')  
dynamodb = session.resource('dynamodb') 
table = dynamodb.Table('Customers')

trans_input = "s3a://my-aws-assessment/transaction.csv"
trans_df = spark.read.csv(trans_input, header=True, inferSchema=True)

# Load customer data from DynamoDB
response = table.scan()
cust_data = response['Items']
# Convert the DynamoDB items to DataFrame
cust_df = spark.createDataFrame(cust_data)

# Flag customers as inactive if they have no transactions in the last year
inactive_cust_df = cust_act_df.withColumn(
    "inactive", (col("last_transaction_date") < lit(one_year_ago))
)

# Filter for inactive customers
inactive_cust_df = inactive_cust_df.filter(col("inactive") == True)
print("inactive customers")
inactive_cust_df.show()

# Calculate churn rate by month
churn_by_month_df = (
    inactive_cust_df
    .withColumn("churn_year", year(col("last_transaction_date")))
    .withColumn("churn_month", month(col("last_transaction_date")))
    .groupBy("churn_year", "churn_month")
    .agg(count("customer_id").alias("churn_count"))
    .orderBy("churn_year", "churn_month")
)

churn_by_month_df.show()
spark.stop()