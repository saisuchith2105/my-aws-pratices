import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, year, count, desc
from pyspark.sql.types import IntegerType
 
spark = SparkSession.builder.appName("CustomerAnalysis")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.access.key", "your_access_key")\
    .config("spark.hadoop.fs.s3a.secret.key", "your_secret_key")\
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.metastore.metrics.enabled", "false") \
    .config("spark.hadoop.io.native.lib.available", "false")\
    .config("spark.executor.memory", "4g")\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()

import os
 
os.environ['AWS_ACCESS_KEY_ID'] = 'your_access_key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'ypur_secret_key'
os.environ['AWS_DEFAULT_REGION'] = 'region'  
 
# Create a session with your credentials
session = boto3.Session(aws_access_key_id='your_access_key', aws_secret_access_key='your_secret_key', region_name='name')  )
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('Customers')

# Load data from s3
trans_input = "s3a://my-aws-assessment/transaction.csv"
trans_df = spark.read.csv(trans_input, header=True, inferSchema=True)
 
# Initialize DynamoDB client
 
# Load customer data from DynamoDB
response = table.scan()
cust_data = response['Items']
 
# Convert the DynamoDB items to DataFrame
cust_df = spark.createDataFrame(cust_data)
 
# Display the initial rows
print("Customer Data from DynamoDB:")
cust_df.show()
print("Transaction Data from S3:")
trans_df.show()
 
# Join customer and transaction data on customer_id
customer_transaction_df = cust_df.join(trans_df, on="customer_id")
 
# Count repeat purchases per customer
rep_pur_df = customer_transaction_df.groupBy("customer_id") \
    .agg(count("transaction_id").alias("purchase_count")) \
    .filter(col("purchase_count") > 1)
rep_pur_df.show()
 
top_rep_cust_df = rep_pur_df.orderBy(col("purchase_count").desc())
top_rep_cust_df.show()
 
# Save results back to S3 in Parquet format
output_path = "s3a://qoutput-bkt/folder2/"
top_rep_cust_df.write.parquet(output_path, mode="overwrite")
 
print("Customer loyalty analysis complete. Results saved to S3.")
spark.stop()
