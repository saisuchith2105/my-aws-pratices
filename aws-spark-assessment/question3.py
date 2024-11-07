from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerTierAnalysis")\
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

# DynamoDB Configuration
dynamodb_table_name = "Customers"
aws_region = "ap-south-1"  

# Load customer data from DynamoDB
customers_df = spark.read \
    .format("dynamodb") \
    .option("dynamodb.tableName", dynamodb_table_name) \
    .option("dynamodb.region", aws_region) \
    .load()

# Load transaction data from S3
transactions_df = spark.read.csv("s3a://my-aws-assessment/transaction.csv", header=True, inferSchema=True)

# Show the loaded data (for verification)
customers_df.show(5)
transactions_df.show(5)

# Join the customer and transaction data on customer_id
joined_df = transactions_df.join(customers_df, transactions_df['customer_id'] == customers_df['customer_id'], "inner")

# Calculate total spending per customer
spending_df = joined_df.groupBy("customer_id") \
    .agg(sum("total_price").alias("total_spending"))

# Assign customer tiers based on total spending
tiered_df = spending_df.withColumn(
    "tier", 
    when(col("total_spending") <= 500, "Bronze")
    .when((col("total_spending") > 500) & (col("total_spending") <= 1000), "Silver")
    .otherwise("Gold")
)

# Show the results with customer tiers
tiered_df.show()

# Save the tiered results back to DynamoDB
tiered_df.write \
    .format("dynamodb") \
    .option("dynamodb.tableName", "CustomerTiers") \
    .option("dynamodb.region", aws_region) \
    .save()
