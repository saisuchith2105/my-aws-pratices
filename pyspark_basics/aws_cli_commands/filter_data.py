from pyspark.sql import SparkSession
from pyspark.sql.functions import col
 
# Initialize SparkSession with corrected JAR paths
spark = SparkSession.builder \
    .appName("PySpark S3 Example") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIATX3PIANRDTPKLPK3") \
    .config("spark.hadoop.fs.s3a.secret.key", "G70VV5Zn1/IaAUHCUXmBUanUmYPFeumR8Q78jXk5e") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
 
 
# Set Spark log level
spark.sparkContext.setLogLevel("DEBUG")
 
# S3 bucket paths
input_bucket = "my-aws-boto-3-buck"
input_key = "floder1/Vehicle.csv"
output_bucket = "output-bucket-21"
output_key = "folder2/"
 
# S3 paths
input_path = f"s3a://{input_bucket}/{input_key}"
output_path = f"s3a://{output_bucket}/{output_key}"
 
# Read CSV data from S3
df = spark.read.csv(input_path, header=True, inferSchema=True)
 
# Filter the DataFrame
filtered_df = df.filter(col("Year") >= 2018)
 
# Write output as Parquet to S3
filtered_df.write.parquet(output_path, mode="overwrite")
 
print("Data processing complete. Output saved to:", output_path)
 
# Stop Spark session
spark.stop()
 
