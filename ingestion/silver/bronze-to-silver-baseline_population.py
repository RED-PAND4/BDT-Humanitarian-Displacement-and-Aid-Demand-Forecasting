from pyspark.sql import SparkSession
import sys
import os


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import get_spark_session

spark = get_spark_session("BronzeToSilver-baselinepopulation")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/baselinepopulation"))

# 2. Clean the Data
# Drop rows where critical fields are null (using original API names)
cleaned_df = bronze_df.dropna(subset=["location_code", "population"])

# Drop duplicates based on the unique code from the API

deduplicated_df = cleaned_df.dropDuplicates(subset=["location_code", "population"])

spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://lakehouse/silver'")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.baselinepopulation
    USING delta
    LOCATION 's3a://lakehouse/silver/baselinepopulation'
""")

# 3. Write to Silver 
query= (deduplicated_df.write 
    .format("delta") 
    .mode("overwrite") 
    .save("s3a://lakehouse/silver/baselinepopulation")

)

#print("Taking out the trash in the Bronze layer...")

#Keep only the last 1 hours of deleted/old data
#spark.sql("VACUUM delta.`s3a://lakehouse/silver/baselinepopulation`")