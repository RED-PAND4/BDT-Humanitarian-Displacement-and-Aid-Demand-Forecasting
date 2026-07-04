from pyspark.sql import SparkSession
import sys
import os


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import get_spark_session

spark = get_spark_session("BronzeToSilver_UNHCR_Population")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/population"))

# 2. Clean the Data
# Drop rows where critical fields are null
cleaned_df = bronze_df.dropna(subset=["year", "coo_iso"])

# Drop duplicates based on a unique ID or code

deduplicated_df = cleaned_df.dropDuplicates(["year", "coo_iso", "coa_iso"])

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.population
    USING delta
    LOCATION 's3a://lakehouse/silver/population'
""")

#  3. Write to Silver using AvailableNow
query= (deduplicated_df.write 
    .format("delta") 
    .mode("overwrite") 
    .save("s3a://lakehouse/silver/population")

)

#print("Taking out the trash in the Bronze layer...")
# spark.sql("VACUUM delta.`s3a://lakehouse/bronze/population` RETAIN 24 HOURS")
