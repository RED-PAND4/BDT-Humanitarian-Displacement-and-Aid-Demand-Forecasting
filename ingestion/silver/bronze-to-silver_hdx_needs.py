from pyspark.sql import SparkSession
import sys
import os


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from utilities import get_spark_session

spark = get_spark_session("BronzeToSilver_HDX_Needs")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/humanitarian_needs"))


# 2. Clean the Data
# Rimuoviamo le righe in cui manca il codice del paese o il numero di popolazione
cleaned_df = bronze_df.dropna(subset=["location_code", "population"])

# Rimuoviamo i duplicati. 
# Per questi dati, un record è unico se la combinazione di paese, settore, categoria e status è unica.
deduplicated_df = cleaned_df.dropDuplicates([
    "location_code", 
    "sector_code", 
    "category", 
    "population_status",
    "reference_period_start"
])

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.humanitarian_needs
    USING delta
    LOCATION 's3a://lakehouse/silver/humanitarian_needs'
""")

# 3. Write to Silver using AvailableNow
# query = (deduplicated_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", "s3a://lakehouse/checkpoints/silver_needs")
#     .trigger(availableNow=True) # 👈 THE MAGIC TRICK: Process new data and shut down
#     .start("s3a://lakehouse/silver/humanitarian_needs"))

# query.awaitTermination()

# print("Taking out the trash in the Bronze layer...")

# # Example A: Keep only the last 24 hours of deleted/old data
# spark.sql("VACUUM delta.`s3a://lakehouse/bronze/humanitarian_needs` RETAIN 24 HOURS")
# Step 2: Write data
query= (deduplicated_df.write 
    .format("delta") 
    #.option("<option_name>", "<option_value>") \
    .mode("overwrite") 
    #.save("silver.baselinepopulation")
    .save("s3a://lakehouse/silver/humanitarian_needs")

)
print("overwrite")
print("Taking out the trash in the Bronze layer...")