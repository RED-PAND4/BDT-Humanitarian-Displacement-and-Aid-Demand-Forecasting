from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.functions import col, from_json, regexp_replace, trim


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import get_spark_session

spark = get_spark_session("SilverToGold-HostPressure")

# 1. Read Bronze as a Stream
population_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/silver/population"))

solutions_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/silver/solutions"))

demographics_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/silver/demographics"))

asyliumDec_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/silver/asyliumDecision"))

idmc_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/silver/idmc"))

unrwa_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/silver/unrwa"))


spark.sql("CREATE DATABASE IF NOT EXISTS gold LOCATION 's3a://lakehouse/gold'")
spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.netPressure
    USING delta
    LOCATION 's3a://lakehouse/gold/netPressure'
""")

# Calculation

# 1. Calculate the total_displacement and perform the grouping
displacement_df = (population_df
    # Group by the host region and the year boundaries
    .groupBy("coa", "coo", "yearFrom", "yearTo")
    
    # Calculate the sum of the population components
    .agg(
        .sum(
            col("population.refugees") + 
            col("population.asylium_seeker") + 
            col("population.idps")
        ).alias("total_displacement")
    )
)

# 1. Calculate the outflow and perform the grouping
outflow_df = (solutions_df
    # Group by the host region and the year boundaries
    .groupBy("coa", "coo", "yearFrom", "yearTo")
    
    # Calculate the sum of the population components
    .agg(
        .sum(
            col("solutions.returned_refugees") + 
            col("solutions.resettlement") + 
            col("solutions.naturalisation")
        ).alias("total_outflow")
    )
)

# 1. Join and calculate the net_pressure column
net_pressure_df = (displacement_df
    .join(outflow_df, on=["coa", "coo", "yearFrom", "yearTo"], how="inner")
    .withColumn("net_pressure", col("total_displacement") - col("total_outflow"))
)

# 1. Calculate the total_displacement and perform the grouping
vulnerability_df = (demographics_df
    # Group by the host region and the year boundaries
    .groupBy("coa", "coo", "yearFrom", "yearTo")
    
    # Calculate the sum of the population components
    .agg(
        .sum(
            col("population.refugees") + 
            col("population.asylium_seeker") + 
            col("population.idps")
        ).alias("total_displacement")
    )
)


# Step 2: Write data
query= (net_pressure_df.write 
    .format("delta") 
    #.option("<option_name>", "<option_value>") \
    .mode("append") 
    .saveAsTable("s3a://lakehouse/gold/netPressure")
)


spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.vulnerabilityScore
    USING delta
    LOCATION 's3a://lakehouse/gold/vulnerabilityScore'
""")


#Calculation







# Step 2: Write data
query= (net_pressure_df.write 
    .format("delta") 
    #.option("<option_name>", "<option_value>") \
    .mode("append") 
    .saveAsTable("s3a://lakehouse/gold/vulnerabilityScore")
)







print("Taking out the trash in the silver layer...")

# Example A: Keep only the last 1 hours of deleted/old data
#spark.sql("VACUUM delta.`s3a://lakehouse/bronze/currency` RETAIN 1 HOURS")