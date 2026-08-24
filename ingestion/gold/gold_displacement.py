import sys
import os
import pyspark.sql.functions as F
from pyspark.sql.window import Window

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

spark = get_spark_session("SilverToGold-Displacement")

print("1. Reading Silver tables...")
pop_df = spark.read.format("delta").load("s3a://lakehouse/silver/population")
sol_df = spark.read.format("delta").load("s3a://lakehouse/silver/solutions")

# =====================================================================
# STEP 1: AGGREGATE STOCKS (People currently displaced/hosted)
# =====================================================================
print("2. Aggregating stocks per route...")
stock_agg = pop_df.groupBy("year", "coo_iso", "coa_iso").agg(
    F.sum("refugees").alias("refugees"),
    F.sum("asylum_seekers").alias("asylum_seekers"),
    F.sum("oip").alias("oip"),
    F.sum("idps").alias("idps") # Populated only when coo_iso == coa_iso
)

# =====================================================================
# STEP 2: AGGREGATE OUTFLOWS (Solutions and returns)
# =====================================================================
print("3. Aggregating outflows per route...")
outflows_agg = sol_df.groupBy("year", "coo_iso", "coa_iso").agg(
    F.sum("returned_refugees").alias("returned_refugees"),
    F.sum("resettlement").alias("resettlement"),
    F.sum("naturalisation").alias("naturalisation"),
    F.sum("returned_idps").alias("returned_idps") # Populated only when coo_iso == coa_iso
)

# =====================================================================
# STEP 3: FULL OUTER JOIN & NULL HANDLING
# =====================================================================
print("4. Joining tables and filling missing values...")
# Join using the composite key [year, coo_iso, coa_iso]
fact_df = stock_agg.join(
    outflows_agg, 
    ["year", "coo_iso", "coa_iso"], 
    "full_outer"
)

# Replace Nulls with 0 for all numerical columns to allow safe math operations
metric_cols = [
    "refugees", "asylum_seekers", "oip", "idps", 
    "returned_refugees", "resettlement", "naturalisation", "returned_idps"
]
fact_df = fact_df.fillna(0, subset=metric_cols)

# =====================================================================
# STEP 4: CALCULATE NET FLOW (Per single route)
# =====================================================================
print("5. Calculating net flow metrics...")

# 1. Total people hosted/displaced on this route (Stock)
fact_df = fact_df.withColumn(
    "stock",
    F.col("refugees") + F.col("asylum_seekers") + F.col("oip") + F.col("idps")
)

# 2. Total outflows on this route
fact_df = fact_df.withColumn(
    "outflows",
    F.col("returned_refugees") + F.col("resettlement") + F.col("naturalisation") + F.col("returned_idps")
)

# 3. Find previous year's stock for this exact route
window_route = Window.partitionBy("coo_iso", "coa_iso").orderBy("year")
fact_df = fact_df.withColumn(
    "stock_lag1", 
    F.lag("stock", 1).over(window_route)
)

# 4. Inflows new arrivals: (Current Stock - Previous Stock) + Outflows
# Null handling: if stock_lag1 is null (first year of the route), net_flow remains null natively
fact_df = fact_df.withColumn(
    "inflows",
    (F.col("stock") - F.col("stock_lag1")) + F.col("outflows")
)

# Drop temporary columns used only for calculations
fact_df = fact_df.drop("stock_lag1")

# =====================================================================
# STEP 5: WRITE TO DELTA LAKE
# =====================================================================
initialize_delta_table(
    spark=spark,
    db_name="gold",
    table_name="gold_displacement"
)

print("6. Writing gold_displacement to Delta Lake...")
(
    fact_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("s3a://lakehouse/gold/gold_displacement")
)

print("Table gold_displacement generated successfully!")
spark.stop()