
""" The World Food Programme serves people in IPC stage 3 or worse. These populations are facing food 
crises and urgently need food assistance to survive and then recover in the long term"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import *

if __name__ == "__main__":
    # ==========================================
    # PHASE 1: Initialize Spark Session
    # ==========================================
    spark = get_spark_session("SilverToGold-FoodSecurity")

    # ==========================================
    # PHASE 2: Read Data from Silver Layer
    # ==========================================
    print("Reading Silver Food Security table...")
    fs_silver_df = (
        spark.read
        .format("delta")
        .load("s3a://lakehouse/silver/foodsecurity")
    )

# ==========================================
    # PHASE 3: Filter National Level & Critical IPC Phase (3+)
    # ==========================================
    # Filter strictly for national assessments (admin_level == 0) and IPC Phase 3+
    # (Crisis, Emergency, Famine) to isolate acute food insecurity peaks
    fs_critical = (
        fs_silver_df
        .filter(
            (F.col("admin_level") == 0) & 
            (F.col("ipc_phase") == "3+")
        )
    )

    # ==========================================
    # PHASE 4: Feature Aggregation (National Annual Peak)
    # ==========================================
    # Aggregate by country and year to capture the worst recorded food insecurity peak
    gold_food_security = (
        fs_critical
        .groupBy("location_code", "year")
        .agg(
            F.max("population_in_phase").alias("peak_population_phase3plus")
        )
    )

    # ==========================================
    # PHASE 5: Initialize and Save Gold Table
    # ==========================================
    initialize_delta_table(
        spark=spark,
        db_name="gold",
        table_name="gold_food_security_features"
    )

    print("Writing Gold table to Delta Lake...")
    (
        gold_food_security.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://lakehouse/gold/gold_food_security_features")
    )

    print("Table gold_food_security_features successfully generated.")

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)