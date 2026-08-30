import sys
import os
from pyspark.sql.window import Window
import pyspark.sql.functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

if __name__ == "__main__":
    # ==========================================
    # PHASE 1: Initialize Spark Session
    # ==========================================
    spark = get_spark_session("SilverToGold-ConflictEvents")

    # ==========================================
    # PHASE 2: Read Data from Silver Layer
    # ==========================================
    print("Reading Silver Conflict Events table...")
    conflict_silver_df = (
        spark.read
        .format("delta")
        .load("s3a://lakehouse/silver/conflict_events")
    )

    # ==========================================
    # PHASE 3: Select Optimal Administrative Granularity & Clean
    # ==========================================
    # For each country-year, dynamically retain the highest available granularity:
    # Fill null event and fatality counts with zero
    window_country_year = Window.partitionBy("location_code", "year")
    
    conflict_clean = (
        conflict_silver_df
        .withColumn("_max_admin_level", F.max("admin_level").over(window_country_year))
        .filter(F.col("admin_level") == F.col("_max_admin_level"))
        .drop("_max_admin_level")
        .fillna({"fatalities": 0, "events": 0})
    )

    # ==========================================
    # PHASE 4: Feature Aggregation (National Annual Level)
    # ==========================================
    # Aggregate conflict metrics per country and year while avoiding double counting:
    # - Total Fatalities & Violent Events: derived strictly from 'political_violence'
    # - Civilian Targeting: sub-metrics capturing direct attacks against civilians
    # - Non-Violent Events: demonstrations and peaceful protests
    gold_conflict_features = (
        conflict_clean
        .groupBy("location_code", "year")
        .agg(
            F.sum(
                F.when(F.col("event_type") == "political_violence", F.col("fatalities"))
                .otherwise(0)
            ).alias("total_fatalities"),
            F.sum(
                F.when(F.col("event_type") == "political_violence", F.col("events"))
                .otherwise(0)
            ).alias("violent_events"),
            F.sum(
                F.when(F.col("event_type") == "civilian_targeting", F.col("events"))
                .otherwise(0)
            ).alias("civilian_targeting_events"),
            F.sum(
                F.when(F.col("event_type") == "civilian_targeting", F.col("fatalities"))
                .otherwise(0)
            ).alias("civilian_targeting_fatalities"),
            F.sum(
                F.when(F.col("event_type") == "demonstration", F.col("events"))
                .otherwise(0)
            ).alias("non_violent_events")
        )
    )

    # ==========================================
    # PHASE 5: Initialize and Save Gold Table
    # ==========================================
    initialize_delta_table(
        spark=spark,
        db_name="gold",
        table_name="gold_conflict_features"
    )

    print("Writing Gold table to Delta Lake...")
    (
        gold_conflict_features.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://lakehouse/gold/gold_conflict_features")
    )

    print("Table gold_conflict_features successfully generated.")

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)