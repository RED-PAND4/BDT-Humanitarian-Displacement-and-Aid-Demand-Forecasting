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
    spark = get_spark_session("SilverToGold-Funding")

    # ==========================================
    # PHASE 2: Read Data from Silver Layer
    # ==========================================
    print("Reading Silver Funding table...")
    fund_silver_df = (
        spark.read
        .format("delta")
        .load("s3a://lakehouse/silver/funding")
    )

    # ==========================================
    # PHASE 3: Prepare Date Bounds and Interval Detection
    # ==========================================
    fund_df = (
        fund_silver_df
        .withColumn("start_date", F.to_date("reference_period_start"))
        .withColumn("end_date", F.to_date("reference_period_end"))
        .withColumn("start_year", F.year("start_date"))
        .withColumn("end_year", F.year("end_date"))
    )

    # ==========================================
    # TRACK A: Single-Year Appeals (start_year == end_year)
    # ==========================================
    single_year_df = fund_df.filter(F.col("start_year") == F.col("end_year"))

    single_year_clean = single_year_df.select(
        "location_code",
        F.col("start_year").alias("year"),
        F.col("funding_usd").alias("allocated_funding_usd"),
        F.col("requirements_usd").alias("allocated_requirements_usd")
    )

    # ==========================================
    # TRACK B: Multi-Year Appeals (Daily Pro-Rata Distribution)
    # ==========================================
    multi_year_df = fund_df.filter(F.col("start_year") != F.col("end_year"))

    # Compute total duration in days for multi-year appeals
    multi_year_df = multi_year_df.withColumn(
        "total_days",
        F.datediff(F.col("end_date"), F.col("start_date")) + 1
    )

    # Explode sequence of individual days across the duration range
    fund_seq = multi_year_df.withColumn(
        "date_array",
        F.expr("sequence(start_date, end_date, interval 1 day)")
    )
    fund_exploded = fund_seq.withColumn("single_day", F.explode("date_array"))
    fund_years = fund_exploded.withColumn("year", F.year("single_day"))

    # Count actual days falling into each calendar year
    appeal_yearly = fund_years.groupBy(
        "location_code",
        "appeal_code",
        "funding_usd",
        "requirements_usd",
        "total_days",
        "year"
    ).agg(
        F.count("single_day").alias("days_in_year")
    )

    # Allocate funding and requirements proportionally to days in each year
    multi_year_clean = appeal_yearly.select(
        "location_code",
        "year",
        ((F.col("days_in_year") / F.col("total_days")) * F.col("funding_usd")).alias("allocated_funding_usd"),
        ((F.col("days_in_year") / F.col("total_days")) * F.col("requirements_usd")).alias("allocated_requirements_usd")
    )

    # ==========================================
    # PHASE 4: Combine Tracks and Aggregate Nationally
    # ==========================================
    combined_df = single_year_clean.unionByName(multi_year_clean)

    # Aggregate funding metrics at country and annual resolution
    gold_funding_features = combined_df.groupBy("location_code", "year").agg(
        F.sum("allocated_funding_usd").alias("funding_received_usd"),
        F.sum("allocated_requirements_usd").alias("requirements_usd")
    )

    # Compute funding coverage percentage with null handling
    gold_funding_features = gold_funding_features.withColumn(
        "funding_coverage_pct",
        F.when(
            F.col("requirements_usd") > 0,
            F.col("funding_received_usd") / F.col("requirements_usd")
        ).otherwise(F.lit(None).cast("double"))
    )

    # Compute lag-1 funding coverage feature
    window_funding = Window.partitionBy("location_code").orderBy("year")
    gold_funding_features = gold_funding_features.withColumn(
        "funding_coverage_lag1",
        F.lag("funding_coverage_pct", 1).over(window_funding)
    )

    # ==========================================
    # PHASE 5: Initialize and Save Gold Table
    # ==========================================
    initialize_delta_table(
        spark=spark,
        db_name="gold",
        table_name="gold_funding_features"
    )

    print("Writing Gold table to Delta Lake...")
    (
        gold_funding_features.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://lakehouse/gold/gold_funding_features")
    )

    print("Table gold_funding_features successfully generated.")

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)