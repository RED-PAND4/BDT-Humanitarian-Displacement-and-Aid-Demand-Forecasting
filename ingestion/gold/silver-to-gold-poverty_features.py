import sys
import os
import pyspark.sql.functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

if __name__ == "__main__":
    # ==========================================
    # PHASE 1: Initialize Spark Session
    # ==========================================
    spark = get_spark_session("SilverToGold-PovertyFeatures")

    # ==========================================
    # PHASE 2: Read Data from Silver Layer
    # ==========================================
    print("Reading Silver poverty tables...")
    hdx_df = spark.read.format("delta").load("s3a://lakehouse/silver/povertyrate")
    wb_mpm_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_mpm")
    wb_ext_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_extreme_poverty")

    # ==========================================
    # PHASE 3: Clean and Aggregate Primary Sources
    # ==========================================
    # A. HDX Poverty Rate: Filter national assessments (admin_level == 0)
    hdx_clean = (
        hdx_df
        .filter(F.col("admin_level") == 0)
        .withColumn("year", F.col("year").cast("integer"))
        .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull())
        .groupBy("location_code", "year")
        .agg(
            F.avg("mpi").alias("mpi"),
            F.avg("headcount_ratio").alias("hdx_head"),
            F.avg("vulnerable_to_poverty").alias("hdx_vuln"),
            F.avg("in_severe_poverty").alias("hdx_sev")
        )
    )

    # B. World Bank Multidimensional Poverty Measure (MPM)
    wb_mpm_clean = (
        wb_mpm_df
        .withColumn("year", F.col("year").cast("integer"))
        .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull())
        .groupBy("location_code", "year")
        .agg(
            F.avg("mpm_value").alias("mpm")
        )
    )

    # C. World Bank Extreme Poverty ($3/day)
    wb_ext_clean = (
        wb_ext_df
        .withColumn("year", F.col("year").cast("integer"))
        .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull())
        .groupBy("location_code", "year")
        .agg(
            F.avg("extreme_poverty_value").alias("ext_pov")
        )
    )

    # ==========================================
    # PHASE 4: Multi-Source Full Outer Join
    # ==========================================
    # Combine all poverty indicators into a unified national profile
    gold_poverty_features = (
        hdx_clean
        .join(wb_mpm_clean, ["location_code", "year"], "outer")
        .join(wb_ext_clean, ["location_code", "year"], "outer")
    )

    # ==========================================
    # PHASE 5: Initialize and Save Gold Table
    # ==========================================
    initialize_delta_table(
        spark=spark,
        db_name="gold",
        table_name="gold_poverty_features"
    )

    print("Writing Gold table to Delta Lake...")
    (
        gold_poverty_features.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://lakehouse/gold/gold_poverty_features")
    )

    print("Table gold_poverty_features successfully generated.")

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)