import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.sql import functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import *

if __name__ == "__main__":
    # ==========================================
    # PHASE 1: Initialize Spark Session
    # ==========================================
    spark = get_spark_session("BronzeToSilver-PovertyRate")

    # ==========================================
    # PHASE 2: Read Data from Bronze
    # ==========================================
    bronze_df = (
        spark.read
        .format("delta")
        .load("s3a://lakehouse/bronze/povertyrate")
        # .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # Process only data ingested today
    )

    # ==========================================
    # PHASE 3: Clean Missing Critical Values
    # ==========================================
    # Drop records missing primary location code
    cleaned_df = bronze_df.dropna(subset=["location_code"])

    # ==========================================
    # PHASE 4: Clean and Deduplicate Data
    # ==========================================
    # Business keys identifying unique poverty surveys:
    subset_cols = [
        "location_code",
        "admin1_code",
        "admin_level",
        # "resource_hdx_id",                 # Excluded: technical metadata/file ID
        # "location_name",                   # Excluded: descriptive name
        # "admin1_name",                     # Excluded: descriptive name
        # "mpi",                             # Excluded: numeric metric subject to revision
        # "headcount_ratio",                 # Excluded: numeric metric subject to revision
        # "intensity_of_deprivation",        # Excluded: numeric metric subject to revision
        # "vulnerable_to_poverty",           # Excluded: numeric metric subject to revision
        # "in_severe_poverty",               # Excluded: numeric metric subject to revision
        "reference_period_start",
        "reference_period_end"
    ]
    deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)

    # ==========================================
    # PHASE 5: Explode Multi-Year Date Intervals
    # ==========================================
    # Expand multi-year survey ranges into distinct annual rows for partition alignment
    silver_ready_df = explode_date_range_to_years(
        deduplicated_df,
        start_col="reference_period_start",
        end_col="reference_period_end"
    )

    # ==========================================
    # PHASE 6: Initialize and Upsert to Silver
    # ==========================================
    initialize_delta_table(
        spark=spark,
        db_name="silver",
        table_name="povertyrate"
    )

    upsert_to_silver_layer(
        spark=spark,
        deduplicated_df=silver_ready_df,
        table_name="povertyrate"
    )

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)