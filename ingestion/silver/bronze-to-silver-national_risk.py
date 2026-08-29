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
    spark = get_spark_session("BronzeToSilver-NationalRisk")

    # ==========================================
    # PHASE 2: Read Data from Bronze
    # ==========================================
    bronze_df = (
        spark.read
        .format("delta")
        .load("s3a://lakehouse/bronze/national_risk")
        .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # Process only data ingested today
    )

    # ==========================================
    # PHASE 3: Clean Missing Critical Values
    # ==========================================
    # Drop records missing primary location code or core composite risk score
    cleaned_df = bronze_df.dropna(subset=["location_code", "overall_risk"])

    # ==========================================
    # PHASE 4: Clean and Deduplicate Data
    # ==========================================
    # Business keys identifying a unique country risk assessment period:
    subset_cols = [
        "location_code",
        # "resource_hdx_id",               # Excluded: technical file ID metadata
        # "risk_class",                    # Excluded: derived categorical risk metric
        # "global_rank",                   # Excluded: relative global rank metric subject to update
        # "overall_risk",                  # Excluded: primary numeric index score
        # "hazard_exposure_risk",          # Excluded: sub-dimension risk metric
        # "vulnerability_risk",            # Excluded: sub-dimension risk metric
        # "coping_capacity_risk",          # Excluded: sub-dimension risk metric
        # "meta_missing_indicators_pct",   # Excluded: data quality percentage metadata
        # "meta_avg_recentness_years",      # Excluded: data quality indicator age metadata
        "reference_period_start",
        "reference_period_end"
    ]
    deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)

    # ==========================================
    # PHASE 5: Extract Date Components
    # ==========================================
    # Extract structured date columns (year, month, day) from reference_period_start
    silver_ready_df = extract_date_components(deduplicated_df, "reference_period_start")

    # ==========================================
    # PHASE 6: Initialize and Upsert to Silver
    # ==========================================
    initialize_delta_table(
        spark=spark,
        db_name="silver",
        table_name="national_risk"
    )

    upsert_to_silver_layer(
        spark=spark,
        deduplicated_df=silver_ready_df,
        table_name="national_risk"
    )

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)