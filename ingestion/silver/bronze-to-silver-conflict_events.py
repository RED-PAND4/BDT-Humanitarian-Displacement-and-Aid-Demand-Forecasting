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
    spark = get_spark_session("BronzeToSilver-ConflictEvents")

    # ==========================================
    # PHASE 2: Read Data from Bronze
    # ==========================================
    bronze_df = (
        spark.read
        .format("delta")
        .load("s3a://lakehouse/bronze/conflict_events")
        .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # Process only data ingested today
    )

    # ==========================================
    # PHASE 3: Clean Missing Critical Values
    # ==========================================
    # Drop records missing critical location identifier or event type classification
    cleaned_df = bronze_df.dropna(subset=["location_code", "event_type"])

    # ==========================================
    # PHASE 4: Clean and Deduplicate Data
    # ==========================================
    # Business keys identifying a unique observational event:
    subset_cols = [
        "location_code",
        "admin1_code",
        "admin2_code",
        "admin_level",
        # "resource_hdx_id",       # Excluded: technical metadata/file ID, not part of the business event
        "event_type",
        # "events",                # Excluded: numerical metric subject to retroactive revisions
        # "fatalities",            # Excluded: numerical metric subject to retroactive revisions
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
        table_name="conflict_events"
    )

    upsert_to_silver_layer(
        spark=spark,
        deduplicated_df=silver_ready_df,
        table_name="conflict_events"
    )

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)