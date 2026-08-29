import sys
import os
from pyspark.sql.functions import col, to_date
from pyspark.sql import functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import *

if __name__ == "__main__":
    # ==========================================
    # PHASE 1: Initialize Spark Session
    # ==========================================
    spark = get_spark_session("BronzeToSilver-WorldBank-ExtremePoverty")

    # ==========================================
    # PHASE 2: Read Data from Bronze
    # ==========================================
    bronze_df = (
        spark.read
        .format("delta")
        .load("s3a://lakehouse/bronze/worldbank_extreme_poverty")
        # .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # Process only data ingested today
    )

    # ==========================================
    # PHASE 3: Clean Missing Critical Values
    # ==========================================
    # Drop rows missing essential identifiers or metrics
    cleaned_df = bronze_df.dropna(subset=["location_code", "year", "extreme_poverty_value"])

    # ==========================================
    # PHASE 4: Clean and Deduplicate Data
    # ==========================================
    # Business keys identifying unique annual World Bank extreme poverty indicators:
    subset_cols = [
        "location_code",
        "year"
        # "extreme_poverty_value"  # Excluded: numeric metric subject to retroactive adjustments
    ]
    deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)

    # ==========================================
    # PHASE 5: Initialize and Upsert to Silver
    # ==========================================
    initialize_delta_table(
        spark=spark,
        db_name="silver",
        table_name="worldbank_extreme_poverty"
    )

    upsert_to_silver_layer(
        spark=spark,
        deduplicated_df=deduplicated_df,
        table_name="worldbank_extreme_poverty"
    )

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)