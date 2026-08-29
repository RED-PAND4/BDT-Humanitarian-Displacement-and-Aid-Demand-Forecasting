import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql import functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import *

if __name__ == "__main__":
    # ==========================================
    # PHASE 1: Initialize Spark Session
    # ==========================================
    spark = get_spark_session("BronzeToSilver-Sector")

    # ==========================================
    # PHASE 2: Read Data from Bronze
    # ==========================================
    bronze_df = (
        spark.read
        .format("delta")
        .load("s3a://lakehouse/bronze/sector")
        # .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # Process only data ingested today
    )

    # ==========================================
    # PHASE 3: Clean Missing Critical Values
    # ==========================================
    # Drop records missing the primary sector code
    cleaned_df = bronze_df.dropna(subset=["code"])

    # ==========================================
    # PHASE 4: Clean and Deduplicate Data
    # ==========================================
    # Business key identifying a unique sector taxonomy entry:
    subset_cols = [
        "code"
        # "name"  # Excluded: descriptive name subject to minor wording updates
    ]
    deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)

    # ==========================================
    # PHASE 5: Initialize and Upsert to Silver
    # ==========================================
    initialize_delta_table(
        spark=spark,
        db_name="silver",
        table_name="sector"
    )

    upsert_to_silver_layer(
        spark=spark,
        deduplicated_df=deduplicated_df,
        table_name="sector"
    )

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)