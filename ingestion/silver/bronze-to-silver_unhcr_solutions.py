import sys
import os
from pyspark.sql.functions import col, when, lit
from pyspark.sql import functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import *

if __name__ == "__main__":
    # ==========================================
    # PHASE 1: Initialize Spark Session
    # ==========================================
    spark = get_spark_session("BronzeToSilver-UNHCR-Solutions")

    # ==========================================
    # PHASE 2: Read Data from Bronze
    # ==========================================
    bronze_df = (
        spark.read
        .format("delta")
        .load("s3a://lakehouse/bronze/solutions")
        # .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # Process only data ingested today
    )

    # ==========================================
    # PHASE 3: Sanitize Special Characters and Clean Missing Values
    # ==========================================
    # Metric columns that may contain "-" as a placeholder for null/confidential values
    solution_metrics = [
        "returned_refugees",
        "resettlement",
        "naturalisation",
        "returned_idps"
    ]

    cleaned_df = bronze_df
    for metric in solution_metrics:
        cleaned_df = cleaned_df.withColumn(
            metric,
            when(col(metric) == "-", lit(None).cast("integer"))
            .otherwise(col(metric).cast("integer"))
        )

    # Drop records missing critical origin, destination, or temporal keys
    cleaned_df = cleaned_df.dropna(subset=["year", "coo_iso", "coa_iso"])

    # ==========================================
    # PHASE 4: Clean and Deduplicate Data
    # ==========================================
    # Business keys identifying unique annual origin-destination solutions:
    subset_cols = [
        "year",
        "coo_iso",
        "coa_iso"
        # Descriptive names and numeric metrics are excluded to allow retroactive updates
    ]
    deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)

    # ==========================================
    # PHASE 5: Initialize and Upsert to Silver
    # ==========================================
    initialize_delta_table(
        spark=spark,
        db_name="silver",
        table_name="solutions"
    )

    upsert_to_silver_layer(
        spark=spark,
        deduplicated_df=deduplicated_df,
        table_name="solutions"
    )

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)