from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message



# Initialize Spark with a local optimization profile
spark = get_spark_session("Gold-Dashboard-Pipeline-Local")

# Optimize execution parameters for personal computers (Low resource consumption)
spark.conf.set("spark.sql.adaptive.enabled", "true")

def extract_clean_year_and_iso(df, iso_col, date_or_year_col):
    """
    Defensive helper to standardize join keys.
    Extracts a 4-digit integer year from strings, timestamps, or full dates.
    Normalizes country ISO codes to trimmed uppercase.
    """
    # Force uppercase ISO code and clear trailing spaces
    df = df.withColumn("location_code", F.upper(F.trim(F.col(iso_col).cast("string"))))
    
    # Cast target column to string and extract the first 4 numerical characters (handles '2024' and '2024-12-31')
    year_str = F.substring(F.col(date_or_year_col).cast("string"), 1, 4)
    df = df.withColumn("year", year_str.cast(IntegerType()))
    
    # Drop rows that failed normalization to maintain referential integrity
    return df.dropna(subset=["location_code", "year"])

def build_gold_layer():
    silver_base = "s3a://lakehouse/silver"
    gold_base = "s3a://lakehouse/gold"
    
    print("Step 1: Reading and aggregating Silver Layer tables...")
    
    # --- 1. UNHCR POPULATION / DISPLACEMENT (Using COA as target location) ---
    unhcr_raw = spark.read.format("delta").load(f"{silver_base}/population")
    unhcr_clean = extract_clean_year_and_iso(unhcr_raw, "coa_iso", "year")
    unhcr_agg = unhcr_clean.groupBy("location_code", "year").agg(
        F.sum("refugees").alias("refugees_count"),
        F.sum("asylum_seekers").alias("asylum_seekers_count")
    )

    # --- 2. HDX IDPS ---
    idps_raw = spark.read.format("delta").load(f"{silver_base}/idps")
    # Assuming reference_period_start or reporting_round tracking year
    idps_clean = extract_clean_year_and_iso(idps_raw, "location_code", "reporting_round") 
    idps_agg = idps_clean.groupBy("location_code", "year").agg(
        F.sum("population").alias("internal_displaced_count")
    )

    # --- 3. CONFLICT EVENTS ---
    conflict_raw = spark.read.format("delta").load(f"{silver_base}/conflict_events")
    conflict_clean = extract_clean_year_and_iso(conflict_raw, "location_code", "reference_period_start")
    conflict_agg = conflict_clean.groupBy("location_code", "year").agg(
        F.sum("events").alias("conflict_events_total"),
        F.sum("fatalities").alias("conflict_fatalities_total")
    )

    # --- 4. HUMANITARIAN FUNDING ---
    funding_raw = spark.read.format("delta").load(f"{silver_base}/funding")
    funding_clean = extract_clean_year_and_iso(funding_raw, "location_code", "reference_period_start")
    funding_agg = funding_clean.groupBy("location_code", "year").agg(
        F.sum("requirements_usd").alias("total_required_usd"),
        F.sum("funding_usd").alias("total_received_usd")
    )

    # --- 5. FOOD SECURITY / IPC PHASES ---
    food_sec_raw = spark.read.format("delta").load(f"{silver_base}/foodsecurity")
    food_sec_clean = extract_clean_year_and_iso(food_sec_raw, "location_code", "resource_hdx_id") # Adjust date tracker column if needed
    food_sec_agg = food_sec_clean.groupBy("location_code", "year").agg(
        F.avg(F.coalesce(F.col("ipc_phase").cast("double"), F.lit(1.0))).alias("avg_food_insecurity_phase")
    )

    # --- 6. BASELINE POPULATION ---
    pop_raw = spark.read.format("delta").load(f"{silver_base}/baselinepopulation")
    pop_clean = extract_clean_year_and_iso(pop_raw, "location_code", "resource_hdx_id")
    pop_agg = pop_clean.groupBy("location_code", "year").agg(
        F.sum("population").alias("total_host_population")
    )

    # --- 7. POVERTY RATE ---
    poverty_raw = spark.read.format("delta").load(f"{silver_base}/povertyrate")
    poverty_clean = extract_clean_year_and_iso(poverty_raw, "location_code", "resource_hdx_id")
    poverty_agg = poverty_clean.groupBy("location_code", "year").agg(
        F.avg("mpi").alias("multidimensional_poverty_index")
    )

    # --- 8. NATIONAL RISK ---
    risk_raw = spark.read.format("delta").load(f"{silver_base}/national_risk")
    risk_clean = extract_clean_year_and_iso(risk_raw, "location_code", "reference_period_start")
    risk_agg = risk_clean.groupBy("location_code", "year").agg(
        F.avg("overall_risk").alias("baseline_national_risk_score")
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.dim_country_risk_profile
        USING delta
        LOCATION 's3a://lakehouse/gold/dim_country_risk_profile'
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.fact_crisis_response_matrix
        USING delta
        LOCATION 's3a://lakehouse/gold/fact_crisis_response_matrix'
    """)

    print("Step 2: Consolidating and saving Dimension Profile (Storage Optimized)...")
    # Combine baseline systemic indicators into the Dimension table
    dim_country_profile = pop_agg \
        .join(poverty_agg, ["location_code", "year"], "outer") \
        .join(risk_agg, ["location_code", "year"], "outer") \
        .fillna(0.0)
        
    dim_country_profile.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{gold_base}/dim_country_risk_profile")

    print("Step 3: Engineering Fact Crisis Matrix & Community Calculations...")
    # Outer join operational metrics to avoid losing edge records
    fact_matrix = unhcr_agg \
        .join(idps_agg, ["location_code", "year"], "outer") \
        .join(conflict_agg, ["location_code", "year"], "outer") \
        .join(funding_agg, ["location_code", "year"], "outer") \
        .join(food_sec_agg, ["location_code", "year"], "outer") \
        .fillna(0) # Zero out null values safely for math functions

    # Inject population baseline from dimension to run relational analytics
    fact_matrix = fact_matrix.join(dim_country_profile.select("location_code", "year", "total_host_population", "baseline_national_risk_score"), ["location_code", "year"], "left").fillna(1)

    # --- COMMUNITY RELEVANT FORMULAS & ANALYTICS ---
    # A. Total Displacement Burden Calculation
    fact_matrix = fact_matrix.withColumn(
        "total_displaced_burden",
        F.col("refugees_count") + F.col("asylum_seekers_count") + F.col("internal_displaced_count")
    )

    # B. Funding Coverage Percentage (Defensive Division)
    fact_matrix = fact_matrix.withColumn(
        "funding_coverage_pct",
        F.when(F.col("total_required_usd") > 0, (F.col("total_received_usd") / F.col("total_required_usd")) * 100)
         .otherwise(0.0)
    )

    # C. Severity Index Modeling (Combining Conflict, Displacement Intensity, and Food Constraints)
    # This standard calculation yields a unified scale from 0 to 100 perfect for Superset heatmaps
    # fact_matrix = fact_matrix.withColumn(
    #     "humanitarian_priority_index",
    #     F.clip(
    #         (F.col("conflict_events_total") * 0.4) + 
    #         ((F.col("total_displaced_burden") / F.col("total_host_population")) * 100 * 0.4) + 
    #         (F.col("avg_food_insecurity_phase") * 10 * 0.2), 
    #         0.0, 100.0
    #     )
    # )
    raw_hpi_score = (
        (F.col("conflict_events_total") * 0.4) + 
        ((F.col("total_displaced_burden") / F.col("total_host_population")) * 100 * 0.4) + 
        (F.col("avg_food_insecurity_phase") * 10 * 0.2)
    )

    # Clip safely between 0.0 and 100.0 using native PySpark F.least and F.greatest
    fact_matrix = fact_matrix.withColumn(
        "humanitarian_priority_index",
        F.least(F.greatest(raw_hpi_score, F.lit(0.0)), F.lit(100.0))
    )

    # D. Trajectory Metrics (Year-over-Year tracking using Window functions)
    window_spec = Window.partitionBy("location_code").orderBy("year")
    fact_matrix = fact_matrix.withColumn(
        "previous_year_displacement", 
        F.lag("total_displaced_burden", 1).over(window_spec)
    ).fillna(0)
    
    fact_matrix = fact_matrix.withColumn(
        "displacement_growth_trajectory",
        F.when(F.col("previous_year_displacement") > 0, 
               ((F.col("total_displaced_burden") - F.col("previous_year_displacement")) / F.col("previous_year_displacement")) * 100)
         .otherwise(0.0)
    )

    print("Step 4: Writing Master Fact Table to Gold Database...")
    fact_matrix.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{gold_base}/fact_crisis_response_matrix")
        
    print("Gold Layer generation finalized successfully.")

if __name__ == "__main__":
    build_gold_layer()