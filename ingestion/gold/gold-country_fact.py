"""
===============================================================================
GOLD LAYER — Country Fact Master Panel (location_code, year)
===============================================================================
Purpose:
This script consolidates all domain-specific Gold feature tables and core 
socio-economic Silver dimensions into a unified, multidimensional panel dataset 
at the country-year granularity (location_code, year).

Upstream Sources:
  - gold.gold_conflict_features       : Political violence, civilian targeting, demonstrations
  - gold.gold_poverty_features        : Multidimensional poverty (HDX MPI, WB MPM, Extreme Poverty)
  - gold.gold_food_security_features  : Peak population in IPC Phase 3+ (Crisis/Emergency/Famine)
  - gold.gold_funding_features        : Requirements, funding received, coverage ratios
  - silver.worldbank_population       : Total national population
  - silver.worldbank_gdp              : GDP per capita (constant USD)
  - silver.location                   : Master country taxonomy and temporal HRP/GHO coverage

Design Principles:
1. Non-Destructive Skeleton:
   Uses FULL OUTER JOIN across all feature tables so that no country-year observation 
   is dropped if it exists in at least one analytical domain.
2. Semantic Missing Value Handling:
   - Conflict metrics -> Imputed with 0 (absence of recorded conflict events represents peace/zero events).
   - Poverty, Food Security, Funding, GDP, Population -> Preserved as NULL (absence represents unmeasured data).
3. Temporal HRP/GHO Logic:
   Evaluates whether a Humanitarian Response Plan (HRP) or Global Humanitarian Overview (GHO)
   was active for a given country relative to the baseline effective year recorded in master data.
4. Metadata & Naming Continuity:
   Preserves standardized country names (location_name) alongside ISO3 codes for direct reporting in dashboards.
===============================================================================
"""

import sys
import os
import pyspark.sql.functions as F
from pyspark.sql.window import Window

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

if __name__ == "__main__":
    # ==========================================
    # PHASE 1: Initialize Spark Session
    # ==========================================
    spark = get_spark_session("Gold-CountryFact")

    # ==========================================
    # PHASE 2: Read Gold Feature Tables and Silver Dimensions
    # ==========================================
    print("Reading upstream Gold features and Silver dimension tables...")
    conflict_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_conflict_features")
    poverty_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_poverty_features")
    food_security_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_food_security_features")
    funding_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_funding_features")
    
    wb_population_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_population")
    wb_gdp_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_gdp")
    location_df = spark.read.format("delta").load("s3a://lakehouse/silver/location")

    # ==========================================
    # PHASE 3: Multi-Source Full Outer Join (Skeleton Panel)
    # ==========================================
    # Join all domain metrics at (location_code, year) resolution.
    # Full outer join guarantees no country-year is dropped if present in any domain.
    print("Consolidating domain metrics into master panel skeleton...")
    country_fact = (
        conflict_df
        .join(poverty_df, ["location_code", "year"], "outer")
        .join(food_security_df, ["location_code", "year"], "outer")
        .join(funding_df, ["location_code", "year"], "outer")
        .join(
            wb_population_df.select("location_code", "year", "total_population"),
            ["location_code", "year"],
            "outer"
        )
        .join(
            wb_gdp_df.select("location_code", "year", "gdp_per_capita"),
            ["location_code", "year"],
            "outer"
        )
    )

    # ==========================================
    # PHASE 4: Handle Missing Values
    # ==========================================
    # Conflict absence represents true zero events/fatalities.
    # Socio-economic, funding, and food security absences remain NULL (unobserved).
    conflict_cols = [
        "total_fatalities",
        "violent_events",
        "civilian_targeting_events",
        "civilian_targeting_fatalities",
        "non_violent_events"
    ]
    country_fact = country_fact.fillna(0, subset=conflict_cols)

    # ==========================================
    # PHASE 5: Integrate Country Metadata & Temporal HRP/GHO Flags
    # ==========================================
    # The location dimension contains baseline years from which HRP/GHO coverage began.
    # For each panel year:
    #   - year < hrp_effective_year -> False (coverage had not started yet)
    #   - year >= hrp_effective_year -> Recorded boolean status
    #   - Country missing in location table -> False
    location_clean = (
        location_df
        .select(
            F.col("code").alias("location_code"),
            F.col("name").alias("location_name"),
            F.col("has_hrp").alias("has_hrp_recorded"),
            F.col("in_gho").alias("in_gho_recorded"),
            F.col("year").alias("hrp_effective_year")
        )
        .dropDuplicates(["location_code"])
    )

    country_fact = country_fact.join(location_clean, "location_code", "left")

    country_fact = (
        country_fact
        .withColumn(
            "has_hrp",
            F.when(F.col("hrp_effective_year").isNull(), F.lit(False))
             .when(F.col("year") < F.col("hrp_effective_year"), F.lit(False))
             .otherwise(F.coalesce(F.col("has_hrp_recorded"), F.lit(False)))
        )
        .withColumn(
            "in_gho",
            F.when(F.col("hrp_effective_year").isNull(), F.lit(False))
             .when(F.col("year") < F.col("hrp_effective_year"), F.lit(False))
             .otherwise(F.coalesce(F.col("in_gho_recorded"), F.lit(False)))
        )
        .drop("has_hrp_recorded", "in_gho_recorded", "hrp_effective_year")
    )

    # ==========================================
    # PHASE 6: Compute/Align Panel-Level Lag Features
    # ==========================================
    # Ensure temporal lag alignment across the full unified panel
    window_by_country = Window.partitionBy("location_code").orderBy("year")
    country_fact = country_fact.withColumn(
        "funding_coverage_lag1",
        F.lag("funding_coverage_pct", 1).over(window_by_country)
    )

    # ==========================================
    # PHASE 7: Select Schema and Save to Gold Layer
    # ==========================================
    gold_country_fact = country_fact.select(
        # Identifiers & Metadata
        "location_code",
        "location_name",
        "year",
        # Conflict Features
        "total_fatalities",
        "violent_events",
        "civilian_targeting_events",
        "civilian_targeting_fatalities",
        "non_violent_events",
        # Poverty Indicators (HDX & World Bank)
        "mpi",
        "hdx_head",
        "hdx_vuln",
        "hdx_sev",
        "mpm",
        "ext_pov",
        # Food Security
        "peak_population_phase3plus",
        # Funding Metrics (OCHA FTS)
        "funding_received_usd",
        "requirements_usd",
        "funding_coverage_pct",
        "funding_coverage_lag1",
        # Socio-Economic Baseline (World Bank)
        "total_population",
        "gdp_per_capita",
        # Systemic Humanitarian Coverage Flags
        "has_hrp",
        "in_gho"
    )

    initialize_delta_table(
        spark=spark,
        db_name="gold",
        table_name="gold_country_fact"
    )

    print("Writing gold_country_fact to Delta Lake...")
    (
        gold_country_fact.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://lakehouse/gold/gold_country_fact")
    )
    print("Table gold_country_fact successfully generated.")

    # ==========================================
    # PHASE 8: Diagnostics & Data Coverage Report
    # ==========================================
    total_rows = gold_country_fact.count()
    print(f"\nTotal rows in gold_country_fact panel: {total_rows}")
    
    coverage_exprs = [
        F.round(F.sum(F.when(F.col(c).isNotNull(), 1).otherwise(0)) / total_rows * 100, 1).alias(c)
        for c in gold_country_fact.columns if c not in ("location_code", "location_name", "year")
    ]
    print("Feature data coverage percentage across all panel rows:")
    gold_country_fact.select(coverage_exprs).show(vertical=True, truncate=False)

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)