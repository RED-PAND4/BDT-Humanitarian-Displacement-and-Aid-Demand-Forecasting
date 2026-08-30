"""
===============================================================================
GOLD LAYER — Host Pressure Indices (coa_iso, year)
===============================================================================
Purpose:
This script combines host country displacement totals (gold.gold_host_aggregates) 
with multidimensional socio-economic context (gold.gold_country_fact) to compute 
structural host pressure indicators and categorical risk tiers:

Indicators Computed:
  1. pressure_per_capita         = total_hosted_stock / total_population
  2. pressure_per_gdp_per_capita = total_hosted_stock / gdp_per_capita
  3. pressure_per_gdp_log_index  = Log-normalized economic pressure (0 - 100 scale)
  4. growth_rate                 = passed from gold_host_aggregates (momentum)
  5. funding_gap                 = 1 - (funding_received / requirements), 
                                   computed strictly when requirements > 0

Analytical Features & Risk Tiers (All Fixed Normative Thresholds):
  - cross_border_stock: Sum of refugees, asylum seekers, and OIP. Differentiates 
    cross-border influx from internal displacement (IDPs).
  - Demographic Tier (pressure_per_capita_tier):
    Categorized using fixed population ratios (<0.5% Low, 0.5%-2% Medium, 
    2%-5% High, >5% Critical).
  - Economic Tier (pressure_per_gdp_per_capita_tier):
    Categorized using fixed order-of-magnitude ratios (<10 Low, 10-100 Medium, 
    100-500 High, >500 Critical).
  - Funding Tier (funding_gap_tier):
    Categorized using fixed normative thresholds (<=0.3 Low, <=0.5 Medium, 
    <=0.7 High, >0.7 Critical).
  - is_forecast: Set to False for all historical records. Serves as partition/filter 
    scaffolding for subsequent ML prediction scripts.
===============================================================================
"""

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
    spark = get_spark_session("Gold-HostPressureIndices")

    # ==========================================
    # PHASE 2: Read Upstream Gold Datasets
    # ==========================================
    print("Reading gold_host_aggregates and gold_country_fact tables...")
    host_aggregates_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_host_aggregates")
    country_fact_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_country_fact")

    # ==========================================
    # PHASE 3: Join Host Aggregates with National Context
    # ==========================================
    panel_df = host_aggregates_df.join(
        country_fact_df.withColumnRenamed("location_code", "coa_iso"),
        ["coa_iso", "year"],
        "left"
    )

    if "location_name" in panel_df.columns:
        panel_df = (
            panel_df
            .withColumn("coa_name", F.coalesce(F.col("coa_name"), F.col("location_name")))
            .drop("location_name")
        )

    # ==========================================
    # PHASE 4: Compute Normalized Pressure Metrics
    # ==========================================
    # 1. Decompose cross-border stock from internal IDPs
    panel_df = panel_df.withColumn(
        "cross_border_stock",
        F.col("refugees_count") + F.col("asylum_seekers_count") + F.col("oip_count")
    )

    # 2. Compute demographic and economic pressure ratios
    panel_df = (
        panel_df
        .withColumn("pressure_per_capita", F.col("total_hosted_stock") / F.col("total_population"))
        .withColumn("pressure_per_gdp_per_capita", F.col("total_hosted_stock") / F.col("gdp_per_capita"))
    )

    # 3. Compute humanitarian funding gap
    panel_df = (
        panel_df
        .withColumn(
            "has_funding_data",
            F.col("funding_received_usd").isNotNull() | F.col("requirements_usd").isNotNull()
        )
        .withColumn(
            "funding_gap",
            F.when(
                F.col("requirements_usd").isNotNull() & (F.col("requirements_usd") > 0),
                1 - (F.coalesce(F.col("funding_received_usd"), F.lit(0.0)) / F.col("requirements_usd"))
            ).otherwise(F.lit(None).cast("double"))
        )
    )

    # 4. Log-normalized economic pressure index (0 - 100 scale)
    panel_df = panel_df.withColumn(
        "pressure_per_gdp_log_index",
        F.when(F.col("pressure_per_gdp_per_capita").isNull(), F.lit(None).cast("double"))
         .otherwise(
             F.least(
                 F.lit(100.0),
                 F.round(F.lit(25.0) * F.log10(F.lit(1.0) + F.col("pressure_per_gdp_per_capita")), 2)
             )
         )
    )

    # ==========================================
    # PHASE 5: Compute Risk Tiers (Fixed Normative Thresholds)
    # ==========================================
    # 1. Demographic pressure tier (<0.5% Low, 0.5%-2% Medium, 2%-5% High, >5% Critical)
    panel_df = panel_df.withColumn(
        "pressure_per_capita_tier",
        F.when(F.col("pressure_per_capita").isNull(), F.lit(None).cast("string"))
         .when(F.col("pressure_per_capita") < 0.005, "Low")
         .when(F.col("pressure_per_capita") < 0.02, "Medium")
         .when(F.col("pressure_per_capita") < 0.05, "High")
         .otherwise("Critical")
    )

    # 2. Economic pressure tier (<10 Low, 10-100 Medium, 100-500 High, >500 Critical)
    panel_df = panel_df.withColumn(
        "pressure_per_gdp_per_capita_tier",
        F.when(F.col("pressure_per_gdp_per_capita").isNull(), F.lit(None).cast("string"))
         .when(F.col("pressure_per_gdp_per_capita") < 10.0, "Low")
         .when(F.col("pressure_per_gdp_per_capita") < 100.0, "Medium")
         .when(F.col("pressure_per_gdp_per_capita") < 500.0, "High")
         .otherwise("Critical")
    )

    # 3. Humanitarian funding gap tier (<=30% Low, 30%-50% Medium, 50%-70% High, >70% Critical)
    panel_df = panel_df.withColumn(
        "funding_gap_tier",
        F.when(F.col("funding_gap").isNull(), F.lit(None).cast("string"))
         .when(F.col("funding_gap") <= 0.3, "Low")
         .when(F.col("funding_gap") <= 0.5, "Medium")
         .when(F.col("funding_gap") <= 0.7, "High")
         .otherwise("Critical")
    )

    # Scaffolding flag distinguishing historical data from future forecasts
    panel_df = panel_df.withColumn("is_forecast", F.lit(False))

    # ==========================================
    # PHASE 6: Select Final Schema & Save to Gold Layer
    # ==========================================
    gold_host_pressure_indices = panel_df.select(
        # Identifiers & Metadata
        "coa_iso",
        "coa_name",
        "year",
        "is_forecast",

        # Category Disaggregations
        "refugees_count",
        "asylum_seekers_count",
        "oip_count",
        "idps_count",
        "cross_border_stock",

        # Displacement Stock & Flow Volumes
        "total_hosted_stock",
        "hosted_stock_lag1",
        "total_inflows",
        "total_outflows",

        # Core Pressure Indicators & Tiers
        "growth_rate",
        "pressure_per_capita",
        "pressure_per_capita_tier",
        "pressure_per_gdp_per_capita",
        "pressure_per_gdp_log_index",
        "pressure_per_gdp_per_capita_tier",
        "has_funding_data",
        "funding_gap",
        "funding_gap_tier",

        # Macro Context Features
        "total_population",
        "gdp_per_capita",
        "funding_received_usd",
        "requirements_usd",
        "has_hrp",
        "in_gho",
        
        # Sparse Context Indicators (Informational)
        "mpi",
        "hdx_head",
        "hdx_vuln",
        "hdx_sev",
        "mpm",
        "ext_pov",
        "peak_population_phase3plus"
    )

    initialize_delta_table(
        spark=spark,
        db_name="gold",
        table_name="gold_host_pressure_indices"
    )

    print("Writing gold_host_pressure_indices to Delta Lake...")
    (
        gold_host_pressure_indices.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://lakehouse/gold/gold_host_pressure_indices")
    )

    print("Table gold_host_pressure_indices successfully generated.")

    # ==========================================
    # PHASE 7: Diagnostics & Quality Report
    # ==========================================
    total_rows = gold_host_pressure_indices.count()
    print(f"\nTotal rows in gold_host_pressure_indices: {total_rows}")
    
    print("\nDistribution of pressure_per_capita_tier:")
    gold_host_pressure_indices.groupBy("pressure_per_capita_tier").count().orderBy(F.desc("count")).show()

    print("\nDistribution of pressure_per_gdp_per_capita_tier:")
    gold_host_pressure_indices.groupBy("pressure_per_gdp_per_capita_tier").count().orderBy(F.desc("count")).show()
    
    print("\nDistribution of funding_gap_tier:")
    gold_host_pressure_indices.groupBy("funding_gap_tier").count().orderBy(F.desc("count")).show()

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)