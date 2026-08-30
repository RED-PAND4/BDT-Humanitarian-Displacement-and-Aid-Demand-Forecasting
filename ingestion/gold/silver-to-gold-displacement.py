"""
===============================================================================
GOLD LAYER — Population Displacement & Host Aggregates Pipeline
===============================================================================
Purpose:
This script consolidates UNHCR population stocks and durable solutions into 
two foundational Gold layer tables:

1. gold.gold_displacement (Route-Level Matrix):
   - Granularity: (year, coo_iso, coo_name, coa_iso, coa_name)
   - Calculates annual displaced stocks, tracked outflows, and estimated net inflows 
     for each bilateral origin-destination corridor.
   - Preserves internal displacement records (coo_iso == coa_iso, IDPs).

2. gold.gold_host_aggregates (Total Territorial Host Panel):
   - Granularity: (coa_iso, coa_name, year)
   - Aggregates all incoming cross-border populations and internal IDPs to measure 
     the overall territorial hosting burden per country-year.
   - Generates a native dense temporal grid (sequence + explode) covering the full 
     historical time range. This ensures structural continuity and prevents 
     window function skipping during lag and growth rate (momentum) calculations.
   - Preserves ISO3 codes alongside standardized country names.
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
    spark = get_spark_session("SilverToGold-Displacement-And-HostAggregates")

    # ==========================================
    # PHASE 2: Read Silver Layer Tables
    # ==========================================
    print("1. Reading Silver tables...")
    pop_df = (
        spark.read
        .format("delta")
        .load("s3a://lakehouse/silver/population")
    )
    sol_df = (
        spark.read
        .format("delta")
        .load("s3a://lakehouse/silver/solutions")
    )

    # ==========================================
    # PHASE 3: Aggregate Route Stocks (Population)
    # ==========================================
    print("2. Aggregating stocks per origin-destination route...")
    stock_agg = pop_df.groupBy("year", "coo_iso", "coa_iso").agg(
        F.first("coo_name", ignorenulls=True).alias("coo_name"),
        F.first("coa_name", ignorenulls=True).alias("coa_name"),
        F.sum("refugees").alias("refugees"),
        F.sum("asylum_seekers").alias("asylum_seekers"),
        F.sum("oip").alias("oip"),
        F.sum("idps").alias("idps")
    )

    # ==========================================
    # PHASE 4: Aggregate Route Outflows (Solutions)
    # ==========================================
    print("3. Aggregating outflows per origin-destination route...")
    outflows_agg = sol_df.groupBy("year", "coo_iso", "coa_iso").agg(
        F.first("coo_name", ignorenulls=True).alias("coo_name_sol"),
        F.first("coa_name", ignorenulls=True).alias("coa_name_sol"),
        F.sum("returned_refugees").alias("returned_refugees"),
        F.sum("resettlement").alias("resettlement"),
        F.sum("naturalisation").alias("naturalisation"),
        F.sum("returned_idps").alias("returned_idps")
    )

    # ==========================================
    # PHASE 5: Join, Sanitize & Compute Route Metrics (gold_displacement)
    # ==========================================
    print("4. Building gold_displacement (Route Level Matrix)...")
    route_joined = stock_agg.join(
        outflows_agg,
        ["year", "coo_iso", "coa_iso"],
        "full_outer"
    )

    # Coalesce country names across both sources
    route_df = (
        route_joined
        .withColumn("coo_name", F.coalesce(F.col("coo_name"), F.col("coo_name_sol")))
        .withColumn("coa_name", F.coalesce(F.col("coa_name"), F.col("coa_name_sol")))
        .drop("coo_name_sol", "coa_name_sol")
    )

    # Replace nulls with 0 for all metric columns
    metric_cols = [
        "refugees", "asylum_seekers", "oip", "idps",
        "returned_refugees", "resettlement", "naturalisation", "returned_idps"
    ]
    route_df = route_df.fillna(0, subset=metric_cols)

    # 1. Total stock currently displaced/hosted on this route
    route_df = route_df.withColumn(
        "stock",
        F.col("refugees") + F.col("asylum_seekers") + F.col("oip") + F.col("idps")
    )

    # 2. Total tracked outflows on this route
    route_df = route_df.withColumn(
        "outflows",
        F.col("returned_refugees") + F.col("resettlement") + F.col("naturalisation") + F.col("returned_idps")
    )

# 3. Calculate lag stock and estimated new inflows per route
    window_route = Window.partitionBy("coo_iso", "coa_iso").orderBy("year")
    route_df = route_df.withColumn("stock_lag1", F.lag("stock", 1).over(window_route))

    # -------------------------------------------------------------------------
    # VARIANT A (Active): Pure algebraic balance
    # Preserves negative values when undocumented departures/corrections exceed arrivals.
    # Matches original baseline logic and guarantees diff_inflows == 0.
    # -------------------------------------------------------------------------
    route_df = route_df.withColumn(
        "inflows",
        F.when(F.col("stock_lag1").isNull(), F.lit(None).cast("long"))
        .otherwise((F.col("stock") - F.col("stock_lag1")) + F.col("outflows"))
    )

    # -------------------------------------------------------------------------
    # VARIANT B (Alternative): Strictly non-negative gross arrivals proxy
    # Floors balance at 0 so contracting routes do not offset real arrivals on other corridors.
    # To enable: uncomment this block and comment out VARIANT A above.
    # -------------------------------------------------------------------------
    # route_df = route_df.withColumn(
    #     "inflows",
    #     F.when(F.col("stock_lag1").isNull(), F.lit(None).cast("long"))
    #     .otherwise(
    #         F.greatest(
    #             F.lit(0),
    #             (F.col("stock") - F.col("stock_lag1")) + F.col("outflows")
    #         )
    #     )
    # )

    route_df = route_df.drop("stock_lag1")

    # Select final schema for gold_displacement
    gold_displacement = route_df.select(
        "year",
        "coo_iso",
        "coo_name",
        "coa_iso",
        "coa_name",
        "refugees",
        "asylum_seekers",
        "oip",
        "idps",
        "returned_refugees",
        "resettlement",
        "naturalisation",
        "returned_idps",
        "stock",
        "inflows",
        "outflows"
    )

    # Write Table 1: gold_displacement
    initialize_delta_table(spark=spark, db_name="gold", table_name="gold_displacement")
    print("Writing gold_displacement to Delta Lake...")
    (
        gold_displacement.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://lakehouse/gold/gold_displacement")
    )
    print("Table gold_displacement successfully saved.")

    # ==========================================
    # PHASE 6: Aggregate by Host Country (gold_host_aggregates)
    # ==========================================
    print("5. Aggregating total territorial pressure per host country...")
    host_sums = gold_displacement.groupBy("coa_iso", "year").agg(
        F.first("coa_name", ignorenulls=True).alias("coa_name"),
        F.sum("refugees").alias("refugees_count"),
        F.sum("asylum_seekers").alias("asylum_seekers_count"),
        F.sum("oip").alias("oip_count"),
        F.sum("idps").alias("idps_count"),
        F.sum("stock").alias("total_hosted_stock"),
        F.sum("inflows").alias("total_inflows"),
        F.sum("outflows").alias("total_outflows")
    )

    # Create country name reference lookup to preserve names during dense grid join
    host_name_lookup = (
        host_sums
        .select("coa_iso", "coa_name")
        .filter(F.col("coa_name").isNotNull())
        .dropDuplicates(["coa_iso"])
    )

    # ==========================================
    # PHASE 7: Generate Dense Temporal Grid (Sequence + Explode)
    # ==========================================
    # Extract global min and max years across the dataset
    year_bounds = gold_displacement.agg(
        F.min("year").cast("integer").alias("min_year"),
        F.max("year").cast("integer").alias("max_year")
    )

    distinct_hosts = host_name_lookup.select("coa_iso", "coa_name")

    # Cross join distinct host countries with full continuous annual sequence
    dense_grid = (
        distinct_hosts
        .crossJoin(F.broadcast(year_bounds))
        .withColumn("year_array", F.expr("sequence(min_year, max_year, 1)"))
        .withColumn("year", F.explode("year_array"))
        .drop("min_year", "max_year", "year_array")
    )

    # ==========================================
    # PHASE 8: Join Grid, Fill Zeros & Calculate Growth Rate
    # ==========================================
    host_panel = (
        dense_grid
        .join(
            host_sums.drop("coa_name"),
            ["coa_iso", "year"],
            "left"
        )
        .fillna(0, subset=[
            "refugees_count", "asylum_seekers_count", "oip_count", "idps_count",
            "total_hosted_stock", "total_inflows", "total_outflows"
        ])
    )

    window_host = Window.partitionBy("coa_iso").orderBy("year")
    host_panel = host_panel.withColumn(
        "hosted_stock_lag1",
        F.lag("total_hosted_stock", 1).over(window_host)
    )

    # Calculate growth rate (momentum) handling initial hosting years and zero bases
    host_panel = host_panel.withColumn(
        "growth_rate",
        F.when(F.col("hosted_stock_lag1").isNull(), F.lit(None).cast("double"))
        .when((F.col("hosted_stock_lag1") == 0) & (F.col("total_hosted_stock") == 0), F.lit(0.0))
        .when((F.col("hosted_stock_lag1") == 0) & (F.col("total_hosted_stock") > 0), F.lit(None).cast("double"))
        .otherwise(
            (F.col("total_hosted_stock") - F.col("hosted_stock_lag1")) / F.col("hosted_stock_lag1")
        )
    )

    # Select final schema for gold_host_aggregates
    gold_host_aggregates = host_panel.select(
        "coa_iso",
        "coa_name",
        "year",
        "refugees_count",
        "asylum_seekers_count",
        "oip_count",
        "idps_count",
        "total_hosted_stock",
        "hosted_stock_lag1",
        "total_inflows",
        "total_outflows",
        "growth_rate"
    )

    # Write Table 2: gold_host_aggregates
    initialize_delta_table(spark=spark, db_name="gold", table_name="gold_host_aggregates")
    print("Writing gold_host_aggregates to Delta Lake...")
    (
        gold_host_aggregates.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("s3a://lakehouse/gold/gold_host_aggregates")
    )
    print("Table gold_host_aggregates successfully saved.")

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)