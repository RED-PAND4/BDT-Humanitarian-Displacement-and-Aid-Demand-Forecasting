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
  3. growth_rate                 = passed from gold_host_aggregates (momentum)
  4. funding_gap                 = 1 - (funding_received / requirements), 
                                   computed strictly when requirements > 0

Analytical Features & Risk Tiers:
  - cross_border_stock: Sum of refugees, asylum seekers, and OIP. Differentiates 
    cross-border influx from internal displacement (IDPs).
  - Quantile Tiers (pressure_per_capita_tier, pressure_per_gdp_per_capita_tier):
    Computed per calendar year using 4 quantiles (ntile) among relevant hosts 
    (stock >= 100). Sub-threshold hosts are assigned 'Low'; missing values 
    remain NULL.
  - Fixed Tiers (funding_gap_tier):
    Categorized using fixed normative thresholds (<=0.3 Low, <=0.5 Medium, 
    <=0.7 High, >0.7 Critical).
  - is_forecast: Set to False for all historical records. Serves as partition/filter 
    scaffolding for subsequent ML prediction scripts.
===============================================================================
"""

import sys
import os
import pyspark.sql.functions as F
from pyspark.sql.window import Window

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

RELEVANT_HOST_THRESHOLD = 100  # Minimum total stock threshold to enter annual quantile ranking


def add_quantile_tier(df, value_col, tier_col_name):
    """
    Computes annual quantile-based risk tiers (Low, Medium, High, Critical) via ntile(4)
    for host countries meeting the relevance threshold (current or lag stock >= 100).
    
    Rules:
      - Sub-threshold hosts -> 'Low' (known low-pressure state).
      - Missing indicator values (e.g., missing GDP or Population) -> NULL (never coerced to Low).
    """
    relevant = (
        df.filter(
            (F.col("total_hosted_stock") >= RELEVANT_HOST_THRESHOLD) |
            (F.col("hosted_stock_lag1") >= RELEVANT_HOST_THRESHOLD)
        )
        .filter(F.col(value_col).isNotNull())
    )

    window_q = Window.partitionBy("year").orderBy(F.col(value_col).asc())
    relevant = relevant.withColumn(
        tier_col_name,
        F.when(F.ntile(4).over(window_q) == 1, "Low")
         .when(F.ntile(4).over(window_q) == 2, "Medium")
         .when(F.ntile(4).over(window_q) == 3, "High")
         .otherwise("Critical")
    ).select("coa_iso", "year", tier_col_name)

    df = df.join(relevant, ["coa_iso", "year"], "left")
    df = df.withColumn(
        tier_col_name,
        F.when(F.col(tier_col_name).isNotNull(), F.col(tier_col_name))
         .when(F.col(value_col).isNull(), F.lit(None).cast("string"))
         .otherwise(F.lit("Low"))
    )
    return df


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
    # Left join country-level context onto host country panel
    panel_df = host_aggregates_df.join(
        country_fact_df.withColumnRenamed("location_code", "coa_iso"),
        ["coa_iso", "year"],
        "left"
    )

    # Coalesce country name if missing in one of the sources
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

    # ==========================================
    # PHASE 5: Compute Risk Tiers (Quantile & Fixed)
    # ==========================================
    panel_df = add_quantile_tier(panel_df, "pressure_per_capita", "pressure_per_capita_tier")
    panel_df = add_quantile_tier(panel_df, "pressure_per_gdp_per_capita", "pressure_per_gdp_per_capita_tier")

    # Funding gap uses fixed normative thresholds (non-relative)
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
    
    print("Distribution of funding_gap_tier:")
    gold_host_pressure_indices.groupBy("funding_gap_tier").count().orderBy(F.desc("count")).show()

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)

























# """
# =====================================================================
#  GOLD LAYER — Host Pressure Indices (coa_iso, year)
# =====================================================================
# Obiettivo (Livello 3):
# Unire gold_host_aggregates (numeri di flusso, lato host — ora "total
# territorial pressure": cross-border + IDPs, per scelta esplicita) con
# gold_country_fact (contesto: popolazione, GDP, funding, HRP/GHO) e
# calcolare qui gli indicatori di pressione:

#   1) pressure_per_capita          = stock totale / popolazione host
#   2) pressure_per_gdp_per_capita  = stock totale / GDP pro capite host
#   3) growth_rate                  = passato da gold_host_aggregates
#   4) funding_gap                  = 1 - (fondi ricevuti / richiesti),
#                                      SOLO dove i requirements sono noti
#                                      e positivi — altrimenti NULL

# NUOVO in questa versione:
#   - cross_border_stock: somma di refugees+asylum_seekers+oip, derivata
#     dalle 4 componenti già scomposte in gold_host_aggregates — utile
#     per confrontare "quanto della pressione è arrivo dall'estero" vs
#     "quanto è IDP interno" senza dover ricostruire un'altra tabella.
#   - *_tier: classificazione Low/Medium/High/Critical per dashboard.
#     Per pressure_per_capita e pressure_per_gdp_per_capita, i tier sono
#     calcolati con quantili PER ANNO (non sull'intero storico insieme),
#     solo tra gli host "rilevanti" (stock >= soglia) — sotto soglia è
#     Low per costruzione, valore NULL resta NULL (mai forzato a Low).
#     Per funding_gap invece si usano soglie FISSE, non quantili: una
#     copertura del 20% è grave a prescindere da come stanno gli altri
#     paesi quell'anno, non ha senso relativizzarla per periodo.
#   - is_forecast: sempre False qui — flag che distingue le righe
#     storiche (questo script, sempre in overwrite) da quelle che un
#     futuro script di forecasting aggiungerà in append.
# =====================================================================
# """

# import sys
# import os
# import pyspark.sql.functions as F
# from pyspark.sql.window import Window

# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)

# from utilities import get_spark_session, initialize_delta_table

# spark = get_spark_session("Gold-HostPressureIndices")


# # =====================================================================
# # STEP 1 — LETTURA DELLE DUE FONTI
# # =====================================================================
# host_aggregates_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_host_aggregates")
# country_fact_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_country_fact")


# # =====================================================================
# # STEP 2 — JOIN: da gold_host_aggregates verso gold_country_fact
# # =====================================================================
# panel_df = host_aggregates_df.join(
#     country_fact_df.withColumnRenamed("location_code", "coa_iso"),
#     ["coa_iso", "year"],
#     "left"
# )


# # =====================================================================
# # STEP 3 — CROSS_BORDER_STOCK (decomposizione, non sostituisce il totale)
# # =====================================================================
# panel_df = panel_df.withColumn(
#     "cross_border_stock",
#     F.col("refugees_count") + F.col("asylum_seekers_count") + F.col("oip_count")
# )


# # =====================================================================
# # STEP 4 — INDICATORE 1 e 2: PRESSIONE NORMALIZZATA
# # =====================================================================
# panel_df = (
#     panel_df
#     .withColumn("pressure_per_capita", F.col("total_hosted_stock") / F.col("total_population"))
#     .withColumn("pressure_per_gdp_per_capita", F.col("total_hosted_stock") / F.col("gdp_per_capita"))
# )


# # =====================================================================
# # STEP 5 — INDICATORE 4: FUNDING GAP (condizionale, ricalcolato pulito)
# # =====================================================================
# panel_df = (
#     panel_df
#     .withColumn(
#         "has_funding_data",
#         F.col("funding_received_usd").isNotNull() | F.col("requirements_usd").isNotNull()
#     )
#     .withColumn(
#         "funding_gap",
#         F.when(
#             F.col("requirements_usd").isNotNull() & (F.col("requirements_usd") > 0),
#             1 - (F.coalesce(F.col("funding_received_usd"), F.lit(0.0)) / F.col("requirements_usd"))
#         ).otherwise(F.lit(None).cast("double"))
#     )
# )


# # =====================================================================
# # STEP 6 — TIER DI RISCHIO
# # =====================================================================
# RELEVANT_HOST_THRESHOLD = 100  # soglia minima di stock TOTALE per entrare nel ranking per-anno

# def add_quantile_tier(df, value_col, tier_col_name):
#     """
#     Tier Low/Medium/High/Critical via ntile(4) PER ANNO, solo tra gli
#     host rilevanti (stock totale >= soglia) e con value_col non NULL.
#     Sotto soglia -> Low (stato noto: poca pressione).
#     value_col NULL (denominatore mancante, es. GDP assente) -> NULL,
#     mai confuso con "Low": qui non sappiamo, non "va tutto bene".
#     """
#     relevant = (
#         df.filter(
#             (F.col("total_hosted_stock") >= RELEVANT_HOST_THRESHOLD) |
#             (F.col("hosted_stock_lag1") >= RELEVANT_HOST_THRESHOLD)
#         )
#         .filter(F.col(value_col).isNotNull())
#     )

#     window_q = Window.partitionBy("year").orderBy(F.col(value_col).asc())
#     relevant = relevant.withColumn(
#         tier_col_name,
#         F.when(F.ntile(4).over(window_q) == 1, "Low")
#          .when(F.ntile(4).over(window_q) == 2, "Medium")
#          .when(F.ntile(4).over(window_q) == 3, "High")
#          .otherwise("Critical")
#     ).select("coa_iso", "year", tier_col_name)

#     df = df.join(relevant, ["coa_iso", "year"], "left")
#     df = df.withColumn(
#         tier_col_name,
#         F.when(F.col(tier_col_name).isNotNull(), F.col(tier_col_name))
#          .when(F.col(value_col).isNull(), F.lit(None).cast("string"))
#          .otherwise(F.lit("Low"))
#     )
#     return df


# panel_df = add_quantile_tier(panel_df, "pressure_per_capita", "pressure_per_capita_tier")
# panel_df = add_quantile_tier(panel_df, "pressure_per_gdp_per_capita", "pressure_per_gdp_per_capita_tier")

# # funding_gap: soglie fisse, non quantili (vedi motivazione nel docstring)
# panel_df = panel_df.withColumn(
#     "funding_gap_tier",
#     F.when(F.col("funding_gap").isNull(), F.lit(None).cast("string"))
#      .when(F.col("funding_gap") <= 0.3, "Low")
#      .when(F.col("funding_gap") <= 0.5, "Medium")
#      .when(F.col("funding_gap") <= 0.7, "High")
#      .otherwise("Critical")
# )


# # =====================================================================
# # STEP 7 — IS_FORECAST (scaffolding per il futuro script di forecasting)
# # =====================================================================
# panel_df = panel_df.withColumn("is_forecast", F.lit(False))


# # =====================================================================
# # STEP 8 — SELEZIONE FINALE
# # =====================================================================
# gold_host_pressure_indices = panel_df.select(
#     "coa_iso",
#     "year",
#     "is_forecast",

#     # dettaglio categorie (ereditato da gold_host_aggregates)
#     "refugees_count",
#     "asylum_seekers_count",
#     "oip_count",
#     "idps_count",
#     "cross_border_stock",

#     # numeri di flusso grezzi
#     "total_hosted_stock",
#     "hosted_stock_lag1",
#     "total_inflows",
#     "total_outflows",

#     # indicatori principali + tier
#     "growth_rate",
#     "pressure_per_capita",
#     "pressure_per_capita_tier",
#     "pressure_per_gdp_per_capita",
#     "pressure_per_gdp_per_capita_tier",
#     "has_funding_data",
#     "funding_gap",
#     "funding_gap_tier",

#     # contesto grezzo dietro agli indicatori
#     "total_population",
#     "gdp_per_capita",
#     "funding_received_usd",
#     "requirements_usd",
#     "has_hrp",
#     "in_gho",
    
#     # contesto sparso, solo per annotazione — NON per ranking
#     "mpi",
#     "hdx_head",
#     "hdx_vuln",
#     "hdx_sev",
#     "mpm",
#     "ext_pov",
#     "peak_population_phase3plus",
# )


# # =====================================================================
# # STEP 9 — SCRITTURA SU DELTA LAKE
# # =====================================================================
# initialize_delta_table(
#     spark=spark,
#     db_name="gold",
#     table_name="gold_host_pressure_indices"
# )

# print("Scrittura della tabella gold_host_pressure_indices su Delta Lake...")
# (
#     gold_host_pressure_indices.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .save("s3a://lakehouse/gold/gold_host_pressure_indices")
# )

# print("gold_host_pressure_indices generata con successo.")

# # Diagnostica rapida
# total_rows = gold_host_pressure_indices.count()
# print(f"\nRighe totali: {total_rows}")
# print("\nDistribuzione pressure_per_capita_tier:")
# gold_host_pressure_indices.groupBy("pressure_per_capita_tier").count().orderBy(F.desc("count")).show()
# print("Distribuzione funding_gap_tier:")
# gold_host_pressure_indices.groupBy("funding_gap_tier").count().orderBy(F.desc("count")).show()

# spark.stop()
