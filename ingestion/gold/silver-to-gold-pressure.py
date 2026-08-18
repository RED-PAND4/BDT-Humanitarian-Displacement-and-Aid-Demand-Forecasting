from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session

spark = get_spark_session("SilverToGold-HostPressureMaster")

# ─── HELPER ──────────────────────────────────────────────────────────────────

def safe_long(col_name):
    """Converte trattini '-' in null e casta a long"""
    return F.when(F.col(col_name) == "-", None) \
             .otherwise(F.col(col_name).cast("long"))

# ─── READ FROM SILVER ────────────────────────────────────────────────────────

population_df = spark.read.format("delta").load("s3a://lakehouse/silver/population")
solutions_df = spark.read.format("delta").load("s3a://lakehouse/silver/solutions")
funding_df = spark.read.format("delta").load("s3a://lakehouse/silver/funding")
conflict_df = spark.read.format("delta").load("s3a://lakehouse/silver/conflict_events")
foodsecurity_df = spark.read.format("delta").load("s3a://lakehouse/silver/foodsecurity")

wb_population_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_population")
wb_gdp_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_gdp")
wb_mpm_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_mpm")
wb_extreme_poverty_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_extreme_poverty")

# ─── VALID COUNTRIES (esclude aggregati regionali World Bank) ─────────────────

valid_countries = population_df.select(
    F.col("coa_iso").alias("location_code")
).union(
    population_df.select(F.col("coo_iso").alias("location_code"))
).distinct()

# ─── PREPROCESSING WORLD BANK ─────────────────────────────────────────────────

wb_population_df = wb_population_df.join(valid_countries, on="location_code", how="inner")
wb_gdp_df = wb_gdp_df.join(valid_countries, on="location_code", how="inner")
wb_mpm_df = wb_mpm_df.join(valid_countries, on="location_code", how="inner")
wb_extreme_poverty_df = wb_extreme_poverty_df.join(valid_countries, on="location_code", how="inner")

# ─── SCAFFOLD ANNI 2000-2025 ──────────────────────────────────────────────────

all_years = spark.range(2000, 2026).withColumnRenamed("id", "year")

# ─── PREPROCESSING: MPI con forward fill ─────────────────────────────────────

window_ff = Window.partitionBy("location_code").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)

all_countries_mpi = wb_mpm_df.select("location_code").distinct()
mpi_scaffold = all_countries_mpi.crossJoin(all_years)

mpi_filled = mpi_scaffold \
    .join(
        wb_mpm_df.select("location_code", "year", "mpm_value"),
        on=["location_code", "year"],
        how="left"
    ) \
    .withColumn("mpi_origin",
        F.last("mpm_value", ignorenulls=True).over(window_ff)
    ) \
    .select("location_code", "year", "mpi_origin")

# ─── PREPROCESSING: EXTREME POVERTY con forward fill ─────────────────────────

all_countries_poverty = wb_extreme_poverty_df.select("location_code").distinct()
poverty_scaffold = all_countries_poverty.crossJoin(all_years)

extreme_poverty_filled = poverty_scaffold \
    .join(
        wb_extreme_poverty_df.select("location_code", "year", "extreme_poverty_value"),
        on=["location_code", "year"],
        how="left"
    ) \
    .withColumn("extreme_poverty_origin",
        F.last("extreme_poverty_value", ignorenulls=True).over(window_ff)
    ) \
    .select("location_code", "year", "extreme_poverty_origin")

# ─── PREPROCESSING: CONFLICT INTENSITY (aggregazione annuale) ─────────────────

conflict_annual = conflict_df \
    .groupBy("location_code", "year") \
    .agg(
        F.sum("events").alias("conflict_events_total"),
        F.sum("fatalities").alias("conflict_fatalities_total"),
        F.round(
            F.sum("events") + F.sum("fatalities") * F.lit(0.5),
            2
        ).alias("conflict_intensity")
    )

# ─── PREPROCESSING: FOOD CRISIS FLAG ─────────────────────────────────────────

food_crisis = foodsecurity_df \
    .filter(
        (F.col("ipc_type") == "current") &
        (F.col("ipc_phase") >= 3)
    ) \
    .groupBy("location_code", "year") \
    .agg(F.lit(1).alias("food_crisis_flag")) \
    .distinct()

# ─── PREPROCESSING: FUNDING ───────────────────────────────────────────────────

funding_agg = funding_df \
    .groupBy("location_code", "year") \
    .agg(
        F.sum("requirements_usd").alias("total_required_usd"),
        F.sum("funding_usd").alias("total_received_usd"),
        F.round(
            F.when(F.sum("requirements_usd") == 0, None)
             .otherwise(F.sum("funding_usd") * 100.0 / F.sum("requirements_usd")),
            2
        ).alias("funding_coverage_pct")
    )

window_funding = Window.partitionBy("location_code").orderBy("year")
funding_with_lag = funding_agg \
    .withColumn("funding_coverage_lag1",
        F.lag("funding_coverage_pct", 1).over(window_funding)
    )

# ─── PREPROCESSING: IDPs INTERNI (coa = coo) ─────────────────────────────────

internal_idps = population_df \
    .filter(F.col("coa_iso") == F.col("coo_iso")) \
    .groupBy("coo_iso", "year") \
    .agg(F.sum(safe_long("idps")).alias("internal_idps"))

# ─── CORE: FLUSSO INTERNAZIONALE (coa ≠ coo) ─────────────────────────────────

flow_df = population_df \
    .filter(F.col("coa_iso") != F.col("coo_iso")) \
    .groupBy("coo_iso", "coa_iso", "year") \
    .agg(
        F.sum(safe_long("refugees")).alias("refugees"),
        F.sum(safe_long("asylum_seekers")).alias("asylum_seekers")
    )

outflow_df = solutions_df \
    .filter(F.col("coa_iso") != F.col("coo_iso")) \
    .groupBy("coo_iso", "coa_iso", "year") \
    .agg(
        F.sum(safe_long("returned_refugees")).alias("returned_refugees"),
        F.sum(safe_long("resettlement")).alias("resettlement"),
        F.sum(safe_long("naturalisation")).alias("naturalisation")
    )

# ─── JOIN FLOW + OUTFLOW ──────────────────────────────────────────────────────

master_df = flow_df \
    .join(outflow_df, on=["coo_iso", "coa_iso", "year"], how="left") \
    .withColumn("net_flow",
        F.coalesce(F.col("refugees"), F.lit(0)) +
        F.coalesce(F.col("asylum_seekers"), F.lit(0)) -
        F.coalesce(safe_long("returned_refugees"), F.lit(0)) -
        F.coalesce(safe_long("resettlement"), F.lit(0)) -
        F.coalesce(safe_long("naturalisation"), F.lit(0))
    )

# ─── INDICATORI PREDITTIVI ────────────────────────────────────────────────────

window_pair = Window.partitionBy("coo_iso", "coa_iso").orderBy("year")

master_df = master_df \
    .withColumn("net_flow_lag1", F.lag("net_flow", 1).over(window_pair)) \
    .withColumn("prev_year", F.lag("year", 1).over(window_pair)) \
    .withColumn("is_consecutive",
        F.when(F.col("year") - F.col("prev_year") == 1, True)
         .otherwise(False)
    ) \
    .withColumn("yoy_growth",
    F.round(
        F.when(
            F.col("net_flow_lag1").isNull() |
            (F.abs(F.col("net_flow_lag1")) < 100) |
            (~F.col("is_consecutive")),
            None
        ).otherwise(
            (F.col("net_flow") - F.col("net_flow_lag1")) /
            F.col("net_flow_lag1") * 100
        ),
        2
    )) \
    .withColumn("yoy_growth_lag1", F.lag("yoy_growth", 1).over(window_pair)) \
    .withColumn("acceleration",
        F.round(
            F.when(
                F.col("yoy_growth").isNull() | F.col("yoy_growth_lag1").isNull(),
                None
            ).otherwise(
                F.col("yoy_growth") - F.col("yoy_growth_lag1")
            ),
            2
        )
    ) \
    .drop("prev_year", "yoy_growth_lag1", "is_consecutive")

# ─── JOIN PUSH FACTORS (coo_iso) ─────────────────────────────────────────────

# Internal IDPs
master_df = master_df \
    .join(internal_idps, on=["coo_iso", "year"], how="left")

# Conflict intensity
master_df = master_df \
    .join(
        conflict_annual.select(
            F.col("location_code").alias("coo_iso"),
            "year",
            "conflict_events_total",
            "conflict_fatalities_total",
            "conflict_intensity"
        ),
        on=["coo_iso", "year"],
        how="left"
    )

# MPI forward fill
master_df = master_df \
    .join(
        mpi_filled.withColumnRenamed("location_code", "coo_iso"),
        on=["coo_iso", "year"],
        how="left"
    )

# Extreme poverty forward fill
master_df = master_df \
    .join(
        extreme_poverty_filled.withColumnRenamed("location_code", "coo_iso"),
        on=["coo_iso", "year"],
        how="left"
    )

# GDP per capita
master_df = master_df \
    .join(
        wb_gdp_df.select(
            F.col("location_code").alias("coo_iso"),
            "year",
            F.col("gdp_per_capita").alias("gdp_origin")
        ),
        on=["coo_iso", "year"],
        how="left"
    )

# Food crisis flag
master_df = master_df \
    .join(
        food_crisis.select(
            F.col("location_code").alias("coo_iso"),
            "year",
            "food_crisis_flag"
        ),
        on=["coo_iso", "year"],
        how="left"
    )

# Funding coverage origine
master_df = master_df \
    .join(
        funding_with_lag.select(
            F.col("location_code").alias("coo_iso"),
            "year",
            F.col("funding_coverage_pct").alias("funding_coverage_origin"),
            F.col("funding_coverage_lag1").alias("funding_coverage_origin_lag1")
        ),
        on=["coo_iso", "year"],
        how="left"
    )

# ─── JOIN CONTESTO (coa_iso) ──────────────────────────────────────────────────

# World Bank population → displaced_per_1000_inhabitants
master_df = master_df \
    .join(
        wb_population_df.select(
            F.col("location_code").alias("coa_iso"),
            "year",
            "total_population"
        ),
        on=["coa_iso", "year"],
        how="left"
    ) \
    .withColumn("displaced_per_1000_inhabitants",
        F.round(
            F.when(
                F.col("total_population").isNull() | (F.col("total_population") == 0),
                None
            ).otherwise(
                (F.coalesce(F.col("refugees"), F.lit(0)) +
                 F.coalesce(F.col("asylum_seekers"), F.lit(0))) /
                F.col("total_population") * 1000
            ),
            4
        )
    ) \
    .drop("total_population")
# GDP paese destinazione — pull factor
master_df = master_df \
    .join(
        wb_gdp_df.select(
            F.col("location_code").alias("coa_iso"),
            "year",
            F.col("gdp_per_capita").alias("gdp_destination")
        ),
        on=["coa_iso", "year"],
        how="left"
    )
# ─── FILTRO ANNI ──────────────────────────────────────────────────────────────

master_df = master_df.filter(
    (F.col("year") >= 2000) & (F.col("year") <= 2025)
)

# ─── WRITE TO GOLD ────────────────────────────────────────────────────────────

spark.sql("CREATE DATABASE IF NOT EXISTS gold LOCATION 's3a://lakehouse/gold'")
spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.host_pressure_master
    USING delta
    LOCATION 's3a://lakehouse/gold/host_pressure_master'
""")

master_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://lakehouse/gold/host_pressure_master")

print("gold.host_pressure_master: done")
spark.stop()