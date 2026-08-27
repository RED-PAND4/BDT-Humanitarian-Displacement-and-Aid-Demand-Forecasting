import sys
import os
import pyspark.sql.functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

spark = get_spark_session("SilverToGold-Funding")

print("Lettura della tabella Silver Funding...")
fund_silver_df = spark.read.format("delta").load("s3a://lakehouse/silver/funding")

# 1. Preparazione Date e Anni
fund_df = fund_silver_df \
    .withColumn("start_date", F.to_date("reference_period_start")) \
    .withColumn("end_date", F.to_date("reference_period_end")) \
    .withColumn("start_year", F.year("start_date")) \
    .withColumn("end_year", F.year("end_date"))

# ==========================================
# BINARIO A: Appelli contenuti in un singolo anno
# ==========================================
single_year_df = fund_df.filter(F.col("start_year") == F.col("end_year"))

single_year_clean = single_year_df.select(
    "location_code",
    F.col("start_year").alias("year"),
    F.col("funding_usd").alias("allocated_funding_usd"),
    F.col("requirements_usd").alias("allocated_requirements_usd")
)

# ==========================================
# BINARIO B: Appelli a cavallo di più anni (La tua intuizione)
# ==========================================
multi_year_df = fund_df.filter(F.col("start_year") != F.col("end_year"))

# Calcoliamo i giorni totali solo per questi record
multi_year_df = multi_year_df.withColumn("total_days", F.datediff(F.col("end_date"), F.col("start_date")) + 1)

# Creiamo la sequenza ed esplodiamo solo chi ne ha bisogno
fund_seq = multi_year_df.withColumn("date_array", F.expr("sequence(start_date, end_date, interval 1 day)"))
fund_exploded = fund_seq.withColumn("single_day", F.explode("date_array"))
fund_years = fund_exploded.withColumn("year", F.year("single_day"))

# Contiamo i giorni per anno ed eseguiamo la proporzione matematica (Pro-Rata)
appeal_yearly = fund_years.groupBy(
    "location_code", "appeal_name", "funding_usd", "requirements_usd", "total_days", "year"
).agg(F.count("single_day").alias("days_in_year"))

multi_year_clean = appeal_yearly.select(
    "location_code",
    "year",
    ( (F.col("days_in_year") / F.col("total_days")) * F.col("funding_usd") ).alias("allocated_funding_usd"),
    ( (F.col("days_in_year") / F.col("total_days")) * F.col("requirements_usd") ).alias("allocated_requirements_usd")
)

from pyspark.sql.window import Window

# ==========================================
# UNIONE E AGGREGAZIONE FINALE NAZIONALE
# ==========================================
# Riuniamo i record singoli e quelli spalmati
combined_df = single_year_clean.unionByName(multi_year_clean)

# Aggreghiamo a livello di Nazione e Anno
gold_funding_features = combined_df.groupBy("location_code", "year").agg(
    F.sum("allocated_funding_usd").alias("funding_received_usd"),
    F.sum("allocated_requirements_usd").alias("requirements_usd")
)

# Calcoliamo la percentuale di copertura finale (Gestione rigorosa dei Null)
gold_funding_features = gold_funding_features.withColumn(
    "funding_coverage_pct",
    F.when(F.col("requirements_usd") > 0, 
           F.col("funding_received_usd") / F.col("requirements_usd"))\
     .otherwise(F.lit(None).cast("double"))
)

# Aggiunta del Lag: indicatore predittivo di "Taglio dei Fondi"
window_funding = Window.partitionBy("location_code").orderBy("year")
gold_funding_features = gold_funding_features.withColumn(
    "funding_coverage_lag1",
    F.lag("funding_coverage_pct", 1).over(window_funding)
)

# Salvataggio
initialize_delta_table(spark, "gold", "gold_funding_features")

print("Scrittura su Delta Lake...")
(
    gold_funding_features.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("s3a://lakehouse/gold/gold_funding_features")
)

print("Tabella gold_funding_features generata con successo.")
spark.stop()