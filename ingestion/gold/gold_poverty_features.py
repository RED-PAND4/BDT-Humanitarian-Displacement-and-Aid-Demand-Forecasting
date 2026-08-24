# import sys
# import os
# import pyspark.sql.functions as F

# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)

# from utilities import get_spark_session, initialize_delta_table

# # 1. Sessione Spark
# spark = get_spark_session("SilverToGold-PovertyFeatures")

# # 2. Lettura Silver
# poverty_silver_df = spark.read.format("delta").load("s3a://lakehouse/silver/povertyrate")

# # 3. Pulizia ed estrazione dell'anno
# poverty_clean = poverty_silver_df \
#     .withColumn("year", F.year("reference_period_start")) \
#     .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull())

# # 4. Aggregazione delle Feature di Povertà per Paese e Anno
# gold_poverty_features = poverty_clean.groupBy("location_code", "year").agg(
    
#     # Indice di povertà multidimensionale medio
#     F.avg("mpi").alias("avg_mpi"),
    
#     # Percentuale media di popolazione povera
#     F.avg("headcount_ratio").alias("poverty_headcount_pct"),
    
#     # Percentuale media di povertà severa (spinta migratoria critica)
#     F.avg("in_severe_poverty").alias("severe_poverty_pct"),
    
#     # Percentuale media di popolazione vulnerabile
#     F.avg("vulnerable_to_poverty").alias("vulnerable_poverty_pct")
# )

# # 5. Scrittura in Gold
# initialize_delta_table(
#     spark=spark,
#     db_name="gold",
#     table_name="gold_poverty_features"
# )

# (
#     gold_poverty_features.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .save("s3a://lakehouse/gold/gold_poverty_features")
# )

# print("Tabella gold_poverty_features creata con successo.")
# spark.stop()








# import sys
# import os
# import pyspark.sql.functions as F
# from pyspark.sql.window import Window

# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)

# from utilities import get_spark_session, initialize_delta_table

# # 1. Sessione Spark
# spark = get_spark_session("SilverToGold-PovertyFeatures")

# print("Lettura delle tabelle Silver...")
# # 2. Lettura Tabelle Silver
# hdx_poverty_df = spark.read.format("delta").load("s3a://lakehouse/silver/povertyrate")
# wb_mpm_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_mpm")
# wb_extreme_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_extreme_poverty")

# print("Pulizia e aggregazione delle fonti...")
# # 3. Pulizia e Aggregazione (Livello Paese-Anno)

# # A. HDX Poverty Rate
# hdx_clean = hdx_poverty_df \
#     .withColumn("year", F.year("reference_period_start")) \
#     .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull()) \
#     .groupBy("location_code", "year").agg(
#         F.avg("mpi").alias("hdx_mpi_score"),
#         F.avg("headcount_ratio").alias("hdx_headcount_pct"),
#         F.avg("in_severe_poverty").alias("hdx_severe_poverty_pct"),
#         F.avg("vulnerable_to_poverty").alias("hdx_vulnerable_pct")
#     )

# # B. World Bank MPM
# wb_mpm_clean = wb_mpm_df \
#     .withColumn("year", F.col("year").cast("integer")) \
#     .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull()) \
#     .groupBy("location_code", "year").agg(
#         F.avg("mpm_value").alias("wb_mpm_pct")
#     )

# # C. World Bank Extreme Poverty ($2.15 a day)
# wb_ext_clean = wb_extreme_df \
#     .withColumn("year", F.col("year").cast("integer")) \
#     .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull()) \
#     .groupBy("location_code", "year").agg(
#         F.avg("extreme_poverty_value").alias("wb_extreme_poverty_pct")
#     )

# print("Esecuzione della FULL OUTER JOIN...")
# # 4. FULL OUTER JOIN per unificare l'anagrafica di tutti i Paesi
# joined_df = hdx_clean \
#     .join(wb_mpm_clean, ["location_code", "year"], "outer") \
#     .join(wb_ext_clean, ["location_code", "year"], "outer")

# print("Applicazione del Forward Fill (LOCF) per coprire i buchi storici...")
# # 5. Forward Fill (LOCF) per propagare in avanti i dati mancanti tra una survey e l'altra
# window_ffill = Window.partitionBy("location_code").orderBy("year") \
#                      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# gold_poverty_features = joined_df.select(
#     "location_code",
#     "year",
#     F.last("hdx_mpi_score", ignorenulls=True).over(window_ffill).alias("hdx_mpi_score"),
#     F.last("hdx_headcount_pct", ignorenulls=True).over(window_ffill).alias("hdx_headcount_pct"),
#     F.last("hdx_severe_poverty_pct", ignorenulls=True).over(window_ffill).alias("hdx_severe_poverty_pct"),
#     F.last("hdx_vulnerable_pct", ignorenulls=True).over(window_ffill).alias("hdx_vulnerable_pct"),
#     F.last("wb_mpm_pct", ignorenulls=True).over(window_ffill).alias("wb_mpm_pct"),
#     F.last("wb_extreme_poverty_pct", ignorenulls=True).over(window_ffill).alias("wb_extreme_poverty_pct")
# )

# # 6. Scrittura in Gold
# initialize_delta_table(
#     spark=spark,
#     db_name="gold",
#     table_name="gold_poverty_features"
# )

# print("Scrittura su Delta Lake...")
# (
#     gold_poverty_features.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .save("s3a://lakehouse/gold/gold_poverty_features")
# )

# print("Tabella gold_poverty_features aggiornata con successo! (Include HDX, WB MPM, WB Extreme Poverty)")
# spark.stop()



# import sys
# import os
# import pyspark.sql.functions as F

# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)

# from utilities import get_spark_session, initialize_delta_table

# spark = get_spark_session("SilverToGold-PovertyFeatures")

# print("Lettura delle tabelle Silver...")
# hdx_df = spark.read.format("delta").load("s3a://lakehouse/silver/povertyrate")
# wb_mpm_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_mpm")
# wb_ext_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_extreme_poverty")

# print("Pulizia e aggregazione delle fonti...")

# # A. HDX Poverty Rate
# hdx_clean = hdx_df \
#     .withColumn("year", F.col("year").cast("integer")) \
#     .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull()) \
#     .groupBy("location_code", "year").agg(
#         F.avg("mpi").alias("mpi"),
#         F.avg("headcount_ratio").alias("hdx_head"),
#         F.avg("in_severe_poverty").alias("hdx_sev"),
#         F.avg("vulnerable_to_poverty").alias("hdx_vuln")
#     )

# # B. World Bank MPM
# wb_mpm_clean = wb_mpm_df \
#     .withColumn("year", F.col("year").cast("integer")) \
#     .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull()) \
#     .groupBy("location_code", "year").agg(
#         F.avg("mpm_value").alias("mpm")
#     )

# # C. World Bank Extreme Poverty
# wb_ext_clean = wb_ext_df \
#     .withColumn("year", F.col("year").cast("integer")) \
#     .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull()) \
#     .groupBy("location_code", "year").agg(
#         F.avg("extreme_poverty_value").alias("ext_pov")
#     )

# print("Esecuzione della FULL OUTER JOIN...")
# # Unione delle tabelle: mantiene solo le righe in cui un dato esiste realmente
# gold_poverty_features = hdx_clean \
#     .join(wb_mpm_clean, ["location_code", "year"], "outer") \
#     .join(wb_ext_clean, ["location_code", "year"], "outer")

# initialize_delta_table(
#     spark=spark,
#     db_name="gold",
#     table_name="gold_poverty_features"
# )

# print("Scrittura su Delta Lake...")
# (
#     gold_poverty_features.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .save("s3a://lakehouse/gold/gold_poverty_features")
# )

# print("Tabella gold_poverty_features aggiornata con successo (dati puri, senza imputazioni)!")
# spark.stop()

import sys
import os
import pyspark.sql.functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

spark = get_spark_session("SilverToGold-PovertyFeatures")

print("Lettura delle tabelle Silver...")
hdx_df = spark.read.format("delta").load("s3a://lakehouse/silver/povertyrate")
wb_mpm_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_mpm")
wb_ext_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_extreme_poverty")

print("Pulizia e aggregazione delle fonti...")

# A. HDX Poverty Rate (Filtro per dati nazionali: admin_level == 0)
hdx_clean = hdx_df \
    .filter(F.col("admin_level") == 0) \
    .withColumn("year", F.col("year").cast("integer")) \
    .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull()) \
    .groupBy("location_code", "year").agg(
        F.avg("mpi").alias("mpi"),
        F.avg("headcount_ratio").alias("hdx_head"),
        F.avg("vulnerable_to_poverty").alias("hdx_vuln"),
        F.avg("in_severe_poverty").alias("hdx_sev")
    )

# B. World Bank MPM
wb_mpm_clean = wb_mpm_df \
    .withColumn("year", F.col("year").cast("integer")) \
    .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull()) \
    .groupBy("location_code", "year").agg(
        F.avg("mpm_value").alias("mpm")
    )

# C. World Bank Extreme Poverty
wb_ext_clean = wb_ext_df \
    .withColumn("year", F.col("year").cast("integer")) \
    .filter(F.col("location_code").isNotNull() & F.col("year").isNotNull()) \
    .groupBy("location_code", "year").agg(
        F.avg("extreme_poverty_value").alias("ext_pov")
    )

print("Esecuzione della FULL OUTER JOIN...")
# Unione delle tabelle: mantiene solo le righe in cui un dato esiste realmente
gold_poverty_features = hdx_clean \
    .join(wb_mpm_clean, ["location_code", "year"], "outer") \
    .join(wb_ext_clean, ["location_code", "year"], "outer")

initialize_delta_table(
    spark=spark,
    db_name="gold",
    table_name="gold_poverty_features"
)

print("Scrittura su Delta Lake...")
(
    gold_poverty_features.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("s3a://lakehouse/gold/gold_poverty_features")
)

print("Tabella gold_poverty_features aggiornata con successo!")
spark.stop()