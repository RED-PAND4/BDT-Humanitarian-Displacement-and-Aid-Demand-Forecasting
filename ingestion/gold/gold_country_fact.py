"""
=====================================================================
 GOLD LAYER — Country Fact (location_code, year)
=====================================================================
Obiettivo:
Consolidare in UN'UNICA tabella panel (location_code, year) le
feature già prodotte dagli script gold esistenti — che restano la
fonte di verità per la LORO logica di aggregazione, qui non viene
ricalcolato nulla, solo unito:

  - gold_conflict_features        (eventi/vittime di conflitto)
  - gold_poverty_features         (HDX MPI + World Bank MPM/Extreme Poverty)
  - gold_food_security_features   (picco popolazione IPC 3+)
  - gold_funding_features         (fondi ricevuti/richiesti)

più due fonti Silver dirette (non hanno ancora un gold dedicato):
  - worldbank_population          (popolazione totale)
  - worldbank_gdp                 (PIL pro capite)
  - location                      (has_hrp / in_gho, con logica temporale)

Le colonne arrivano già tipizzate correttamente dai rispettivi script
di origine: nessun cast viene applicato qui.
=====================================================================
"""

import sys
import os
import pyspark.sql.functions as F
from pyspark.sql.window import Window

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

spark = get_spark_session("Gold-CountryFact")


# =====================================================================
# STEP 1 — LETTURA DELLE FONTI
# =====================================================================
conflict_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_conflict_features")
poverty_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_poverty_features")
food_security_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_food_security_features")
funding_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_funding_features")
wb_population_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_population")
wb_gdp_df = spark.read.format("delta").load("s3a://lakehouse/silver/worldbank_gdp")
location_df = spark.read.format("delta").load("s3a://lakehouse/silver/location")


# =====================================================================
# STEP 2 — SKELETON (location_code, year): OUTER JOIN su tutte le fonti
# Ogni fonte è già alla grana (location_code, year) corretta, quindi
# qui non si aggrega nulla: si uniscono solo le colonne. Usiamo
# "outer" per non perdere un paese-anno che esiste in una sola fonte
# (es. un paese con solo dato GDP ma nessun evento di conflitto).
# =====================================================================
country_fact = (
    conflict_df
    .join(poverty_df, ["location_code", "year"], "outer")
    .join(food_security_df, ["location_code", "year"], "outer")
    .join(funding_df, ["location_code", "year"], "outer")
    .join(
        wb_population_df.select("location_code", "year", "total_population"),
        ["location_code", "year"], "outer"
    )
    .join(
        wb_gdp_df.select("location_code", "year", "gdp_per_capita"),
        ["location_code", "year"], "outer"
    )
)


# =====================================================================
# STEP 3 — GESTIONE VUOTI (coerente con le note del progetto)
# Conflitti -> 0: l'assenza di una riga per un (location_code, year)
# in gold_conflict_features significa "nessun evento registrato",
# quindi qui è un vero zero, non un dato mancante.
# Povertà, food security, funding, GDP, popolazione -> NULL: qui
# l'assenza è genuinamente "dato non disponibile" e va lasciata NULL,
# come già indicato nelle note originali di ciascuna feature.
# =====================================================================
conflict_cols = [
    "total_fatalities",
    "violent_events",
    "civilian_targeting_events",
    "civilian_targeting_fatalities",
    "non_violent_events",
]
country_fact = country_fact.fillna(0, subset=conflict_cols)


# =====================================================================
# STEP 4 — HAS_HRP / IN_GHO CON LOGICA TEMPORALE
# La tabella location ha UN solo record per paese (con l'anno da cui
# vale il suo has_hrp/in_gho, già disponibile come colonna "year"
# grazie a extract_date_components). Per ogni anno del pannello:
#   - se l'anno è PRIMA di quello del record location -> False, False
#     (il paese non era ancora coperto)
#   - se l'anno è UGUALE o SUCCESSIVO -> usa il valore reale
#   - se il paese non ha nessun record location -> False, False
# =====================================================================
location_clean = location_df.select(
    F.col("code").alias("location_code"),
    F.col("has_hrp").alias("has_hrp_recorded"),
    F.col("in_gho").alias("in_gho_recorded"),
    F.col("year").alias("hrp_effective_year"),
)

country_fact = country_fact.join(location_clean, "location_code", "left")

country_fact = (
    country_fact
    .withColumn(
        "has_hrp",
        F.when(F.col("hrp_effective_year").isNull(), F.lit(False))
         .when(F.col("year") < F.col("hrp_effective_year"), F.lit(False))
         .otherwise(F.col("has_hrp_recorded"))
    )
    .withColumn(
        "in_gho",
        F.when(F.col("hrp_effective_year").isNull(), F.lit(False))
         .when(F.col("year") < F.col("hrp_effective_year"), F.lit(False))
         .otherwise(F.col("in_gho_recorded"))
    )
    .drop("has_hrp_recorded", "in_gho_recorded", "hrp_effective_year")
)


# =====================================================================
# STEP 5 — FUNDING_COVERAGE_LAG1
# Le note originali descrivono questa feature per gold_funding_features,
# ma lo script attuale non la calcola. La aggiungiamo qui perché la
# Country Fact è la sede naturale di una feature laggata panel-based
# (richiede una window function su location_code ordinata per anno,
# che gold_funding_features non ha).
# =====================================================================
window_by_country = Window.partitionBy("location_code").orderBy("year")

country_fact = country_fact.withColumn(
    "funding_coverage_lag1",
    F.lag("funding_coverage_pct", 1).over(window_by_country)
)


# =====================================================================
# STEP 6 — SELEZIONE FINALE E SCRITTURA
# =====================================================================
gold_country_fact = country_fact.select(
    "location_code",
    "year",
    # conflitti
    "total_fatalities",
    "violent_events",
    "civilian_targeting_events",
    "civilian_targeting_fatalities",
    "non_violent_events",
    # povertà (HDX + World Bank)
    "mpi",
    "hdx_head",
    "hdx_vuln",
    "hdx_sev",
    "mpm",
    "ext_pov",
    # food security
    "peak_population_phase3plus",
    # funding
    "funding_received_usd",
    "requirements_usd",
    "funding_coverage_pct",
    "funding_coverage_lag1",
    # popolazione e ricchezza
    "total_population",
    "gdp_per_capita",
    # copertura sistema HDX
    "has_hrp",
    "in_gho",
)

initialize_delta_table(
    spark=spark,
    db_name="gold",
    table_name="gold_country_fact"
)

print("Scrittura della tabella gold_country_fact su Delta Lake...")
(
    gold_country_fact.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("s3a://lakehouse/gold/gold_country_fact")
)

print("gold_country_fact generata con successo.")

# Diagnostica rapida: copertura (% di righe non-NULL) per ogni colonna,
# utile per capire subito quali feature sono davvero utilizzabili su
# larga scala e quali restano sparse per pochi paesi/anni.
print("\nCopertura dati (righe non-NULL su totale):")
total_rows = gold_country_fact.count()
print(f"Righe totali nel pannello: {total_rows}")
coverage_exprs = [
    F.round(F.sum(F.when(F.col(c).isNotNull(), 1).otherwise(0)) / total_rows * 100, 1).alias(c)
    for c in gold_country_fact.columns if c not in ("location_code", "year")
]
gold_country_fact.select(coverage_exprs).show(vertical=True, truncate=False)

spark.stop()