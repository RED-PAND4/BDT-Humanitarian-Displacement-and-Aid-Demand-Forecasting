"""
=====================================================================
 GOLD LAYER — Host Pressure Indices (coa_iso, year)
=====================================================================
Obiettivo (Livello 3):
Unire gold_host_aggregates (numeri di flusso puri, lato host) con
gold_country_fact (contesto: popolazione, GDP, funding, HRP/GHO) e
calcolare qui — solo qui — gli indicatori di pressione da mostrare in
dashboard:

  1) pressure_per_capita          = stock ospitato / popolazione host
  2) pressure_per_gdp_per_capita  = stock ospitato / GDP pro capite host
  3) growth_rate                  = passato da gold_host_aggregates
  4) funding_gap                  = 1 - (fondi ricevuti / richiesti),
                                     SOLO dove i requirements sono noti
                                     e positivi — altrimenti NULL

Il funding_gap viene RICALCOLATO qui invece di riusare
"funding_coverage_pct" di gold_country_fact: quella colonna eredita da
gold_funding_features un comportamento .otherwise(0.0) che confonde
"nessun dato sui requirements" con "0% di copertura reale" (circa
1.000 righe su ~2.000 con dati funding, per i controlli fatti sui
dati). Qui la distinzione resta esplicita.

Indicatori 1-2-3 vengono calcolati per OGNI host-year; il funding_gap
resta condizionale (solo dove esiste un appello con requirements noti)
e le colonne di povertà/food-security restano contestuali, non entrano
in nessun ranking: sono troppo sparse (4-30% di copertura) per essere
trattate come componenti di un punteggio.
=====================================================================
"""

import sys
import os
import pyspark.sql.functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

spark = get_spark_session("Gold-HostPressureIndices")


# =====================================================================
# STEP 1 — LETTURA DELLE DUE FONTI
# =====================================================================
host_aggregates_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_host_aggregates")
country_fact_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_country_fact")


# =====================================================================
# STEP 2 — JOIN: da gold_host_aggregates verso gold_country_fact
# LEFT join (non outer): la base è "i paesi che ospitano qualcuno",
# non tutti i paesi del mondo — non ha senso iniettare qui country-year
# che non hanno alcuna rilevanza di hosting solo perché presenti nella
# Country Fact.
# =====================================================================
panel_df = host_aggregates_df.join(
    country_fact_df.withColumnRenamed("location_code", "coa_iso"),
    ["coa_iso", "year"],
    "left"
)


# =====================================================================
# STEP 3 — INDICATORE 1 e 2: PRESSIONE NORMALIZZATA
# Se popolazione o GDP pro capite mancano (nessuna riga di contesto
# trovata), il risultato resta NULL — non ha senso imputare un
# denominatore inventato per un rapporto di pressione.
# =====================================================================
panel_df = (
    panel_df
    .withColumn("pressure_per_capita", F.col("total_hosted_stock") / F.col("total_population"))
    .withColumn("pressure_per_gdp_per_capita", F.col("total_hosted_stock") / F.col("gdp_per_capita"))
)


# =====================================================================
# STEP 4 — INDICATORE 4: FUNDING GAP (condizionale, ricalcolato pulito)
# has_funding_data: esiste ALMENO un valore (ricevuto o richiesto) per
# questo host-year, indipendentemente da come gold_funding_features ha
# gestito il caso mancante.
# funding_gap: calcolabile SOLO se i requirements sono noti e > 0.
# Se manca funding_received_usd ma i requirements sono noti, lo
# trattiamo come 0 ricevuto (non come "sconosciuto") — è l'assunzione
# più naturale quando un appello è registrato ma senza fondi associati.
# =====================================================================
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


# =====================================================================
# STEP 5 — SELEZIONE FINALE
# Indicatori "da ranking" separati concettualmente dalle colonne di
# contesto (povertà/food-security), che restano per l'annotazione in
# dashboard ma non entrano in nessun punteggio.
# =====================================================================
gold_host_pressure_indices = panel_df.select(
    "coa_iso",
    "year",
    # dettaglio categorie (ereditato da host_aggregates)
    "refugees_count",
    "asylum_seekers_count",
    "oip_count",
    "idps_count",
    # numeri di flusso grezzi (da gold_host_aggregates)
    "total_hosted_stock",
    "hosted_stock_lag1",
    "total_inflows",
    "total_outflows",
    # indicatori principali
    "growth_rate",
    "pressure_per_capita",
    "pressure_per_gdp_per_capita",
    "has_funding_data",
    "funding_gap",
    # contesto grezzo dietro agli indicatori
    "total_population",
    "gdp_per_capita",
    "funding_received_usd",
    "requirements_usd",
    "has_hrp",
    "in_gho",
    # contesto sparso, solo per annotazione — NON per ranking
    "mpi",
    "hdx_head",
    "hdx_vuln",
    "hdx_sev",
    "mpm",
    "ext_pov",
    "peak_population_phase3plus",
)


# =====================================================================
# STEP 6 — SCRITTURA SU DELTA LAKE
# =====================================================================
initialize_delta_table(
    spark=spark,
    db_name="gold",
    table_name="gold_host_pressure_indices"
)

print("Scrittura della tabella gold_host_pressure_indices su Delta Lake...")
(
    gold_host_pressure_indices.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("s3a://lakehouse/gold/gold_host_pressure_indices")
)

print("gold_host_pressure_indices generata con successo.")

# Diagnostica rapida
total_rows = gold_host_pressure_indices.count()
funding_rows = gold_host_pressure_indices.filter(F.col("has_funding_data")).count()
gap_rows = gold_host_pressure_indices.filter(F.col("funding_gap").isNotNull()).count()
print(f"\nRighe totali: {total_rows}")
print(f"Righe con has_funding_data = True: {funding_rows}")
print(f"Righe con funding_gap calcolabile: {gap_rows}")

spark.stop()
