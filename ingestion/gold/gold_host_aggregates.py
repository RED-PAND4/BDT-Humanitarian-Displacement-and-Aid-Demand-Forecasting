# """
# =====================================================================
#  GOLD LAYER — Host Aggregates (coa_iso, year)
# =====================================================================
# Obiettivo:
# Aggregare gold_master_displacement per singolo paese ospitante
# (coa_iso), sommando su tutte le origini (coo_iso), per ottenere una
# serie storica pulita di stock/inflow/outflow per host-year — la base
# su cui costruire sia gli indici di pressione (Livello 3) sia, più
# avanti, il forecasting.

# Le righe con coo_iso == coa_iso (sfollamento interno, IDPs) vengono
# escluse: qui ci interessa solo la popolazione ospitata dall'estero.

# PUNTO CHIAVE — griglia densa:
# Se un host-year non compare in nessuna rotta, questo NON è un dato
# mancante: nei dati UNHCR significa "quell'anno non ha ospitato
# nessuno", cioè uno zero vero (a differenza di povertà/food-security,
# dove l'assenza è "non misurato"). Per calcolare un lag/growth_rate
# corretto serve però che OGNI anno del range sia rappresentato con una
# riga esplicita a 0 — altrimenti F.lag() salterebbe silenziosamente
# agli anni con dati disponibili più vicini, calcolando una crescita
# completamente falsata su un intervallo di anni sbagliato.
# =====================================================================
# """

# import sys
# import os
# import pyspark.sql.functions as F
# from pyspark.sql.window import Window

# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)

# from utilities import get_spark_session, initialize_delta_table

# spark = get_spark_session("Gold-HostAggregates")


# # =====================================================================
# # STEP 1 — LETTURA E AGGREGAZIONE PER (coa_iso, year)
# # Escludiamo le rotte "self" (coo_iso == coa_iso): rappresentano
# # sfollamento interno, non pressione da arrivi dall'estero.
# # =====================================================================
# displacement_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_displacement")

# cross_border_df = displacement_df.filter(F.col("coo_iso") != F.col("coa_iso"))

# host_year_sums = cross_border_df.groupBy("coa_iso", "year").agg(
#     F.sum("stock").alias("total_hosted_stock"),
#     F.sum("inflows").alias("total_inflows"),
#     F.sum("outflows").alias("total_outflows"),
# )


# # =====================================================================
# # STEP 2 — GRIGLIA DENSA (coa_iso × year)
# # Ogni paese che compare almeno una volta come host, incrociato con
# # OGNI anno del range osservato nell'intero dataset — non solo gli
# # anni in cui quel paese ha effettivamente ospitato qualcuno.
# # =====================================================================
# year_bounds = displacement_df.agg(
#     F.min("year").alias("min_year"),
#     F.max("year").alias("max_year")
# ).collect()[0]

# all_years_df = spark.createDataFrame(
#     [(y,) for y in range(year_bounds["min_year"], year_bounds["max_year"] + 1)],
#     ["year"]
# )

# all_hosts_df = host_year_sums.select("coa_iso").distinct()

# dense_grid = all_hosts_df.crossJoin(all_years_df)


# # =====================================================================
# # STEP 3 — JOIN DEI VALORI SULLA GRIGLIA + RIEMPIMENTO A ZERO
# # Qui, e SOLO qui, un NULL dopo il join viene trasformato in 0: non è
# # un'imputazione arbitraria, è la conferma che quell'host-year non ha
# # nessuna rotta registrata, cioè zero persone ospitate quell'anno.
# # =====================================================================
# host_panel = (
#     dense_grid
#     .join(host_year_sums, ["coa_iso", "year"], "left")
#     .fillna(0, subset=["total_hosted_stock", "total_inflows", "total_outflows"])
# )


# # =====================================================================
# # STEP 4 — LAG E GROWTH RATE
# # =====================================================================
# window_host = Window.partitionBy("coa_iso").orderBy("year")

# host_panel = host_panel.withColumn(
#     "hosted_stock_lag1",
#     F.lag("total_hosted_stock", 1).over(window_host)
# )

# # Tre casi distinti, nessuno dei quali è una divisione "normale":
# #  - hosted_stock_lag1 NULL       -> primissimo anno del range per
# #                                     quell'host: nessun confronto
# #                                     possibile, resta NULL.
# #  - lag1 == 0 e stock attuale == 0 -> nessun cambiamento reale: 0.0
# #  - lag1 == 0 e stock attuale >  0 -> non è "crescita", è l'inizio
# #                                     di una relazione di hosting
# #                                     che prima non esisteva: NULL,
# #                                     non un numero enorme/infinito.
# host_panel = host_panel.withColumn(
#     "growth_rate",
#     F.when(F.col("hosted_stock_lag1").isNull(), F.lit(None).cast("double"))
#      .when((F.col("hosted_stock_lag1") == 0) & (F.col("total_hosted_stock") == 0), F.lit(0.0))
#      .when((F.col("hosted_stock_lag1") == 0) & (F.col("total_hosted_stock") > 0), F.lit(None).cast("double"))
#      .otherwise(
#         (F.col("total_hosted_stock") - F.col("hosted_stock_lag1")) / F.col("hosted_stock_lag1")
#      )
# )


# # =====================================================================
# # STEP 5 — SCRITTURA SU DELTA LAKE
# # =====================================================================
# gold_host_aggregates = host_panel.select(
#     "coa_iso",
#     "year",
#     "total_hosted_stock",
#     "hosted_stock_lag1",
#     "total_inflows",
#     "total_outflows",
#     "growth_rate",
# )

# initialize_delta_table(
#     spark=spark,
#     db_name="gold",
#     table_name="gold_host_aggregates"
# )

# print("Scrittura della tabella gold_host_aggregates su Delta Lake...")
# (
#     gold_host_aggregates.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .save("s3a://lakehouse/gold/gold_host_aggregates")
# )

# print("gold_host_aggregates generata con successo.")

# # Diagnostica rapida
# total_rows = gold_host_aggregates.count()
# null_growth = gold_host_aggregates.filter(F.col("growth_rate").isNull()).count()
# print(f"\nRighe totali nel pannello: {total_rows}")
# print(f"Righe con growth_rate NULL (primo anno del range o nuova relazione di hosting): {null_growth}")

# spark.stop()


























# """
# =====================================================================
#  GOLD LAYER — Host Aggregates (coa_iso, year)
# =====================================================================
# """

# import sys
# import os
# import pyspark.sql.functions as F
# from pyspark.sql.window import Window
# from pyspark.sql.types import IntegerType, StructType, StructField # AGGIUNTA FONDAMENTALE

# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)

# from utilities import get_spark_session, initialize_delta_table

# spark = get_spark_session("Gold-HostAggregates")

# # =====================================================================
# # STEP 1 — LETTURA E AGGREGAZIONE PER (coa_iso, year)
# # =====================================================================
# displacement_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_displacement")

# cross_border_df = displacement_df.filter(F.col("coo_iso") != F.col("coa_iso"))

# host_year_sums = cross_border_df.groupBy("coa_iso", "year").agg(
#     F.sum("stock").alias("total_hosted_stock"),
#     F.sum("inflows").alias("total_inflows"),
#     F.sum("outflows").alias("total_outflows"),
# )

# # =====================================================================
# # STEP 2 — GRIGLIA DENSA CON TIPI BLINDATI
# # =====================================================================
# year_bounds = displacement_df.agg(
#     F.min("year").alias("min_year"),
#     F.max("year").alias("max_year")
# ).collect()[0]

# # CORREZIONE 1: Definiamo esplicitamente lo schema come Integer per evitare il Type Mismatch
# schema = StructType([StructField("year", IntegerType(), nullable=False)])

# all_years_df = spark.createDataFrame(
#     [(int(y),) for y in range(int(year_bounds["min_year"]), int(year_bounds["max_year"]) + 1)],
#     schema
# )

# all_hosts_df = host_year_sums.select("coa_iso").distinct()

# # CORREZIONE 2: Usiamo il Broadcast per la tabella piccola. Previene il crash di memoria.
# dense_grid = all_hosts_df.crossJoin(F.broadcast(all_years_df))

# # =====================================================================
# # STEP 3 — JOIN DEI VALORI SULLA GRIGLIA
# # =====================================================================
# host_panel = (
#     dense_grid
#     .join(host_year_sums, ["coa_iso", "year"], "left")
#     .fillna(0, subset=["total_hosted_stock", "total_inflows", "total_outflows"])
# )

# # =====================================================================
# # STEP 4 — LAG E GROWTH RATE
# # =====================================================================
# window_host = Window.partitionBy("coa_iso").orderBy("year")

# host_panel = host_panel.withColumn(
#     "hosted_stock_lag1",
#     F.lag("total_hosted_stock", 1).over(window_host)
# )

# host_panel = host_panel.withColumn(
#     "growth_rate",
#     F.when(F.col("hosted_stock_lag1").isNull(), F.lit(None).cast("double"))
#      .when((F.col("hosted_stock_lag1") == 0) & (F.col("total_hosted_stock") == 0), F.lit(0.0))
#      .when((F.col("hosted_stock_lag1") == 0) & (F.col("total_hosted_stock") > 0), F.lit(None).cast("double"))
#      .otherwise(
#         (F.col("total_hosted_stock") - F.col("hosted_stock_lag1")) / F.col("hosted_stock_lag1")
#      )
# )

# # =====================================================================
# # STEP 5 — SCRITTURA SU DELTA LAKE
# # =====================================================================
# gold_host_aggregates = host_panel.select(
#     "coa_iso",
#     "year",
#     "total_hosted_stock",
#     "hosted_stock_lag1",
#     "total_inflows",
#     "total_outflows",
#     "growth_rate",
# )

# initialize_delta_table(
#     spark=spark,
#     db_name="gold",
#     table_name="gold_host_aggregates"
# )

# (
#     gold_host_aggregates.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .save("s3a://lakehouse/gold/gold_host_aggregates")
# )

# spark.stop()








# """
# =====================================================================
#  GOLD LAYER — Host Aggregates (coa_iso, year)
# =====================================================================
# """

# import sys
# import os
# import pyspark.sql.functions as F
# from pyspark.sql.window import Window

# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)

# from utilities import get_spark_session, initialize_delta_table

# spark = get_spark_session("Gold-HostAggregates")

# # =====================================================================
# # STEP 1 — LETTURA E AGGREGAZIONE
# # =====================================================================
# displacement_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_displacement")

# cross_border_df = displacement_df.filter(F.col("coo_iso") != F.col("coa_iso"))

# host_year_sums = cross_border_df.groupBy("coa_iso", "year").agg(
#     F.sum("stock").alias("total_hosted_stock"),
#     F.sum("inflows").alias("total_inflows"),
#     F.sum("outflows").alias("total_outflows"),
# )

# # =====================================================================
# # STEP 2 — GRIGLIA DENSA NATIVA SPARK (Sequence + Explode)
# # =====================================================================
# # Estraiamo gli estremi temporali in un DataFrame da 1 SOLA riga
# year_bounds = displacement_df.agg(
#     F.min("year").cast("integer").alias("min_year"),
#     F.max("year").cast("integer").alias("max_year")
# )

# # Prendiamo i paesi unici
# all_hosts_df = host_year_sums.select("coa_iso").distinct()

# # Facciamo crossJoin solo con la riga singola (costo in memoria zero)
# # Creiamo un array da min_year a max_year e poi usiamo explode per generare le righe
# dense_grid = (
#     all_hosts_df.crossJoin(F.broadcast(year_bounds))
#     .withColumn("year_array", F.expr("sequence(min_year, max_year, 1)"))
#     .withColumn("year", F.explode("year_array"))
#     .drop("min_year", "max_year", "year_array")
# )

# # =====================================================================
# # STEP 3 — JOIN E RIEMPIMENTO
# # =====================================================================
# host_panel = (
#     dense_grid
#     .join(host_year_sums, ["coa_iso", "year"], "left")
#     .fillna(0, subset=["total_hosted_stock", "total_inflows", "total_outflows"])
# )

# # =====================================================================
# # STEP 4 — LAG E GROWTH RATE
# # =====================================================================
# window_host = Window.partitionBy("coa_iso").orderBy("year")

# host_panel = host_panel.withColumn(
#     "hosted_stock_lag1",
#     F.lag("total_hosted_stock", 1).over(window_host)
# )

# host_panel = host_panel.withColumn(
#     "growth_rate",
#     F.when(F.col("hosted_stock_lag1").isNull(), F.lit(None).cast("double"))
#      .when((F.col("hosted_stock_lag1") == 0) & (F.col("total_hosted_stock") == 0), F.lit(0.0))
#      .when((F.col("hosted_stock_lag1") == 0) & (F.col("total_hosted_stock") > 0), F.lit(None).cast("double"))
#      .otherwise(
#         (F.col("total_hosted_stock") - F.col("hosted_stock_lag1")) / F.col("hosted_stock_lag1")
#      )
# )

# # =====================================================================
# # STEP 5 — SCRITTURA SU DELTA LAKE
# # =====================================================================
# gold_host_aggregates = host_panel.select(
#     "coa_iso",
#     "year",
#     "total_hosted_stock",
#     "hosted_stock_lag1",
#     "total_inflows",
#     "total_outflows",
#     "growth_rate",
# )

# initialize_delta_table(
#     spark=spark,
#     db_name="gold",
#     table_name="gold_host_aggregates"
# )

# (
#     gold_host_aggregates.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .save("s3a://lakehouse/gold/gold_host_aggregates")
# )

# spark.stop()














"""
=====================================================================
 GOLD LAYER — Host Aggregates (coa_iso, year)
=====================================================================
Obiettivo:
Aggregare gold_displacement per singolo paese (coa_iso) per ottenere 
una serie storica completa di stock, inflows e outflows per host-year.

In questa versione, a differenza della precedente, NON viene applicato 
il filtro (coo_iso != coa_iso). Si calcola la pressione TOTALE 
territoriale, includendo sia l'accoglienza dall'estero (Rifugiati, 
Asilo, OIP) sia le crisi di sfollamento interno (IDPs).

Viene utilizzata una "griglia densa" nativa in Spark (Sequence+Explode)
per garantire continuità temporale e affidabilità matematica nel calcolo 
del Growth Rate (Momentum) e dei Lag.
=====================================================================
"""

import sys
import os
import pyspark.sql.functions as F
from pyspark.sql.window import Window

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

spark = get_spark_session("Gold-HostAggregates")

# =====================================================================
# STEP 1 — LETTURA E AGGREGAZIONE (Nessun Filtro Origine)
# =====================================================================
displacement_df = spark.read.format("delta").load("s3a://lakehouse/gold/gold_displacement")

# Raggruppiamo su tutto il dataset, catturando sia rotte estere che interne
host_year_sums = displacement_df.groupBy("coa_iso", "year").agg(
    
    # Spacchettamento delle 4 categorie
    F.sum("refugees").alias("refugees_count"),
    F.sum("asylum_seekers").alias("asylum_seekers_count"),
    F.sum("oip").alias("oip_count"),
    F.sum("idps").alias("idps_count"),
    
    # Totali pre-calcolati a livello di rotta (comprensivi di IDPs)
    F.sum("stock").alias("total_hosted_stock"),
    F.sum("inflows").alias("total_inflows"),
    F.sum("outflows").alias("total_outflows"),
)

# =====================================================================
# STEP 2 — GRIGLIA DENSA NATIVA SPARK (Sequence + Explode)
# =====================================================================
# Estraiamo gli estremi temporali globali
year_bounds = displacement_df.agg(
    F.min("year").cast("integer").alias("min_year"),
    F.max("year").cast("integer").alias("max_year")
)

# Prendiamo tutti i paesi unici registrati
all_hosts_df = host_year_sums.select("coa_iso").distinct()

# Generiamo l'array continuo di anni e lo esplodiamo in righe
dense_grid = (
    all_hosts_df.crossJoin(F.broadcast(year_bounds))
    .withColumn("year_array", F.expr("sequence(min_year, max_year, 1)"))
    .withColumn("year", F.explode("year_array"))
    .drop("min_year", "max_year", "year_array")
)

# =====================================================================
# STEP 3 — JOIN E RIEMPIMENTO A ZERO
# =====================================================================
# Uniamo i dati aggregati alla griglia. Le righe "mancanti" diventano veri e propri 0
host_panel = (
    dense_grid
    .join(host_year_sums, ["coa_iso", "year"], "left")
    .fillna(0, subset=[
        "refugees_count", "asylum_seekers_count", "oip_count", "idps_count",
        "total_hosted_stock", "total_inflows", "total_outflows"
    ])
)

# =====================================================================
# STEP 4 — LAG E GROWTH RATE (Momentum)
# =====================================================================
window_host = Window.partitionBy("coa_iso").orderBy("year")

host_panel = host_panel.withColumn(
    "hosted_stock_lag1",
    F.lag("total_hosted_stock", 1).over(window_host)
)

host_panel = host_panel.withColumn(
    "growth_rate",
    F.when(F.col("hosted_stock_lag1").isNull(), F.lit(None).cast("double"))
     .when((F.col("hosted_stock_lag1") == 0) & (F.col("total_hosted_stock") == 0), F.lit(0.0))
     .when((F.col("hosted_stock_lag1") == 0) & (F.col("total_hosted_stock") > 0), F.lit(None).cast("double"))
     .otherwise(
        (F.col("total_hosted_stock") - F.col("hosted_stock_lag1")) / F.col("hosted_stock_lag1")
     )
)

# =====================================================================
# STEP 5 — SELEZIONE E SCRITTURA SU DELTA LAKE
# =====================================================================
gold_host_aggregates = host_panel.select(
    "coa_iso",
    "year",
    "refugees_count",
    "asylum_seekers_count",
    "oip_count",
    "idps_count",
    "total_hosted_stock",
    "hosted_stock_lag1",
    "total_inflows",
    "total_outflows",
    "growth_rate",
)

initialize_delta_table(
    spark=spark,
    db_name="gold",
    table_name="gold_host_aggregates"
)

print("Scrittura della tabella gold_host_aggregates (Total Territorial) su Delta Lake...")
(
    gold_host_aggregates.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("s3a://lakehouse/gold/gold_host_aggregates")
)

print("Tabella gold_host_aggregates generata con successo.")
spark.stop()