# import sys
# import os
# import pyspark.sql.functions as F

# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)

# from utilities import get_spark_session, initialize_delta_table

# spark = get_spark_session("SilverToGold-FoodSecurity")

# print("Lettura della tabella Silver Food Security...")
# fs_silver_df = spark.read.format("delta").load("s3a://lakehouse/silver/foodsecurity")

# # 1. Estraiamo DIRETTAMENTE la riga di riepilogo '3+' 
# # (Più affidabile in caso di omissioni delle singole fasi da parte dell'API)
# fs_critical = fs_silver_df.filter(F.col("ipc_phase") == "3+")

# # 2. Estraiamo l'anno dalla data di inizio validità del dato
# fs_critical = fs_critical.withColumn("year", F.year("reference_period_start"))

# # 3. Estrazione del totale nazionale per singola indagine (assessment)
# # Essendoci già una sola riga '3+' per survey, usiamo F.max() al posto di F.sum() 
# # per prevenire qualsiasi rischio di duplicazione accidentale
# survey_totals = fs_critical.groupBy(
#     "location_code", "year", "resource_hdx_id", "ipc_type"
# ).agg(
#     F.max("population_in_phase").alias("total_critical_pop")
# )

# # 4. Aggregazione finale per Anno e Paese
# # Se ci sono più indagini in un anno, prendiamo il picco massimo (la situazione peggiore)
# gold_food_security = survey_totals.groupBy("location_code", "year").agg(
#     F.max("total_critical_pop").alias("peak_population_phase3plus")
# )

# initialize_delta_table(
#     spark=spark,
#     db_name="gold",
#     table_name="gold_food_security_features"
# )

# print("Scrittura su Delta Lake...")
# (
#     gold_food_security.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .save("s3a://lakehouse/gold/gold_food_security_features")
# )

# print("Tabella gold_food_security_features generata con successo.")
# spark.stop()

""" The World Food Programme serves people in IPC stage 3 or worse. These populations are facing food 
crises and urgently need food assistance to survive and then recover in the long term"""

import sys
import os
import pyspark.sql.functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# 1. Aggiungiamo l'import della tua funzione explode_date_range_to_years
from utilities import get_spark_session, initialize_delta_table, explode_date_range_to_years

spark = get_spark_session("SilverToGold-FoodSecurity")

print("Lettura della tabella Silver Food Security...")
fs_silver_df = spark.read.format("delta").load("s3a://lakehouse/silver/foodsecurity")

# 2. Filtriamo DIRETTAMENTE la riga di riepilogo '3+' 
# (Risolve i vuoti nei dati e previene il doppio conteggio)
fs_critical = fs_silver_df.filter(F.col("ipc_phase") == "3+")

# 3. Esplosione Temporale: usiamo la tua funzione per gestire gli anni a cavallo!
# Se una survey va da Nov 2021 a Mar 2022, creerà una riga per il 2021 e una per il 2022
fs_critical = explode_date_range_to_years(
    fs_critical, 
    start_col="reference_period_start", 
    end_col="reference_period_end"
)

# 4. Aggregazione Nazionale Annuale (Estrazione del Picco)
# Raggruppiamo per Paese e Anno, e prendiamo il picco massimo registrato.
# Avendo già filtrato solo per "3+", non ci serve più raggruppare per resource_hdx_id.
gold_food_security = fs_critical.groupBy("location_code", "year").agg(
    F.max("population_in_phase").alias("peak_population_phase3plus")
)

initialize_delta_table(
    spark=spark,
    db_name="gold",
    table_name="gold_food_security_features"
)

print("Scrittura su Delta Lake...")
(
    gold_food_security.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("s3a://lakehouse/gold/gold_food_security_features")
)

print("Tabella gold_food_security_features generata con successo.")
spark.stop()