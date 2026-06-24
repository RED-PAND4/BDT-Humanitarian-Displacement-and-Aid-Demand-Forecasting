import sys
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session

# Inizializzazione Sessione
spark = get_spark_session("SilverToGold_AidDemand_Prioritization")

def process_aid_demand_model(spark):
    """
    Costruzione della tabella dei fatti per la stima della Likely Aid Demand.
    """
    print("Elaborazione Modello Likely Aid Demand (Macro-Livello)...")
    
    # Lettura tabelle Silver necessarie
    needs_df = spark.read.format("delta").load("s3a://lakehouse/silver/humanitarian_needs")
    pop_df = spark.read.format("delta").load("s3a://lakehouse/silver/population")
    idps_df = spark.read.format("delta").load("s3a://lakehouse/silver/idps")
    food_df = spark.read.format("delta").load("s3a://lakehouse/silver/foodsecurity")
    risk_df = spark.read.format("delta").load("s3a://lakehouse/silver/national_risk")

    # Standardizzazione base: Anno e Country Code per tutte le tabelle
    needs_df = needs_df.withColumn("year", F.year(F.col("reference_period_start"))).withColumnRenamed("location_code", "country_code")
    idps_df = idps_df.withColumn("year", F.year(F.col("reference_period_start"))).withColumnRenamed("location_code", "country_code")
    food_df = food_df.withColumn("year", F.year(F.col("reference_period_start"))).withColumnRenamed("location_code", "country_code")
    risk_df = risk_df.withColumn("year", F.year(F.col("reference_period_start"))).withColumnRenamed("location_code", "country_code")
    pop_df = pop_df.withColumnRenamed("coa_iso", "country_code") # Analizziamo il paese ospitante (Asylum)

    # ---------------------------------------------------------
    # PARTE 1: La Variabile Target (Y) - Likely Aid Demand
    # Filtro: population_status = 'INN' e sector_code = 'Intersectoral'
    # ---------------------------------------------------------
    target_y = needs_df.filter((F.col("population_status") == "INN") & (F.col("sector_code") == "Intersectoral")) \
                       .groupBy("country_code", "year") \
                       .agg(F.sum("population").alias("target_inn_population"))

    # ---------------------------------------------------------
    # PARTE 2: Pilastro A - Shock Demografico (L'Innesco)
    # ---------------------------------------------------------
    # 1. Flussi Transfrontalieri (UNHCR) - Calcolo del Delta (Flusso Netto)
    window_spec = Window.partitionBy("country_code").orderBy("year")
    unhcr_agg = pop_df.groupBy("country_code", "year") \
                      .agg((F.sum("refugees") + F.sum("asylum_seekers")).alias("total_hosted_stock"))
    
    unhcr_agg = unhcr_agg.withColumn("prev_hosted_stock", F.lag("total_hosted_stock", 1).over(window_spec)) \
                         .withColumn("crossborder_inflow_delta", 
                                     F.when(F.col("prev_hosted_stock").isNotNull(), 
                                            F.col("total_hosted_stock") - F.col("prev_hosted_stock"))
                                      .otherwise(0))

    # 2. Flussi Interni (IDPs)
    idps_agg = idps_df.groupBy("country_code", "year") \
                      .agg(F.sum("population").alias("idps_stock"))

    # Creazione del DPI (Demographic Pressure Index)
    # Usiamo outer join per coprire paesi che hanno solo IDP o solo Rifugiati
    demographic_shock = unhcr_agg.join(idps_agg, ["country_code", "year"], "outer") \
                                 .fillna(0, subset=["crossborder_inflow_delta", "idps_stock"]) \
                                 .withColumn("x1_demographic_pressure_index", 
                                             F.col("crossborder_inflow_delta") + F.col("idps_stock"))

    # ---------------------------------------------------------
    # PARTE 2: Pilastro B - Capacità di Assorbimento (Fragilità)
    # ---------------------------------------------------------
    # 1. Vulnerabilità Acuta (Food Security IPC >= 3)
    # L'IPC usa spesso numeri in stringa. "3", "4", "5" rappresentano Crisi, Emergenza e Carestia.
    acute_vuln = food_df.filter(F.col("ipc_phase").isin("3", "4", "5")) \
                        .groupBy("country_code", "year") \
                        .agg(F.sum("population_fraction_in_phase").alias("x2_ipc_phase3plus_fraction"))

    # 2. Vulnerabilità Strutturale (National Risk Coping Capacity)
    # Nota: Dallo schema in bronze/kafka-to-bronze-national_risk.py il campo si chiama coping_capacity_risk
    struct_vuln = risk_df.groupBy("country_code", "year") \
                         .agg(F.max("coping_capacity_risk").alias("x3_coping_capacity_risk"))

    # ---------------------------------------------------------
    # PARTE 3: Modello Analitico (Tabella dei Fatti Consolidata)
    # ---------------------------------------------------------
    master_model_df = target_y.join(demographic_shock.select("country_code", "year", "x1_demographic_pressure_index"), ["country_code", "year"], "left") \
                              .join(acute_vuln, ["country_code", "year"], "left") \
                              .join(struct_vuln, ["country_code", "year"], "left") \
                              .fillna(0) # Sostituisce null con 0 per le variabili indipendenti

    # Scrittura su Gold
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.model_likely_aid_demand
        USING delta
        LOCATION 's3a://lakehouse/gold/model_likely_aid_demand'
    """)
    master_model_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/model_likely_aid_demand")
    print("Tabella model_likely_aid_demand salvata con successo.")


def process_sectoral_gap(spark):
    """
    Allocazione e Gap Analitico Settoriale a livello nazionale.
    """
    print("Elaborazione Resource Prioritization (Micro-Livello Settoriale)...")
    
    needs_df = spark.read.format("delta").load("s3a://lakehouse/silver/humanitarian_needs")
    op_presence_df = spark.read.format("delta").load("s3a://lakehouse/silver/operational_presence")

    # Standardizzazione anno per il matching
    needs_df = needs_df.withColumn("year", F.year(F.col("reference_period_start")))
    op_presence_df = op_presence_df.withColumn("year", F.year(F.col("reference_period_start")))

    # 1. Misurare la Domanda Specifica (Escludendo Intersectoral)
    # Livello di granularità: country, admin1 (Provincia), year, sector
    demand_df = needs_df.filter((F.col("population_status") == "INN") & (F.col("sector_code") != "Intersectoral")) \
                        .groupBy("location_code", "year", "sector_code") \
                        .agg(F.sum("population").alias("sector_inn_population"))

    # 2. Misurare l'Offerta Operativa (Numero di Organizzazioni attive)
    # Contiamo quante organizzazioni distinte (acronym) operano in quel settore e in quella provincia
    supply_df = op_presence_df.filter(F.col("sector_code").isNotNull()) \
                              .groupBy("location_code", "year", "sector_code") \
                              .agg(F.countDistinct("org_acronym").alias("active_organizations_count"))

    # 3. Calcolare il Gap di Copertura (La Priorità)
    # Full outer join per trovare i "Blind Spot" (Domanda senza offerta) o gli "Oversupply" (Offerta senza domanda critica)
    gap_df = demand_df.join(supply_df, ["location_code", "year", "sector_code"], "full_outer") \
                      .fillna(0, subset=["sector_inn_population", "active_organizations_count"])

    # Formula: Priorità = INN / Organizzazioni. 
    # Gestione del divieto di divisione per zero: se ci sono 0 organizzazioni, il punteggio di priorità schizza al valore della popolazione stessa (Massima gravità).
    gap_df = gap_df.withColumn("sectoral_priority_score", 
                               F.when(F.col("active_organizations_count") == 0, F.col("sector_inn_population"))
                                .otherwise(F.col("sector_inn_population") / F.col("active_organizations_count")))

    # Scrittura su Gold
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.priority_sectoral_gap
        USING delta
        LOCATION 's3a://lakehouse/gold/priority_sectoral_gap'
    """)
    gap_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/priority_sectoral_gap")
    print("Tabella priority_sectoral_gap salvata con successo.")


if __name__ == "__main__":
    process_aid_demand_model(spark)
    process_sectoral_gap(spark)
    print("Pipeline Silver-to-Gold completata.")