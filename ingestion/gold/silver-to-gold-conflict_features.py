import sys
import os
import pyspark.sql.functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

# ==========================================
# 1. Initialization Spark Session
# ==========================================
spark = get_spark_session("SilverToGold-ConflictEvents")

# ==========================================
# 2. Reading the Silver Table
# ==========================================
conflict_silver_df = spark.read.format("delta").load("s3a://lakehouse/silver/conflict_events")

# 3. Pulizia preliminare: gestiamo i NULL e l'estrazione dell'anno
conflict_clean = conflict_silver_df \
    .withColumn("year", F.year("reference_period_start")) \
    .fillna({"fatalities": 0, "events": 0})

# ==========================================
# 4. Annual National Aggregation (No Double Counting)
# ==========================================
gold_conflict_features = conflict_clean.groupBy("location_code", "year").agg(
    
    # Total Fatalities: prese SOLO da political_violence per evitare il doppio conteggio
    F.sum(
        F.when(F.col("event_type") == "political_violence", F.col("fatalities"))
        .otherwise(0)
    ).alias("total_fatalities"),
    
    # Eventi Violenti Totali: presi SOLO da political_violence
    F.sum(
        F.when(F.col("event_type") == "political_violence", F.col("events"))
        .otherwise(0)
    ).alias("violent_events"),
    
    # Sotto-Feature specifica: misura la quota di violenza mirata ai civili (ottima feature per Random Forest)
    F.sum(
        F.when(F.col("event_type") == "civilian_targeting", F.col("events"))
        .otherwise(0)
    ).alias("civilian_targeting_events"),

    # Sotto-Feature specifica: morti civili dirette
    F.sum(
        F.when(F.col("event_type") == "civilian_targeting", F.col("fatalities"))
        .otherwise(0)
    ).alias("civilian_targeting_fatalities"),
    
    # Eventi Non Violenti: manifestazioni e proteste
    F.sum(
        F.when(F.col("event_type") == "demonstration", F.col("events"))
        .otherwise(0)
    ).alias("non_violent_events")
)

# ==========================================
# 5. Initialization and Writing in the Gold Layer
# ==========================================
initialize_delta_table(
    spark=spark,
    db_name="gold",
    table_name="gold_conflict_features"
)

(
    gold_conflict_features.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("s3a://lakehouse/gold/gold_conflict_features")
)

print("Tabella gold_conflict_features aggiornata con successo senza sovrapposizioni.")
spark.stop()