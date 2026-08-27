"""
=====================================================================
 ML LAYER — Forecast del Tier di Pressione (t+1)
=====================================================================
Obiettivo:
Predire pressure_per_capita_tier (Low/Medium/High/Critical) all'anno
t+1 usando lo stato noto all'anno t, e SCRIVERE il risultato dentro
gold_host_pressure_indices stessa (non una tabella a parte), con
is_forecast=True a distinguere le righe previste da quelle storiche.

Target scelto: pressure_per_capita_tier, non growth_rate — è l'unico
tra i tre indicatori a non avere il problema "divisione per stock_lag1
= 0" discusso per il growth rate, ed è già l'indicatore "headline"
della dashboard: usare lo stesso tier per storico e forecast rende il
grafico continuo senza bisogno di logiche diverse in Superset.

Feature: solo lato host (baseline), coerente con quanto già discusso —
nessun push factor dell'origine in questa versione.

SCRITTURA IDEMPOTENTE (delete + append):
gold_host_pressure_indices.py sovrascrive SEMPRE l'intera tabella con
sole righe storiche. Se questo script girasse più volte, o dopo un
refresh della pipeline principale, le vecchie righe di forecast
andrebbero cancellate esplicitamente prima di riscriverle, altrimenti
si accumulano duplicati. Nel DAG Airflow l'ordine deve restare:
gold_host_pressure_indices (overwrite) -> questo script (delete+append).

Le righe di forecast valorizzano SOLO le chiavi, is_forecast e il tier
previsto: non stiamo prevedendo i numeri grezzi (stock, funding, mpi,
ecc.), quindi restano NULL, coerentemente con l'aver scartato la
regressione sui volumi nel tentativo precedente.
=====================================================================
"""

import sys
import os
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array
# from pyspark.mllib.evaluation import MulticlassMetrics
from delta.tables import DeltaTable

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, initialize_delta_table

spark = get_spark_session("ML-HostPressureForecast")

TABLE_PATH = "s3a://lakehouse/gold/gold_host_pressure_indices"


# =====================================================================
# STEP 1 — LETTURA (solo righe storiche: mai allenare su un forecast
# precedente rimasto in tabella da un run passato)
# =====================================================================
full_table_df = spark.read.format("delta").load(TABLE_PATH)
TARGET_SCHEMA = full_table_df.schema  # serve allo Step 9 per costruire le righe di forecast

historical_df = full_table_df.filter(F.col("is_forecast") == False)


# =====================================================================
# STEP 2 — CODIFICA NUMERICA DEL TIER (ordine fissato manualmente,
# stessa convenzione già usata per il tentativo di classificazione V1)
# =====================================================================
TIER_TO_LABEL = {"Low": 0.0, "Medium": 1.0, "High": 2.0, "Critical": 3.0}


def tier_to_numeric(tier_col_name):
    return (
        F.when(F.col(tier_col_name) == "Low", 0.0)
         .when(F.col(tier_col_name) == "Medium", 1.0)
         .when(F.col(tier_col_name) == "High", 2.0)
         .when(F.col(tier_col_name) == "Critical", 3.0)
    )


historical_df = historical_df.withColumn(
    "pressure_tier_numeric", tier_to_numeric("pressure_per_capita_tier")
)


# =====================================================================
# STEP 3 — SHIFT TEMPORALE: LA LABEL GUARDA UN ANNO AVANTI
# =====================================================================
window_by_host = Window.partitionBy("coa_iso").orderBy("year")

historical_df = (
    historical_df
    .withColumn("label", F.lead("pressure_tier_numeric", 1).over(window_by_host))
    .withColumn("target_year", F.col("year") + 1)
)


# =====================================================================
# STEP 4 — FEATURE (solo lato host)
# =====================================================================
for bool_col in ["has_funding_data", "has_hrp", "in_gho"]:
    historical_df = historical_df.withColumn(bool_col, F.col(bool_col).cast("double"))

numeric_cols_with_missing = [
    "growth_rate", "pressure_per_gdp_per_capita", "funding_gap", "gdp_per_capita", "pressure_tier_numeric"
]
historical_df = historical_df.fillna(-1.0, subset=numeric_cols_with_missing)

feature_cols = [
    "pressure_per_capita",
    "pressure_tier_numeric",
    "pressure_per_gdp_per_capita",
    "growth_rate",
    "funding_gap",
    "has_funding_data",
    "has_hrp",
    "in_gho",
    "gdp_per_capita",
]

# Non possiamo allenare/valutare su righe la cui label è NULL:
# è esattamente l'ultimo anno del pannello, quello che useremo per il
# forecast finale.
labelled_df = historical_df.filter(F.col("label").isNotNull())

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
model_data = assembler.transform(labelled_df)


# =====================================================================
# STEP 5 — SPLIT TEMPORALE (relativo all'ultimo anno disponibile, non
# hardcoded: resta corretto anche quando il dataset si estenderà)
# Test = ultimi due target_year con esito noto, Train = tutto il resto.
# =====================================================================
max_target_year = labelled_df.agg(F.max("target_year")).collect()[0][0]

train_data = model_data.filter(F.col("target_year") < max_target_year - 1)
test_data = model_data.filter(F.col("target_year") >= max_target_year - 1)

print(f"Ultimo target_year con esito noto: {max_target_year}")
print(f"Training set: {train_data.count()} righe (target_year < {max_target_year - 1})")
print(f"Test set: {test_data.count()} righe (target_year >= {max_target_year - 1})")


# =====================================================================
# STEP 6 — ADDESTRAMENTO (stessi iperparametri baseline di V1)
# =====================================================================
rf = RandomForestClassifier(
    featuresCol="features", labelCol="label", predictionCol="prediction",
    numTrees=100, maxDepth=6, minInstancesPerNode=5, seed=42
)
rf_model = rf.fit(train_data)


# =====================================================================
# STEP 7 — VALUTAZIONE
# =====================================================================
predictions = rf_model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

per_class_f1 = {
    tier: evaluator.evaluate(predictions, {evaluator.metricName: "fMeasureByLabel", evaluator.metricLabel: label})
    for tier, label in TIER_TO_LABEL.items()
}
f1_macro = sum(per_class_f1.values()) / len(per_class_f1)
recall_critical = evaluator.evaluate(
    predictions, {evaluator.metricName: "recallByLabel", evaluator.metricLabel: TIER_TO_LABEL["Critical"]}
)

print("\n--- VALUTAZIONE ---")
for tier, f1 in per_class_f1.items():
    print(f"F1 '{tier}': {f1:.4f}")
print(f"F1 Macro: {f1_macro:.4f}")
print(f"Recall 'Critical': {recall_critical:.4f}")



# confusion_metrics = MulticlassMetrics(predictions.select("prediction", "label").rdd.map(tuple))
# print("\nMatrice di confusione (0=Low, 1=Medium, 2=High, 3=Critical):")
# print(confusion_metrics.confusionMatrix().toArray())

print("\nMatrice di confusione (Reale 'label' vs Predetto 'prediction'):")
predictions.stat.crosstab("label", "prediction").orderBy("label_prediction").show()



importances = rf_model.featureImportances.toArray()
print("\n--- IMPORTANZA FEATURE ---")
for feat, imp in sorted(zip(feature_cols, importances), key=lambda x: -x[1]):
    print(f"- {feat}: {imp:.4f}")


# =====================================================================
# STEP 8 — PREVISIONE FINALE (anno successivo all'ultimo disponibile)
# =====================================================================
latest_year_available = historical_df.agg(F.max("year")).collect()[0][0]
forecast_year = latest_year_available + 1
print(f"\nUltimo anno disponibile: {latest_year_available} -> genero il forecast per {forecast_year}")

forecast_input_df = historical_df.filter(F.col("year") == latest_year_available)
forecast_features = assembler.transform(forecast_input_df)
forecast_predictions = rf_model.transform(forecast_features)

label_to_tier_expr = (
    F.when(F.col("prediction") == 0.0, "Low")
     .when(F.col("prediction") == 1.0, "Medium")
     .when(F.col("prediction") == 2.0, "High")
     .when(F.col("prediction") == 3.0, "Critical")
)

forecast_predictions = (
    forecast_predictions
    .withColumn("predicted_tier", label_to_tier_expr)
    .withColumn("prediction_confidence", F.array_max(vector_to_array(F.col("probability"))))
    .withColumn("forecast_year", F.lit(forecast_year))
)

print(f"\nAnteprima previsioni 'Critical' per il {forecast_year} (non salvata in tabella, solo diagnostica):")
forecast_predictions.filter(F.col("predicted_tier") == "Critical") \
    .select("coa_iso", "predicted_tier", "prediction_confidence").show(truncate=False)


# =====================================================================
# STEP 9 — COSTRUZIONE RIGHE DI FORECAST
# Schema costruito dinamicamente da TARGET_SCHEMA: solo coa_iso, year,
# is_forecast e pressure_per_capita_tier vengono valorizzati, tutte le
# altre colonne restano NULL — coerente con il fatto che stiamo
# prevedendo una fascia di rischio, non i numeri grezzi sottostanti.
# =====================================================================
select_exprs = []
for field in TARGET_SCHEMA.fields:
    if field.name == "coa_iso":
        select_exprs.append(F.col("coa_iso"))
    elif field.name == "year":
        select_exprs.append(F.lit(forecast_year).cast(field.dataType).alias("year"))
    elif field.name == "is_forecast":
        select_exprs.append(F.lit(True).alias("is_forecast"))
    elif field.name == "pressure_per_capita_tier":
        select_exprs.append(F.col("predicted_tier").alias("pressure_per_capita_tier"))
    else:
        select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))

forecast_rows_df = forecast_predictions.select(*select_exprs)


# # =====================================================================
# # STEP 10 — SCRITTURA IDEMPOTENTE: DELETE + APPEND
# # =====================================================================
# initialize_delta_table(spark=spark, db_name="gold", table_name="gold_host_pressure_indices")

# print("\nRimozione di eventuali forecast precedenti (idempotenza)...")
# spark.sql("DELETE FROM gold.gold_host_pressure_indices WHERE is_forecast = true")

# print("Scrittura delle nuove righe di forecast (append)...")
# (
#     forecast_rows_df.write
#     .format("delta")
#     .mode("append")
#     .save(TABLE_PATH)
# )

# print(f"Forecast per il {forecast_year} scritto con successo in gold.gold_host_pressure_indices.")

# spark.stop()


# =====================================================================
# STEP 10 — SCRITTURA IDEMPOTENTE: DELETE + APPEND (via API nativa)
# =====================================================================
print("\nRimozione di eventuali forecast precedenti (idempotenza)...")
delta_table = DeltaTable.forPath(spark, TABLE_PATH)
delta_table.delete(F.col("is_forecast") == True)

print("Scrittura delle nuove righe di forecast (append)...")
(
    forecast_rows_df.write
    .format("delta")
    .mode("append")
    .save(TABLE_PATH)
)

print(f"Forecast per il {forecast_year} scritto con successo in {TABLE_PATH}")
spark.stop()
