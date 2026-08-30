"""
===============================================================================
ML LAYER — Host Pressure Tier Forward Forecast (t+1)
===============================================================================
Purpose:
This script trains a Machine Learning classification model (Random Forest) 
on historical host pressure metrics to predict the categorical pressure tier 
(pressure_per_capita_tier: Low, Medium, High, Critical) for the upcoming year (t+1).

Architecture & Integration:
1. Target Selection:
   - Target: 'pressure_per_capita_tier' (t+1) shifted via lead window function.
   - Rationale: Avoids instability and zero-division artifacts of growth rates 
     while maintaining continuity on the primary dashboard indicator.
2. Feature Space (Host-Side Baseline):
   - Demographic & economic ratios: pressure_per_capita, pressure_per_gdp_per_capita.
   - Dynamics & Context: growth_rate, funding_gap, gdp_per_capita, HRP/GHO flags.
3. Class Imbalance Mitigation:
   - Dynamic inverse class frequency weighting (class_weight) applied during training 
     to ensure high recall on rare, critical displacement states.
4. Out-of-Sample Temporal Validation:
   - Evaluated on the last two known historical years using Macro-F1 and Critical Recall.
5. Schema-Conforming Idempotent Write (Delete + Append):
   - Forecast records populate keys (coa_iso, coa_name, year), is_forecast=True, 
     and the predicted tier. Underlying raw numerical volumes remain NULL.
   - Existing forecast records (is_forecast == True) are purged via Delta Lake 
     API prior to appending new projections, preventing duplicate rows across runs.
===============================================================================
"""

import sys
import os
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from delta.tables import DeltaTable

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session

TABLE_PATH = "s3a://lakehouse/gold/gold_host_pressure_indices"
TIER_TO_LABEL = {"Low": 0.0, "Medium": 1.0, "High": 2.0, "Critical": 3.0}


def tier_to_numeric(tier_col_name):
    """Encodes categorical tiers into numerical class labels."""
    return (
        F.when(F.col(tier_col_name) == "Low", 0.0)
         .when(F.col(tier_col_name) == "Medium", 1.0)
         .when(F.col(tier_col_name) == "High", 2.0)
         .when(F.col(tier_col_name) == "Critical", 3.0)
    )


if __name__ == "__main__":
    # ==========================================
    # PHASE 1: Initialize Spark Session
    # ==========================================
    spark = get_spark_session("ML-HostPressureForecast")

    # ==========================================
    # PHASE 2: Read Historical Gold Dataset
    # ==========================================
    print(f"Reading target gold table from {TABLE_PATH}...")
    full_table_df = spark.read.format("delta").load(TABLE_PATH)
    target_schema = full_table_df.schema
    historical_df = full_table_df.filter(F.col("is_forecast") == False)

    # ==========================================
    # PHASE 3: Encode Target & Temporal Lead Alignment (t+1)
    # ==========================================
    historical_df = historical_df.withColumn(
        "pressure_tier_numeric",
        tier_to_numeric("pressure_per_capita_tier")
    )

    # Create forward-looking label (state at t+1) partitioned by host country
    window_by_host = Window.partitionBy("coa_iso").orderBy("year")
    historical_df = (
        historical_df
        .withColumn("label", F.lead("pressure_tier_numeric", 1).over(window_by_host))
        .withColumn("target_year", F.col("year") + 1)
    )

    # ==========================================
    # PHASE 4: Feature Engineering & Preprocessing
    # ==========================================
    for bool_col in ["has_funding_data", "has_hrp", "in_gho"]:
        if bool_col in historical_df.columns:
            historical_df = historical_df.withColumn(bool_col, F.col(bool_col).cast("double"))

    numeric_cols_with_missing = [
        "growth_rate",
        "pressure_per_gdp_per_capita",
        "funding_gap",
        "gdp_per_capita",
        "pressure_tier_numeric"
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
        "gdp_per_capita"
    ]

    # Filter out rows where label is NULL (e.g., latest observed historical year)
    labelled_df = historical_df.filter(F.col("label").isNotNull())

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    model_data = assembler.transform(labelled_df)

    # ==========================================
    # PHASE 5: Temporal Split (Train / Test) & Class Balancing
    # ==========================================
    max_target_year = labelled_df.agg(F.max("target_year")).collect()[0][0]

    train_data = model_data.filter(F.col("target_year") < max_target_year - 1)
    test_data = model_data.filter(F.col("target_year") >= max_target_year - 1)

    print(f"Max target year in labeled set: {max_target_year}")
    print(f"Training set count: {train_data.count()} rows (target_year < {max_target_year - 1})")
    print(f"Test set count: {test_data.count()} rows (target_year >= {max_target_year - 1})")

    # Calculate inverse class frequency weights on training data to balance rare risk tiers
    class_counts = train_data.groupBy("label").count().collect()
    total_train_rows = train_data.count()
    num_classes = len(class_counts)

    weight_mapping = {
        row["label"]: total_train_rows / (num_classes * row["count"])
        for row in class_counts
    }

    weight_expr = F.lit(1.0)
    for label_val, weight_val in weight_mapping.items():
        weight_expr = F.when(F.col("label") == label_val, F.lit(weight_val)).otherwise(weight_expr)

    train_data = train_data.withColumn("class_weight", weight_expr)

    # ==========================================
    # PHASE 6: Model Training (Balanced Random Forest)
    # ==========================================
    print("Training Random Forest Classifier with class weights...")
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        weightCol="class_weight",
        numTrees=100,
        maxDepth=6,
        minInstancesPerNode=5,
        seed=42
    )
    rf_model = rf.fit(train_data)

    # ==========================================
    # PHASE 7: Out-of-Sample Evaluation
    # ==========================================
    predictions = rf_model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

    per_class_f1 = {
        tier: evaluator.evaluate(
            predictions,
            {evaluator.metricName: "fMeasureByLabel", evaluator.metricLabel: label}
        )
        for tier, label in TIER_TO_LABEL.items()
    }
    f1_macro = sum(per_class_f1.values()) / len(per_class_f1)
    recall_critical = evaluator.evaluate(
        predictions,
        {evaluator.metricName: "recallByLabel", evaluator.metricLabel: TIER_TO_LABEL["Critical"]}
    )

    print("\n" + "=" * 50)
    print("OUT-OF-SAMPLE EVALUATION METRICS")
    print("=" * 50)
    for tier, f1 in per_class_f1.items():
        print(f"F1 Score '{tier}': {f1:.4f}")
    print(f"Macro F1 Score:      {f1_macro:.4f}")
    print(f"Recall 'Critical':   {recall_critical:.4f}")

    print("\nConfusion Matrix (Actual vs Predicted):")
    predictions.stat.crosstab("label", "prediction").orderBy("label_prediction").show()

    importances = rf_model.featureImportances.toArray()
    print("Feature Importances:")
    for feat, imp in sorted(zip(feature_cols, importances), key=lambda x: -x[1]):
        print(f"  - {feat:<30}: {imp:.4f}")

    # ==========================================
    # PHASE 8: Generate Final Forecast (Latest Year + 1)
    # ==========================================
    latest_year_available = historical_df.agg(F.max("year")).collect()[0][0]
    forecast_year = latest_year_available + 1
    print(f"\nGenerating forward forecast for year {forecast_year} (from base year {latest_year_available})...")

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

    print(f"\nPreview of 'Critical' tier predictions for {forecast_year}:")
    forecast_predictions.filter(F.col("predicted_tier") == "Critical") \
        .select("coa_iso", "coa_name", "predicted_tier", "prediction_confidence") \
        .show(truncate=False)

    # ==========================================
    # PHASE 9: Construct Conforming Forecast Records
    # ==========================================
    select_exprs = []
    for field in target_schema.fields:
        if field.name == "coa_iso":
            select_exprs.append(F.col("coa_iso"))
        elif field.name == "coa_name":
            select_exprs.append(F.col("coa_name"))
        elif field.name == "year":
            select_exprs.append(F.lit(forecast_year).cast(field.dataType).alias("year"))
        elif field.name == "is_forecast":
            select_exprs.append(F.lit(True).alias("is_forecast"))
        elif field.name == "pressure_per_capita_tier":
            select_exprs.append(F.col("predicted_tier").alias("pressure_per_capita_tier"))
        else:
            select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))

    forecast_rows_df = forecast_predictions.select(*select_exprs)

    # ==========================================
    # PHASE 10: Idempotent Write (Delete Stale Forecast + Append)
    # ==========================================
    print(f"\nPurging previous forecast records (is_forecast = True) from {TABLE_PATH}...")
    delta_table = DeltaTable.forPath(spark, TABLE_PATH)
    delta_table.delete(F.col("is_forecast") == True)

    print("Appending new forecast records to Delta table...")
    (
        forecast_rows_df.write
        .format("delta")
        .mode("append")
        .save(TABLE_PATH)
    )

    print(f"Forecast for year {forecast_year} successfully committed to {TABLE_PATH}")

    # ==========================================
    # Shutdown Spark Session
    # ==========================================
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)