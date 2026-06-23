from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message

# Using your existing session generator
spark = get_spark_session("Dashboard-Expanded-Pipeline")

def load_expanded_silver_tables(spark):
    """Loads core displacement and expanded contextual datasets from the Silver Delta layer."""
    print("Reading expanded Silver Layer tables...")
    base = "s3a://lakehouse/silver"
    
    # 1. UNHCR Displacement Data
    refugees_df = spark.read.format("delta").load(f"{base}/population")
    idps_df = spark.read.format("delta").load(f"{base}/idps") 
    
    # 2. HDX HAPI Contextual Risk & Economics
    conflict_df = spark.read.format("delta").load("s3a://lakehouse/bronze/conflict_events")
    food_sec_df = spark.read.format("delta").load(f"{base}/foodsecurity")
    poverty_df = spark.read.format("delta").load(f"{base}/povertyrate") 
    
    # 3. HDX HAPI Infrastructure & Finance
    population_df = spark.read.format("delta").load(f"{base}/baselinepopulation")
    funding_df = spark.read.format("delta").load("s3a://lakehouse/bronze/funding") 
    
    return refugees_df, idps_df, conflict_df, food_sec_df, poverty_df, population_df, funding_df

def process_dimensions_and_facts(refugees_df, idps_df, conflict_df, food_sec_df, poverty_df, population_df, funding_df):
    """Processes raw tables into structured Fact and Dimension dataframes for BI ingestion."""
    
    # --- A. AGGREGATE BURDEN (Fact Table Base) ---
    # Combine Refugees and Internally Displaced Persons (IDPs)
    refugees_agg = refugees_df.groupBy("coa_iso", "year").agg(
        (F.sum("refugees") + F.sum("asylum_seekers")).alias("cross_border_displaced")
    )
    idps_agg = idps_df.withColumn("year", F.year(F.col("reference_period_start"))).groupBy("location_code", "year").agg(
        F.sum("population").alias("internal_displaced")
    )
    
    # --- B. AGGREGATE VULNERABILITY (Dimension Table Base) ---
    conflict_agg = conflict_df.withColumn("year", F.year(F.col("reference_period_start"))).groupBy("location_code", "year").agg(F.sum("events").alias("conflict_events"))
    food_agg = food_sec_df.withColumn("year", F.year(F.col("reference_period_start"))).groupBy("location_code", "year").agg(F.avg("population_in_phase").alias("avg_food_phase"))
    poverty_agg = poverty_df.withColumn("year", F.year(F.col("reference_period_start"))).groupBy("location_code", "year").agg(F.avg("mpi").alias("multidimensional_poverty_index"))
    pop_agg = population_df.withColumn("year", F.year(F.col("reference_period_start"))).groupBy("location_code", "year").agg(F.sum("population").alias("total_host_population"))
    
    # --- C. AGGREGATE FUNDING (Fact Table Base) ---
    funding_agg = funding_df.withColumn("year", F.year(F.col("reference_period_start"))).groupBy("location_code", "year").agg(
        F.sum("funding_usd").alias("total_usd_funding")
    )

    # --- BUILD GOLD TABLES ---
    
    # 1. Dim Vulnerability Context (Contextual Country Metrics)
    dim_vulnerability = pop_agg \
        .join(conflict_agg, ["location_code", "year"], "left") \
        .join(food_agg, ["location_code", "year"], "left") \
        .join(poverty_agg, ["location_code", "year"], "left") \
        .fillna(0) # Clean nulls for ML
        
    # 2. Fact Displacement & Funding (Core Metrics)
    fact_displacement = refugees_agg.withColumnRenamed("coa_iso", "location_code") \
        .join(idps_agg, ["location_code", "year"], "outer") \
        .join(funding_agg, ["location_code", "year"], "left") \
        .fillna(0) \
        .withColumn("total_displaced_burden", F.col("cross_border_displaced") + F.col("internal_displaced")) \
        .withColumn("funding_per_capita", 
                    F.when(F.col("total_displaced_burden") > 0, F.col("total_usd_funding") / F.col("total_displaced_burden"))
                     .otherwise(0))
        
    return fact_displacement, dim_vulnerability

# def calculate_advanced_metrics(fact_df, dim_df):
#     """Applies Forecasting and K-Means Clustering to create the Unified Dashboard Matrix."""
    
#     # Join Facts and Dimensions for modeling
#     unified_df = fact_df.join(dim_df, ["location_code", "year"], "inner")
    
#     # 1. Forecasting: Year-Over-Year Velocity
#     window_spec = Window.partitionBy("location_code").orderBy("year")
#     unified_df = unified_df.withColumn("prev_burden", F.lag("total_displaced_burden", 1).over(window_spec)) \
#         .withColumn("growth_rate", 
#                     F.when(F.col("prev_burden") > 0, (F.col("total_displaced_burden") - F.col("prev_burden")) / F.col("prev_burden"))
#                      .otherwise(0.0)) \
#         .withColumn("forecasted_burden_next_year", F.greatest(F.col("total_displaced_burden") * (1 + F.col("growth_rate")), F.lit(0.0)))
    
#     # 2. Machine Learning: Risk Clustering
#     features = ["total_displaced_burden", "conflict_events", "avg_food_phase", "multidimensional_poverty_index", "funding_per_capita"]
    
#     assembler = VectorAssembler(inputCols=features, outputCol="raw_features")
#     assembled_df = assembler.transform(unified_df)
    
#     scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
#     scaled_df = scaler.fit(assembled_df).transform(assembled_df)
    
#     # K-Means to identify 4 operational archetypes (e.g., Well-Funded Stable, Underfunded Crisis, etc.)
#     kmeans = KMeans(featuresCol="features", predictionCol="risk_cluster", k=4, seed=42)
#     clustered_df = kmeans.fit(scaled_df).transform(scaled_df).drop("raw_features", "features", "prev_burden")
    
#     # 3. Prioritization Rank
#     clustered_df = clustered_df.withColumn(
#         "pressure_score",
#         (F.col("total_displaced_burden") / F.col("total_host_population")) * 1000 + # Displaced per 1000 inhabitants
#         (F.col("conflict_events") * 0.5) +
#         (F.col("avg_food_phase") * 10) -
#         (F.col("funding_per_capita") * 0.1) # Higher funding reduces pressure score
#     )
    
#     rank_window = Window.partitionBy("year").orderBy(F.col("pressure_score").desc())
#     final_matrix = clustered_df.withColumn("global_priority_rank", F.dense_rank().over(rank_window))
    
#     return final_matrix

def calculate_advanced_metrics(fact_df, dim_df):
    """Applies Forecasting and K-Means Clustering to create the Unified Dashboard Matrix."""
    
    # Join Facts and Dimensions for modeling
    unified_df = fact_df.join(dim_df, ["location_code", "year"], "inner")
    
    # 1. Forecasting: Year-Over-Year Velocity
    window_spec = Window.partitionBy("location_code").orderBy("year")
    unified_df = unified_df.withColumn("prev_burden", F.lag("total_displaced_burden", 1).over(window_spec)) \
        .withColumn("growth_rate", 
                    F.when(F.col("prev_burden") > 0, (F.col("total_displaced_burden") - F.col("prev_burden")) / F.col("prev_burden"))
                     .otherwise(0.0)) \
        .withColumn("forecasted_burden_next_year", F.greatest(F.col("total_displaced_burden") * (1 + F.col("growth_rate")), F.lit(0.0)))
    
    # --- FIX STARTS HERE ---
    features = ["total_displaced_burden", "conflict_events", "avg_food_phase", "multidimensional_poverty_index", "funding_per_capita"]
    
    # Ensure all ML features are strictly numerical (Double) and explicitly drop any remaining Nulls/NaNs
    for col_name in features:
        unified_df = unified_df.withColumn(col_name, F.col(col_name).cast("double"))
    
    # Spark MLlib will crash if any nulls remain. This is a hard requirement before VectorAssembler.
    ml_ready_df = unified_df.dropna(subset=features)
    
    # 2. Machine Learning: Risk Clustering
    # Added handleInvalid="skip" as a safeguard. If a malformed row arrives, Spark skips it instead of failing the job.
    assembler = VectorAssembler(inputCols=features, outputCol="raw_features", handleInvalid="skip")
    assembled_df = assembler.transform(ml_ready_df)
    
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    
    # The fit() method will now execute safely over cleaned, strictly double-typed data.
    scaler_model = scaler.fit(assembled_df)
    scaled_df = scaler_model.transform(assembled_df)
    # --- FIX ENDS HERE ---
    
    # K-Means to identify 4 operational archetypes
    kmeans = KMeans(featuresCol="features", predictionCol="risk_cluster", k=4, seed=42)
    clustered_df = kmeans.fit(scaled_df).transform(scaled_df).drop("raw_features", "features", "prev_burden")
    
    # 3. Prioritization Rank
    clustered_df = clustered_df.withColumn(
        "pressure_score",
        (F.col("total_displaced_burden") / F.col("total_host_population")) * 1000 + 
        (F.col("conflict_events") * 0.5) +
        (F.col("avg_food_phase") * 10) -
        (F.col("funding_per_capita") * 0.1)
    )
    
    rank_window = Window.partitionBy("year").orderBy(F.col("pressure_score").desc())
    final_matrix = clustered_df.withColumn("global_priority_rank", F.dense_rank().over(rank_window))
    
    return final_matrix

def write_to_gold(df, table_name):
    """Helper to write multiple tables to the Gold database."""
    path = f"s3a://lakehouse/gold/{table_name}"
    print(f"Writing {table_name} to {path}...")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)

# --- PIPELINE EXECUTION ---
if __name__ == "__main__":

    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.fact_displacement_funding
        USING delta
        LOCATION 's3a://lakehouse/gold/fact_displacement_funding'
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.dim_country_vulnerability
        USING delta
        LOCATION 's3a://lakehouse/gold/dim_country_vulnerability'
    """)

    # 1. Extract
    datasets = load_expanded_silver_tables(spark)
    
    # 2. Transform into Star Schema
    fact_displacement, dim_vulnerability = process_dimensions_and_facts(*datasets)
    
    # 3. Apply AI/ML and Forecasting to build the Master View
    #dashboard_master_matrix = calculate_advanced_metrics(fact_displacement, dim_vulnerability)
    
    # 4. Load multiple tables to Gold Database
    # Storing separate tables allows your dashboard to be highly performant and modular
    write_to_gold(fact_displacement, "fact_displacement_funding")
    write_to_gold(dim_vulnerability, "dim_country_vulnerability")
    #write_to_gold(dashboard_master_matrix, "dashboard_unified_priority_matrix")
    
    print("Dashboard data model successfully saved to Gold.")