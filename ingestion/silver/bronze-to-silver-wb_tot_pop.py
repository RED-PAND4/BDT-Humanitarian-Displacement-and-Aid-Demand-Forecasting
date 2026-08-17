from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.functions import col
from pyspark.sql import functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import *

spark = get_spark_session("BronzeToSilver-WorldBank")

# 1. Read Bronze data for today's ingestion
bronze_df = (spark.read
    .format("delta")
    .load("s3a://lakehouse/bronze/worldbank_population")
    #.filter(F.to_date(F.col("ingested_at")) >= F.current_date()) 
)

# 2. Clean the Data
# Eliminiamo i record senza chiavi primarie o senza un valore di popolazione effettivo
cleaned_df = bronze_df.dropna(subset=["location_code", "year", "total_population"])

# Le nostre chiavi di deduplicazione
subset_cols = [
    "location_code",
    "year"
]

deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)

# 3. Inizializzazione della tabella
initialize_delta_table(
    spark=spark,
    db_name="silver",
    table_name="worldbank_population"
)

# 4. Upsert nel layer Silver
upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_df, 
    table_name="worldbank_population"
)