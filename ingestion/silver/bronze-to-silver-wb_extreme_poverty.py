import sys
import os
from pyspark.sql.functions import col
from pyspark.sql import functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, clean_and_deduplicate_data, initialize_delta_table, upsert_to_silver_layer

spark = get_spark_session("BronzeToSilver-WorldBank-ExtremePoverty")

bronze_df = (spark.read
    .format("delta")
    .load("s3a://lakehouse/bronze/worldbank_extreme_poverty")
    #.filter(F.to_date(F.col("ingested_at")) >= F.current_date()) 
)

# # Correggiamo il nome della colonna in arrivo dal Bronze
# if "mpm_value" in bronze_df.columns:
#     bronze_df = bronze_df.withColumnRenamed("mpm_value", "extreme_poverty_value")

cleaned_df = bronze_df.dropna(subset=["location_code", "year", "extreme_poverty_value"])

subset_cols = [
    "location_code",
    "year"
]

deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)

initialize_delta_table(
    spark=spark,
    db_name="silver",
    table_name="worldbank_extreme_poverty"
)

upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_df, 
    table_name="worldbank_extreme_poverty"
)