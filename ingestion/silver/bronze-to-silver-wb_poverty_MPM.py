import sys
import os
from pyspark.sql.functions import col, to_date
from pyspark.sql import functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, clean_and_deduplicate_data, initialize_delta_table, upsert_to_silver_layer

spark = get_spark_session("BronzeToSilver-WorldBank-MPM")

bronze_df = (spark.read
    .format("delta")
    .load("s3a://lakehouse/bronze/worldbank_mpm")
    #.filter(F.to_date(F.col("ingested_at")) >= F.current_date()) 
)

cleaned_df = bronze_df.dropna(subset=["location_code", "year", "mpm_value"])

subset_cols = [
    "location_code",
    "year"
]

deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)

initialize_delta_table(
    spark=spark,
    db_name="silver",
    table_name="worldbank_mpm"
)

upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_df, 
    table_name="worldbank_mpm"
)