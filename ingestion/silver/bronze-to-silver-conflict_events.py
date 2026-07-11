from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.sql import functions as F
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import *

spark = get_spark_session('BronzeToSilver-ConflictEvents')

# 1. Read from Bronze
bronze_df = (spark.read
    .format('delta')
    .load('s3a://lakehouse/bronze/conflict_events')
    .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # process only the data from kafka done today
)

# 2. Clean and deduplicate
cleaned_df = bronze_df.dropna(subset=['location_code', 'location_name', 'event_type'])

subset_cols = [
    "location_code",
    "location_name",
    "admin1_code",
    "admin1_name",
    "admin2_code",
    "admin2_name",
    "admin_level",
    "resource_hdx_id",
    "event_type",
    "events",
    "fatalities",
    "reference_period_start",
    "reference_period_end"
]
deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)
deduplicated_with_column_df = extract_date_components(deduplicated_df, "reference_period_start")

# 3. Create Silver database and table

initialize_delta_table(
    spark=spark,
    db_name="silver",
    table_name="conflict_events"
)


# 4. Write to Silver
upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_with_column_df, 
    table_name="conflict_events"
)
# deduplicated_df.write \
#     .format('delta') \
#     .mode('overwrite') \
#     .option('overwriteSchema', 'true') \
#     .save('s3a://lakehouse/silver/conflict_events')

# print('Conflict Events Silver layer updated successfully.')
# spark.stop()