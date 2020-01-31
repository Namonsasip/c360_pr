import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window

def derive_month(input_df):

    output_df = input_df.withColumn("start_of_month", f.to_date(f.date_trunc('month', "date_id")))
    return  output_df

def previous_device_features_with_config_sort(input_df):

    output_df = input_df.orderBy(f.col("device_last_use_time").desc())
    return  output_df