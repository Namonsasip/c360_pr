import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window

def previous_device_features_with_config_sort(input_df):

    output_df = input_df.orderBy(f.col("device_last_use_time").desc())
    return  output_df