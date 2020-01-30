import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window

def previous_device_features_with_config(hs_summary):

    spark = SparkSession.builder.getOrCreate()

    hs_summary = hs_summary.withColumn("start_of_month",f.date_trunc('month',"date_id"))

    output_df = hs_summary.groupBy("start_of_month",
                            "mobile_no",
                            f.to_date("register_date").alias("register_date"),
                            "handset_imei",
                            "handset_brand_code",
                            "handset_model_code",
                            "handset_brand_name",
                            "handset_model_name")\
        .agg(f.max("handset_last_use_time").alias("device_last_use_time"))\
        .orderBy(f.col("device_last_use_time").desc())

    return output_df