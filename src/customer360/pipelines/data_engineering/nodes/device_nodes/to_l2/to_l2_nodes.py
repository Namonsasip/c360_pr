import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window

def device_features_with_config(hs_summary,hs_configs):

    hs_summary = hs_summary.withColumn("start_of_month",f.to_date(f.date_trunc('month',"date_id")))\
        .withColumn("start_of_week",f.to_date(f.date_trunc('week',"date_id")))

    hs_configs = hs_configs.withColumn("start_of_month",f.to_date(f.date_trunc('month',"month_id")))

    joined_data = hs_summary.join(hs_configs,
                                  (hs_summary.handset_brand_code == hs_configs.hs_brand_code) &
                                  (hs_summary.handset_model_code == hs_configs.hs_model_code) &
                                  (hs_summary.start_of_month == hs_configs.start_of_month), "left")\
        .drop(hs_configs.start_of_month)\
        .drop(hs_summary.handset_type)\
        .drop(hs_configs.dual_sim)\
        .drop(hs_configs.hs_support_lte_1800)

    joined_data.createOrReplaceTempView("joined_data")

    return joined_data

def filter(input_df):

    output_df = input_df.where("rank = 1").drop("rank")

    return output_df

def derive_month_and_week(input_df):

    output_df = input_df.withColumn("start_of_month", f.to_date(f.date_trunc('month', "date_id")))\
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', "date_id")))
    return  output_df