import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window

def device_features_with_config(hs_summary,hs_configs):

    spark = SparkSession.builder.getOrCreate()

    hs_summary = hs_summary.withColumn("start_of_month",f.date_trunc('month',"date_id"))\
        .withColumn("start_of_week",f.date_trunc('week',"date_id"))

    hs_configs = hs_configs.withColumn("start_of_month",f.date_trunc('month',"month_id"))

    joined_data = hs_summary.join(hs_configs,
                                  (hs_summary.handset_brand_code == hs_configs.hs_brand_code) &
                                  (hs_summary.handset_model_code == hs_configs.hs_model_code) &
                                  (hs_summary.start_of_month == hs_configs.start_of_month), "left")\
        .drop(hs_configs.start_of_month)\
        .drop(hs_configs.handset_type)\
        .drop(hs_configs.dual_sim)\
        .drop(hs_configs.hs_support_lte_1800)

    joined_data.createOrReplaceTempView("joined_data")

    df = spark.sql("""select 
    start_of_month,
    start_of_week,
    mobile_no,
    date(register_date),
    handset_last_use_time as device_last_use_time,
    handset_brand_code as device_brand_code,
    handset_model_code as device_model_code,
    handset_brand_name as device_brand_name,
    handset_model_name as device_model_name,
    handset_imei as device_imei,
    handset_type as device_type,
    handset_support_3g_2100_yn as device_supports_umts,
    handset_wds_flag as device_supports_wds,
    handset_channel as device_channel,
    dual_sim as device_supports_dual_sim,
    hs_support_lte_1800 as device_supports_lte,
    dual_user_yn as devices_uses_both_sim_slot,
    hsdpa as device_supports_hsdpa,
    video_call as device_supports_video_call,
    os as device_operating_system,
    gprs_handset_support as device_supports_gprs,
    google_map as device_supports_google_map,
    row_number() 
    over(partition by start_of_month,start_of_week,mobile_no,date(register_date) 
    order by handset_last_use_time desc) as rank 
    from joined_data""")

    output_df = df.where("rank = 1").drop("rank")

    return output_df