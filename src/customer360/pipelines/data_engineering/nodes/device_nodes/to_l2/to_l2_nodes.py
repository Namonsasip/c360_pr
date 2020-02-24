import pyspark.sql.functions as f

def device_summary_with_customer_profile(customer_prof,hs_summary):

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date")

    customer_prof = customer_prof.withColumn("start_of_month",f.to_date(f.date_trunc('month',customer_prof.event_partition_date)))\
        .withColumn("start_of_week",f.to_date(f.date_trunc('week',customer_prof.event_partition_date)))

    hs_summary_with_customer_profile = customer_prof.join(hs_summary,
                                  (customer_prof.access_method_num == hs_summary.mobile_no) &
                                  (customer_prof.register_date.eqNullSafe(f.to_date(hs_summary.register_date))) &
                                  (customer_prof.event_partition_date == f.to_date(hs_summary.date_id)), "left")

    hs_summary_with_customer_profile = hs_summary_with_customer_profile.drop(hs_summary.register_date)

    return hs_summary_with_customer_profile


def device_summary_with_config(hs_summary,hs_configs):

    hs_configs = hs_configs.withColumn("start_of_month",f.to_date(f.date_trunc('month',"month_id")))

    joined_data = hs_summary.join(hs_configs,
                                  (hs_summary.handset_brand_code == hs_configs.hs_brand_code) &
                                  (hs_summary.handset_model_code == hs_configs.hs_model_code) &
                                  (hs_summary.start_of_month == hs_configs.start_of_month), "left")\
        .drop(hs_configs.start_of_month)\
        .drop(hs_summary.handset_type)\
        .drop(hs_configs.dual_sim)\
        .drop(hs_configs.hs_support_lte_1800)

    return joined_data

def filter(input_df):

    output_df = input_df.where("rank = 1").drop("rank")

    return output_df