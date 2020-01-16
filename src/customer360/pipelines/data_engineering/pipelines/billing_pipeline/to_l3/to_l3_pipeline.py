from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config
import pyspark.sql.functions as f
from pyspark.sql import Window

def top_up_monthly_converted_data(input_df):
    output_df = input_df.withColumn("year", f.year("recharge_date"))\
        .withColumn("month", f.month("recharge_date")) \
        .groupBy("year", "month", "access_method_num", f.to_date("register_date").alias("register_date"))\
        .agg(f.sum("number_of_top_ups").alias("monthly_top_ups"),
             f.sum("top_up_volume").alias("monthly_top_up_volume"))\
        .drop("recharge_date")

    return output_df

def arpu_monthly_converted_data(input_df):
    output_df = input_df.withColumn("year", f.year("month_id"))\
        .withColumn("month", f.month("month_id")) \
        .groupBy("year", "month", "access_method_num", f.to_date("register_date").alias("register_date"))\
        .agg(f.format_number(f.avg("norms_net_revenue"), 2).alias("monthly_arpu"),
             f.format_number(f.avg("norms_net_revenue_vas"), 2).alias("monthly_arpu_vas"),
             f.format_number(f.avg("norms_net_revenue_gprs"), 2).alias("monthly_arpu_gprs"),
             f.format_number(f.avg("norms_net_revenue_voice"), 2).alias("monthly_arpu_voice"))\

    return output_df

def top_up_time_diff_monthly_data(input_df):
    window = Window.\
        partitionBy("access_method_num", "register_date").\
        orderBy("recharge_time")

    output_df = input_df.withColumn("year", f.year("recharge_date"))\
        .withColumn("month", f.month("recharge_date"))\
        .withColumn("time_diff",f.datediff("recharge_time",f.lag("recharge_time",1).over(window)))\
        .select("year","month","access_method_num",f.to_date("register_date").alias("register_date"),"recharge_time","time_diff")\
        .groupBy("year", "month", "access_method_num", "register_date")\
        .agg(f.max("time_diff").alias("max_time_diff_in_days"),
             f.min("time_diff").alias("min_time_diff_in_days"),
             f.format_number(f.avg("time_diff"),2).alias("avg_time_diff_in_days"))

    return output_df

def billing_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                top_up_monthly_converted_data,
                ["l1_billing_and_payments_topup_and_volume"],
                "l3_billing_and_payments_monthly_topup_and_volume"
            ),
            node(
                arpu_monthly_converted_data,
                ["l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly"],
                "l3_billing_and_payments_monthly_arpu"
            ),
            node(
                top_up_time_diff_monthly_data,
                ["l0_billing_and_payments_rt_t_recharge_daily"],
                "l3_billing_and_payments_monthly_topup_time_diff"
            ),
        ]
    )