import pyspark.sql.functions as f
from pyspark.sql import Window

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