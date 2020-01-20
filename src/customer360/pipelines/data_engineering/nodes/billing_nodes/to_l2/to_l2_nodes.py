import pyspark.sql.functions as f
from pyspark.sql import Window


def top_up_weekly_converted_data(input_df):
    output_df = input_df.withColumn("year", f.year("recharge_date"))\
        .withColumn("week", f.weekofyear("recharge_date")) \
        .groupBy("year", "week", "access_method_num", f.to_date("register_date").alias("register_date"))\
        .agg(f.sum("number_of_top_ups").alias("weekly_top_ups"),
             f.sum("top_up_volume").alias("weekly_top_up_volume"))\
        .drop("recharge_date")

    return output_df

def top_up_time_diff_weekly_data(input_df):
    window = Window.\
        partitionBy("access_method_num", "register_date").\
        orderBy("recharge_time")

    output_df = input_df.withColumn("year", f.year("recharge_date"))\
        .withColumn("week", f.weekofyear("recharge_date"))\
        .withColumn("time_diff",f.datediff("recharge_time",f.lag("recharge_time",1).over(window)))\
        .select("year","week","access_method_num",f.to_date("register_date").alias("register_date"),"recharge_time","time_diff")\
        .groupBy("year", "week", "access_method_num", "register_date")\
        .agg(f.max("time_diff").alias("max_time_diff_in_days"),
             f.min("time_diff").alias("min_time_diff_in_days"),
             f.format_number(f.avg("time_diff"),2).alias("avg_time_diff_in_days"))

    return output_df