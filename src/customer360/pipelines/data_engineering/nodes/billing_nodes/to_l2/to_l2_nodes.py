import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window


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

def automated_payment_weekly(input_df):
    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    df = spark.sql("""select year(payment_date) as year, weekofyear(payment_date) as week, account_identifier,
    case when channel_identifier in ('PM_13','PM_14') then 1 else 0 end as automatic_pay_flag from input_df""")
    df2 = df.groupBy("year","week","account_identifier")\
        .agg(f.sum("automatic_pay_flag").alias("sum_automated_pay_flag"))

    output_df = df2.withColumn("automatic_payment",f.when(df2.sum_automated_pay_flag > 0 , 'YES').otherwise('NO'))\
        .drop(df2.sum_automated_pay_flag,df2.automatic_pay_flag)

    return output_df

