import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window


def top_up_time_diff_weekly_data(input_df):
    window = Window.\
        partitionBy("start_of_month", "start_of_week", "access_method_num", "register_date").\
        orderBy("recharge_time")

    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    df = spark.sql("""select date(date_trunc('month',recharge_date)) as start_of_month,
    date(date_trunc('week',recharge_date)) as start_of_week,recharge_time,access_method_num,register_date
     from input_df""")

    output_df = df.withColumn("time_diff",f.datediff("recharge_time",f.lag("recharge_time",1).over(window)))
    return output_df

def automated_payment_weekly(input_df):
    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    df = spark.sql("""select date(date_trunc('month',payment_date)) as start_of_month, date(date_trunc('week',payment_date)) as start_of_week, account_identifier,
    case when channel_identifier in ('PM_13','PM_14') then 1 else 0 end as automatic_pay_flag from input_df""")

    df2 = df.groupBy("start_of_month","start_of_week","account_identifier")\
        .agg(f.sum("automatic_pay_flag").alias("sum_automated_pay_flag"))

    output_df = df2.withColumn("automated_payment_flag",f.when(df2.sum_automated_pay_flag > 0 , 'YES').otherwise('NO'))\
        .drop(df2.sum_automated_pay_flag)

    return output_df

