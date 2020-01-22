import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def automated_payment_monthly(input_df):
    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    df = spark.sql("""select date(date_trunc('month',payment_date)) as start_of_month, account_identifier,
    case when channel_identifier in ('PM_13','PM_14') then 1 else 0 end as automatic_pay_flag from input_df""")

    df2 = df.groupBy("start_of_month","account_identifier")\
        .agg(f.sum("automatic_pay_flag").alias("sum_automated_pay_flag"))

    output_df = df2.withColumn("automated_payment_flag",f.when(df2.sum_automated_pay_flag > 0 , 'Y').otherwise('N'))\
        .drop(df2.sum_automated_pay_flag)

    return output_df