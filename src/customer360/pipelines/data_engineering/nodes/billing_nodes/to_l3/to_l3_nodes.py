import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def automated_payment_monthly(input_df):
    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    df = spark.sql("""select date(date_trunc('month',payment_date)) as start_of_month, 
    account_identifier,
    case when channel_identifier in ('PM_13','PM_14') then 1 
    else 0 end as automatic_pay_flag 
    from input_df""")

    df2 = df.groupBy("start_of_month","account_identifier")\
        .agg(f.sum("automatic_pay_flag").alias("sum_automated_pay_flag"))

    output_df = df2.withColumn("automated_payment_flag",f.when(df2.sum_automated_pay_flag > 0 , 'Y').otherwise('N'))\
        .drop(df2.sum_automated_pay_flag)

    return output_df

def popular_top_up_channel_with_rank(input_df):

    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    df = spark.sql("""select start_of_month,
    access_method_num,
    register_date,
    top_up_channel,
    sum(total_top_up) as total_top_up
    from input_df group by start_of_month,access_method_num,register_date,top_up_channel order by total_top_up desc""")

    df.createOrReplaceTempView("df")

    output_df = spark.sql("""select *,
    row_number() over(partition by start_of_month,access_method_num,register_date order by total_top_up desc) as rank
    from df""")

    return output_df

def bill_volume(input_df):
    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    output_df = spark.sql("""select date(date_trunc('month',billing_stmt_period_eff_date)) as start_of_month,
    account_identifier,
    sum(bill_stmt_tot_invoiced_amt) as bill_volume
    from input_df group by date(date_trunc('month',billing_stmt_period_eff_date)),account_identifier""")

    return output_df

