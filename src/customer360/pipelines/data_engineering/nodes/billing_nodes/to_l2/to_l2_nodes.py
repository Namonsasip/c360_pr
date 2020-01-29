import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window

def popular_top_up_channel_with_rank(input_df,topup_type_ref):

    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")
    topup_type_ref.createOrReplaceTempView("topup_type_ref")

    df = spark.sql("""select start_of_month,
    start_of_week,
    access_method_num,
    register_date,
    recharge_topup_event_type_name,
    sum(total_top_up) as total_top_up
    from input_df 
    left join topup_type_ref on input_df.recharge_type = topup_type_ref.recharge_topup_event_type_cd
    group by start_of_month,
    start_of_week,
    access_method_num,
    register_date,
    topup_type_ref.recharge_topup_event_type_name order by total_top_up desc""")

    df.createOrReplaceTempView("df")

    output_df = spark.sql("""select start_of_month,
    start_of_week,
    access_method_num,
    register_date,
    recharge_topup_event_type_name as top_up_channel,
    total_top_up,
    row_number() over(partition by start_of_week,
    access_method_num,
    register_date order by total_top_up desc) as rank from df""")

    return output_df


def last_top_up_channel(input_df,topup_type_ref):
    spark = SparkSession.builder.getOrCreate()

    df = input_df.join(topup_type_ref,input_df.recharge_type == topup_type_ref.recharge_topup_event_type_cd, 'left')
    df.createOrReplaceTempView("df")

    df2 = spark.sql("""select date(date_trunc('month',recharge_date)) as start_of_month,
    date(date_trunc('week',recharge_date)) as start_of_week,
    access_method_num,
    date(register_date),
    recharge_time,
    recharge_topup_event_type_name as last_top_up_channel,
    row_number() over(partition by date(date_trunc('month',recharge_date)),
    date(date_trunc('week',recharge_date)),
    access_method_num,
    date(register_date) order by recharge_time desc) as rank
    from df""")

    output_df = df2.where("rank = 1").drop("rank")

    return output_df




