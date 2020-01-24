import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window


def popular_top_up_channel(input_df):

    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    df = spark.sql("""select access_method_num,
    date(date_trunc('month',date(recharge_date))) as start_of_month,
    date(date_trunc('week',date(recharge_date))) as start_of_week,
    date(register_date),
    date(recharge_date) as event_partition_date,
    recharge_type,
    count(*) as total_top_up
    from input_df group by access_method_num,
    date(register_date),
    date(recharge_date),
    recharge_type order by count(*) desc""")

    df.createOrReplaceTempView("df")
    output_df = spark.sql("""select *,
    row_number() over(partition by event_partition_date,
    access_method_num,
    register_date order by total_top_up desc) as rank 
    from df""")

    return output_df
