import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window


def top_up_time_diff_weekly_data(input_df):
    window = Window.\
        partitionBy("start_of_month", "start_of_week", "access_method_num", "register_date").\
        orderBy("recharge_time")

    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    df = spark.sql("""select date(date_trunc('month',recharge_date)) as start_of_month,
    date(date_trunc('week',recharge_date)) as start_of_week,
    recharge_time,
    access_method_num,
    register_date 
    from input_df""")

    output_df = df.withColumn("time_diff",f.datediff("recharge_time",f.lag("recharge_time",1).over(window)))
    return output_df

def popular_top_up_channel_with_rank(input_df):

    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    df = spark.sql("""select start_of_month,
    start_of_week,
    access_method_num,
    register_date,
    top_up_channel,
    sum(total_top_up) as total_top_up
    from input_df group by start_of_month,start_of_week,access_method_num,register_date,top_up_channel order by total_top_up desc""")

    df.createOrReplaceTempView("df")

    output_df = spark.sql("""select *,
    row_number() over(partition by start_of_week,access_method_num,register_date order by total_top_up desc) as rank from df""")

    return output_df

def most_popular_top_up_channel(input_df):

    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    output_df = spark.sql("""select * from input_df where rank=1""")

    return output_df


