from pyspark.sql import SparkSession, DataFrame

import pyspark.sql.functions as f

def dynamics_topups_and_volume(df):

    output_df = df.withColumn("week_dynamics_top_up_no",df.avg_top_ups_avg_last_week/df.avg_top_ups_avg_last_two_week)\
        .withColumn("month_dynamics_top_up_no",df.avg_top_ups_avg_last_month/df.avg_top_ups_avg_last_three_month)\
        .withColumn("week_dynamics_top_up_volume",df.avg_top_up_volume_avg_last_week/df.avg_top_up_volume_avg_last_two_week)\
        .withColumn("month_dynamics_top_up_volume",df.avg_top_up_volume_avg_last_month/df.avg_top_up_volume_avg_last_three_month)

    return output_df

def dynamics_rpu(df):

    output_df = df.withColumn("month_dynamics_arpu",df.avg_rpu_last_month/df.avg_rpu_last_three_month)

    return output_df

def dynamics_bill_volume(df):

    output_df = df.withColumn("month_dynamics_bill_volume",df.avg_bill_volume_last_month/df.avg_bill_volume_last_three_month)

    return output_df

def most_popular_topup_channel(df):

    spark = SparkSession.builder.getOrCreate()
    df.createOrReplaceTempView("df")

    output_df = spark.sql(""" with table_with_sum as 
    (select start_of_month,
            access_method_num,
            register_date,
            top_up_channel, 
            sum(total_top_up) over 
            (partition by access_method_num,
                          register_date,
                          top_up_channel
                          order by cast(cast(start_of_month as timestamp) as long) asc
                          range between 90 * 24 * 60 * 60 preceding and current row) as sum_last_three_months
    from df),
    
    table_with_rank_on_sum as 
    (select *,
            rank() over 
            (partition by access_method_num,
                          register_date,
                          start_of_month
                          order by sum_last_three_months desc) as rank
    from table_with_sum)
    
    select * from table_with_rank_on_sum where rank = 1 """)

    return output_df


