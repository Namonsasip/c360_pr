from pyspark.sql import SparkSession, DataFrame

import pyspark.sql.functions as f

def bill_shock(input_df):
    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    df = spark.sql("""select to_date(cast(partition_month as STRING), 'yyyyMM') as start_of_month,
    account_identifier,
    bill_stmt_tot_balance_due_amt 
    from input_df""")

    df.createOrReplaceTempView("df")

    df2 = spark.sql("""select start_of_month,
    account_identifier,
    bill_stmt_tot_balance_due_amt,
    avg(bill_stmt_tot_balance_due_amt) 
    over(partition by account_identifier order by cast(cast(start_of_month as timestamp) as long) asc 
    RANGE BETWEEN 6*30*24*60*60 PRECEDING AND 1 PRECEDING) as last_6_months_avg 
    from df""")

    output_df = df2.withColumn("bill_shock_flag",f.when(df2.bill_stmt_tot_balance_due_amt > (2*df2.last_6_months_avg),"Y").otherwise("N"))\
        .drop("last_6_months_avg")

    return output_df

def dynamics_topups_and_volume(df):

    output_df = df.withColumn("week_dynamics_top_up_no",df.avg_top_ups_avg_last_week/df.avg_top_ups_avg_last_two_week)\
        .withColumn("month_dynamics_top_up_no",df.avg_top_ups_avg_last_month/df.avg_top_ups_avg_last_two_month)\
        .withColumn("week_dynamics_top_up_volume",df.avg_top_up_volume_avg_last_week/df.avg_top_up_volume_avg_last_two_week)\
        .withColumn("month_dynamics_top_up_volume",df.avg_top_up_volume_avg_last_month/df.avg_top_up_volume_avg_last_two_month)

    return output_df

def dynamics_rpu(df):

    output_df = df.withColumn("month_dynamics_arpu",df.avg_rpu_last_month/df.avg_rpu_last_two_month)

    return output_df

def dynamics_bill_volume(df):

    output_df = df.withColumn("month_dynamics_bill_volume",df.avg_bill_volume_last_month/df.avg_bill_volume_last_two_month)

    return output_df

def last_3_topup_volume(df):

    spark = SparkSession.builder.getOrCreate()
    df.createOrReplaceTempView("df")

    output_df = spark.sql("""select date(date_trunc('month', recharge_date)) as start_of_month,
                          date(date_trunc('week', recharge_date)) as start_of_week,
                          access_method_num,
                          date(register_date),
                          recharge_time,
                          sum(face_value)
                          over(partition by access_method_num,date(register_date) order by recharge_time 
                          rows between 2 PRECEDING and current row) as last_3_top_up_volume from df
                          """)

    return output_df