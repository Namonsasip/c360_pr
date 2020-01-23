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

    output_df = df.withColumn("week_dynamics_top_up_no",df.sum_top_ups_last_week/df.sum_top_ups_last_two_week)\
        .withColumn("month_dynamics_top_up_no",df.sum_top_ups_last_month/df.sum_top_ups_last_two_months)\
        .withColumn("week_dynamics_top_up_volume",df.sum_top_up_volume_last_week/df.sum_top_up_volume_last_two_week)\
        .withColumn("month_dynamics_top_up_volume",df.sum_top_up_volume_last_month/df.sum_top_up_volume_last_two_months)

    return output_df