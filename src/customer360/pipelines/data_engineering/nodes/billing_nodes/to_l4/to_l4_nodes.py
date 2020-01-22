from pyspark.sql import SparkSession, DataFrame

import pyspark.sql.functions as f
from pyspark.sql import Window
import pyspark.sql.types as t

#def billshock_flag(bills):
#  if len(bills)>1 and (bills[0] > (2 * f.avg(bills[1:]))):
#    return "YES"
#  else:
#    return "NO"

# def billshock(input_df):
#     df = input_df.select(f.year("billing_stmt_period_eff_date").alias("year"),
#                          f.month("billing_stmt_period_eff_date").alias("month"),
#                          "account_identifier",
#                          "bill_stmt_tot_balance_due_amt")
#
#     window = Window.partitionBy("year", "account_identifier").\
#         orderBy(f.col("month").desc()).\
#         rowsBetween(Window.currentRow, 6)
#
#     billshock_udf = f.udf(billshock_flag, t.StringType())
#
#     output_df = df.withColumn("bills", f.collect_list("bill_stmt_tot_balance_due_amt").over(window)).\
#         withColumn("bill_shock_flag", billshock_udf("bills")).drop("bills")
#
#     return output_df

def bill_shock(input_df):
    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    df = spark.sql("""select to_date(cast(partition_month as STRING), 'yyyyMM') as start_of_month,account_identifier,bill_stmt_tot_balance_due_amt from input_df""")

    df.createOrReplaceTempView("df")

    df2 = spark.sql("""select start_of_month,account_identifier,bill_stmt_tot_balance_due_amt,
    avg(bill_stmt_tot_balance_due_amt) over(partition by account_identifier order by cast(cast(start_of_month as timestamp) as long) asc 
    RANGE BETWEEN 6*30*24*60*60 PRECEDING AND 1 PRECEDING) as last_6_months_avg from df""")

    output_df = df2.withColumn("bill_shock_flag",f.when(df2.bill_stmt_tot_balance_due_amt > (2*df2.last_6_months_avg),"Y").otherwise("N")).drop("last_6_months_avg")

    return output_df