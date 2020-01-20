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
    df = spark.sql("""select billing_stmt_period_eff_date,account_identifier,bill_stmt_tot_balance_due_amt,
    sum(bill_stmt_tot_balance_due_amt) over(partition by account_identifier order by billing_stmt_period_eff_date asc RANGE BETWEEN INTERVAL 180 DAYS PRECEDING AND CURRENT ROW) as last_months_total, count(bill_stmt_tot_balance_due_amt) over(partition by account_identifier order by billing_stmt_period_eff_date asc RANGE BETWEEN INTERVAL 180 DAYS PRECEDING AND CURRENT ROW) as last_months_count from input_df""")
    output_df = df.withColumn("last_6_month_avg",(df.last_months_total - df.bill_stmt_tot_balance_due_amt)/(df.last_months_count - 1))\
        .withColumn("bill_shock_flag",f.when(df.bill_stmt_tot_balance_due_amt > (2*f.col("last_6_month_avg")),"YES").otherwise("NO"))\
        .drop("last_6_month_avg","last_months_total","last_months_count")
    return output_df