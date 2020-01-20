import pyspark.sql.functions as f
from pyspark.sql import Window
import pyspark.sql.types as t

def billshock_flag(bills):
  if len(bills)>1 and (bills[0] > (2 * f.avg(bills[1:]))):
    return "YES"
  else:
    return "NO"

def billshock(input_df):
    df = input_df.select(f.year("billing_stmt_period_eff_date").alias("year"),
                         f.month("billing_stmt_period_eff_date").alias("month"),
                         "account_identifier",
                         "bill_stmt_tot_balance_due_amt")

    window = Window.partitionBy("year", "account_identifier").\
        orderBy(f.col("month").desc()).\
        rowsBetween(Window.currentRow, 6)

    billshock_udf = f.udf(billshock_flag, t.StringType())

    output_df = df.withColumn("bills", f.collect_list("bill_stmt_tot_balance_due_amt").over(window)).\
        withColumn("bill_shock_flag", billshock_udf("bills")).drop("bills")

    return output_df