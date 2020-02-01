import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def derive_month_automated_payment(input_df):


    output_df = input_df.withColumn("start_of_month",
                                    f.to_date(f.date_trunc('month',"payment_date")))

    return output_df

def derive_month_bill_volume(input_df):

    output_df = input_df.withColumn("start_of_month",
                                    f.to_date(f.date_trunc('month',"billing_stmt_period_eff_date")))

    return output_df

