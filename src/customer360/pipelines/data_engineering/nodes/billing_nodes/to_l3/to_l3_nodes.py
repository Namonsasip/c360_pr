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

def billing_data_joined(billing_monthly,payment_daily):

    billing_monthly = billing_monthly.withColumn("start_of_month",
                                    f.to_date(f.date_trunc('month',"billing_stmt_period_eff_date")))

    output_df = billing_monthly.join(payment_daily,
                                     (billing_monthly.account_identifier == payment_daily.account_identifier) &
                                     (billing_monthly.billing_statement_identifier == payment_daily.billing_statement_identifier) &
                                     (billing_monthly.billing_statement_seq_no == payment_daily.bill_seq_no), 'left')

    output_df = output_df.drop(payment_daily.billing_statement_identifier)\
        .drop(payment_daily.account_identifier)

    return output_df

