import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def bill_payment_daily_data_with_customer_profile(customer_prof,pc_t_data):

    customer_prof = customer_prof.select("access_method_num",
                                         "billing_account_no"
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "start_of_month")

    output_df = customer_prof.join(pc_t_data,(customer_prof.billing_account_no == pc_t_data.ba_no) &
                                   (customer_prof.start_of_month == f.to_date(f.date_trunc('month',pc_t_data.payment_date))),'left')


    return output_df

def billing_data_joined(billing_monthly,payment_daily):

    output_df = billing_monthly.join(payment_daily,
                                     (billing_monthly.account_identifier == payment_daily.account_identifier) &
                                     (billing_monthly.billing_statement_identifier == payment_daily.billing_statement_identifier) &
                                     (billing_monthly.billing_statement_seq_no == payment_daily.bill_seq_no), 'left')

    output_df = output_df.drop(payment_daily.billing_statement_identifier)\
        .drop(payment_daily.account_identifier)

    return output_df


def billing_rpu_data_with_customer_profile(customer_prof,rpu_data):

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "start_of_month")

    output_df = customer_prof.join(rpu_data,(customer_prof.access_method_num == rpu_data.access_method_num) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(rpu_data.register_date))) &
                                   (customer_prof.start_of_month == f.to_date(f.date_trunc('month',rpu_data.month_id))),'left')

    output_df = output_df.drop(rpu_data.access_method_num)\
        .drop(rpu_data.register_date)

    return output_df

def billing_statement_hist_data_with_customer_profile(customer_prof,billing_hist):

    customer_prof = customer_prof.select("access_method_num",
                                         "billing_account_no",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "start_of_month")

    output_df = customer_prof.join(billing_hist,(customer_prof.billing_account_no == billing_hist.account_num) &
                                   (customer_prof.start_of_month == f.to_date(f.date_trunc('month',billing_hist.billing_stmt_period_eff_date))),'left')

    return output_df


