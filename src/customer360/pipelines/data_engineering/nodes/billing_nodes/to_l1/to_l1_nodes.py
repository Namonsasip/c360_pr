import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window



def daily_recharge_data_with_customer_profile(customer_prof,recharge_data):

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date")

    customer_prof = customer_prof.withColumn("start_of_month",f.to_date(f.date_trunc('month',customer_prof.event_partition_date)))\
        .withColumn("start_of_week",f.to_date(f.date_trunc('week',customer_prof.event_partition_date)))

    output_df = customer_prof.join(recharge_data,(customer_prof.access_method_num == recharge_data.access_method_num) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(recharge_data.register_date))) &
                                   (customer_prof.event_partition_date == f.to_date(recharge_data.recharge_date)),'left')

    output_df = output_df.drop(recharge_data.access_method_num)\
        .drop(recharge_data.register_date)

    return output_df

def daily_roaming_data_with_customer_profile(customer_prof,roaming_data):

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date")

    customer_prof = customer_prof.withColumn("start_of_month",f.to_date(f.date_trunc('month',customer_prof.event_partition_date)))\
        .withColumn("start_of_week",f.to_date(f.date_trunc('week',customer_prof.event_partition_date)))

    output_df = customer_prof.join(roaming_data,(customer_prof.access_method_num == roaming_data.access_method_number) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(roaming_data.mobile_register_date))) &
                                   (customer_prof.event_partition_date == f.to_date(roaming_data.date_id)),'left')


    return output_df

def daily_sa_account_data_with_customer_profile(customer_prof,sa_account_data):

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date")

    customer_prof = customer_prof.withColumn("start_of_month",f.to_date(f.date_trunc('month',customer_prof.event_partition_date)))\
        .withColumn("start_of_week",f.to_date(f.date_trunc('week',customer_prof.event_partition_date)))

    output_df = customer_prof.join(sa_account_data,(customer_prof.access_method_num == sa_account_data.access_method_num) &
                                   (customer_prof.event_partition_date == f.to_date(sa_account_data.recharge_date)),'left')

    output_df = output_df.drop(sa_account_data.access_method_num)

    return output_df





