import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
from src.customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)


def massive_processing(input_df, customer_prof_input_df, join_function, sql, partition_date, cust_partition_date,
                       cust_type, output_df_catalog):
    """
    :return:
    """

    if len(input_df.head(1)) == 0 or len(customer_prof_input_df.head(1)) == 0:
        return get_spark_empty_df

    def divide_chunks(l, n):

        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    print("Filter " + cust_type)
    cust_data_frame = customer_prof_input_df.where("charge_type = '" + cust_type + "'")
    dates_list = cust_data_frame.select(f.to_date(cust_partition_date).alias(cust_partition_date)).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.to_date(partition_date).isin(*[curr_item]))
        customer_prof_df = cust_data_frame.filter(F.col(cust_partition_date).isin(*[curr_item]))
        joined_df = join_function(customer_prof_df, small_df)
        output_df = node_from_config(joined_df, sql)
        CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.to_date(partition_date).isin(*[first_item]))
    customer_prof_df = cust_data_frame.filter(F.col(cust_partition_date).isin(*[first_item]))
    joined_df = join_function(customer_prof_df, return_df)
    final_df = node_from_config(joined_df, sql)

    return final_df


def billing_topup_count_and_volume_node(input_df, customer_prof, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, customer_prof, daily_recharge_data_with_customer_profile, sql,
                                   'recharge_date', 'event_partition_date', "Pre-paid",
                                   "l1_billing_and_payments_daily_topup_and_volume")
    return return_df


def billing_daily_rpu_roaming(input_df, customer_prof, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, customer_prof, daily_roaming_data_with_customer_profile, sql, 'date_id',
                                   'event_partition_date', "Post-paid", "l1_billing_and_payments_daily_rpu_roaming")
    return return_df


def billing_before_topup_balance(input_df, customer_prof, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, customer_prof, daily_sa_account_data_with_customer_profile, sql,
                                   'recharge_date', 'event_partition_date', "Pre-paid",
                                   "l1_billing_and_payments_daily_before_top_up_balance")
    return return_df


def billing_topup_channels(input_df, customer_prof, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, customer_prof, daily_recharge_data_with_customer_profile, sql,
                                   'recharge_date', 'event_partition_date', "Pre-paid",
                                   "l1_billing_and_payments_daily_top_up_channels")
    return return_df


def billing_most_popular_topup_channel(input_df, customer_prof, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, customer_prof, daily_recharge_data_with_customer_profile, sql,
                                   'recharge_date', 'event_partition_date', "Pre-paid",
                                   "l1_billing_and_payments_daily_most_popular_top_up_channel")
    return return_df


def billing_popular_topup_day_hour(input_df, customer_prof, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, customer_prof, daily_recharge_data_with_customer_profile, sql,
                                   'recharge_date', 'event_partition_date', "Pre-paid",
                                   "l1_billing_and_payments_daily_popular_topup_day")
    return return_df


def billing_time_since_last_topup(input_df, customer_prof, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing(input_df, customer_prof, daily_recharge_data_with_customer_profile, sql,
                                   'recharge_date', 'event_partition_date', "Pre-paid",
                                   "l1_billing_and_payments_daily_time_since_last_top_up")
    return return_df


def derives_in_customer_profile(customer_prof):
    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date",
                                         "charge_type")

    customer_prof = customer_prof.withColumn("start_of_month",
                                             f.to_date(f.date_trunc('month', customer_prof.event_partition_date))) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', customer_prof.event_partition_date)))

    return customer_prof


def daily_recharge_data_with_customer_profile(customer_prof, recharge_data):
    customer_prof = derives_in_customer_profile(customer_prof)

    output_df = customer_prof.join(recharge_data, (customer_prof.access_method_num == recharge_data.access_method_num) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(recharge_data.register_date))) &
                                   (customer_prof.event_partition_date == f.to_date(recharge_data.recharge_date)),
                                   'left')

    output_df = output_df.drop(recharge_data.access_method_num) \
        .drop(recharge_data.register_date)

    return output_df


def daily_roaming_data_with_customer_profile(customer_prof, roaming_data):
    customer_prof = derives_in_customer_profile(customer_prof)

    output_df = customer_prof.join(roaming_data,
                                   (customer_prof.access_method_num == roaming_data.access_method_number) &
                                   (customer_prof.register_date.eqNullSafe(
                                       f.to_date(roaming_data.mobile_register_date))) &
                                   (customer_prof.event_partition_date == f.to_date(roaming_data.date_id)), 'left')

    return output_df


def daily_sa_account_data_with_customer_profile(customer_prof, sa_account_data):
    customer_prof = derives_in_customer_profile(customer_prof)

    output_df = customer_prof.join(sa_account_data,
                                   (customer_prof.access_method_num == sa_account_data.access_method_num) &
                                   (customer_prof.event_partition_date == f.to_date(sa_account_data.recharge_date)),
                                   'left')

    output_df = output_df.drop(sa_account_data.access_method_num)

    return output_df
