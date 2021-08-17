import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
from src.customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, add_event_week_and_month_from_yyyymmdd
from pyspark.sql.types import *

conf = os.getenv("CONF", None)

def massive_processing_de(input_df, sql, columns):
    input_df = add_event_week_and_month_from_yyyymmdd(input_df, "partition_date")
    if (columns.lower() == "pre"):
        input_df = input_df.withColumn('subscription_identifier', F.concat("access_method_num", F.lit('-'),
                                                                           F.date_format("register_date", 'yyyyMMdd')))
    else:
        input_df = input_df.withColumn('subscription_identifier', F.col(columns.lower()))
    output_df = node_from_config(input_df, sql)
    return output_df


def massive_processing(input_df, customer_prof_input_df, join_function, sql, partition_date, cust_partition_date,
                       cust_type, output_df_catalog):
    """
    :return:
    """

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            customer_prof_input_df.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    customer_prof_input_df = customer_prof_input_df.filter(F.col("event_partition_date") <= min_value)

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
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 15))
    add_list = mvv_new

    first_item = add_list[-1]

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


def billing_topup_count_and_volume_node(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_billing_and_payments_daily_topup_and_volume")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    # return_df = massive_processing(input_df, customer_prof, daily_recharge_data_with_customer_profile, sql,
    #                                'recharge_date', 'event_partition_date', "Pre-paid",
    #                                "l1_billing_and_payments_daily_topup_and_volume")
	
	
    return_df = massive_processing_de(input_df, sql, "pre")
    return_df = return_df.withColumn("register_date", F.col('register_date').cast(DateType()))

    return return_df


def billing_daily_rpu_roaming(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_billing_and_payments_daily_rpu_roaming")

    # customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
    #                                                         par_col="event_partition_date",
    #                                                         target_table_name="l1_billing_and_payments_daily_rpu_roaming")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    # return_df = massive_processing(input_df, customer_prof, daily_roaming_data_with_customer_profile, sql, 'date_id',
    #                                'event_partition_date', "Post-paid", "l1_billing_and_payments_daily_rpu_roaming")

    input_df = input_df.withColumnRenamed("access_method_number", "access_method_num")
    input_df = input_df.withColumn('register_date', F.to_date('mobile_register_date'))
    return_df = massive_processing_de(input_df, sql, "crm_sub_id")
    return return_df


def billing_before_topup_balance(input_df, customer_prof, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_billing_and_payments_daily_before_top_up_balance")

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
                                                            par_col="event_partition_date",
                                                            target_table_name="l1_billing_and_payments_daily_before_top_up_balance")

    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing(input_df, customer_prof, daily_sa_account_data_with_customer_profile, sql,
                                   'recharge_date', 'event_partition_date', "Pre-paid",
                                   "l1_billing_and_payments_daily_before_top_up_balance")
    return return_df


def billing_topup_channels(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_billing_and_payments_daily_top_up_channels")

    # input_df = input_df.join(topup_type_ref, input_df.recharge_type == topup_type_ref.recharge_topup_event_type_cd,
    #                         'left')

    # customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
    #                                                         par_col="event_partition_date",
    #                                                         target_table_name="l1_billing_and_payments_daily_top_up_channels")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    # return_df = massive_processing(input_df, customer_prof, daily_recharge_data_with_customer_profile, sql,
    #                                'recharge_date', 'event_partition_date', "Pre-paid",
    #                                "l1_billing_and_payments_daily_top_up_channels")

    return_df = massive_processing_de(input_df, sql, "pre")
    return_df = return_df.withColumn("register_date", F.col('register_date').cast(DateType()))

    return return_df


def billing_most_popular_topup_channel(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_billing_and_payments_daily_most_popular_top_up_channel")

    # customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
    #                                                         par_col="event_partition_date",
    #                                                         target_table_name="l1_billing_and_payments_daily_most_popular_top_up_channel")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    # return_df = massive_processing(input_df, customer_prof, daily_recharge_data_with_customer_profile, sql,
    #                                'recharge_date', 'event_partition_date', "Pre-paid",
    #                                "l1_billing_and_payments_daily_most_popular_top_up_channel")

    return_df = massive_processing_de(input_df, sql, "pre")
    return_df = return_df.withColumn("register_date", F.col('register_date').cast(DateType()))
    return return_df


def billing_popular_topup_day_hour(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_billing_and_payments_daily_popular_topup_day")

    # customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
    #                                                         par_col="event_partition_date",
    #                                                         target_table_name="l1_billing_and_payments_daily_popular_topup_day")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    # return_df = massive_processing(input_df, customer_prof, daily_recharge_data_with_customer_profile, sql,
    #                                'recharge_date', 'event_partition_date', "Pre-paid",
    #                                "l1_billing_and_payments_daily_popular_topup_day")

    return_df = massive_processing_de(input_df, sql, "pre")
    return_df = return_df.withColumn("register_date", F.col('register_date').cast(DateType()))

    return return_df


def billing_time_since_last_topup(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_billing_and_payments_daily_time_since_last_top_up")

    # customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
    #                                                         par_col="event_partition_date",
    #                                                         target_table_name="l1_billing_and_payments_daily_time_since_last_top_up")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    # return_df = massive_processing(input_df, customer_prof, daily_recharge_data_with_customer_profile, sql,
    #                                'recharge_date', 'event_partition_date', "Pre-paid",
    #                                "l1_billing_and_payments_daily_time_since_last_top_up")

    return_df = massive_processing_de(input_df, sql, "pre")
    return_df = return_df.withColumn("register_date", F.col('register_date').cast(DateType()))

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
                                   (customer_prof.subscription_identifier == roaming_data.crm_sub_id) &
                                   (customer_prof.event_partition_date == f.to_date(roaming_data.date_id)), 'left')
    # (customer_prof.access_method_num == roaming_data.access_method_number) &
    # (customer_prof.register_date.eqNullSafe(
    #     f.to_date(roaming_data.mobile_register_date))) &

    return output_df


def daily_sa_account_data_with_customer_profile(customer_prof, sa_account_data):
    customer_prof = derives_in_customer_profile(customer_prof)

    output_df = customer_prof.join(sa_account_data,
                                   (customer_prof.access_method_num == sa_account_data.access_method_num) &
                                   (customer_prof.event_partition_date == f.to_date(sa_account_data.recharge_date)),
                                   'left')

    output_df = output_df.drop(sa_account_data.access_method_num)

    return output_df