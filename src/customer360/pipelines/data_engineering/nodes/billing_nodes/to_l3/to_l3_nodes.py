import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import expr
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
from customer360.pipelines.data_engineering.nodes.billing_nodes.to_l1.to_l1_nodes import massive_processing
import os
from src.customer360.utilities.spark_util import get_spark_empty_df
from pyspark.sql.types import *
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check

conf = os.getenv("CONF", None)


def massive_processing(input_df, customer_prof_input_df, join_function, sql, partition_date, cust_partition_date,
                       cust_type, output_df_catalog):
    """
    :return:
    """

    if check_empty_dfs([input_df, customer_prof_input_df]):
        return get_spark_empty_df()

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


def top_up_channel_joined_data_for_monthly_most_popular_top_up_channel(input_df, topup_type_ref):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly", par_col="event_partition_date",
                                                       target_table_name="l3_billing_and_payments_monthly_most_popular_top_up_channel",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    output_df = input_df.join(topup_type_ref, input_df.recharge_type == topup_type_ref.recharge_topup_event_type_cd,
                              'left')

    return output_df


def top_up_channel_joined_data_for_monthly_last_top_up_channel(input_df, topup_type_ref):
    output_df = input_df.join(topup_type_ref, input_df.recharge_type == topup_type_ref.recharge_topup_event_type_cd,
                              'left')

    return output_df


def massive_processing_monthly(data_frame: DataFrame, dict_obj: dict, output_df_catalog) -> DataFrame:
    """
    :param data_frame:
    :param dict_obj:
    :return:
    """

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = data_frame
    dates_list = data_frame.select('start_of_month').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 1))
    add_list = mvv_new
    first_item = add_list[0]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        output_df = node_from_config(small_df, dict_obj)
        CNTX.catalog.save(output_df_catalog, output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    return_df = node_from_config(return_df, dict_obj)
    return return_df


def process_last_topup_channel(data_frame: DataFrame, cust_prof: DataFrame, sql: dict, output_df_catalog) -> DataFrame:
    """
    :return:
    """

    # if len(data_frame.head(1)) == 0:
    #     return data_frame

    min_value = union_dataframes_with_missing_cols(
        [
            data_frame.select(
                f.max(f.col("start_of_month")).alias("max_date")),
            cust_prof.select(
                f.max(f.col("start_of_month")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    data_frame = data_frame.filter(f.col("start_of_month") <= min_value)
    cust_prof = cust_prof.filter(f.col("start_of_month") <= min_value)

    def divide_chunks(l, n):

        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    cust_data_frame = cust_prof
    dates_list = cust_data_frame.select('start_of_month').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 1))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.to_date('start_of_month').isin(*[curr_item]))
        customer_prof_df = cust_data_frame.filter(F.col('start_of_month').isin(*[curr_item]))
        result_df = node_from_config(small_df, sql)
        output_df = recharge_data_with_customer_profile_joined(customer_prof_df, result_df)
        CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(f.to_date('start_of_month').isin(*[first_item]))
    customer_prof_df = cust_data_frame.filter(F.col('start_of_month').isin(*[first_item]))
    result_df = node_from_config(small_df, sql)
    output_df = recharge_data_with_customer_profile_joined(customer_prof_df, result_df)

    return output_df


def billing_topup_count_and_volume_node_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly", par_col="event_partition_date",
                                                       target_table_name="l3_billing_and_payments_monthly_topup_and_volume",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_topup_and_volume")
    return return_df


def billing_arpu_roaming_node_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly", par_col="event_partition_date",
                                                       target_table_name="l3_billing_and_payments_monthly_rpu_roaming",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_monthly_rpu_roaming")
    return return_df


def billing_before_topup_balance_node_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly", par_col="event_partition_date",
                                                       target_table_name="l3_billing_and_payments_monthly_before_top_up_balance",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_before_top_up_balance")
    return return_df


def billing_topup_channels_node_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly", par_col="event_partition_date",
                                                       target_table_name="l3_billing_and_payments_monthly_top_up_channels",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_top_up_channels")
    return return_df


def billing_time_since_last_topup_node_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly", par_col="event_partition_date",
                                                       target_table_name="l3_billing_and_payments_monthly_time_since_last_top_up",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_time_since_last_top_up")
    return return_df


def billing_arpu_node_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_rpu")
    return return_df


def billing_most_popular_topup_channel_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_most_popular_top_up_channel")
    return return_df


def billing_volume_of_bills_and_roaming_bills_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_bill_volume")
    return return_df


def billing_missed_bills_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_missed_bills")
    return return_df


def billing_overdue_bills_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_overdue_bills")
    return return_df


def billing_last_overdue_bill_volume_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    return_df = massive_processing_monthly(input_df, sql,
                                           "l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume")
    return return_df


def copy_df_for_l3_billing_and_payments_monthly_popular_topup_day(input_df) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly", par_col="event_partition_date",
                                                       target_table_name="l3_billing_and_payments_monthly_popular_topup_day",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def billing_popular_topup_day_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_popular_topup_day")
    return return_df


def copy_df_for_l3_billing_and_payments_monthly_popular_topup_hour(input_df) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly", par_col="event_partition_date",
                                                       target_table_name="l3_billing_and_payments_monthly_popular_topup_hour",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def billing_popular_topup_hour_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_popular_topup_hour")
    return return_df


def copy_df_for_l3_billing_and_payments_monthly_last_three_topup_volume(input_df) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly", par_col="event_partition_date",
                                                       target_table_name="l3_billing_and_payments_monthly_last_three_topup_volume",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def billing_last_three_topup_volume_monthly(input_df, sql) -> DataFrame:
    """
    :return:
    """

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    return_df = massive_processing_monthly(input_df, sql, "l3_billing_and_payments_monthly_last_three_topup_volume")
    return return_df


def billing_last_topup_channel_monthly(input_df, customer_df, recharge_type, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, customer_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="partition_date",
                                                       target_table_name="l3_billing_and_payments_monthly_last_top_up_channel",
                                                       missing_data_check_flg='Y')

    customer_df = data_non_availability_and_missing_check(df=customer_df, grouping="monthly",
                                                          par_col="partition_month",
                                                          target_table_name="l3_billing_and_payments_monthly_last_top_up_channel")

    if check_empty_dfs([input_df, customer_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    recharge_data_with_topup_channel = top_up_channel_joined_data_for_monthly_last_top_up_channel(input_df,
                                                                                                  recharge_type)
    recharge_data_with_topup_channel = recharge_data_with_topup_channel.withColumn('start_of_month', F.to_date(
        F.date_trunc('month', input_df.recharge_date)))
    customer_df = derives_in_customer_profile(customer_df) \
        .where("charge_type = 'Pre-paid' and cust_active_this_month = 'Y'")
    return_df = process_last_topup_channel(recharge_data_with_topup_channel, customer_df, sql,
                                           "l3_billing_and_payments_monthly_last_top_up_channel")

    return_df = return_df.withColumn("rn", expr(
        "row_number() over(partition by start_of_month,access_method_num,register_date order by register_date desc)"))

    return_df = return_df.filter("rn = 1").drop("rn")

    return return_df


def copy_df_for_l3_billing_and_payments_monthly_topup_time_diff(input_df) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="partition_date",
                                                       target_table_name="l3_billing_and_payments_monthly_topup_time_diff",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def billing_time_diff_between_topups_monthly(customer_profile_df, input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, customer_profile_df]):
        return get_spark_empty_df()

    customer_prof = derives_in_customer_profile(customer_profile_df) \
        .where("charge_type = 'Pre-paid' and cust_active_this_month = 'Y'")

    customer_profile_df = data_non_availability_and_missing_check(df=customer_profile_df, grouping="monthly",
                                                                  par_col="start_of_month",
                                                                  target_table_name="l3_billing_and_payments_monthly_topup_time_diff")

    if check_empty_dfs([input_df, customer_profile_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.col("start_of_month")).alias("max_date")),
            customer_prof.select(
                f.max(f.col("start_of_month")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(f.col("start_of_month") <= min_value)
    customer_prof = customer_prof.filter(f.col("start_of_month") <= min_value)

    return_df = massive_processing(input_df, customer_prof, recharge_data_with_customer_profile_joined, sql,
                                   'start_of_month', 'start_of_month', 'Pre-paid',
                                   "l3_billing_and_payments_monthly_topup_time_diff")

    return return_df


def billing_data_joined(billing_monthly, payment_daily, target_table_name: str):
    # Need to check becasue billing_monthly and payment_daily are getting joined on a different column than partition_month

    table_name = target_table_name.split('_tbl')[0]

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([billing_monthly, payment_daily]):
        return get_spark_empty_df()

    payment_daily = data_non_availability_and_missing_check(df=payment_daily, grouping="monthly",
                                                            par_col="partition_date",
                                                            target_table_name=table_name,
                                                            missing_data_check_flg='Y')

    if check_empty_dfs([billing_monthly, payment_daily]):
        return get_spark_empty_df()

    min_value = union_dataframes_with_missing_cols(
        [
            payment_daily.select(
                f.max(f.to_date(
                    f.date_trunc('month', f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd')))).alias(
                    "max_date")),
            billing_monthly.select(
                f.max(f.col("start_of_month")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    payment_daily = payment_daily.filter(f.to_date(
        f.date_trunc('month', f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd'))) <= min_value)
    billing_monthly = billing_monthly.filter(f.col("start_of_month") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    output_df = billing_monthly.join(payment_daily,
                                     (billing_monthly.account_identifier == payment_daily.account_identifier) &
                                     (
                                             billing_monthly.billing_statement_identifier == payment_daily.billing_statement_identifier) &
                                     (billing_monthly.billing_statement_seq_no == payment_daily.bill_seq_no), 'left')

    output_df = output_df.drop(payment_daily.billing_statement_identifier) \
        .drop(payment_daily.account_identifier)

    return output_df


def derives_in_customer_profile(customer_prof):
    customer_prof = customer_prof.select("access_method_num",
                                         "billing_account_no",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "partition_month",
                                         "charge_type",
                                         "cust_active_this_month")

    customer_prof = customer_prof.withColumn("start_of_month", customer_prof.partition_month)

    return customer_prof


def billing_rpu_data_with_customer_profile(customer_prof, rpu_data):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([rpu_data, customer_prof]):
        return get_spark_empty_df()

    rpu_data = data_non_availability_and_missing_check(df=rpu_data, grouping="monthly", par_col="partition_month",
                                                       target_table_name="l3_billing_and_payments_monthly_rpu")

    customer_prof = derives_in_customer_profile(customer_prof) \
        .where("cust_active_this_month = 'Y'")

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="monthly",
                                                            par_col="start_of_month",
                                                            target_table_name="l3_billing_and_payments_monthly_rpu")

    if check_empty_dfs([rpu_data, customer_prof]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    min_value = union_dataframes_with_missing_cols(
        [
            rpu_data.select(
                f.to_date(f.max(f.col("partition_month")).cast(StringType()), 'yyyyMM').alias("max_date")),
            customer_prof.select(
                f.max(f.col("start_of_month")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    rpu_data = rpu_data.filter(f.to_date(f.col("partition_month").cast(StringType()), 'yyyyMM') <= min_value)
    customer_prof = customer_prof.filter(f.col("start_of_month") <= min_value)

    output_df = customer_prof.join(rpu_data, (customer_prof.access_method_num == rpu_data.access_method_num) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(rpu_data.register_date))) &
                                   (customer_prof.start_of_month == f.to_date(
                                       f.date_trunc('month', rpu_data.month_id))), 'left')

    output_df = output_df.drop(rpu_data.access_method_num) \
        .drop(rpu_data.register_date)

    return output_df


def billing_statement_hist_data_with_customer_profile(customer_prof, billing_hist, target_table_name: str):
    # Need to check becasue billing_hist is getting joined with customer on a different column than partition_month

    #table_name = target_table_name.split('_tbl')[0]

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([billing_hist, customer_prof]):
        return get_spark_empty_df()

    billing_hist = data_non_availability_and_missing_check(df=billing_hist, grouping="monthly",
                                                           par_col="partition_month",
                                                           target_table_name=target_table_name)

    customer_prof = derives_in_customer_profile(customer_prof) \
        .where("charge_type = 'Post-paid' and cust_active_this_month = 'Y'")

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="monthly",
                                                            par_col="start_of_month",
                                                            target_table_name=target_table_name)

    if check_empty_dfs([billing_hist, customer_prof]):
        return get_spark_empty_df()

    min_value = union_dataframes_with_missing_cols(
        [
            billing_hist.select(
                f.max(f.to_date(
                    f.date_trunc('month', f.to_date((f.col("partition_month")).cast(StringType()), 'yyyyMM')))).alias(
                    "max_date")),
            customer_prof.select(
                f.max(f.col("start_of_month")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    billing_hist = billing_hist.filter(f.to_date(
        f.date_trunc('month', f.to_date((f.col("partition_month")).cast(StringType()), 'yyyyMM'))) <= min_value)
    customer_prof = customer_prof.filter(f.col("start_of_month") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    output_df = customer_prof.join(billing_hist, (customer_prof.billing_account_no == billing_hist.account_num) &
                                   (customer_prof.start_of_month == f.to_date(
                                       f.date_trunc('month', billing_hist.billing_stmt_period_eff_date))), 'left')

    return output_df


def bill_payment_daily_data_with_customer_profile(customer_prof, pc_t_data):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([pc_t_data, customer_prof]):
        return get_spark_empty_df()

    pc_t_data = data_non_availability_and_missing_check(df=pc_t_data, grouping="monthly", par_col="partition_date",
                                                        target_table_name="l3_billing_and_payments_monthly_automated_payments",
                                                        missing_data_check_flg='Y')

    customer_prof = derives_in_customer_profile(customer_prof) \
        .where("charge_type = 'Post-paid' and cust_active_this_month = 'Y'")

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="monthly",
                                                            par_col="start_of_month",
                                                            target_table_name="l3_billing_and_payments_monthly_automated_payments")

    if check_empty_dfs([pc_t_data, customer_prof]):
        return get_spark_empty_df()

    min_value = union_dataframes_with_missing_cols(
        [
            pc_t_data.select(
                f.max(f.to_date(
                    f.date_trunc('month', f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd')))).alias(
                    "max_date")),
            customer_prof.select(
                f.max(f.col("start_of_month")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    pc_t_data = pc_t_data.filter(f.to_date(
        f.date_trunc('month', f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd'))) <= min_value)
    customer_prof = customer_prof.filter(f.col("start_of_month") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    output_df = customer_prof.join(pc_t_data, (customer_prof.billing_account_no == pc_t_data.ba_no) &
                                   (customer_prof.start_of_month == f.to_date(
                                       f.date_trunc('month', pc_t_data.payment_date))), 'left')

    return output_df


def recharge_data_with_customer_profile_joined(customer_prof, recharge_data):
    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "start_of_month",
                                         "charge_type")

    output_df = customer_prof.join(recharge_data, (customer_prof.access_method_num == recharge_data.access_method_num) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(recharge_data.register_date))) &
                                   (customer_prof.start_of_month == f.to_date(recharge_data.start_of_month)), 'left')

    output_df = output_df.drop(recharge_data.access_method_num) \
        .drop(recharge_data.register_date) \
        .drop(recharge_data.start_of_month)

    output_df = output_df.withColumn("rn", expr(
        "row_number() over(partition by start_of_month,access_method_num,register_date order by register_date desc)"))

    output_df = output_df.filter("rn = 1").drop("rn")

    return output_df
