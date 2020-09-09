import pyspark.sql.functions as f
from pyspark.sql.functions import expr
from pyspark.sql import DataFrame
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
from src.customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check
import os
from pyspark.sql.types import *

conf = os.getenv("CONF", None)


def massive_processing(input_df, customer_prof_input_df, join_function, sql, partition_date, cust_partition_date,
                       cust_type, output_df_catalog):
    """
    :return:
    """

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.col("start_of_week")).alias("max_date")),
            customer_prof_input_df.select(
                f.max(f.col("start_of_week")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(f.col("start_of_week") <= min_value)
    customer_prof_input_df = customer_prof_input_df.filter(f.col("start_of_week") <= min_value)

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

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.to_date(partition_date).isin(*[curr_item]))
        customer_prof_df = cust_data_frame.filter(f.col(cust_partition_date).isin(*[curr_item]))
        joined_df = join_function(customer_prof_df, small_df)
        output_df = node_from_config(joined_df, sql)
        CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.to_date(partition_date).isin(*[first_item]))
    customer_prof_df = cust_data_frame.filter(f.col(cust_partition_date).isin(*[first_item]))
    joined_df = join_function(customer_prof_df, return_df)
    final_df = node_from_config(joined_df, sql)

    return final_df


def massive_processing_weekly(data_frame: DataFrame, dict_obj: dict, output_df_catalog) -> DataFrame:
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
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col("start_of_week").isin(*[curr_item]))
        output_df = node_from_config(small_df, dict_obj)
        CNTX.catalog.save(output_df_catalog, output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col("start_of_week").isin(*[first_item]))
    return_df = node_from_config(return_df, dict_obj)
    return return_df


def massive_processing_weekly_2(data_frame: DataFrame, dict_obj1: dict, dict_obj2: dict, output_df_temp_catalog, output_df_catalog) -> [DataFrame, DataFrame]:
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
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col("start_of_week").isin(*[curr_item]))
        output_df_temp = node_from_config(small_df, dict_obj1)
        output_df = node_from_config(output_df_temp, dict_obj2)
        CNTX.catalog.save(output_df_temp_catalog, output_df_temp)
        CNTX.catalog.save(output_df_catalog, output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df_2 = data_frame.filter(f.col("start_of_week").isin(*[first_item]))
    return_df_temp = node_from_config(small_df_2, dict_obj1)
    return_df = node_from_config(return_df_temp, dict_obj2)
    return [return_df_temp, return_df]


def customized_processing(data_frame: DataFrame, cust_prof: DataFrame, recharge_type_df: DataFrame, sql: dict,
                          output_df_catalog) -> DataFrame:
    """
    :return:
    """

    min_value = union_dataframes_with_missing_cols(
        [
            data_frame.select(
                f.to_date(f.date_trunc('week', f.to_date(f.max(f.col("partition_date")).cast(StringType()), 'yyyyMMdd'))).alias(
                    "max_date")),
            cust_prof.select(
                f.max(f.col("start_of_week")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    data_frame = data_frame.filter(f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    cust_prof = cust_prof.filter(f.col("start_of_week") <= min_value)

    def divide_chunks(l, n):

        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    cust_data_frame = cust_prof.where("charge_type = 'Pre-paid'")
    dates_list = cust_prof.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.withColumn("start_of_week", f.to_date(f.date_trunc('week', data_frame.recharge_date)))
        small_df = small_df.filter(f.to_date('start_of_week').isin(*[curr_item]))
        customer_prof_df = cust_data_frame.filter(f.col('start_of_week').isin(*[curr_item]))
        joined_df_with_recharge_type = top_up_channel_joined_data_for_weekly_last_top_up_channel(small_df, recharge_type_df)
        df = node_from_config(joined_df_with_recharge_type, sql)
        output_df = recharge_data_with_customer_profile_joined(customer_prof_df, df)
        CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.withColumn("start_of_week", f.to_date(f.date_trunc('week', data_frame.recharge_date)))
    small_df = small_df.filter(f.to_date('start_of_week').isin(*[first_item]))
    customer_prof_df = cust_data_frame.filter(f.col('start_of_week').isin(*[first_item]))
    joined_df_with_recharge_type = top_up_channel_joined_data_for_weekly_last_top_up_channel(small_df, recharge_type_df)
    df = node_from_config(joined_df_with_recharge_type, sql)
    output_df = recharge_data_with_customer_profile_joined(customer_prof_df, df)

    return output_df


def billing_topup_count_and_volume_node_weekly(input_df, sql, exception_partition=None) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name="l2_billing_and_payments_weekly_topup_and_volume",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_topup_and_volume")
    return return_df


def billing_arpu_roaming_weekly(input_df, sql, exception_partition=None) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name="l2_billing_and_payments_weekly_rpu_roaming",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_rpu_roaming")
    return return_df


def billing_before_topup_balance_weekly(input_df, sql, exception_partition=None) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name="l2_billing_and_payments_weekly_before_top_up_balance",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_before_top_up_balance")
    return return_df


def billing_top_up_channels_weekly(input_df, sql, exception_partition=None) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name="l2_billing_and_payments_weekly_top_up_channels",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_top_up_channels")
    return return_df


# def billing_most_popular_top_up_channel_weekly(input_df, sql) -> DataFrame:
#     """
#     :return:
#     """
#
#     ################################# Start Implementing Data availability checks #############################
#     if check_empty_dfs([input_df]):
#         return get_spark_empty_df()
#
#     input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="start_of_week",
#                                                        target_table_name="l2_billing_and_payments_weekly_most_popular_top_up_channel")
#
#     if check_empty_dfs([input_df]):
#         return get_spark_empty_df()
#
#     ################################# End Implementing Data availability checks ###############################
#
#     return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_most_popular_top_up_channel")
#     return return_df


def billing_popular_topup_day_weekly(input_df, sql1, sql2, exception_partition=None) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name="l2_billing_and_payments_weekly_popular_topup_day",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################
    [final_temp_df, final_df] = massive_processing_weekly_2(input_df, sql1, sql2, "l2_billing_and_payments_weekly_popular_topup_day_intermediate",
                                           "l2_billing_and_payments_weekly_popular_topup_day")

    return [final_temp_df, final_df]


# def billing_popular_topup_day_weekly(input_df, sql) -> DataFrame:
#     """
#     :return:
#     """
#
#     ################################# Start Implementing Data availability checks #############################
#     if check_empty_dfs([input_df]):
#         return get_spark_empty_df()
#
#     input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="start_of_week",
#                                                        target_table_name="l2_billing_and_payments_weekly_popular_topup_day")
#
#     if check_empty_dfs([input_df]):
#         return get_spark_empty_df()
#
#     ################################# End Implementing Data availability checks ###############################
#
#     return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_popular_topup_day")
#     return return_df


def billing_popular_topup_hour_weekly(input_df, sql1, sql2, exception_partition=None) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name="l2_billing_and_payments_weekly_popular_topup_hour",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    [return_temp_df, return_df] = massive_processing_weekly_2(input_df, sql1, sql2, "l2_billing_and_payments_weekly_popular_topup_hour_intermediate",
                                                              "l2_billing_and_payments_weekly_popular_topup_hour")
    return [return_temp_df, return_df]


# def billing_popular_topup_hour_weekly(input_df, sql) -> DataFrame:
#     """
#     :return:
#     """
#
#     ################################# Start Implementing Data availability checks #############################
#     if check_empty_dfs([input_df]):
#         return get_spark_empty_df()
#
#     input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="start_of_week",
#                                                        target_table_name="l2_billing_and_payments_weekly_popular_topup_hour")
#
#     if check_empty_dfs([input_df]):
#         return get_spark_empty_df()
#
#     ################################# End Implementing Data availability checks ###############################
#
#     return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_popular_topup_hour")
#     return return_df


def billing_time_since_last_topup_weekly(input_df, sql, exception_partition=None) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name="l2_billing_and_payments_weekly_time_since_last_top_up",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_time_since_last_top_up")
    return return_df


def df_copy_for_l2_billing_and_payments_weekly_last_three_topup_volume(input_df, exception_partition=None) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name="l2_billing_and_payments_weekly_last_three_topup_volume",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def billing_last_three_topup_volume_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_last_three_topup_volume")
    return return_df


def derives_in_customer_profile(customer_prof):
    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date",
                                         "charge_type")

    customer_prof = customer_prof.withColumn("start_of_week",
                                             f.to_date(f.date_trunc('week', customer_prof.event_partition_date)))
    customer_prof = customer_prof.dropDuplicates(
        ["start_of_week", "access_method_num", "register_date", "subscription_identifier"])

    return customer_prof


def billing_last_top_up_channel_weekly(input_df, customer_profile_df, recharge_type_df, sql, exception_partition_of_input_df, exception_partition_of_customer_profile_df) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, customer_profile_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="partition_date",
                                                       target_table_name="l2_billing_and_payments_weekly_last_top_up_channel",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition_of_input_df)

    customer_profile_df = data_non_availability_and_missing_check(df=customer_profile_df, grouping="weekly",
                                                                  par_col="event_partition_date",
                                                                  target_table_name="l2_billing_and_payments_weekly_last_top_up_channel",
                                                                  missing_data_check_flg='Y',
                                                                  exception_partitions=exception_partition_of_customer_profile_df)

    if check_empty_dfs([input_df, customer_profile_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    customer_prof = derives_in_customer_profile(customer_profile_df)

    customer_prof = customer_prof.drop("event_partition_date")

    return_df = customized_processing(input_df, customer_prof, recharge_type_df, sql,
                                      "l2_billing_and_payments_weekly_last_top_up_channel")

    # return_df = return_df.withColumn("rn", expr(
    #     "row_number() over(partition by start_of_week,access_method_num,register_date order by recharge_time desc)"))
    #
    # return_df = return_df.filter("rn = 1").drop("rn")

    return return_df


def billing_time_diff_between_topups_weekly(customer_profile_df, input_df, sql, exception_partition_of_customer_profile_df, exception_partition_of_input_df) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, customer_profile_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name="l2_billing_and_payments_weekly_topup_time_diff",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition_of_customer_profile_df)

    customer_profile_df = data_non_availability_and_missing_check(df=customer_profile_df, grouping="weekly",
                                                                  par_col="event_partition_date",
                                                                  target_table_name="l2_billing_and_payments_weekly_topup_time_diff",
                                                                  missing_data_check_flg='Y',
                                                                  exception_partitions=exception_partition_of_input_df)

    if check_empty_dfs([input_df, customer_profile_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    customer_prof = derives_in_customer_profile(customer_profile_df)

    customer_prof = customer_prof.drop("event_partition_date")

    return_df = massive_processing(input_df, customer_prof, recharge_data_with_customer_profile_joined, sql,
                                   'start_of_week', 'start_of_week', "Pre-paid",
                                   "l2_billing_and_payments_weekly_topup_time_diff")

    return return_df


def recharge_data_with_customer_profile_joined(customer_prof, recharge_data):
    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "start_of_week",
                                         "charge_type")

    output_df = customer_prof.join(recharge_data, (customer_prof.access_method_num == recharge_data.access_method_num) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(recharge_data.register_date))) &
                                   (customer_prof.start_of_week == f.to_date(recharge_data.start_of_week)), 'left')

    output_df = output_df.drop(recharge_data.access_method_num) \
        .drop(recharge_data.register_date) \
        .drop(recharge_data.start_of_week)

    output_df = output_df.withColumn("rn", expr(
        "row_number() over(partition by start_of_week,access_method_num,register_date order by recharge_time desc)"))

    output_df = output_df.filter("rn = 1").drop("rn", "access_method_num", "register_date")

    return output_df


def billing_most_popular_top_up_channel_weekly(input_df, topup_type_ref, sql1, sql2, exception_partition=None):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name="l2_billing_and_payments_weekly_most_popular_top_up_channel",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return [get_spark_empty_df(),get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    topup_type_ref = topup_type_ref.withColumn("rn", expr(
        "row_number() over(partition by recharge_topup_event_type_cd order by partition_date desc)"))

    topup_type_ref = topup_type_ref.where("rn = 1").drop("rn")

    output_df_temp = input_df.join(topup_type_ref,
                                   input_df.recharge_type == topup_type_ref.recharge_topup_event_type_cd,
                                   'left')

    [output_temp_df, output_df] = massive_processing_weekly_2(output_df_temp, sql1, sql2, "l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate",
                                            "l2_billing_and_payments_weekly_most_popular_top_up_channel")

    return [output_temp_df, output_df]


def top_up_channel_joined_data_for_weekly_last_top_up_channel(input_df, topup_type_ref):

    topup_type_ref = topup_type_ref.withColumn("rn", expr(
        "row_number() over(partition by recharge_topup_event_type_cd order by partition_date desc)"))

    topup_type_ref = topup_type_ref.where("rn = 1").drop("rn")

    output_df = input_df.join(topup_type_ref, input_df.recharge_type == topup_type_ref.recharge_topup_event_type_cd,
                              'left')

    return output_df


