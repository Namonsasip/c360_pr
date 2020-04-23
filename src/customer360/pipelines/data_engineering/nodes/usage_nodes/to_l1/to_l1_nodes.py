import logging
import os
from pathlib import Path

from kedro.context.context import load_context
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from customer360.utilities.config_parser import node_from_config
from src.customer360.utilities.spark_util import get_spark_empty_df

from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, execute_sql

conf = os.getenv("CONF", None)


def gen_max_sql(data_frame, table_name, group):
    grp_str = ', '.join(group)
    col_to_iterate = ["max(" + x + ")" + " as " + x for x in data_frame.columns if x not in group]
    all_cols = ', '.join(col_to_iterate)
    final_str = "select {0}, {1} {2} {3} group by {4}".format(grp_str, all_cols, "from", table_name, grp_str)
    return final_str


def massive_processing(input_df, sql, output_df_catalog):
    """
    :return:
    """

    if len(input_df.head(1)) == 0:
        return input_df

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("partition_date").isin(*[curr_item]))
        output_df = node_from_config(small_df, sql)
        print("schema:", output_df.printSchema())
        CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("partition_date").isin(*[first_item]))
    return_df = node_from_config(return_df, sql)

    return return_df


def merge_with_customer_df(source_df: DataFrame,
                           cust_df: DataFrame) -> DataFrame:
    """

    :param source_df:
    :param cust_df:
    :return:
    """
    # This code will populate a subscriber id to the data set.
    cust_df_cols = ['access_method_num', 'subscription_identifier', 'event_partition_date']
    join_key = ['access_method_num', 'event_partition_date']
    final_df = source_df.join(cust_df.select(cust_df_cols), join_key, how="left")

    return final_df


def usage_outgoing_ir_call_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_outgoing_call_relation_sum_ir_daily")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing(input_df, sql, "l1_usage_outgoing_call_relation_sum_ir_daily")
    return return_df


def usage_incoming_ir_call_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_incoming_call_relation_sum_ir_daily")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing(input_df, sql, "l1_usage_incoming_call_relation_sum_ir_daily")
    return return_df


def usage_outgoing_call_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_outgoing_call_relation_sum_daily",
                                                       exception_partitions=['2019-12-01'])

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing(input_df, sql, "l1_usage_outgoing_call_relation_sum_daily")
    return return_df


def usage_incoming_call_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_incoming_call_relation_sum_daily",
                                                       exception_partitions=['2019-12-01'])

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing(input_df, sql, "l1_usage_incoming_call_relation_sum_daily")
    return return_df


def usage_data_prepaid_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_ru_a_gprs_cbs_usage_daily")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing(input_df, sql, "l1_usage_ru_a_gprs_cbs_usage_daily")
    return return_df


def usage_data_postpaid_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_ru_a_vas_postpaid_usg_daily")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing(input_df, sql, "l1_usage_ru_a_vas_postpaid_usg_daily")
    return return_df


def build_data_for_prepaid_postpaid_vas(prepaid: DataFrame
                                        , postpaid: DataFrame) -> DataFrame:
    """

    :param prepaid:
    :param postpaid:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([prepaid, postpaid]):
        return get_spark_empty_df()

    prepaid = data_non_availability_and_missing_check(df=prepaid, grouping="daily", par_col="partition_date",
                                                      target_table_name="l1_usage_ru_a_vas_postpaid_prepaid_daily")

    postpaid = data_non_availability_and_missing_check(df=postpaid, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_ru_a_vas_postpaid_prepaid_daily")

    if check_empty_dfs([prepaid, postpaid]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    prepaid = prepaid.select("access_method_num", "number_of_call", 'day_id')
    postpaid = postpaid.where("call_type_cd = 5") \
        .select("access_method_num", F.col("no_transaction").alias("number_of_call"), 'day_id')

    final_df = union_dataframes_with_missing_cols(prepaid, postpaid)

    return final_df


def merge_all_dataset_to_one_table(l1_usage_outgoing_call_relation_sum_daily_stg: DataFrame,
                                   l1_usage_incoming_call_relation_sum_daily_stg: DataFrame,
                                   l1_usage_outgoing_call_relation_sum_ir_daily_stg: DataFrame,
                                   l1_usage_incoming_call_relation_sum_ir_daily_stg: DataFrame,
                                   l1_usage_ru_a_gprs_cbs_usage_daily_stg: DataFrame,
                                   l1_usage_ru_a_vas_postpaid_usg_daily_stg: DataFrame,
                                   l1_usage_ru_a_vas_postpaid_prepaid_daily_stg: DataFrame,
                                   l1_customer_profile_union_daily_feature: DataFrame) -> DataFrame:
    """
    :param l1_usage_outgoing_call_relation_sum_daily_stg:
    :param l1_usage_incoming_call_relation_sum_daily_stg:
    :param l1_usage_outgoing_call_relation_sum_ir_daily_stg:
    :param l1_usage_incoming_call_relation_sum_ir_daily_stg:
    :param l1_usage_ru_a_gprs_cbs_usage_daily_stg:
    :param l1_usage_ru_a_vas_postpaid_usg_daily_stg:
    :param l1_usage_ru_a_vas_postpaid_prepaid_daily_stg:
    :param l1_customer_profile_union_daily_feature:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([l1_usage_outgoing_call_relation_sum_daily_stg, l1_usage_incoming_call_relation_sum_daily_stg,
                        l1_usage_outgoing_call_relation_sum_ir_daily_stg,
                        l1_usage_incoming_call_relation_sum_ir_daily_stg,
                        l1_usage_ru_a_gprs_cbs_usage_daily_stg, l1_usage_ru_a_vas_postpaid_usg_daily_stg,
                        l1_usage_ru_a_vas_postpaid_prepaid_daily_stg, l1_customer_profile_union_daily_feature]):
        return get_spark_empty_df()

    l1_usage_outgoing_call_relation_sum_daily_stg = data_non_availability_and_missing_check(
        df=l1_usage_outgoing_call_relation_sum_daily_stg,
        grouping="daily", par_col="event_partition_date",
        target_table_name="l1_usage_postpaid_prepaid_daily")

    l1_usage_incoming_call_relation_sum_daily_stg = data_non_availability_and_missing_check(
        df=l1_usage_incoming_call_relation_sum_daily_stg,
        grouping="daily", par_col="event_partition_date",
        target_table_name="l1_usage_postpaid_prepaid_daily")

    l1_usage_outgoing_call_relation_sum_ir_daily_stg = data_non_availability_and_missing_check(
        df=l1_usage_outgoing_call_relation_sum_ir_daily_stg,
        grouping="daily", par_col="event_partition_date",
        target_table_name="l1_usage_postpaid_prepaid_daily")

    l1_usage_incoming_call_relation_sum_ir_daily_stg = data_non_availability_and_missing_check(
        df=l1_usage_incoming_call_relation_sum_ir_daily_stg,
        grouping="daily", par_col="event_partition_date",
        target_table_name="l1_usage_postpaid_prepaid_daily")

    l1_usage_ru_a_gprs_cbs_usage_daily_stg = data_non_availability_and_missing_check(
        df=l1_usage_ru_a_gprs_cbs_usage_daily_stg,
        grouping="daily", par_col="event_partition_date",
        target_table_name="l1_usage_postpaid_prepaid_daily")

    l1_usage_ru_a_vas_postpaid_usg_daily_stg = data_non_availability_and_missing_check(
        df=l1_usage_ru_a_vas_postpaid_usg_daily_stg,
        grouping="daily", par_col="event_partition_date",
        target_table_name="l1_usage_postpaid_prepaid_daily")

    l1_usage_ru_a_vas_postpaid_prepaid_daily_stg = data_non_availability_and_missing_check(
        df=l1_usage_ru_a_vas_postpaid_prepaid_daily_stg,
        grouping="daily", par_col="event_partition_date",
        target_table_name="l1_usage_postpaid_prepaid_daily")

    l1_customer_profile_union_daily_feature = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature,
        grouping="daily", par_col="event_partition_date",
        target_table_name="l1_usage_postpaid_prepaid_daily")

    # new section to handle data latency
    min_value = union_dataframes_with_missing_cols(
        [
            l1_usage_outgoing_call_relation_sum_daily_stg.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
            l1_usage_incoming_call_relation_sum_daily_stg.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
            l1_usage_outgoing_call_relation_sum_ir_daily_stg.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
            l1_usage_incoming_call_relation_sum_ir_daily_stg.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
            l1_usage_ru_a_gprs_cbs_usage_daily_stg.select(F.max(F.col("event_partition_date")).alias("max_date")),
            l1_usage_ru_a_vas_postpaid_usg_daily_stg.select(F.max(F.col("event_partition_date")).alias("max_date")),
            l1_usage_ru_a_vas_postpaid_prepaid_daily_stg.select(F.max(F.col("event_partition_date")).alias("max_date")),
            l1_customer_profile_union_daily_feature.select(F.max(F.col("event_partition_date")).alias("max_date"))
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    drop_cols = ["access_method_num", "called_no", "caller_no", "call_start_dt", "day_id"]
    union_df = union_dataframes_with_missing_cols([
        l1_usage_outgoing_call_relation_sum_daily_stg, l1_usage_incoming_call_relation_sum_daily_stg,
        l1_usage_outgoing_call_relation_sum_ir_daily_stg, l1_usage_incoming_call_relation_sum_ir_daily_stg,
        l1_usage_ru_a_gprs_cbs_usage_daily_stg, l1_usage_ru_a_vas_postpaid_usg_daily_stg,
        l1_usage_ru_a_vas_postpaid_prepaid_daily_stg
    ])

    union_df = union_df.filter(F.col("event_partition_date") <= min_value)

    if check_empty_dfs([union_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    group_cols = ['access_method_num', 'event_partition_date']
    final_df_str = gen_max_sql(union_df, 'roaming_incoming_outgoing_data', group_cols)
    sel_cols = ['access_method_num',
                'event_partition_date',
                "subscription_identifier",
                "start_of_week"]

    join_cols = ['access_method_num',
                 'event_partition_date',
                 "start_of_week"]

    """
    :return:
    """
    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = union_df
    dates_list = data_frame.select('event_partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_array = list(divide_chunks(mvv_array, 5))
    add_list = mvv_array

    first_item = add_list[0]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("event_partition_date").isin(*[curr_item]))
        output_df = execute_sql(data_frame=small_df, table_name='roaming_incoming_outgoing_data', sql_str=final_df_str)
        cust_df = l1_customer_profile_union_daily_feature.filter((F.col("event_partition_date").isin(*[curr_item]))) \
            .select(sel_cols)

        output_df = cust_df.join(output_df, join_cols, how="left")
        CNTX.catalog.save("l1_usage_postpaid_prepaid_daily", output_df.drop(*drop_cols))

    logging.info("running for dates {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("event_partition_date").isin(*[first_item]))
    cust_df = l1_customer_profile_union_daily_feature.filter(F.col("event_partition_date").isin(*[first_item])) \
        .select(sel_cols)
    return_df = execute_sql(data_frame=return_df, table_name='roaming_incoming_outgoing_data', sql_str=final_df_str)
    return_df = cust_df.join(return_df, join_cols, how="left")

    return return_df.drop(*drop_cols)
