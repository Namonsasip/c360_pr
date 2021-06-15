import logging
import logging
import os
from pathlib import Path

from kedro.context.context import load_context
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
import dateutil
from datetime import *

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, execute_sql, gen_max_sql, add_event_week_and_month_from_yyyymmdd
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

conf = os.getenv("CONF", None)


def l1_usage_most_idd_features(input_df, input_cust):

    if check_empty_dfs([input_df, input_cust]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_most_idd_features",
                                                       exception_partitions="")

    input_cust = data_non_availability_and_missing_check(df=input_cust, grouping="daily",
                                                         par_col="event_partition_date",
                                                         target_table_name="l1_usage_most_idd_features")

    if check_empty_dfs([input_df, input_cust]):
        return get_spark_empty_df()

    input_cust = input_cust.select('access_method_num', 'subscription_identifier', 'event_partition_date')

    spark = get_spark_session()

    input_df.registerTempTable("usage_call_relation_sum_daily")

    stmt_full = """select cast(regexp_replace(substr(day_id,1,10),'-','') as int) as partition_date
                    ,called_network_type
                    ,caller_no as access_method_num
                    ,idd_country
                    ,sum(total_successful_call) as usage_total_idd_successful_call
                    ,sum(total_minutes) as usage_total_idd_minutes
                    ,sum(total_durations) as usage_total_idd_durations
                    ,sum(total_net_revenue) as usage_total_idd_net_revenue
                    from usage_call_relation_sum_daily
                    where  idd_flag ='Y'
                    group by 1,2,3,4
                   """

    df = spark.sql(stmt_full)
    df = add_event_week_and_month_from_yyyymmdd(df, 'partition_date')
    join_key = {
        'on': [df.access_method_num == input_cust.access_method_num,
               df.event_partition_date == input_cust.event_partition_date],
        'how': 'left'
    }
    df_output = df.alias("a").join(input_cust.alias("b"), join_key['on'],
                                   join_key['how']).select('a.partition_date','b.subscription_identifier','a.called_network_type','a.idd_country','a.usage_total_idd_successful_call','a.usage_total_idd_minutes','a.usage_total_idd_durations','a.usage_total_idd_net_revenue','a.start_of_week', 'a.start_of_month', 'a.event_partition_date')

    return df_output


def l1_usage_last_idd_features_join_profile(input_df: DataFrame, input_cust: DataFrame, config):

    if check_empty_dfs([input_df, input_cust]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_last_idd_features",
                                                       exception_partitions="")

    input_cust = data_non_availability_and_missing_check(df=input_cust, grouping="daily", par_col="event_partition_date",
                                                         target_table_name="l1_usage_last_idd_features")

    if check_empty_dfs([input_df, input_cust]):
        return get_spark_empty_df()

    age_df = node_from_config(input_df, config)
    spark = get_spark_session()
    age_df.registerTempTable("usage_call_relation_sum_daily")

    sql_stmt = """select day_id as partition_date
                , caller_no as access_method_num
                , called_network_type
                , idd_country as last_idd_country
                , total_successful_call as usage_total_idd_successful_call
                , total_minutes as usage_total_idd_minutes
                , total_durations as usage_total_idd_durations
                , total_net_revenue as usage_total_idd_net_revenue
                , start_of_week
                , start_of_month
                , event_partition_date
                from (
                select  row_number() over(partition by caller_no, day_id order by hour_id desc) as row_num
                ,*
                from (usage_call_relation_sum_daily) tmp
                ) a
                where row_num = 1"""

    df = spark.sql(sql_stmt)
    input_cust = input_cust.select('access_method_num', 'subscription_identifier', 'event_partition_date')
    df_join_profile = df.join(input_cust, ['access_method_num', 'event_partition_date'], 'left')
    df_output = df_join_profile.select('partition_date', 'subscription_identifier', 'called_network_type', 'last_idd_country', 'usage_total_idd_successful_call', 'usage_total_idd_minutes', 'usage_total_idd_durations', 'usage_total_idd_net_revenue', 'start_of_week', 'start_of_month', 'event_partition_date')

    return df_output


def massive_processing_join_master(input_df: DataFrame
                                   , master_data: DataFrame
                                   , sql: dict
                                   , output_df_catalog: str):
    """
    :param input_df:
    :param master_data:
    :param sql:
    :param output_df_catalog:
    :return:
    """
    max_date = master_data.groupby().max('execute_date').collect()[0].asDict()['max(execute_date)']
    master_data = master_data.where("execute_date ={}".format(max_date))

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

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("partition_date").isin(*[curr_item]))
        small_df = small_df.join(master_data, ["caller_no", "called_no"], how="left")
        output_df = node_from_config(small_df, sql)
        CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("partition_date").isin(*[first_item]))
    return_df = return_df.join(master_data, ["caller_no", "called_no"], how="left")
    return_df = node_from_config(return_df, sql)

    return return_df


def massive_processing(input_df, sql, output_df_catalog):
    """
    :param input_df:
    :param sql:
    :param output_df_catalog:
    :return:
    """

    if check_empty_dfs([input_df]):
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

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("partition_date").isin(*[curr_item]))
        output_df = node_from_config(small_df, sql)
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


def usage_outgoing_ir_call_pipeline(input_df: DataFrame, master_df: DataFrame, sql: dict) -> DataFrame:
    """
    :param input_df:
    :param master_df:
    :param sql:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, master_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_outgoing_call_relation_sum_ir_daily")

    if check_empty_dfs([input_df, master_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_join_master(input_df, master_df, sql, "l1_usage_outgoing_call_relation_sum_ir_daily")
    return return_df


def usage_incoming_ir_call_pipeline(input_df: DataFrame, master_df: DataFrame, sql: dict) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, master_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_incoming_call_relation_sum_ir_daily")

    if check_empty_dfs([input_df, master_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_join_master(input_df, master_df, sql, "l1_usage_incoming_call_relation_sum_ir_daily")
    return return_df


def usage_outgoing_call_pipeline(input_df: DataFrame
                                 , master_data: DataFrame
                                 , sql: dict
                                 , exception_partition=None) -> DataFrame:
    """
    :param input_df:
    :param master_data:
    :param sql:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, master_data]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_outgoing_call_relation_sum_daily",
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df, master_data]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_join_master(input_df, master_data, sql, "l1_usage_outgoing_call_relation_sum_daily")
    return return_df


def usage_incoming_call_pipeline(input_df: DataFrame
                                 , master_data: DataFrame
                                 , sql: dict
                                 , exception_partition=None) -> DataFrame:
    """
    :param input_df:
    :param master_data:
    :param sql:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_incoming_call_relation_sum_daily",
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return_df = massive_processing_join_master(input_df, master_data, sql, "l1_usage_incoming_call_relation_sum_daily")
    return return_df


def usage_data_prepaid_pipeline(input_df, sql, exception_partition=None) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_ru_a_gprs_cbs_usage_daily",
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################
    input_df = input_df.withColumn("data_upload_amt", F.col("data_upload_amt") / F.lit(1024)) \
        .withColumn("data_download_amt", F.col("data_download_amt") / F.lit(1024))
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


def usage_data_postpaid_roaming(input_df, sql) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_usage_data_postpaid_roaming")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################
    input_df = input_df.withColumn("ir_gprs_call_uplink_vol", F.col("ir_gprs_call_uplink_vol")/F.lit(1024)) \
                       .withColumn("ir_gprs_call_downlink_vol", F.col("ir_gprs_call_downlink_vol")/F.lit(1024))

    return_df = massive_processing(input_df, sql, "l1_usage_data_postpaid_roaming")
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
                                   l1_usage_data_postpaid_roaming_stg: DataFrame,
                                   l1_customer_profile_union_daily_feature: DataFrame,
                                   # exception_partition_of_l1_usage_outgoing_call_relation_sum_daily_stg=None,
                                   # exception_partition_of_l1_usage_incoming_call_relation_sum_daily_stg=None,
                                   # exception_partition_of_l1_usage_ru_a_gprs_cbs_usage_daily_stg=None,
                                   ) -> DataFrame:
    """
    :param l1_usage_outgoing_call_relation_sum_daily_stg:
    :param l1_usage_incoming_call_relation_sum_daily_stg:
    :param l1_usage_outgoing_call_relation_sum_ir_daily_stg:
    :param l1_usage_incoming_call_relation_sum_ir_daily_stg:
    :param l1_usage_ru_a_gprs_cbs_usage_daily_stg:
    :param l1_usage_ru_a_vas_postpaid_usg_daily_stg:
    :param l1_usage_ru_a_vas_postpaid_prepaid_daily_stg:
    :param l1_customer_profile_union_daily_feature:
    :param l1_usage_data_postpaid_roaming_stg:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([l1_usage_outgoing_call_relation_sum_daily_stg, l1_usage_incoming_call_relation_sum_daily_stg,
                        l1_usage_outgoing_call_relation_sum_ir_daily_stg,
                        l1_usage_incoming_call_relation_sum_ir_daily_stg,
                        l1_usage_ru_a_gprs_cbs_usage_daily_stg, l1_usage_ru_a_vas_postpaid_usg_daily_stg,
                        l1_usage_ru_a_vas_postpaid_prepaid_daily_stg, l1_customer_profile_union_daily_feature,
                        l1_usage_data_postpaid_roaming_stg]):
        return get_spark_empty_df()

    # l1_usage_outgoing_call_relation_sum_daily_stg = data_non_availability_and_missing_check(
    #     df=l1_usage_outgoing_call_relation_sum_daily_stg,
    #     grouping="daily", par_col="event_partition_date",
    #     target_table_name="l1_usage_postpaid_prepaid_daily",
    #     exception_partitions=exception_partition_of_l1_usage_outgoing_call_relation_sum_daily_stg)
    #
    # l1_usage_incoming_call_relation_sum_daily_stg = data_non_availability_and_missing_check(
    #     df=l1_usage_incoming_call_relation_sum_daily_stg,
    #     grouping="daily", par_col="event_partition_date",
    #     target_table_name="l1_usage_postpaid_prepaid_daily",
    #     exception_partitions=exception_partition_of_l1_usage_incoming_call_relation_sum_daily_stg)
    #
    # l1_usage_outgoing_call_relation_sum_ir_daily_stg = data_non_availability_and_missing_check(
    #     df=l1_usage_outgoing_call_relation_sum_ir_daily_stg,
    #     grouping="daily", par_col="event_partition_date",
    #     target_table_name="l1_usage_postpaid_prepaid_daily")
    #
    # l1_usage_incoming_call_relation_sum_ir_daily_stg = data_non_availability_and_missing_check(
    #     df=l1_usage_incoming_call_relation_sum_ir_daily_stg,
    #     grouping="daily", par_col="event_partition_date",
    #     target_table_name="l1_usage_postpaid_prepaid_daily")
    #
    # l1_usage_ru_a_gprs_cbs_usage_daily_stg = data_non_availability_and_missing_check(
    #     df=l1_usage_ru_a_gprs_cbs_usage_daily_stg,
    #     grouping="daily", par_col="event_partition_date",
    #     target_table_name="l1_usage_postpaid_prepaid_daily",
    #     exception_partitions=exception_partition_of_l1_usage_ru_a_gprs_cbs_usage_daily_stg)
    #
    # l1_usage_ru_a_vas_postpaid_usg_daily_stg = data_non_availability_and_missing_check(
    #     df=l1_usage_ru_a_vas_postpaid_usg_daily_stg,
    #     grouping="daily", par_col="event_partition_date",
    #     target_table_name="l1_usage_postpaid_prepaid_daily")
    #
    # l1_usage_ru_a_vas_postpaid_prepaid_daily_stg = data_non_availability_and_missing_check(
    #     df=l1_usage_ru_a_vas_postpaid_prepaid_daily_stg,
    #     grouping="daily", par_col="event_partition_date",
    #     target_table_name="l1_usage_postpaid_prepaid_daily")
    #
    # l1_customer_profile_union_daily_feature = data_non_availability_and_missing_check(
    #     df=l1_customer_profile_union_daily_feature,
    #     grouping="daily", par_col="event_partition_date",
    #     target_table_name="l1_usage_postpaid_prepaid_daily")

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
            l1_usage_data_postpaid_roaming_stg.select(F.max(F.col("event_partition_date")).alias("max_date")),
            l1_customer_profile_union_daily_feature.select(F.max(F.col("event_partition_date")).alias("max_date"))
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    drop_cols = ["called_no", "caller_no", "call_start_dt", "day_id"]
    union_df = union_dataframes_with_missing_cols([
        l1_usage_outgoing_call_relation_sum_daily_stg, l1_usage_incoming_call_relation_sum_daily_stg,
        l1_usage_outgoing_call_relation_sum_ir_daily_stg, l1_usage_incoming_call_relation_sum_ir_daily_stg,
        l1_usage_ru_a_gprs_cbs_usage_daily_stg, l1_usage_ru_a_vas_postpaid_usg_daily_stg,
        l1_usage_ru_a_vas_postpaid_prepaid_daily_stg, l1_usage_data_postpaid_roaming_stg
    ])

    union_df = union_df.filter(F.col("event_partition_date") <= min_value)



    if check_empty_dfs([union_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    group_cols = ['access_method_num', 'event_partition_date', 'start_of_week', 'start_of_month']
    final_df_str = gen_max_sql(union_df, 'roaming_incoming_outgoing_data', group_cols)
    sel_cols = ['access_method_num',
                'event_partition_date',
                "subscription_identifier",
                "start_of_week",
                "start_of_month"
                ]

    join_cols = ['access_method_num', 'event_partition_date', "start_of_week", "start_of_month"]
    l1_customer_profile_union_daily_feature = l1_customer_profile_union_daily_feature\
        .where("charge_type in ('Pre-paid', 'Post-paid') ")

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = union_df
    dates_list = data_frame.select('event_partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_array = list(divide_chunks(mvv_array, 30))
    add_list = mvv_array

    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("event_partition_date").isin(*[curr_item]))
        output_df = execute_sql(data_frame=small_df, table_name='roaming_incoming_outgoing_data', sql_str=final_df_str)
        cust_df = l1_customer_profile_union_daily_feature.filter((F.col("event_partition_date").isin(*[curr_item]))) \
            .select(sel_cols)

        output_df = cust_df.join(output_df, join_cols, how="left")
        output_df = output_df.where(
            "subscription_identifier is not null and access_method_num is not null")
        CNTX.catalog.save("l1_usage_postpaid_prepaid_daily", output_df.drop(*drop_cols))

    logging.info("running for dates {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("event_partition_date").isin(*[first_item]))
    cust_df = l1_customer_profile_union_daily_feature.filter(F.col("event_partition_date").isin(*[first_item])) \
        .select(sel_cols)
    return_df = execute_sql(data_frame=return_df, table_name='roaming_incoming_outgoing_data', sql_str=final_df_str)
    return_df = cust_df.join(return_df, join_cols, how="left")
    return_df = return_df.where(
        "subscription_identifier is not null and access_method_num is not null")

    return return_df.drop(*drop_cols)


def usage_favourite_number_master_pipeline(input_df, sql) -> DataFrame:
    """
    :return:
    """
    last_month_nb = -3
    today = date.today() + timedelta(hours=7)  # UTC+7
    first = today.replace(day=1)
    start_period = str(first + dateutil.relativedelta.relativedelta(months=last_month_nb)).replace('-', '')
    exception_date = "('20210301')"

    input_df = input_df.where("partition_date >= " + start_period + "and partition_date not in " + exception_date)
    today_str = str(today).replace('-', '')

    return_df = node_from_config(input_df, sql)
    win = Window.partitionBy("caller_no").orderBy(F.col("cnt_call").desc(), F.col("sum_durations").desc())
    return_df = return_df.withColumn("rnk", F.row_number().over(win)).filter("rnk <= 10") \
        .withColumn("favourite_flag", F.lit('Y')) \
        .withColumn("start_period", F.lit(start_period)) \
        .withColumn("execute_date", F.lit(today_str))

    return return_df
