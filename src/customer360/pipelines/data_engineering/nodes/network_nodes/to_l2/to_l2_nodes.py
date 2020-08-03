import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from customer360.utilities.re_usable_functions import check_empty_dfs, \
    data_non_availability_and_missing_check
from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.config_parser import node_from_config

from typing import List
import os
from kedro.context import load_context
from pathlib import Path
conf = os.getenv("CONF", "base")


def dac_for_voice_features(
        input_df: DataFrame,
        exception_partitions: List[str]) -> DataFrame:
    """
    :param input_df:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_voice_features",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return input_df

def build_l2_network_good_and_bad_cells_features(
        input_df: DataFrame,
        parameter: dict,
        exception_partitions: List[str]) -> DataFrame:
    """
    :param input_df:
    :param parameter:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_good_and_bad_cells_features",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_share_of_3g_time_in_total_time(
        input_df: DataFrame,
        parameter: dict,
        exception_partitions: List[str]) -> DataFrame:
    """
    :param input_df:
    :param parameter:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_share_of_3g_time_in_total_time",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_data_traffic_features(
        input_df: DataFrame,
        parameter: dict,
        exception_partitions: List[str]) -> DataFrame:

    """
    :param input_df:
    :param parameter:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_data_traffic_features",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_data_cqi(
        input_df: DataFrame,
        parameter: dict,
        exception_partitions: List[str]) -> DataFrame:

    """
    :param input_df:
    :param parameter:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_data_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_im_cqi(
        input_df: DataFrame,
        parameter: dict,
        exception_partitions: List[str]) -> DataFrame:

    """
    :param input_df:
    :param parameter:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_im_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_streaming_cqi(
        input_df: DataFrame,
        parameter: dict,
        exception_partitions: List[str]) -> DataFrame:

    """
    :param input_df:
    :param parameter:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_streaming_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_web_cqi(
        input_df: DataFrame,
        parameter: dict,
        exception_partitions: List[str]) -> DataFrame:

    """
    :param input_df:
    :param parameter:
    :param exception_partitions:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_web_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_voip_cqi(
        input_df: DataFrame,
        parameter: dict,
        exception_partitions: List[str]) -> DataFrame:

    """
    :param input_df:
    :param parameter:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_voip_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_volte_cqi(
        input_df: DataFrame,
        parameter: dict,
        exception_partitions: List[str]) -> DataFrame:

    """
    :param input_df:
    :param parameter:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_volte_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_user_cqi(
        input_df: DataFrame,
        parameter: dict,
        exception_partitions: List[str]) -> DataFrame:

    """
    :param input_df:
    :param parameter:
    :param exception_partitions:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_user_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_file_transfer_cqi(
        input_df: DataFrame,
        parameter: dict,
        exception_partitions: List[str]) -> DataFrame:

    """
    :param input_df:
    :param parameter:
    :param exception_partitions:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="weekly",
            par_col="event_partition_date",
            target_table_name="l2_network_file_transfer_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_features(
        input_df: DataFrame,
        parameter: dict,
        target_table: str,
        exception_partitions: List[str]) -> DataFrame:

    """
    :param input_df:
    :param parameter:
    :param target_table:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name=target_table, missing_data_check_flg='Y',
                                                       exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l2_network_features_lookback(
        input_df: DataFrame,
        int_parameter: dict,
        parameter: dict,
        target_table: str,
        exception_partitions: List[str]) -> DataFrame:

    """
    :param input_df:
    :param int_parameter:
    :param parameter:
    :param target_table:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name=target_table, missing_data_check_flg='Y',
                                                       exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df_int = node_from_config(input_df, int_parameter)

    return_df_int2 = node_from_config(return_df_int, parameter)

    CNTX = load_context(Path.cwd(), env=conf)

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(f.col("table_name") == target_table) \
        .select(f.max(f.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", f.coalesce(f.col("max_date"), f.to_date(f.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .collect()[0].max_date

    return_df = return_df_int2.filter(f.col("start_of_week") > max_date)

    return return_df
