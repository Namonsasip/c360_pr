import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from customer360.utilities.re_usable_functions import check_empty_dfs, \
    data_non_availability_and_missing_check
from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.config_parser import node_from_config

from typing import List

def build_l3_network_voice_features(
        input_df: DataFrame,
        parameter: dict) -> DataFrame:
    """

    :param input_df:
    :param parameter:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_voice_features",
            missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l3_network_good_and_bad_cells_features(
        input_df: DataFrame,
        parameter: dict) -> DataFrame:
    """

    :param input_df:
    :param parameter:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_good_and_bad_cells_features",
            missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l3_network_share_of_3g_time_in_total_time(
        input_df: DataFrame,
        parameter: dict) -> DataFrame:
    """

    :param input_df:
    :param parameter:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_share_of_3g_time_in_total_time",
            missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l3_network_data_traffic_features(
        input_df: DataFrame,
        parameter: dict) -> DataFrame:
    """

    :param input_df:
    :param parameter:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_data_traffic_features",
            missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l3_network_data_cqi(
        input_df: DataFrame,
        parameter: dict) -> DataFrame:
    """

    :param input_df:
    :param parameter:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_data_cqi",
            missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l3_network_im_cqi(
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
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_im_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def builld_l3_network_im_cqi(
        input_df: DataFrame,
        parameter: dict) -> DataFrame:
    """

    :param input_df:
    :param parameter:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_im_cqi",
            missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l3_network_streaming_cqi(
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
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_streaming_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l3_network_web_cqi(
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
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_web_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l3_network_voip_cqi(
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
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_voip_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l3_network_volte_cqi(
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
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_volte_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l3_network_user_cqi(
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
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_user_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df


def build_l3_network_file_transfer_cqi(
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
            df=input_df, grouping="monthly",
            par_col="event_partition_date",
            target_table_name="l3_network_file_transfer_cqi",
            missing_data_check_flg='Y',
            exception_partitions=exception_partitions)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    return_df = node_from_config(input_df, parameter)

    return return_df
