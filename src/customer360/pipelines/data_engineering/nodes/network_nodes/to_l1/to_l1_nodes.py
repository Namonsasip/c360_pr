import pyspark.sql.functions as f
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from customer360.utilities.re_usable_functions import check_empty_dfs, \
    data_non_availability_and_missing_check, _l1_join_with_customer_profile
from customer360.utilities.re_usable_functions import l1_massive_processing, union_dataframes_with_missing_cols
from customer360.utilities.spark_util import get_spark_empty_df, get_spark_session
from kedro.context import load_context
from pathlib import Path
from pyspark.sql.types import *
from customer360.utilities.config_parser import node_from_config
import logging
import os
from typing import List

conf = os.getenv("CONF", "base")


def __divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def __is_valid_input_df_de(
        input_df,
        cust_profile_df
):
    """
    Valid input criteria:
    1. input_df is provided and it is not empty
    2. cust_profile_df is either:
        - provided with non empty data OR
        - not provided at all
    """
    return (input_df is not None and len(input_df.head(1)) > 0) and \
            (cust_profile_df is None or len(cust_profile_df.head(1)) > 0)


def _massive_processing_de(
        input_df,
        config,
        source_partition_col="partition_date",
        sql_generator_func=node_from_config,
        cust_profile_df=None,
        cust_profile_join_func=None
) -> DataFrame:
    """
    Purpose: TO perform massive processing by dividing the massive data into small chunks to reduce load on cluster.
    :param input_df:
    :param config:
    :param source_partition_col:
    :param sql_generator_func:
    :param cust_profile_df:
    :param cust_profile_join_func:
    :return:
    """

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select(source_partition_col).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    partition_num_per_job = config.get("partition_num_per_job", 1)
    mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col(source_partition_col).isin(*[curr_item]))

        output_df = sql_generator_func(small_df, config)

        if cust_profile_df is not None:
            output_df = cust_profile_join_func(input_df=output_df,
                                               cust_profile_df=cust_profile_df,
                                               config=config,
                                               current_item=curr_item)

        CNTX.catalog.save(config["output_catalog"], output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col(source_partition_col).isin(*[first_item]))
    return_df = sql_generator_func(return_df, config)

    if cust_profile_df is not None:
        return_df = cust_profile_join_func(input_df=return_df,
                                           cust_profile_df=cust_profile_df,
                                           config=config,
                                           current_item=first_item)

    return return_df



def l1_massive_processing_de(
        input_df,
        config,
        cust_profile_df=None
) -> DataFrame:
    """
    Purpose: To perform the L1 level massive processing
    :param input_df:
    :param config:
    :param cust_profile_df:
    :return:
    """

    # if not __is_valid_input_df_de(input_df, cust_profile_df):
    #     return get_spark_empty_df()

    return_df = _massive_processing_de(input_df=input_df,
                                    config=config,
                                    source_partition_col="partition_date",
                                    cust_profile_df=cust_profile_df,
                                    cust_profile_join_func=_l1_join_with_customer_profile)

    return_df.show(3)
    logging.info("Output DF : {0}".format(str(return_df.select((return_df.columns)[-1]).limit(1).rdd.count())))
    return return_df



def l1_network_lookback_massive_processing(
        input_df,
        config,
        cust_profile_df,
        source_partition_col="partition_date",
        sql_generator_func=node_from_config,
        cust_profile_join_func=_l1_join_with_customer_profile
) -> DataFrame:
    if check_empty_dfs([input_df, cust_profile_df]):
        return get_spark_empty_df()

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select(source_partition_col).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    partition_num_per_job = config.get("partition_num_per_job", 1)
    mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)

    target_table_name = config["output_catalog"]
    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(f.col("table_name") == target_table_name) \
        .select(f.max(f.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", f.coalesce(f.col("max_date"), f.to_date(f.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .collect()[0].max_date

    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col(source_partition_col).isin(*[curr_item]))

        output_df = sql_generator_func(small_df, config)

        output_df = cust_profile_join_func(input_df=output_df,
                                           cust_profile_df=cust_profile_df,
                                           config=config,
                                           current_item=curr_item)

        output_df = output_df.filter(f.col("event_partition_date") > max_date)
        CNTX.catalog.save(config["output_catalog"], output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col(source_partition_col).isin(*[first_item]))
    return_df = sql_generator_func(return_df, config)

    if cust_profile_df is not None:
        return_df = cust_profile_join_func(input_df=return_df,
                                           cust_profile_df=cust_profile_df,
                                           config=config,
                                           current_item=first_item)

    return_df = return_df.filter(f.col("event_partition_date") > max_date)

    return return_df


def build_network_voice_features(int_l1_network_voice_features: DataFrame,
                                 l1_network_voice_features: dict,
                                 l1_customer_profile_union_daily_feature_for_l1_network_voice_features: DataFrame,
                                 exception_partitions: List[str]) -> DataFrame:
    """
    :param int_l1_network_voice_features:
    :param l1_network_voice_features:
    :param l1_customer_profile_union_daily_feature_for_l1_network_voice_features:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [int_l1_network_voice_features, l1_customer_profile_union_daily_feature_for_l1_network_voice_features]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=int_l1_network_voice_features, grouping="daily",
                                                       par_col="event_partition_date",
                                                       target_table_name="l1_network_voice_features",
                                                       exception_partitions=exception_partitions)

    l1_customer_profile_union_daily_feature_for_l1_network_voice_features = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_voice_features, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_voice_features")

    #Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([input_df, l1_customer_profile_union_daily_feature_for_l1_network_voice_features]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(input_df,
                                      l1_network_voice_features,
                                      l1_customer_profile_union_daily_feature_for_l1_network_voice_features)
    return return_df


def build_network_good_and_bad_cells_features(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features: DataFrame,

        l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features: DataFrame,

        l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features: DataFrame,
        l1_network_good_and_bad_cells_features: dict,
        exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day: List[str],
        exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day: List[str],
        exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day: List[str],
        exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day: List[str],
        exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day: List[str],
        exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day: List[str]
) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features:
    :param l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features:
    :param l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features:
    :param l1_network_good_and_bad_cells_features:
    :param exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day:
    :param exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day:
    :param exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day:
    :param exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day:
    :param exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day:
    :param exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features,

             l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features,
             l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features,

             l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features,
             ]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features",
            exception_partitions=exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day)

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features",
            exception_partitions=exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day)

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features",
            exception_partitions=exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day)

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features",
            exception_partitions=exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day)

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features",
            exception_partitions=exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day)

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features",
            exception_partitions=exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day)

    l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features")

    l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features, grouping="daily",
            par_col="event_partition_date",
            target_table_name="l1_network_good_and_bad_cells_features")

    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features,

             l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features,
             l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features,

             l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features]):
        return get_spark_empty_df()

    # For min custoner check is not required as it will be a left join to customer from driving table
    min_value = union_dataframes_with_missing_cols(
        [
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date


    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features = \
        l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    get_good_and_bad_cells_for_each_customer_df = get_good_and_bad_cells_for_each_customer(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features
    )

    l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features_max_partition_date = \
    l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features.select(f.max(f.col("partition_date")).alias("max_date")).collect()[0].max_date

    l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features = \
        l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features.filter(f.col("partition_date") >= l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features_max_partition_date)

    get_transaction_on_good_and_bad_cells_df = get_transaction_on_good_and_bad_cells(
        get_good_and_bad_cells_for_each_customer_df,
        l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features,
        l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features)

    return_df = l1_massive_processing(get_transaction_on_good_and_bad_cells_df,
                                      l1_network_good_and_bad_cells_features,
                                      l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features
                                      )

    return return_df


def get_good_and_bad_cells_for_each_customer(
        im_df,
        streaming_df,
        web_df,
        voip_df,
        volte_df,
        voice_df
) -> DataFrame:
    spark = get_spark_session()

    voice_df.createOrReplaceTempView("voice_df")
    im_df.createOrReplaceTempView("im_df")
    streaming_df.createOrReplaceTempView("streaming_df")
    web_df.createOrReplaceTempView("web_df")
    voip_df.createOrReplaceTempView("voip_df")
    volte_df.createOrReplaceTempView("volte_df")

    result_df = spark.sql("""
        with unioned_df as (
            select msisdn, cs_cgi as cell_id, cei_voice_qoe as qoe, partition_date
            from voice_df
            union all
            
            select msisdn, cgisai as cell_id, cei_im_qoe as qoe, partition_date
            from im_df
            union all
            
            select msisdn, cgisai as cell_id, cei_stream_qoe as qoe, partition_date
            from streaming_df
            union all
            
            select msisdn, cgisai as cell_id, cei_web_qoe as qoe, partition_date
            from web_df
            union all
            
            select msisdn, cgisai as cell_id, cei_voip_qoe as qoe, partition_date
            from voip_df
            union all
            
            select msisdn, cgisai as cell_id, cei_volte_qoe as qoe, partition_date
            from volte_df
        ),
        grouped_cells as (
            select msisdn, cell_id, avg(qoe) as avg_qoe, partition_date
            from unioned_df
            group by msisdn, cell_id, partition_date
        )
        select *,
                boolean(avg_qoe >= 0.75*max(avg_qoe) over (partition by msisdn, partition_date)) as good_cells,
                boolean(avg_qoe <= 0.25*max(avg_qoe) over (partition by msisdn, partition_date)) as bad_cells,
                count(*) over (partition by msisdn, partition_date) as cell_id_count                                        
        from grouped_cells  
    """)

    return result_df


def get_transaction_on_good_and_bad_cells(
        ranked_cells_df,
        cell_master_plan_df,
        sum_voice_daily_location_df
) -> DataFrame:
    spark = get_spark_session()

    ranked_cells_df.createOrReplaceTempView("ranked_cells_df")
    cell_master_plan_df.createOrReplaceTempView("cell_master_plan_df")
    sum_voice_daily_location_df.createOrReplaceTempView("sum_voice_daily_location_df")

    result = spark.sql("""
        with geo as (
            select *, 
                   cast(substr(cgi,6) as integer) as trunc_cgi
            from cell_master_plan_df
        ),
        voice_tx_location as (
            select *,
                cast(case when char_length(ci) > 5 then ci else concat(lac, ci) end as integer) as trunc_cgi
            from sum_voice_daily_location_df
        ),
        enriched_voice_tx_location as (
            select /*+ BROADCAST(geo) */
                t1.access_method_num as access_method_num,
                t1.partition_date as partition_date,
                t1.service_type as service_type,
                t1.no_of_call + t1.no_of_inc as total_transaction,
                
                t2.cgi as cgi,
                t2.soc_cgi_hex as soc_cgi_hex                
            from voice_tx_location t1
            inner join geo t2
            on t1.trunc_cgi = t2.trunc_cgi
        ),
        joined_ranked_cells_df as (
            select 
                t1.msisdn as access_method_num,
                t1.partition_date as partition_date,
                sum(case when t1.good_cells == true then 1 else 0 end) as sum_of_good_cells,
                sum(case when t1.bad_cells == true then 1 else 0 end) as sum_of_bad_cells,
                max(cell_id_count) as cell_id_count,
                sum(case when t1.good_cells == true then t2.total_transaction else 0 end) as total_transaction_good_cells,
                sum(case when t1.bad_cells == true then t2.total_transaction else 0 end) as total_transaction_bad_cells
            from ranked_cells_df t1
            inner join enriched_voice_tx_location t2
            on t1.msisdn = t2.access_method_num
                and t1.cell_id = t2.soc_cgi_hex
                and t1.partition_date = t2.partition_date
            group by t1.msisdn, t1.partition_date
        )
        select *,
            (sum_of_good_cells/cell_id_count) as share_of_good_cells,
            (sum_of_bad_cells/cell_id_count) as share_of_bad_cells
        from joined_ranked_cells_df
    """)

    return result


def build_network_share_of_3g_time_in_total_time(
        l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time: DataFrame,
        l1_network_share_of_3g_time_in_total_time: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time: DataFrame) -> DataFrame:
    """
    :param l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time:
    :param l1_network_share_of_3g_time_in_total_time:
    :param l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time,
             l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time]):
        return get_spark_empty_df()

    l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time = \
        data_non_availability_and_missing_check(
            df=l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_share_of_3g_time_in_total_time")

    l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_share_of_3g_time_in_total_time")

    #Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time, l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time,
                                      l1_network_share_of_3g_time_in_total_time,
                                      l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time)
    return return_df


def build_network_data_traffic_features(
        int_l1_network_data_traffic_features: DataFrame,
        l1_network_data_traffic_features: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features: DataFrame) -> DataFrame:
    """

    :param int_l1_network_data_traffic_features:
    :param l1_network_data_traffic_features:
    :param l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [int_l1_network_data_traffic_features,
             l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features]):
        return get_spark_empty_df()

    int_l1_network_data_traffic_features = int_l1_network_data_traffic_features.\
        drop("event_partition_date", "start_of_month", "start_of_week")

    int_l1_network_data_traffic_features = \
        data_non_availability_and_missing_check(
            df=int_l1_network_data_traffic_features, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_data_traffic_features")

    l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_data_traffic_features")

    #Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([int_l1_network_data_traffic_features, l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(int_l1_network_data_traffic_features,
                                      l1_network_data_traffic_features,
                                      l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features)
    return return_df


def build_network_data_cqi(
        l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi: DataFrame,
        l1_network_data_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_data_cqi: DataFrame) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi:
    :param l1_network_data_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_data_cqi:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_data_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_data_cqi")

    l1_customer_profile_union_daily_feature_for_l1_network_data_cqi = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_data_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_data_cqi")

    #Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi, l1_customer_profile_union_daily_feature_for_l1_network_data_cqi]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi,
                                      l1_network_data_cqi,
                                      l1_customer_profile_union_daily_feature_for_l1_network_data_cqi)
    return return_df


def build_network_im_cqi(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi: DataFrame,
                         l1_network_im_cqi: dict,
                         l1_customer_profile_union_daily_feature_for_l1_network_im_cqi: DataFrame,
                         exception_partitions: List[str]) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi:
    :param l1_network_im_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_im_cqi:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_im_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_im_cqi",
            exception_partitions=exception_partitions)

    l1_customer_profile_union_daily_feature_for_l1_network_im_cqi = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_im_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_im_cqi")

   # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi, l1_customer_profile_union_daily_feature_for_l1_network_im_cqi]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi,
                                      l1_network_im_cqi,
                                      l1_customer_profile_union_daily_feature_for_l1_network_im_cqi)
    return return_df


def build_network_streaming_cqi(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi: DataFrame,
        l1_network_streaming_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi: DataFrame,
        exception_partitions: List[str]) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi:
    :param l1_network_streaming_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_streaming_cqi",
            exception_partitions=exception_partitions)

    l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_streaming_cqi")

   # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi, l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi,
                                      l1_network_streaming_cqi,
                                      l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi)
    return return_df


def build_network_web_cqi(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi: DataFrame,
        l1_network_web_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_web_cqi: DataFrame,
        exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day: List[str]) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi:
    :param l1_network_web_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_web_cqi:
    :param exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_web_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_web_cqi",
            exception_partitions=exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day)

    l1_customer_profile_union_daily_feature_for_l1_network_web_cqi = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_web_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_web_cqi")

   # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi, l1_customer_profile_union_daily_feature_for_l1_network_web_cqi]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi,
                                      l1_network_web_cqi,
                                      l1_customer_profile_union_daily_feature_for_l1_network_web_cqi)
    return return_df


def build_network_voip_cqi(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi: DataFrame,
        l1_network_voip_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi: DataFrame,
        exception_partitions_list: List[str]) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi:
    :param l1_network_voip_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi:
    :param exception_partitions_list:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_voip_cqi",
            exception_partitions=exception_partitions_list)

    l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_voip_cqi")

    #Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi, l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi,
                                      l1_network_voip_cqi,
                                      l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi)
    return return_df


def build_network_volte_cqi(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi: DataFrame,
        l1_network_volte_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi: DataFrame,
        exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day: List[str]) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi:
    :param l1_network_volte_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi:
    :param exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_volte_cqi",
            exception_partitions=exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day)

    l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_volte_cqi")

   # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi, l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi,
                                      l1_network_volte_cqi,
                                      l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi)
    return return_df


def build_network_user_cqi(
        l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi: DataFrame,
        l1_network_user_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_user_cqi: DataFrame,
        exception_partitions: List[str]) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi:
    :param l1_network_user_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_user_cqi:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_user_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_user_cqi",
            exception_partitions=exception_partitions)

    l1_customer_profile_union_daily_feature_for_l1_network_user_cqi = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_user_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_user_cqi")

    #Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi, l1_customer_profile_union_daily_feature_for_l1_network_user_cqi]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi,
                                      l1_network_user_cqi,
                                      l1_customer_profile_union_daily_feature_for_l1_network_user_cqi)
    return return_df


def build_network_file_transfer_cqi(
        l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi: DataFrame,
        l1_network_file_transfer_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi: DataFrame,
        exception_partitions: List[str]) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi:
    :param l1_network_file_transfer_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_file_transfer_cqi",
            exception_partitions=exception_partitions)

    l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_file_transfer_cqi")

   # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi, l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi,
                                      l1_network_file_transfer_cqi,
                                      l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi)
    return return_df


def build_network_cei_voice_qoe_incoming(
        voice_1day: DataFrame,
        volte_1day: DataFrame,
        call_leg_sip: DataFrame,
        cust_df: DataFrame,
        l1_network_cei_voice_qoe_incoming_dict: dict,
        exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day: List[str],
        exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day: List[str]) -> DataFrame:

    """
    :param voice_1day:
    :param volte_1day:
    :param call_leg_sip:
    :param cust_df:
    :param l1_network_cei_voice_qoe_incoming_dict:
    :param exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day:
    :param exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [voice_1day, volte_1day, call_leg_sip, cust_df]):
        return get_spark_empty_df()

    voice_1day = data_non_availability_and_missing_check(df=voice_1day, grouping="daily", par_col="partition_date",
                                                         target_table_name="l1_network_cei_voice_qoe_incoming",
                                                         exception_partitions=exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day)

    volte_1day = data_non_availability_and_missing_check(df=volte_1day, grouping="daily", par_col="partition_date",
                                                         target_table_name="l1_network_cei_voice_qoe_incoming",
                                                         exception_partitions=exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day)

    call_leg_sip = data_non_availability_and_missing_check(df=call_leg_sip, grouping="daily", par_col="partition_date",
                                                           target_table_name="l1_network_cei_voice_qoe_incoming")

    cust_df = data_non_availability_and_missing_check(df=cust_df, grouping="daily", par_col="event_partition_date",
                                                      target_table_name="l1_network_cei_voice_qoe_incoming")

    if check_empty_dfs(
            [voice_1day, volte_1day, call_leg_sip, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    voice_1day = voice_1day.withColumn(
        "event_partition_date", f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd'))

    volte_1day = volte_1day.withColumn(
        "event_partition_date", f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd'))

    call_leg_sip = call_leg_sip.withColumn(
        "event_partition_date", f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd'))

    min_value = union_dataframes_with_missing_cols(
        [
            voice_1day.select(f.max(f.col("event_partition_date")).alias("max_date")),
            volte_1day.select(f.max(f.col("event_partition_date")).alias("max_date")),
            call_leg_sip.select(f.max(f.col("event_partition_date")).alias("max_date")),
            cust_df.select(f.max(f.col("event_partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    voice_1day = voice_1day.filter(f.col("event_partition_date") <= min_value)

    volte_1day = volte_1day.filter(f.col("event_partition_date") <= min_value)

    call_leg_sip = call_leg_sip.filter(f.col("event_partition_date") <= min_value)

    cust_df = cust_df.filter(f.col("event_partition_date") <= min_value)

    # voice column derivation
    voice_1day = voice_1day.select("CEI_VOICE_PAGING_SUCCESS_RATE", "CEI_VOICE_PERCEIVED_CALL_DROP_RATE", "msisdn",
                                   "event_partition_date", "partition_date")
    voice_1day = voice_1day.groupBy("msisdn", "event_partition_date", "partition_date").agg(
        f.sum(f.col("CEI_VOICE_PAGING_SUCCESS_RATE")).alias("CEI_VOICE_PAGING_SUCCESS_RATE"),
        f.sum(f.col("CEI_VOICE_PERCEIVED_CALL_DROP_RATE")).alias("CEI_VOICE_PERCEIVED_CALL_DROP_RATE"),
        f.count("*").alias("count_cscgi"))
    voice_1day = voice_1day.select("msisdn","event_partition_date",
                                   "partition_date",
                                   (voice_1day.CEI_VOICE_PAGING_SUCCESS_RATE/voice_1day.count_cscgi).alias("CEI_VOICE_PAGING_SUCCESS_RATE"),
                                   (voice_1day.CEI_VOICE_PERCEIVED_CALL_DROP_RATE/voice_1day.count_cscgi).alias("CEI_VOICE_PERCEIVED_CALL_DROP_RATE") )
    # volte column derivation
    volte_1day = volte_1day.select("CEI_VOLTE_VOICE_MT_DROP_TIMES", "CEI_VOLTE_VOICE_MT_ANSWER_TIMES", "msisdn",
                                   "event_partition_date", "partition_date")
    volte_1day = volte_1day.groupBy("msisdn", "event_partition_date", "partition_date").agg(
        f.sum(f.col("CEI_VOLTE_VOICE_MT_DROP_TIMES")).alias("CEI_VOLTE_VOICE_MT_DROP_TIMES"),
        f.sum(f.col("CEI_VOLTE_VOICE_MT_ANSWER_TIMES")).alias("CEI_VOLTE_VOICE_MT_ANSWER_TIMES"),
        f.count("*").alias("count_cscgi"))
    volte_1day = volte_1day.select("msisdn","event_partition_date","partition_date",
                                   (volte_1day.CEI_VOLTE_VOICE_MT_ANSWER_TIMES/volte_1day.count_cscgi).alias("CEI_VOLTE_VOICE_MT_ANSWER_TIMES"),
                                   (volte_1day.CEI_VOLTE_VOICE_MT_DROP_TIMES/volte_1day.count_cscgi).alias("CEI_VOLTE_VOICE_MT_DROP_TIMES") )

    # volte paging success rate column derivation from call_leg_sip dataset
    call_leg_sip = call_leg_sip.select("ACCESS_TYPE", "P_CSCF_ID", "SERVICE_TYPE", "ALERTING_TIME", "ANSWER_TIME", "IMPU_TEL_URI", "event_partition_date", "partition_date")
    call_leg_sip = call_leg_sip.withColumn("VOLTE_MT_CONN_FAIL_TIMES", f.expr(
        "case when (ACCESS_TYPE in (1,2,43) and P_CSCF_ID is not null and SERVICE_TYPE = 0 and ALERTING_TIME is null and ANSWER_TIME is null ) then 1 else 0 end")) \
        .withColumn("VOLTE_MT_REQ_TIMES", f.expr(
        "case when (ACCESS_TYPE in (1,2,43) and P_CSCF_ID is not null and SERVICE_TYPE = 0) then 1 else 0 end"))

    call_leg_sip = call_leg_sip.groupBy("IMPU_TEL_URI", "event_partition_date", "partition_date").agg(
        f.sum(f.col("VOLTE_MT_CONN_FAIL_TIMES")).alias("VOLTE_MT_CONN_FAIL_TIMES"),
        f.sum(f.col("VOLTE_MT_REQ_TIMES")).alias("VOLTE_MT_REQ_TIMES"))

    call_leg_sip = call_leg_sip.withColumnRenamed("IMPU_TEL_URI", "msisdn")

    #join volte and call leg sip
    volte_joined = volte_1day.join(call_leg_sip, on=["msisdn", "event_partition_date", "partition_date"], how="inner")

    volte_joined = volte_joined.withColumn("VOLTE_PAGING_SUCCESS_RATE", f.expr(" 100 - ((VOLTE_MT_CONN_FAIL_TIMES * 100)/ VOLTE_MT_REQ_TIMES)")) \
                                .withColumn("VOLTE_CALL_DROP_RATE", f.expr(" (CEI_VOLTE_VOICE_MT_DROP_TIMES * 100)/ CEI_VOLTE_VOICE_MT_ANSWER_TIMES "))

    #join voice and volte for final feature derivation
    join_key_between_network_df = ['event_partition_date', 'msisdn', 'partition_date']
    joined_df = voice_1day.join(
        volte_joined, on=join_key_between_network_df, how='inner')
    joined_df = joined_df.drop('event_partition_date')

    return_df = l1_massive_processing_de(joined_df,
                                      l1_network_cei_voice_qoe_incoming_dict, cust_df)

    # return_df = node_from_config(joined_df, l1_network_cei_voice_qoe_incoming_dict)
    # return_df = return_df.alias('a').join(cust_df.alias('b'),
    #                                       [return_df.msisdn == cust_df.access_method_num ,
    #                                        return_df.event_partition_date == cust_df.event_partition_date],
    #                                       "left").select("a.partition_date",
    #                                                      "a.network_cei_voice_qoe_incoming",
    #                                                      "a.event_partition_date",
    #                                                      "a.start_of_week",
    #                                                      "b.access_method_num",
    #                                                      "b.subscription_identifier",
    #                                                      "a.start_of_month"
    #
    # )
    return return_df


def build_network_cei_voice_qoe_outgoing(
        voice_1day: DataFrame,
        volte_1day: DataFrame,
        cust_df: DataFrame,
        l1_network_cei_voice_qoe_outgoing_dict: dict,
        exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day: List[str],
        exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day: List[str]) -> DataFrame:

    """
    :param voice_1day:
    :param volte_1day:
    :param cust_df:
    :param l1_network_cei_voice_qoe_outgoing_dict:
    :param exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day:
    :param exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([voice_1day, volte_1day, cust_df]):
        return get_spark_empty_df()

    voice_1day = data_non_availability_and_missing_check(df=voice_1day, grouping="daily", par_col="partition_date",
                                                         target_table_name="l1_network_cei_voice_qoe_outgoing",
                                                         exception_partitions=exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day)

    volte_1day = data_non_availability_and_missing_check(df=volte_1day, grouping="daily", par_col="partition_date",
                                                         target_table_name="l1_network_cei_voice_qoe_outgoing",
                                                         exception_partitions=exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day)

    cust_df = data_non_availability_and_missing_check(df=cust_df, grouping="daily", par_col="event_partition_date",
                                                      target_table_name="l1_network_cei_voice_qoe_outgoing")

    if check_empty_dfs([voice_1day, volte_1day, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    voice_1day = voice_1day.withColumn(
        "event_partition_date", f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd'))
    volte_1day = volte_1day.withColumn(
        "event_partition_date", f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd'))

    min_value = union_dataframes_with_missing_cols(
        [
            voice_1day.select(f.max(f.col("event_partition_date")).alias("max_date")),
            volte_1day.select(f.max(f.col("event_partition_date")).alias("max_date")),
            cust_df.select(f.max(f.col("event_partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    voice_1day = voice_1day.filter(f.col("event_partition_date") <= min_value)

    volte_1day = volte_1day.filter(f.col("event_partition_date") <= min_value)

    cust_df = cust_df.filter(f.col("event_partition_date") <= min_value)

    voice_1day = voice_1day.select("CEI_VOICE_PERCEIVED_CALL_SUCCESS_RATE", "CEI_VOICE_PERCEIVED_CALL_DROP_RATE", "msisdn", "event_partition_date", "partition_date")
    voice_1day = voice_1day.groupBy("msisdn", "event_partition_date", "partition_date").agg(
        f.sum(f.col("CEI_VOICE_PERCEIVED_CALL_SUCCESS_RATE")).alias("CEI_VOICE_PERCEIVED_CALL_SUCCESS_RATE"),
        f.sum(f.col("CEI_VOICE_PERCEIVED_CALL_DROP_RATE")).alias("CEI_VOICE_PERCEIVED_CALL_DROP_RATE"),
        f.count("*").alias("count_cscgi"))
    voice_1day = voice_1day.select("msisdn","event_partition_date","partition_date",
                                   (voice_1day.CEI_VOICE_PERCEIVED_CALL_SUCCESS_RATE/voice_1day.count_cscgi).alias("CEI_VOICE_PERCEIVED_CALL_SUCCESS_RATE"),
                                   (voice_1day.CEI_VOICE_PERCEIVED_CALL_DROP_RATE/voice_1day.count_cscgi).alias("CEI_VOICE_PERCEIVED_CALL_DROP_RATE") )

    volte_1day = volte_1day.select("CEI_VOLTE_MO_CONN_RATE", "CEI_VOLTE_CALL_DROP_RATE", "msisdn", "event_partition_date", "partition_date")
    volte_1day = volte_1day.groupBy("msisdn", "event_partition_date", "partition_date").agg(
        f.sum(f.col("CEI_VOLTE_MO_CONN_RATE")).alias("CEI_VOLTE_MO_CONN_RATE"),
        f.sum(f.col("CEI_VOLTE_CALL_DROP_RATE")).alias("CEI_VOLTE_CALL_DROP_RATE"),
        f.count("*").alias("count_cscgi"))
    volte_1day = volte_1day.select("msisdn","event_partition_date","partition_date",
                                   (volte_1day.CEI_VOLTE_MO_CONN_RATE/volte_1day.count_cscgi).alias("CEI_VOLTE_MO_CONN_RATE"),
                                   (volte_1day.CEI_VOLTE_CALL_DROP_RATE/volte_1day.count_cscgi).alias("CEI_VOLTE_CALL_DROP_RATE") )

    join_key_between_network_df = ['event_partition_date', 'msisdn', 'partition_date']
    joined_df = voice_1day.join(volte_1day, on=join_key_between_network_df, how='inner')
    joined_df = joined_df.drop('event_partition_date')

    return_df = l1_massive_processing(joined_df,
                                      l1_network_cei_voice_qoe_outgoing_dict, cust_df)

    return return_df


def build_network_voice_data_features(
        input_df: DataFrame,
        l1_customer_profile_union_daily_feature: DataFrame,
        feature_dict: dict,
        target_table: str,
        exception_partitions: List[str]) -> DataFrame:
    """
    :param input_df:
    :param l1_customer_profile_union_daily_feature:
    :param feature_dict:
    :param target_table:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df, l1_customer_profile_union_daily_feature]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="daily",
            par_col="partition_date",
            target_table_name=target_table,
            exception_partitions=exception_partitions)

    l1_customer_profile_union_daily_feature = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature, grouping="daily",
        par_col="event_partition_date",
        target_table_name=target_table)

   # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs(
            [input_df, l1_customer_profile_union_daily_feature]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(input_df,
                                      feature_dict, l1_customer_profile_union_daily_feature)

    return return_df


def build_network_lookback_voice_data_features(
        input_df: DataFrame,
        l1_customer_profile_union_daily_feature: DataFrame,
        feature_dict: dict,
        target_table: str,
        exception_partitions: List[str]) -> DataFrame:
    """
    :param input_df:
    :param l1_customer_profile_union_daily_feature:
    :param feature_dict:
    :param target_table:
    :param exception_partitions:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [input_df, l1_customer_profile_union_daily_feature]):
        return get_spark_empty_df()

    input_df = \
        data_non_availability_and_missing_check(
            df=input_df, grouping="daily",
            par_col="partition_date",
            target_table_name=target_table,
            exception_partitions=exception_partitions)

    l1_customer_profile_union_daily_feature = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature, grouping="daily",
        par_col="event_partition_date",
        target_table_name=target_table)

   # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs(
            [input_df, l1_customer_profile_union_daily_feature]):
        return get_spark_empty_df()
    ################################ End Implementing Data availability checks ###############################

    return_df = l1_network_lookback_massive_processing(input_df, feature_dict, l1_customer_profile_union_daily_feature)

    return return_df


def build_geo_home_work_location_master(
        work_home_location_master: DataFrame,
        geo_cell_master: DataFrame ) -> DataFrame:
    """

    :param work_home_location_master:
    :param geo_cell_master:
    :return:
    """
    #Work home location master
    work_home_location_master = work_home_location_master.select("imsi", "home_weekend_location_id", "start_of_month")
    work_home_location_master_max_partition_date = \
    work_home_location_master.select(f.max(f.col("start_of_month")).alias("max_date")).collect()[0].max_date

    work_home_location_master = work_home_location_master.filter(f.col("start_of_month") >= work_home_location_master_max_partition_date)
    work_home_location_master = work_home_location_master.drop("start_of_month").withColumnRenamed("home_weekend_location_id",
                                                                                   "location_id")


    #Geo cellplan master
    geo_cell_master = geo_cell_master.select("soc_cgi_hex", "location_id", "partition_date")
    geo_cell_master_max_partition_date = geo_cell_master.select(f.max(f.col("partition_date")).alias("max_date")).collect()[0].max_date

    geo_cell_master = geo_cell_master.filter(f.col("partition_date") >= geo_cell_master_max_partition_date)
    geo_cell_master = geo_cell_master.drop("partition_date")

    joined_geo = geo_cell_master.join(work_home_location_master, on=["location_id"], how="inner")
    joined_geo = joined_geo.withColumn("rn", f.expr("row_number() over (partition by imsi, location_id, soc_cgi_hex order by imsi)"))
    joined_geo = joined_geo.where("rn = 1").drop("rn")

    return joined_geo


def build_network_failed_calls_home_location(
        geo_work_home_location_master: DataFrame,
        voice_1day: DataFrame,
        volte_1day: DataFrame,
        cust_df: DataFrame,
        l1_network_failed_calls_home_location_dict: dict,
        exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day: List[str],
        exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day: List[str]) -> DataFrame:

    """
    :param geo_work_home_location_master:
    :param voice_1day:
    :param volte_1day:
    :param cust_df:
    :param l1_network_failed_calls_home_location_dict:
    :param exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day:
    :param exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([voice_1day, volte_1day, cust_df]):
        return get_spark_empty_df()

    voice_1day = data_non_availability_and_missing_check(df=voice_1day, grouping="daily", par_col="partition_date",
                                                         target_table_name="l1_network_failed_calls_home_location",
                                                         exception_partitions=exception_partition_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day)

    volte_1day = data_non_availability_and_missing_check(df=volte_1day, grouping="daily", par_col="partition_date",
                                                         target_table_name="l1_network_failed_calls_home_location",
                                                         exception_partitions=exception_partitions_list_for_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day)

    cust_df = data_non_availability_and_missing_check(df=cust_df, grouping="daily", par_col="event_partition_date",
                                                      target_table_name="l1_network_failed_calls_home_location")

    if check_empty_dfs([voice_1day, volte_1day, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    voice_1day = voice_1day.withColumn(
        "event_partition_date", f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd'))

    volte_1day = volte_1day.withColumn(
        "event_partition_date", f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd'))

    min_value = union_dataframes_with_missing_cols(
        [
            voice_1day.select(f.max(f.col("event_partition_date")).alias("max_date")),
            volte_1day.select(f.max(f.col("event_partition_date")).alias("max_date")),
            cust_df.select(f.max(f.col("event_partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    voice_1day = voice_1day.filter(f.col("event_partition_date") <= min_value)

    volte_1day = volte_1day.filter(f.col("event_partition_date") <= min_value)

    cust_df = cust_df.filter(f.col("event_partition_date") <= min_value)


    #Voice daily
    voice_1day = voice_1day.select("imsi", "msisdn", "cs_cgi", "access_type_id", "CEI_VOICE_VOICE_DROPS_AFTER_ANSWERS_MOC",
                         "CEI_VOICE_VOICE_DROPS_AFTER_ANSWERS_MTC", "partition_date")
    voice_1day = voice_1day.withColumn("CEI_VOICE_VOICE_DROPS_AFTER_ANSWERS_MOC", f.expr("case when access_type_id = 1 then CEI_VOICE_VOICE_DROPS_AFTER_ANSWERS_MOC else 0 end")) \
        .withColumn("CEI_VOICE_VOICE_DROPS_AFTER_ANSWERS_MTC", f.expr("case when access_type_id = 1 then CEI_VOICE_VOICE_DROPS_AFTER_ANSWERS_MTC else 0 end")) \
        .withColumnRenamed("cs_cgi", "soc_cgi_hex") \
        .drop("access_type_id")

    #Voice joined with geo
    voice_geo = voice_1day.join(geo_work_home_location_master, on=["imsi", "soc_cgi_hex"], how="inner")
    voice_geo = voice_geo.drop("imsi", "soc_cgi_hex")

    voice_geo = voice_geo.groupBy("msisdn", "partition_date").agg(
        f.sum(f.col("CEI_VOICE_VOICE_DROPS_AFTER_ANSWERS_MOC")).alias("CEI_VOICE_VOICE_DROPS_AFTER_ANSWERS_MOC"),
        f.sum(f.col("CEI_VOICE_VOICE_DROPS_AFTER_ANSWERS_MTC")).alias("CEI_VOICE_VOICE_DROPS_AFTER_ANSWERS_MTC"))



    #volte daily
    volte_1day = volte_1day.select("imsi", "msisdn", "cgisai", "CEI_VOLTE_VOICE_MT_DROP_TIMES", "CEI_VOLTE_VOICE_MO_DROP_TIMES",
                                    "partition_date")
    volte_1day = volte_1day.withColumnRenamed("cgisai", "soc_cgi_hex")

    #volte joined with geo
    volte_geo = volte_1day.join(geo_work_home_location_master, on=["imsi", "soc_cgi_hex"], how="inner")
    volte_geo = volte_geo.drop("imsi", "soc_cgi_hex")

    volte_geo = volte_geo.groupBy("msisdn", "partition_date").agg(
        f.sum(f.col("CEI_VOLTE_VOICE_MT_DROP_TIMES")).alias("CEI_VOLTE_VOICE_MT_DROP_TIMES"),
        f.sum(f.col("CEI_VOLTE_VOICE_MO_DROP_TIMES")).alias("CEI_VOLTE_VOICE_MO_DROP_TIMES"))

    join_key_between_network_df = ['msisdn', 'partition_date']
    joined_df = voice_geo.join(volte_geo, on=join_key_between_network_df, how='inner')

    return_df = l1_massive_processing(joined_df,
                                      l1_network_failed_calls_home_location_dict, cust_df)

    return return_df
