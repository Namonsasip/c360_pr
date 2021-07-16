from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import expansion, node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging, os
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

conf = os.getenv("CONF", None)


def l3_usage_most_idd_features_aggregate(input_df: DataFrame, config, exception_partition=None):

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l3_usage_most_idd_features",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    age_df = node_from_config(input_df, config)
    spark = get_spark_session()
    age_df.registerTempTable("usage_most_idd_features_aggregate")

    sql_stmt = """SELECT  tbl1.subscription_identifier
                        , tbl1.start_of_month
                        , tbl1.called_network_type
                        , tbl1.idd_country
                        , tbl1.usage_total_idd_successful_call
                        , tbl1.usage_total_idd_minutes
                        , tbl1.usage_total_idd_durations
                        , tbl1.usage_total_idd_net_revenue
                FROM    usage_most_idd_features_aggregate tbl1
                INNER JOIN
                        (
                        SELECT  start_of_month, subscription_identifier, MAX(usage_total_idd_successful_call) AS max_total_call
                        FROM    usage_most_idd_features_aggregate
                        GROUP BY start_of_month, subscription_identifier
                        ) tbl2
                ON      tbl1.subscription_identifier = tbl2.subscription_identifier
                        AND tbl1.start_of_month = tbl2.start_of_month
                        AND tbl1.usage_total_idd_successful_call = tbl2.max_total_call"""

    df = spark.sql(sql_stmt)

    return df


def l3_usage_last_idd_features_aggregate(input_df: DataFrame, config, exception_partition=None):

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l3_usage_last_idd_features",
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    age_df = node_from_config(input_df, config)
    spark = get_spark_session()
    age_df.registerTempTable("usage_last_idd_features_aggregate")

    sql_stmt = """select subscription_identifier
                        ,start_of_month
                        ,called_network_type
                        ,last_idd_country
                        ,last_date_call_idd
                        ,usage_last_total_idd_minutes
                        ,usage_last_total_idd_net_revenue
                        from (usage_last_idd_features_aggregate)
                        where row_num = 1"""

    df = spark.sql(sql_stmt)

    return df


def build_usage_l3_layer(data_frame: DataFrame, dict_obj: dict) -> DataFrame:
    """
    :param data_frame:
    :param dict_obj:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([data_frame]):
        return get_spark_empty_df()

    data_frame = data_non_availability_and_missing_check(df=data_frame, grouping="monthly",
                                                         par_col="event_partition_date",
                                                         target_table_name="l3_usage_postpaid_prepaid_monthly",
                                                         missing_data_check_flg='Y')

    if check_empty_dfs([data_frame]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

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

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        output_df = expansion(small_df, dict_obj)
        CNTX.catalog.save("l3_usage_postpaid_prepaid_monthly", output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    return_df = expansion(return_df, dict_obj)

    return return_df
