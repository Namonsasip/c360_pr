from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check
from pyspark.sql import functions as f, DataFrame
from src.customer360.utilities.spark_util import get_spark_empty_df
from pyspark.sql.types import *


def change_grouped_column_name(
        input_df,
        config
):
    df = node_from_config(input_df, config)
    for alias, col_name in config["rename_column"].items():
        df = df.withColumnRenamed(col_name, alias)

    return df


def dac_for_complaints_to_l1_pipeline(
        input_df: DataFrame,
        cust_df: DataFrame,
        target_table_name: str,
        exception_partiton_list=None):
    """
    :param input_df:
    :param cust_df:
    :param target_table_name:
    :param exception_partiton_list:
    :return:
    """

    input_df = input_df.where("partition_date >= '20200201' and partition_date<= '20200229'")
    cust_df = cust_df.where("event_partition_date >= '2020-02-01' and event_partition_date<= '2020-02-29'")
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name=target_table_name,
                                                       exception_partitions = exception_partiton_list)

    cust_df = data_non_availability_and_missing_check(df=cust_df, grouping="daily", par_col="event_partition_date",
                                                       target_table_name=target_table_name)

    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            cust_df.select(
                f.max(f.col("event_partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    cust_df = cust_df.filter(f.col("event_partition_date") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    return [input_df, cust_df]
